// Package outboxpublisher é o worker que publica a transactional outbox na
// fila FIFO de eventos de integração (wager-events.fifo). ARCHITECTURE.md,
// ADR-024.
//
// Decisões:
//
//   - Ciclo: claim de um lote com lease persistido (Store.Claim, em autocommit,
//     sem transação aberta), publicação FORA de transação e marcação com
//     fencing pelo lease (MarkPublished/MarkFailed só têm efeito para o lease
//     vigente). Nada fica em memória: qualquer instância retoma o trabalho de
//     outra depois que o lease expira.
//   - Roteamento FIFO: MessageGroupId = aggregateId e MessageDeduplicationId =
//     eventId. O corpo é o snapshot gravado na outbox, byte a byte, então uma
//     republicação (lease expirado, crash entre publish e mark) carrega o
//     mesmo eventId e o mesmo corpo. O SQS FIFO a descarta dentro da janela de
//     deduplicação de 5 minutos; fora dela, o consumidor deduplica por eventId.
//   - Ordem por agregado: o claim só devolve o evento não publicado mais
//     antigo de cada agregado (head-of-line, postgres.OutboxRepository.Claim).
//     Um lote tem no máximo um evento por grupo, então os eventos do lote são
//     publicados em paralelo (Concurrency) sem quebrar a ordem. O evento N+1
//     de um agregado só fica elegível depois que N foi marcado como publicado.
//   - Lote não vazio → novo ciclo imediato (o próximo evento de cada agregado
//     acabou de ficar elegível); lote vazio ou falha no claim → espera
//     PollInterval.
//   - Falha no envio → MarkFailed: last_error e next_attempt_at = agora +
//     backoff exponencial com teto (persistido, ADR-015). Não há limite de
//     tentativas nem DLQ: descartar um evento confirmado violaria a garantia
//     de não perda. Um evento que falha sempre bloqueia só o seu agregado, e
//     isso aparece em outbox_oldest_pending_age_seconds e em last_error.
//   - Prazo: cada chamada de um evento (envio; marcação) tem PublishTimeout.
//     Um item só é iniciado se ainda couberem as duas chamadas no lease
//     (claimedAt + Lease − 2·PublishTimeout). Os que não cabem são devolvidos
//     com Release, sem backoff, para nova reivindicação.
//   - Shutdown (Stop): para de reivindicar e de iniciar itens. Os itens
//     reivindicados e não iniciados são devolvidos (Release). Os envios em voo
//     terminam até DrainTimeout. Estourado o prazo, o contexto é cancelado,
//     o lease fica gravado, expira e outra instância republica com o mesmo
//     eventId. Stop devolve ErrDrainTimeout. Stop sempre espera as goroutines,
//     salvo se o próprio ctx do OnStop vencer.
//   - Métricas: atraso (now − occurred_at) na publicação confirmada,
//     pendentes e idade do mais antigo (Store.Backlog, no máximo uma vez por
//     PollInterval), retries e latência por outcome.
package outboxpublisher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/platform/failpoint"
	"github.com/junglegaming/backend-challenge-go/internal/platform/logging"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

// ErrDrainTimeout indica que publicações em voo foram canceladas no shutdown.
var ErrDrainTimeout = errors.New("outboxpublisher: prazo de drenagem estourado; publicações em voo canceladas")

// Atributos SQS de cada mensagem publicada (roteamento sem abrir o corpo).
const (
	AttrEventType     = "eventType"
	AttrEventVersion  = "eventVersion"
	AttrEventID       = "eventId"
	AttrCorrelationID = "correlationId"
)

// Store é a outbox persistida (PostgresStore em produção).
type Store interface {
	// Claim reivindica até limit eventos (no máximo um por agregado) com
	// lease de duração lease para owner.
	Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]postgres.OutboxMessage, error)
	// MarkPublished confirma a publicação; postgres.ErrOutboxLeaseLost se o
	// lease não é mais vigente.
	MarkPublished(ctx context.Context, lease postgres.OutboxLease) error
	// MarkFailed registra a falha e reagenda com backoff.
	MarkFailed(ctx context.Context, lease postgres.OutboxLease, cause string) error
	// Release devolve o evento sem backoff.
	Release(ctx context.Context, lease postgres.OutboxLease) error
	// Backlog mede os pendentes (métricas).
	Backlog(ctx context.Context) (postgres.OutboxBacklog, error)
}

// Sender é o subconjunto do cliente SQS usado pelo publisher. *awssqs.Client
// e sqs.API o satisfazem.
type Sender interface {
	SendMessage(ctx context.Context, in *awssqs.SendMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error)
}

var _ Sender = (*awssqs.Client)(nil)

// Config são os parâmetros do publisher.
type Config struct {
	// QueueURL é a fila FIFO de eventos (SQS_EVENTS_QUEUE_URL).
	QueueURL string
	// Owner identifica a instância no lease (locked_by). Vazio → hostname +
	// UUIDv7, único por processo.
	Owner          string
	PollInterval   time.Duration
	BatchSize      int
	Concurrency    int
	Lease          time.Duration
	PublishTimeout time.Duration
	DrainTimeout   time.Duration
}

// maxOwnerLength espelha o limite de locked_by aceito pelo repositório.
const maxOwnerLength = 255

// Validate valida a configuração.
func (c Config) Validate() error {
	switch {
	case strings.TrimSpace(c.QueueURL) == "":
		return errors.New("outboxpublisher: queue URL is required")
	case len(c.Owner) > maxOwnerLength:
		return fmt.Errorf("outboxpublisher: owner with %d bytes (max %d)", len(c.Owner), maxOwnerLength)
	case c.PollInterval <= 0:
		return fmt.Errorf("outboxpublisher: poll interval %v must be positive", c.PollInterval)
	case c.BatchSize < 1 || c.BatchSize > postgres.MaxOutboxClaimBatch:
		return fmt.Errorf("outboxpublisher: batch size %d must be in 1..%d", c.BatchSize, postgres.MaxOutboxClaimBatch)
	case c.Concurrency < 1:
		return fmt.Errorf("outboxpublisher: concurrency %d must be >= 1", c.Concurrency)
	case c.Lease < time.Millisecond || c.Lease > postgres.MaxOutboxLease:
		return fmt.Errorf("outboxpublisher: lease %v must be in 1ms..%v", c.Lease, postgres.MaxOutboxLease)
	case c.PublishTimeout <= 0 || 2*c.PublishTimeout >= c.Lease:
		return fmt.Errorf("outboxpublisher: publish timeout %v must be positive and twice it below lease %v", c.PublishTimeout, c.Lease)
	case c.DrainTimeout <= 0:
		return fmt.Errorf("outboxpublisher: drain timeout %v must be positive", c.DrainTimeout)
	}
	return nil
}

// Option configura o Publisher.
type Option func(*Publisher)

// WithLogger define o logger (nil = descarta).
func WithLogger(l *slog.Logger) Option { return func(p *Publisher) { p.logger = l } }

// WithMetrics define as métricas (nil = sem métricas).
func WithMetrics(m *metrics.Metrics) Option { return func(p *Publisher) { p.metrics = m } }

// WithClock substitui o relógio (prazo do lease e atraso; testes).
func WithClock(now func() time.Time) Option { return func(p *Publisher) { p.now = now } }

// Publisher publica a outbox em ciclos.
type Publisher struct {
	cfg     Config
	store   Store
	sender  Sender
	owner   string
	logger  *slog.Logger
	metrics *metrics.Metrics
	now     func() time.Time

	mu         sync.Mutex
	started    bool
	stopped    bool
	loopCancel context.CancelFunc
	procCtx    context.Context
	procCancel context.CancelFunc
	loopDone   chan struct{}

	inFlight    atomic.Int64
	lastBacklog atomic.Int64 // UnixNano da última medição do backlog
}

// New cria o publisher.
func New(cfg Config, store Store, sender Sender, opts ...Option) (*Publisher, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if store == nil {
		return nil, errors.New("outboxpublisher: nil store")
	}
	if sender == nil {
		return nil, errors.New("outboxpublisher: nil sender")
	}
	owner := cfg.Owner
	if owner == "" {
		var err error
		if owner, err = defaultOwner(); err != nil {
			return nil, err
		}
	}
	p := &Publisher{cfg: cfg, store: store, sender: sender, owner: owner, now: time.Now}
	for _, o := range opts {
		o(p)
	}
	if p.logger == nil {
		p.logger = slog.New(slog.DiscardHandler)
	}
	p.logger = p.logger.With(slog.String("component", "outboxpublisher"))
	return p, nil
}

// defaultOwner devolve hostname-UUIDv7: legível nos diagnósticos e único por
// processo (duas réplicas no mesmo host não se confundem).
func defaultOwner() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	host, _ := os.Hostname()
	host = strings.Map(func(r rune) rune {
		if r < 0x21 || r > 0x7e {
			return -1
		}
		return r
	}, host)
	if len(host) > 200 {
		host = host[:200]
	}
	if host == "" {
		return id.String(), nil
	}
	return host + "-" + id.String(), nil
}

// Owner devolve a identidade da instância nos leases.
func (p *Publisher) Owner() string { return p.owner }

// Start inicia o laço em background. Não bloqueia.
func (p *Publisher) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return errors.New("outboxpublisher: já iniciado")
	}
	loopCtx, loopCancel := context.WithCancel(context.Background())
	procCtx, procCancel := context.WithCancel(context.Background())
	p.loopCancel, p.procCtx, p.procCancel = loopCancel, procCtx, procCancel
	p.loopDone = make(chan struct{})
	p.started = true
	go p.loop(loopCtx, procCtx, p.loopDone)
	p.logger.InfoContext(ctx, "publisher da outbox iniciado",
		slog.String("owner", p.owner), slog.Int("batchSize", p.cfg.BatchSize),
		slog.Int("concurrency", p.cfg.Concurrency), slog.Duration("lease", p.cfg.Lease),
		slog.Duration("pollInterval", p.cfg.PollInterval))
	return nil
}

// Stop para de reivindicar, devolve os itens não iniciados, drena os envios
// em voo e espera as goroutines.
func (p *Publisher) Stop(ctx context.Context) error {
	p.mu.Lock()
	if !p.started || p.stopped {
		p.mu.Unlock()
		return nil
	}
	p.stopped = true
	p.mu.Unlock()

	p.loopCancel()
	drainCtx, cancel := context.WithTimeout(ctx, p.cfg.DrainTimeout)
	defer cancel()
	var result error
	select {
	case <-p.loopDone:
	case <-drainCtx.Done():
		p.logger.WarnContext(context.WithoutCancel(ctx), "drenagem do publisher estourou o prazo; cancelando publicações em voo",
			slog.Int64("inFlight", p.inFlight.Load()))
		result = ErrDrainTimeout
		p.procCancel()
		select {
		case <-p.loopDone:
		case <-ctx.Done():
			return errors.Join(ErrDrainTimeout, ctx.Err())
		}
	}
	p.procCancel()
	if result == nil {
		p.logger.InfoContext(context.WithoutCancel(ctx), "publisher da outbox parado")
	}
	return result
}

// loop executa ciclos até o cancelamento de ctx. procCtx é o contexto das
// publicações (só cancelado quando a drenagem estoura).
func (p *Publisher) loop(ctx, procCtx context.Context, done chan<- struct{}) {
	defer close(done)
	for {
		n, err := p.runCycle(ctx, procCtx)
		if ctx.Err() != nil {
			return
		}
		p.refreshBacklog(ctx, false)
		if err == nil && n > 0 {
			continue // o próximo evento de cada agregado acabou de ficar elegível
		}
		t := time.NewTimer(p.cfg.PollInterval)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// RunOnce executa um ciclo (claim, publicação e marcação do lote) e devolve
// quantos eventos foram reivindicados. O laço do Start chama o mesmo código;
// usado por testes e ferramentas.
func (p *Publisher) RunOnce(ctx context.Context) (int, error) {
	p.mu.Lock()
	procCtx := p.procCtx
	p.mu.Unlock()
	if procCtx == nil {
		procCtx = ctx
	}
	return p.runCycle(ctx, procCtx)
}

// RefreshBacklog atualiza as métricas de pendentes imediatamente.
func (p *Publisher) RefreshBacklog(ctx context.Context) { p.refreshBacklog(ctx, true) }

// runCycle reivindica um lote com ctx (cancelado no Stop: para de reivindicar
// e de iniciar itens) e publica os itens com procCtx.
func (p *Publisher) runCycle(ctx, procCtx context.Context) (int, error) {
	claimedAt := p.now()
	msgs, err := p.store.Claim(ctx, p.owner, p.cfg.Lease, p.cfg.BatchSize)
	if err != nil {
		if ctx.Err() == nil {
			p.logger.WarnContext(ctx, "falha ao reivindicar eventos da outbox; tentando no próximo ciclo", logging.Err(err))
		}
		return 0, err
	}
	if len(msgs) == 0 {
		return 0, nil
	}
	// Teste de recuperação (ADR-028): crash com o lote reivindicado e
	// nada enviado. No build normal é uma função vazia.
	failpoint.Hit(failpoint.OutboxAfterClaim)
	// Um item só começa se envio e marcação ainda couberem no lease. claimedAt
	// é anterior ao statement_timestamp() do claim, então o prazo local é
	// conservador.
	startBy := claimedAt.Add(p.cfg.Lease - 2*p.cfg.PublishTimeout)

	sem := make(chan struct{}, p.cfg.Concurrency)
	var wg sync.WaitGroup
	next := 0
	for ; next < len(msgs); next++ {
		if !acquire(ctx, sem) {
			break
		}
		if p.now().After(startBy) {
			<-sem
			p.logger.WarnContext(ctx, "lease do lote perto do fim; eventos devolvidos para nova reivindicação",
				slog.Int("returned", len(msgs)-next))
			break
		}
		m := msgs[next]
		wg.Add(1)
		p.inFlight.Add(1)
		go func() {
			defer func() {
				p.inFlight.Add(-1)
				<-sem
				wg.Done()
			}()
			p.publish(procCtx, m)
		}()
	}
	if next < len(msgs) {
		p.release(procCtx, msgs[next:])
	}
	wg.Wait()
	return len(msgs), nil
}

func acquire(ctx context.Context, sem chan struct{}) bool {
	select {
	case <-ctx.Done():
		return false
	case sem <- struct{}{}:
	}
	if ctx.Err() != nil {
		<-sem
		return false
	}
	return true
}

// release devolve itens reivindicados e não iniciados. Uma falha aqui não é
// grave: o lease expira e o item volta do mesmo jeito.
func (p *Publisher) release(ctx context.Context, msgs []postgres.OutboxMessage) {
	for _, m := range msgs {
		rctx, cancel := context.WithTimeout(ctx, p.cfg.PublishTimeout)
		err := p.store.Release(rctx, m.Lease)
		cancel()
		if err != nil {
			p.logger.WarnContext(ctx, "falha ao devolver evento da outbox; volta quando o lease expirar",
				slog.String("eventId", m.EventID.String()), logging.Err(err))
		}
	}
}

// SendInput monta a mensagem SQS de um evento da outbox: corpo = snapshot
// gravado, MessageGroupId = aggregateId, MessageDeduplicationId = eventId.
func SendInput(queueURL string, m postgres.OutboxMessage) *awssqs.SendMessageInput {
	attrs := map[string]types.MessageAttributeValue{
		AttrEventType:    {DataType: aws.String("String"), StringValue: aws.String(m.EventType)},
		AttrEventVersion: {DataType: aws.String("Number"), StringValue: aws.String(strconv.Itoa(m.EventVersion))},
		AttrEventID:      {DataType: aws.String("String"), StringValue: aws.String(m.EventID.String())},
	}
	if m.CorrelationID != "" {
		attrs[AttrCorrelationID] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(m.CorrelationID)}
	}
	return &awssqs.SendMessageInput{
		QueueUrl:               aws.String(queueURL),
		MessageBody:            aws.String(string(m.Payload)),
		MessageGroupId:         aws.String(m.AggregateID),
		MessageDeduplicationId: aws.String(m.EventID.String()),
		MessageAttributes:      attrs,
	}
}

// publish envia um evento e marca o resultado. Nunca devolve erro: falhas
// ficam persistidas (backoff) ou no lease, que expira.
func (p *Publisher) publish(ctx context.Context, m postgres.OutboxMessage) {
	ctx = logging.WithCorrelationID(ctx, m.CorrelationID)
	start := time.Now()
	sendCtx, cancel := context.WithTimeout(ctx, p.cfg.PublishTimeout)
	out, err := p.sender.SendMessage(sendCtx, SendInput(p.cfg.QueueURL, m))
	cancel()
	if err == nil && (out == nil || aws.ToString(out.MessageId) == "") {
		err = errors.New("SendMessage sem MessageId na resposta")
	}
	if err != nil {
		p.failed(ctx, m, err, start)
		return
	}
	// Teste de recuperação (ADR-028): crash entre a publicação e a
	// marcação. No build normal é uma função vazia.
	failpoint.Hit(failpoint.OutboxAfterSend)
	publishedAt := p.now()

	markCtx, cancel := context.WithTimeout(ctx, p.cfg.PublishTimeout)
	err = p.store.MarkPublished(markCtx, m.Lease)
	cancel()
	if err != nil {
		outcome := metrics.OutcomeTransient
		msg := "evento publicado, mas a marcação falhou; o lease expira e outro publisher republica com o mesmo eventId"
		if errors.Is(err, postgres.ErrOutboxLeaseLost) {
			outcome = metrics.OutcomeConflict
			msg = "evento publicado com lease já perdido; a republicação pelo novo detentor usa o mesmo eventId"
		}
		p.observe(outcome, time.Since(start))
		p.logger.WarnContext(ctx, msg, p.eventAttrs(m, logging.Err(err))...)
		return
	}
	p.observe(metrics.OutcomeSuccess, time.Since(start))
	if p.metrics != nil {
		p.metrics.OutboxPublished(m.OccurredAt, publishedAt)
	}
	p.logger.DebugContext(ctx, "evento publicado", p.eventAttrs(m)...)
}

// failed registra a falha de envio e reagenda o evento com backoff.
func (p *Publisher) failed(ctx context.Context, m postgres.OutboxMessage, sendErr error, start time.Time) {
	if ctx.Err() != nil {
		// Drenagem estourada: o envio foi cancelado e o estado no SQS é
		// incerto. O lease fica e expira; a republicação usa o mesmo eventId.
		p.observe(metrics.OutcomeTransient, time.Since(start))
		p.logger.WarnContext(context.WithoutCancel(ctx), "publicação cancelada no shutdown; o evento volta quando o lease expirar",
			p.eventAttrs(m, logging.Err(sendErr))...)
		return
	}
	markCtx, cancel := context.WithTimeout(ctx, p.cfg.PublishTimeout)
	err := p.store.MarkFailed(markCtx, m.Lease, sendErr.Error())
	cancel()
	p.observe(metrics.OutcomeTransient, time.Since(start))
	if p.metrics != nil {
		p.metrics.Retry(metrics.OperationOutboxPublish)
	}
	attrs := p.eventAttrs(m, logging.Err(sendErr))
	if err != nil {
		attrs = append(attrs, slog.String("markError", err.Error()))
	}
	p.logger.WarnContext(ctx, "falha ao publicar evento; reagendado com backoff", attrs...)
}

func (p *Publisher) eventAttrs(m postgres.OutboxMessage, extra ...any) []any {
	attrs := []any{
		slog.String("eventId", m.EventID.String()), slog.String("eventType", m.EventType),
		slog.String("aggregateId", m.AggregateID), slog.Int("attempt", m.Lease.Attempt),
	}
	if m.EventType == string(domain.EventTypeWalletBalanceChanged) {
		attrs = append(attrs, logging.WalletID(m.AggregateID))
	} else {
		attrs = append(attrs, logging.TransactionID(m.AggregateID))
	}
	return append(attrs, extra...)
}

func (p *Publisher) observe(outcome string, d time.Duration) {
	if p.metrics != nil {
		p.metrics.ObserveProcessing(metrics.SourceWorker, metrics.OperationOutboxPublish, outcome, d)
	}
}

// refreshBacklog atualiza pendentes e idade do mais antigo, no máximo uma
// vez por PollInterval (force ignora o intervalo).
func (p *Publisher) refreshBacklog(ctx context.Context, force bool) {
	if p.metrics == nil {
		return
	}
	now := p.now().UnixNano()
	last := p.lastBacklog.Load()
	if !force && last != 0 && time.Duration(now-last) < p.cfg.PollInterval {
		return
	}
	p.lastBacklog.Store(now)
	bctx, cancel := context.WithTimeout(ctx, p.cfg.PublishTimeout)
	defer cancel()
	b, err := p.store.Backlog(bctx)
	if err != nil {
		if ctx.Err() == nil {
			p.logger.DebugContext(ctx, "falha ao medir pendências da outbox", logging.Err(err))
		}
		return
	}
	p.metrics.SetOutboxBacklog(b.Pending, b.OldestAge())
}
