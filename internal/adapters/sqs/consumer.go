package sqs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
	"github.com/junglegaming/backend-challenge-go/internal/platform/logging"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

// Consumidor da fila de entrada wager-transactions.fifo (ARCHITECTURE.md,
// ADR-022).
//
// Fluxo de uma mensagem:
//
//  1. DecodeEnvelope (estrito). Inválida → enviada explicitamente à DLQ
//     (SendMessage com o corpo original e o motivo em atributos) e só então
//     apagada da fila de entrada, na primeira entrega. OPENING é inválido
//     (domain.ParseExternalWagerKind).
//  2. ProcessWagerTransaction.Execute com Inbox{consumerName, messageId do
//     envelope}: inbox, efeitos financeiros, ledger, outbox e conclusão da
//     inbox no MESMO commit (T15).
//  3. Resultado confirmado (PROCESSED, REJECTED, PENDING_REFERENCE ou replay)
//     → DeleteMessage, só depois do commit. Rejeição de negócio é resultado
//     persistido: a mensagem é apagada.
//  4. Erro permanente (entrada inválida, conflito de idempotência/inbox) →
//     igual à mensagem inválida (DLQ explícita na primeira tentativa).
//  5. Erro transitório (indisponibilidade, prazo de processamento, erro sem
//     classe) → ChangeMessageVisibility com backoff exponencial pela
//     tentativa; esgotadas MaxAttempts tentativas, DLQ explícita.
//
// Se o envio à DLQ falhar, a mensagem é tratada como transitória (não é
// apagada; volta com backoff). O redrive nativo do SQS (maxReceiveCount >
// MaxAttempts, exigido no Start) fica só como rede de segurança: crash
// repetido, DLQ indisponível por muito tempo.
//
// Ordem FIFO: as mensagens de um ReceiveMessage são agrupadas por
// MessageGroupId e cada grupo é processado em sequência por uma única
// goroutine; grupos diferentes seguem em paralelo (até Concurrency). Uma
// mensagem enviada à DLQ não interrompe o grupo: as seguintes do lote são
// processadas em seguida. Se uma mensagem do grupo não for concluída
// (transitória, shutdown), as seguintes do mesmo lote não são processadas:
// voltam à fila (visibilidade 0) atrás dela. Enquanto houver mensagem do grupo
// em voo, o próprio SQS FIFO não entrega as seguintes.
//
// Tentativa ≠ ApproximateReceiveCount: cada ReceiveMessage incrementa a
// contagem também das seguintes devolvidas sem processar. O consumidor
// desconta, por MessageId do SQS, os recebimentos que ele mesmo devolveu sem
// tentar (releaseTracker); assim a seguinte de uma mensagem com falhas não
// herda as tentativas dela.
//
// Shutdown (Stop): para de buscar (o long polling é cancelado), deixa as
// mensagens em processamento terminarem até DrainTimeout e libera
// (visibilidade 0) as recebidas e ainda não iniciadas. Estourado o prazo, o
// contexto do processamento é cancelado (a transação faz rollback) e essas
// mensagens também são liberadas.

// WagerProcessor é o caso de uso consumido (*app.ProcessWagerTransaction).
type WagerProcessor interface {
	Execute(ctx context.Context, cmd app.ProcessWagerCommand) (app.WagerResult, error)
}

// Limites do SQS.
const (
	maxReceiveBatch       = 10
	maxVisibilitySeconds  = 43200 // 12h
	attrReceiveCount      = string(types.MessageSystemAttributeNameApproximateReceiveCount)
	attrMessageGroupID    = string(types.MessageSystemAttributeNameMessageGroupId)
	defaultCallTimeout    = 5 * time.Second
	defaultReceiveBackoff = time.Second
	maxReceiveBackoff     = 30 * time.Second
	defaultDLQInterval    = 30 * time.Second
)

// Atributos (MessageAttributes) gravados na cópia enviada à DLQ. Nunca levam
// o corpo nem valores de campos: só o motivo, códigos e identificadores.
const (
	DLQAttrReason          = "dlqReason"          // metrics.DLQReason*
	DLQAttrCode            = "dlqCode"            // código do erro (se houver)
	DLQAttrField           = "dlqField"           // caminho do campo inválido (se houver)
	DLQAttrSourceQueue     = "sourceQueue"        // nome da fila de origem
	DLQAttrSourceMessageID = "sourceMessageId"    // MessageId do SQS na origem
	DLQAttrMessageID       = "envelopeMessageId"  // messageId do envelope (se decodificado)
	DLQAttrReceiveCount    = "receiveCount"       // ApproximateReceiveCount na origem
	DLQAttrAttempts        = "attempts"           // tentativas contadas pelo consumidor
	DLQAttrConsumer        = "consumerName"       // consumer_name da inbox
	dlqDefaultGroup        = "dlq-ungrouped"      // grupo na DLQ FIFO de mensagem sem grupo
	codeProcessTimeout     = "PROCESS_TIMEOUT"    // prazo de processamento estourado
	codeUnclassifiedError  = "UNCLASSIFIED_ERROR" // erro sem classe do caso de uso
)

// ErrDrainTimeout: o prazo de drenagem estourou; as mensagens ainda em
// processamento foram canceladas e liberadas para reentrega.
var ErrDrainTimeout = errors.New("sqs: prazo de drenagem do consumidor estourado; mensagens em processamento liberadas")

// ErrNoRedrivePolicy: a fila de entrada não tem RedrivePolicy (DLQ). Sem ela
// não há para onde enviar as mensagens inválidas, nem rede de segurança.
var ErrNoRedrivePolicy = errors.New("sqs: a fila de entrada não tem RedrivePolicy (DLQ) configurada")

// ErrMaxAttemptsTooHigh: MaxAttempts precisa ser menor que o maxReceiveCount
// da RedrivePolicy; senão o redrive nativo levaria a mensagem à DLQ antes do
// envio explícito (sem motivo) e as seguintes devolvidas junto com ela.
var ErrMaxAttemptsTooHigh = errors.New("sqs: SQS_MAX_ATTEMPTS deve ser menor que o maxReceiveCount da RedrivePolicy")

// ConsumerConfig são os parâmetros do consumidor.
type ConsumerConfig struct {
	QueueURL string
	// DLQURL é opcional: só alimenta a métrica de profundidade da DLQ. O
	// destino dos envios é a DLQ da RedrivePolicy (resolvida no Start).
	DLQURL            string
	Name              string
	Concurrency       int
	WaitTime          time.Duration
	VisibilityTimeout time.Duration
	ProcessTimeout    time.Duration
	RetryBaseDelay    time.Duration
	RetryMaxDelay     time.Duration
	DrainTimeout      time.Duration
	// MaxAttempts limita as tentativas de uma falha transitória; esgotado, a
	// mensagem é enviada à DLQ. Precisa ser < maxReceiveCount (Start).
	MaxAttempts int

	// Ajustes internos (zero = padrão).
	// ReceiveErrorBackoff é a espera inicial após falha do ReceiveMessage
	// (dobra até 30s).
	ReceiveErrorBackoff time.Duration
	// CallTimeout limita Delete/ChangeMessageVisibility/GetQueueAttributes.
	CallTimeout time.Duration
	// DLQDepthInterval é o intervalo da leitura da profundidade da DLQ.
	DLQDepthInterval time.Duration
}

// ConsumerConfigFrom traduz a configuração validada da aplicação.
func ConsumerConfigFrom(c config.Config) ConsumerConfig {
	cc := c.SQS.Consumer
	return ConsumerConfig{
		QueueURL: c.SQS.InboundQueueURL, DLQURL: c.SQS.InboundDLQURL, Name: cc.Name,
		Concurrency: cc.Concurrency, WaitTime: cc.WaitTime, VisibilityTimeout: cc.VisibilityTimeout,
		ProcessTimeout: cc.ProcessTimeout, RetryBaseDelay: cc.RetryBaseDelay, RetryMaxDelay: cc.RetryMaxDelay,
		DrainTimeout: cc.DrainTimeout, MaxAttempts: cc.MaxAttempts,
	}
}

func (c *ConsumerConfig) validate() error {
	var errs []error
	if c.QueueURL == "" {
		errs = append(errs, errors.New("QueueURL obrigatória"))
	}
	if c.Name == "" || len(c.Name) > domain.MaxWagerIdentifierLength {
		errs = append(errs, errors.New("Name com 1..255 bytes"))
	}
	if c.Concurrency < 1 {
		errs = append(errs, errors.New("Concurrency >= 1"))
	}
	if c.WaitTime < 0 || c.WaitTime > 20*time.Second {
		errs = append(errs, errors.New("WaitTime entre 0s e 20s"))
	}
	if c.VisibilityTimeout < time.Second || c.VisibilityTimeout > maxVisibilitySeconds*time.Second {
		errs = append(errs, errors.New("VisibilityTimeout entre 1s e 12h"))
	}
	if c.ProcessTimeout <= 0 || c.ProcessTimeout >= c.VisibilityTimeout {
		errs = append(errs, errors.New("ProcessTimeout positivo e menor que VisibilityTimeout"))
	}
	if c.RetryBaseDelay < time.Second || c.RetryMaxDelay < c.RetryBaseDelay {
		errs = append(errs, errors.New("RetryBaseDelay >= 1s e RetryMaxDelay >= RetryBaseDelay"))
	}
	if c.DrainTimeout <= 0 {
		errs = append(errs, errors.New("DrainTimeout positivo"))
	}
	if c.MaxAttempts < 1 {
		errs = append(errs, errors.New("MaxAttempts >= 1"))
	}
	if c.ReceiveErrorBackoff <= 0 {
		c.ReceiveErrorBackoff = defaultReceiveBackoff
	}
	if c.CallTimeout <= 0 {
		c.CallTimeout = defaultCallTimeout
	}
	if c.DLQDepthInterval <= 0 {
		c.DLQDepthInterval = defaultDLQInterval
	}
	if len(errs) > 0 {
		return fmt.Errorf("sqs: configuração do consumidor inválida: %w", errors.Join(errs...))
	}
	return nil
}

// Option ajusta o consumidor.
type Option func(*Consumer)

// WithLogger define o logger (padrão: descarta).
func WithLogger(l *slog.Logger) Option {
	return func(c *Consumer) {
		if l != nil {
			c.logger = l
		}
	}
}

// WithMetrics liga as métricas (opcional).
func WithMetrics(m *metrics.Metrics) Option { return func(c *Consumer) { c.metrics = m } }

// Consumer é o consumidor da fila de entrada com Start/Stop para o
// fx.Lifecycle.
type Consumer struct {
	cfg       ConsumerConfig
	api       API
	proc      WagerProcessor
	logger    *slog.Logger
	metrics   *metrics.Metrics
	dlqName   string // fila da métrica de profundidade (DLQURL)
	queueName string

	// Destino dos envios à DLQ, resolvido no Start pela RedrivePolicy.
	dlqTargetURL  string
	dlqTargetFIFO bool

	released *releaseTracker

	maxReceiveCount atomic.Int32
	stopping        atomic.Bool

	mu         sync.Mutex
	started    bool
	stopped    bool
	recvCancel context.CancelFunc
	procCtx    context.Context
	procCancel context.CancelFunc
	loopDone   chan struct{}

	sem chan struct{} // vagas de grupo (Concurrency)
	wg  sync.WaitGroup
}

// NewConsumer valida a configuração e monta o consumidor; nada é chamado no
// SQS até Start.
func NewConsumer(cfg ConsumerConfig, api API, proc WagerProcessor, opts ...Option) (*Consumer, error) {
	if api == nil || proc == nil {
		return nil, errors.New("sqs: consumidor sem cliente SQS ou caso de uso")
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	c := &Consumer{
		cfg: cfg, api: api, proc: proc,
		logger:    slog.New(slog.DiscardHandler),
		queueName: queueName(cfg.QueueURL),
		released:  newReleaseTracker(),
		sem:       make(chan struct{}, cfg.Concurrency),
	}
	for _, o := range opts {
		o(c)
	}
	c.logger = c.logger.With(slog.String("component", "sqs-consumer"))
	if cfg.DLQURL != "" {
		c.dlqName = queueName(cfg.DLQURL)
		if c.metrics != nil {
			c.metrics.AllowQueues(c.dlqName)
		}
	}
	return c, nil
}

func queueName(url string) string { return url[strings.LastIndex(url, "/")+1:] }

// MaxReceiveCount devolve o maxReceiveCount lido da RedrivePolicy no Start.
func (c *Consumer) MaxReceiveCount() int32 { return c.maxReceiveCount.Load() }

// Start lê a RedrivePolicy da fila, confere MaxAttempts < maxReceiveCount,
// resolve a URL da DLQ (GetQueueUrl pelo ARN; chamadas com retry até o prazo
// do ctx) e inicia o laço de recebimento em background.
func (c *Consumer) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return errors.New("sqs: consumidor já iniciado")
	}
	policy, err := c.loadRedrivePolicy(ctx)
	if err != nil {
		return err
	}
	if int64(c.cfg.MaxAttempts) >= int64(policy.maxReceiveCount) {
		return fmt.Errorf("%w (SQS_MAX_ATTEMPTS=%d, maxReceiveCount=%d)",
			ErrMaxAttemptsTooHigh, c.cfg.MaxAttempts, policy.maxReceiveCount)
	}
	dlqURL, err := c.resolveDLQ(ctx, policy.dlq)
	if err != nil {
		return err
	}
	c.maxReceiveCount.Store(policy.maxReceiveCount)
	c.dlqTargetURL = dlqURL
	c.dlqTargetFIFO = strings.HasSuffix(policy.dlq.name, ".fifo")
	if c.dlqName != "" && c.dlqName != policy.dlq.name {
		c.logger.WarnContext(ctx, "SQS_INBOUND_DLQ_URL difere da DLQ da RedrivePolicy; os envios vão para a da RedrivePolicy",
			slog.String("configuredDLQ", c.dlqName), slog.String("redriveDLQ", policy.dlq.name))
	}

	recvCtx, recvCancel := context.WithCancel(context.Background())
	procCtx, procCancel := context.WithCancel(context.Background())
	c.recvCancel, c.procCtx, c.procCancel = recvCancel, procCtx, procCancel
	c.loopDone = make(chan struct{})
	c.started = true
	go c.loop(recvCtx, c.loopDone)
	c.logger.InfoContext(ctx, "consumidor SQS iniciado",
		slog.String("queue", c.queueName),
		slog.String("dlq", policy.dlq.name),
		slog.Int("concurrency", c.cfg.Concurrency),
		slog.Int("maxAttempts", c.cfg.MaxAttempts),
		slog.Int("maxReceiveCount", int(policy.maxReceiveCount)))
	return nil
}

// startCall repete call com backoff até o prazo do ctx. Fila inexistente é
// erro definitivo (não adianta repetir).
func (c *Consumer) startCall(ctx context.Context, what string, call func(context.Context) error) error {
	delay := 100 * time.Millisecond
	for {
		callCtx, cancel := context.WithTimeout(ctx, c.cfg.CallTimeout)
		err := call(callCtx)
		cancel()
		if err == nil {
			return nil
		}
		var notFound *types.QueueDoesNotExist
		if errors.As(err, &notFound) {
			return fmt.Errorf("sqs: %s: %w", what, err)
		}
		c.logger.WarnContext(ctx, "SQS indisponível no start; tentando novamente",
			slog.String("operation", what), logging.Err(err), slog.Duration("retryIn", delay))
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return fmt.Errorf("sqs: %s: %w", what, errors.Join(err, ctx.Err()))
		case <-t.C:
		}
		delay = min(delay*2, 2*time.Second)
	}
}

// loadRedrivePolicy exige RedrivePolicy com DLQ e maxReceiveCount >= 1.
func (c *Consumer) loadRedrivePolicy(ctx context.Context) (redrivePolicy, error) {
	var raw string
	err := c.startCall(ctx, "ler atributos da fila de entrada", func(ctx context.Context) error {
		out, err := c.api.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
			QueueUrl:       aws.String(c.cfg.QueueURL),
			AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameRedrivePolicy},
		})
		if err == nil {
			raw = out.Attributes[string(types.QueueAttributeNameRedrivePolicy)]
		}
		return err
	})
	if err != nil {
		return redrivePolicy{}, err
	}
	return parseRedrivePolicy(raw)
}

// resolveDLQ obtém a URL da DLQ pelo nome e conta do ARN da RedrivePolicy.
func (c *Consumer) resolveDLQ(ctx context.Context, dlq queueARN) (string, error) {
	var url string
	err := c.startCall(ctx, "resolver a URL da DLQ", func(ctx context.Context) error {
		in := &awssqs.GetQueueUrlInput{QueueName: aws.String(dlq.name)}
		if dlq.account != "" {
			in.QueueOwnerAWSAccountId = aws.String(dlq.account)
		}
		out, err := c.api.GetQueueUrl(ctx, in)
		if err == nil {
			url = aws.ToString(out.QueueUrl)
		}
		return err
	})
	if err != nil {
		return "", err
	}
	if url == "" {
		return "", fmt.Errorf("sqs: resolver a URL da DLQ %s: resposta sem QueueUrl", dlq.name)
	}
	return url, nil
}

// redrivePolicy é a RedrivePolicy da fila de entrada.
type redrivePolicy struct {
	dlq             queueARN
	maxReceiveCount int32
}

// queueARN: arn:<partição>:sqs:<região>:<conta>:<nome>.
type queueARN struct{ account, name string }

func parseQueueARN(arn string) (queueARN, bool) {
	parts := strings.Split(arn, ":")
	if len(parts) != 6 || parts[0] != "arn" || parts[2] != "sqs" || parts[5] == "" {
		return queueARN{}, false
	}
	return queueARN{account: parts[4], name: parts[5]}, true
}

// parseRedrivePolicy lê deadLetterTargetArn e maxReceiveCount (número ou
// string, conforme o provedor) da RedrivePolicy em JSON.
func parseRedrivePolicy(raw string) (redrivePolicy, error) {
	if raw == "" {
		return redrivePolicy{}, ErrNoRedrivePolicy
	}
	var p struct {
		DeadLetterTargetArn string          `json:"deadLetterTargetArn"`
		MaxReceiveCount     json.RawMessage `json:"maxReceiveCount"`
	}
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return redrivePolicy{}, fmt.Errorf("%w: RedrivePolicy malformada", ErrNoRedrivePolicy)
	}
	s := strings.Trim(string(p.MaxReceiveCount), `"`)
	n, err := strconv.ParseInt(s, 10, 32)
	arn, ok := parseQueueARN(p.DeadLetterTargetArn)
	if err != nil || n < 1 || !ok {
		return redrivePolicy{}, fmt.Errorf("%w: RedrivePolicy sem deadLetterTargetArn ou maxReceiveCount válido", ErrNoRedrivePolicy)
	}
	return redrivePolicy{dlq: arn, maxReceiveCount: int32(n)}, nil
}

// Stop para de buscar mensagens, drena as em processamento até DrainTimeout
// (ou o prazo do ctx) e libera as não concluídas. Sempre espera todas as
// goroutines do consumidor terminarem.
func (c *Consumer) Stop(ctx context.Context) error {
	c.mu.Lock()
	if !c.started || c.stopped {
		c.mu.Unlock()
		return nil
	}
	c.stopped = true
	c.mu.Unlock()

	c.stopping.Store(true)
	c.recvCancel()
	<-c.loopDone // o laço não cria grupos depois disto

	drainCtx, cancel := context.WithTimeout(ctx, c.cfg.DrainTimeout)
	defer cancel()
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	var result error
	select {
	case <-done:
	case <-drainCtx.Done():
		c.logger.WarnContext(context.WithoutCancel(ctx), "drenagem do consumidor SQS estourou o prazo; cancelando processamento")
		result = ErrDrainTimeout
		c.procCancel()
		// Cancelado, cada mensagem é liberada com prazo próprio
		// (CallTimeout). Se nem isso couber no prazo do OnStop, o Stop
		// retorna para não travar a parada da aplicação.
		select {
		case <-done:
		case <-ctx.Done():
			return errors.Join(ErrDrainTimeout, ctx.Err())
		}
	}
	c.procCancel()
	if result == nil {
		c.logger.InfoContext(context.WithoutCancel(ctx), "consumidor SQS parado")
	}
	return result
}

// Ready verifica o acesso à fila de entrada (readiness).
func (c *Consumer) Ready(ctx context.Context) error {
	if c.stopping.Load() {
		return errors.New("consumidor SQS em parada")
	}
	_, err := c.api.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(c.cfg.QueueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	return err
}

// loop recebe lotes enquanto houver vaga de grupo e despacha cada grupo para
// uma goroutine.
func (c *Consumer) loop(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	backoff := c.cfg.ReceiveErrorBackoff
	var lastDLQ time.Time
	for {
		if c.dlqName != "" && c.metrics != nil && time.Since(lastDLQ) >= c.cfg.DLQDepthInterval {
			lastDLQ = time.Now()
			c.refreshDLQDepth(ctx)
		}
		// Vagas: pelo menos uma (bloqueia), até 10 sem bloquear.
		select {
		case c.sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		slots := 1
	fill:
		for slots < maxReceiveBatch {
			select {
			case c.sem <- struct{}{}:
				slots++
			default:
				break fill
			}
		}

		out, err := c.api.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.cfg.QueueURL),
			MaxNumberOfMessages: int32(slots),
			WaitTimeSeconds:     int32(c.cfg.WaitTime / time.Second),
			VisibilityTimeout:   int32(c.cfg.VisibilityTimeout / time.Second),
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount,
				types.MessageSystemAttributeNameMessageGroupId,
			},
		})
		receivedAt := time.Now()
		if err != nil {
			c.releaseSlots(slots)
			if ctx.Err() != nil {
				return
			}
			c.logger.Warn("falha no ReceiveMessage; tentando novamente", logging.Err(err), slog.Duration("retryIn", backoff))
			if !sleep(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, maxReceiveBackoff)
			continue
		}
		backoff = c.cfg.ReceiveErrorBackoff

		groups := groupMessages(out.Messages)
		c.releaseSlots(slots - len(groups))
		for _, g := range groups {
			c.wg.Add(1)
			go c.handleGroup(g, receivedAt)
		}
	}
}

func (c *Consumer) releaseSlots(n int) {
	for range n {
		<-c.sem
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// groupMessages agrupa por MessageGroupId preservando a ordem de chegada
// (dos grupos e dentro de cada grupo). Sem MessageGroupId (fila padrão),
// cada mensagem é um grupo.
func groupMessages(msgs []types.Message) [][]types.Message {
	var groups [][]types.Message
	index := make(map[string]int)
	for _, m := range msgs {
		key := m.Attributes[attrMessageGroupID]
		if key == "" {
			groups = append(groups, []types.Message{m})
			continue
		}
		if i, ok := index[key]; ok {
			groups[i] = append(groups[i], m)
			continue
		}
		index[key] = len(groups)
		groups = append(groups, []types.Message{m})
	}
	return groups
}

// handleGroup processa as mensagens do grupo em ordem. A primeira que não
// for concluída (apagada, ou enviada à DLQ e apagada) interrompe o grupo; as
// seguintes são liberadas.
func (c *Consumer) handleGroup(msgs []types.Message, receivedAt time.Time) {
	defer c.wg.Done()
	defer func() { <-c.sem }()
	visibility := c.cfg.VisibilityTimeout
	for i, m := range msgs {
		if c.stopping.Load() {
			c.release(msgs[i:], "shutdown")
			return
		}
		// A visibilidade conta desde o recebimento: se o que resta não cobre
		// um processamento completo, renova antes de começar.
		if time.Until(receivedAt.Add(visibility)) < c.cfg.ProcessTimeout {
			if err := c.changeVisibility(m, visibility); err != nil {
				c.logger.Warn("não foi possível renovar a visibilidade; a mensagem será reentregue",
					c.msgAttrs(m, "", logging.Err(err))...)
				// m não foi tentada: o recebimento não conta como tentativa.
				c.released.add(aws.ToString(m.MessageId))
				c.release(msgs[i+1:], "group_interrupted")
				return
			}
		}
		if !c.handleMessage(m) {
			c.release(msgs[i+1:], "group_interrupted")
			return
		}
	}
}

// release devolve mensagens à fila (visibilidade 0) sem processá-las. Esses
// recebimentos não contam como tentativa (releaseTracker).
func (c *Consumer) release(msgs []types.Message, reason string) {
	for _, m := range msgs {
		c.released.add(aws.ToString(m.MessageId))
		if err := c.changeVisibility(m, 0); err != nil {
			c.logger.Warn("falha ao liberar mensagem; reaparece ao fim da visibilidade",
				c.msgAttrs(m, "", slog.String("reason", reason), logging.Err(err))...)
			continue
		}
		c.logger.Debug("mensagem liberada para reentrega", c.msgAttrs(m, "", slog.String("reason", reason))...)
	}
}

// disposition é o destino de uma mensagem depois da tentativa.
type disposition int

const (
	dispDelete    disposition = iota + 1 // resultado confirmado
	dispPermanent                        // permanente: DLQ explícita
	dispTransient                        // retry com backoff; esgotado, DLQ explícita
	dispRelease                          // shutdown: reentrega imediata
)

// dlqNote é o motivo gravado nos atributos da cópia enviada à DLQ.
type dlqNote struct {
	reason, code, field string
}

// handleMessage trata uma mensagem e informa se o grupo pode seguir (a
// mensagem foi apagada, ou enviada à DLQ e apagada).
func (c *Consumer) handleMessage(m types.Message) bool {
	start := time.Now()
	receiveCount := receiveCountOf(m)
	attempt := c.attemptOf(m, receiveCount)

	env, err := DecodeEnvelope([]byte(aws.ToString(m.Body)))
	if err != nil {
		var ime *InvalidMessageError
		code := CodeInvalidMessage
		field := ""
		if errors.As(err, &ime) {
			code, field = ime.Code, ime.Field
		}
		c.logger.Warn("mensagem inválida; enviada à DLQ sem processar",
			c.msgAttrs(m, "", slog.String("code", code), slog.String("field", field),
				slog.Int("receiveCount", int(receiveCount)))...)
		c.observe(metrics.OutcomeRejected, start)
		return c.deadLetter(m, "", dlqNote{reason: metrics.DLQReasonInvalidMessage, code: code, field: field}, receiveCount, attempt)
	}

	cmd := env.Command
	cmd.CorrelationID = env.MessageID
	cmd.CausationID = env.MessageID
	cmd.Inbox = &app.InboxMessageRef{ConsumerName: c.cfg.Name, MessageID: env.MessageID}

	ctx, cancel := context.WithTimeout(c.procCtx, c.cfg.ProcessTimeout)
	res, err := c.proc.Execute(ctx, cmd)
	cancel()

	attrs := []any{
		slog.String("providerId", cmd.ProviderID), logging.WalletID(cmd.WalletID.String()),
		slog.String("kind", cmd.Kind.String()), slog.Int("receiveCount", int(receiveCount)),
		slog.Int("attempt", int(attempt)),
	}
	switch disp, code := c.classify(err); disp {
	case dispDelete:
		c.recordResult(cmd, res, receiveCount)
		attrs = append(attrs, logging.TransactionID(res.TransactionID.String()),
			slog.String("status", res.Status.String()), slog.Bool("idempotentReplay", res.IdempotentReplay))
		if res.FailureCode != "" {
			attrs = append(attrs, slog.String("failureCode", res.FailureCode.String()))
		}
		if derr := c.delete(m); derr != nil {
			// O resultado já está confirmado: a reentrega encontra a inbox
			// COMPLETED e devolve o replay, e então a mensagem é apagada.
			c.logger.Warn("resultado confirmado, mas DeleteMessage falhou; a reentrega será um replay",
				c.msgAttrs(m, env.MessageID, append(attrs, logging.Err(derr))...)...)
		} else {
			c.released.forget(aws.ToString(m.MessageId))
			c.logger.Info("mensagem processada e apagada", c.msgAttrs(m, env.MessageID, attrs...)...)
		}
		outcome := metrics.OutcomeSuccess
		if res.Status == domain.WagerStatusRejected || res.Status == domain.WagerStatusFailed {
			outcome = metrics.OutcomeRejected
		}
		c.observe(outcome, start)
		return true
	case dispPermanent:
		if c.metrics != nil && errors.Is(err, app.ErrConflict) {
			c.metrics.Duplicate(metrics.SourceSQS, metrics.DuplicateConflict)
		}
		c.logger.Warn("erro permanente; mensagem enviada à DLQ",
			c.msgAttrs(m, env.MessageID, append(attrs, slog.String("code", code), logging.Err(err))...)...)
		outcome := metrics.OutcomeRejected
		if errors.Is(err, app.ErrConflict) {
			outcome = metrics.OutcomeConflict
		}
		c.observe(outcome, start)
		return c.deadLetter(m, env.MessageID, dlqNote{reason: metrics.DLQReasonPermanentError, code: code}, receiveCount, attempt)
	case dispRelease:
		c.logger.Info("processamento interrompido pelo shutdown; mensagem liberada",
			c.msgAttrs(m, env.MessageID, attrs...)...)
		c.release([]types.Message{m}, "shutdown")
		c.observe(metrics.OutcomeTransient, start)
		return false
	default: // dispTransient
		c.observe(metrics.OutcomeTransient, start)
		// Tentativas da aplicação esgotadas, ou último recebimento antes do
		// redrive nativo: envio explícito, com o motivo.
		if attempt >= int32(c.cfg.MaxAttempts) || receiveCount >= c.MaxReceiveCount() {
			c.logger.Warn("falha transitória com tentativas esgotadas; mensagem enviada à DLQ",
				c.msgAttrs(m, env.MessageID, append(attrs, slog.String("code", code), logging.Err(err))...)...)
			return c.deadLetter(m, env.MessageID, dlqNote{reason: metrics.DLQReasonMaxReceives, code: code}, receiveCount, attempt)
		}
		delay := c.backoff(attempt)
		c.logger.Warn("falha transitória; mensagem volta à fila com backoff",
			c.msgAttrs(m, env.MessageID, append(attrs, slog.String("code", code),
				slog.Duration("retryIn", delay), logging.Err(err))...)...)
		if c.metrics != nil {
			c.metrics.Retry(metrics.OperationSQSConsume)
		}
		if verr := c.changeVisibility(m, delay); verr != nil {
			c.logger.Warn("falha ao aplicar backoff; reaparece ao fim da visibilidade",
				c.msgAttrs(m, env.MessageID, logging.Err(verr))...)
		}
		return false
	}
}

// classify decide o destino a partir do erro do caso de uso.
func (c *Consumer) classify(err error) (disposition, string) {
	if err == nil {
		return dispDelete, ""
	}
	if c.procCtx.Err() != nil {
		return dispRelease, ""
	}
	if ae, ok := app.AsError(err); ok {
		switch ae.Kind {
		case app.KindInvalidInput, app.KindConflict, app.KindNotFound:
			return dispPermanent, ae.Code
		case app.KindUnavailable:
			return dispTransient, ae.Code
		}
	}
	// Prazo de processamento ou erro sem classe: transitório até MaxAttempts
	// (depois, DLQ).
	if errors.Is(err, context.DeadlineExceeded) {
		return dispTransient, codeProcessTimeout
	}
	return dispTransient, codeUnclassifiedError
}

// deadLetter envia uma cópia da mensagem à DLQ e, só depois do envio
// confirmado, apaga a original. Informa se o grupo pode seguir.
//
// Falha no envio → tratada como transitória: a mensagem não é apagada e
// volta com backoff (o redrive nativo é a rede de segurança). Falha no
// delete depois do envio → a original reaparece ao fim da visibilidade e o
// reenvio à DLQ FIFO é deduplicado (MessageDeduplicationId determinístico,
// janela de 5 min); o grupo é interrompido para que as seguintes não passem
// à frente dela.
func (c *Consumer) deadLetter(m types.Message, messageID string, n dlqNote, receiveCount, attempt int32) bool {
	attrs := []any{
		slog.String("dlqReason", n.reason), slog.String("code", n.code),
		slog.Int("receiveCount", int(receiveCount)), slog.Int("attempt", int(attempt)),
	}
	if err := c.sendToDLQ(m, messageID, n, receiveCount, attempt); err != nil {
		delay := c.backoff(attempt)
		c.logger.Warn("falha ao enviar à DLQ; mensagem mantida na fila com backoff",
			c.msgAttrs(m, messageID, append(attrs, slog.Duration("retryIn", delay), logging.Err(err))...)...)
		if c.metrics != nil {
			c.metrics.Retry(metrics.OperationSQSConsume)
		}
		if verr := c.changeVisibility(m, delay); verr != nil {
			c.logger.Warn("falha ao aplicar backoff; reaparece ao fim da visibilidade",
				c.msgAttrs(m, messageID, logging.Err(verr))...)
		}
		return false
	}
	if c.metrics != nil {
		c.metrics.SentToDLQ(n.reason)
	}
	if err := c.delete(m); err != nil {
		c.logger.Warn("mensagem copiada para a DLQ, mas DeleteMessage falhou; reaparece e o reenvio é deduplicado",
			c.msgAttrs(m, messageID, append(attrs, logging.Err(err))...)...)
		return false
	}
	c.released.forget(aws.ToString(m.MessageId))
	c.logger.Info("mensagem enviada à DLQ e removida da fila", c.msgAttrs(m, messageID, attrs...)...)
	return true
}

// sendToDLQ copia corpo e MessageGroupId para a DLQ, com o motivo em
// MessageAttributes (sem payload). Na DLQ FIFO a deduplicação usa um id
// derivado do MessageId do SQS na origem, estável entre reentregas.
func (c *Consumer) sendToDLQ(m types.Message, messageID string, n dlqNote, receiveCount, attempt int32) error {
	attrs := map[string]types.MessageAttributeValue{
		DLQAttrReason:          stringAttr(n.reason),
		DLQAttrSourceQueue:     stringAttr(c.queueName),
		DLQAttrSourceMessageID: stringAttr(aws.ToString(m.MessageId)),
		DLQAttrReceiveCount:    numberAttr(receiveCount),
		DLQAttrAttempts:        numberAttr(attempt),
		DLQAttrConsumer:        stringAttr(c.cfg.Name),
	}
	if n.code != "" {
		attrs[DLQAttrCode] = stringAttr(n.code)
	}
	if n.field != "" {
		attrs[DLQAttrField] = stringAttr(n.field)
	}
	if messageID != "" {
		attrs[DLQAttrMessageID] = stringAttr(messageID)
	}
	in := &awssqs.SendMessageInput{
		QueueUrl:          aws.String(c.dlqTargetURL),
		MessageBody:       m.Body,
		MessageAttributes: attrs,
	}
	if c.dlqTargetFIFO {
		group := m.Attributes[attrMessageGroupID]
		if group == "" {
			group = dlqDefaultGroup
		}
		in.MessageGroupId = aws.String(group)
		in.MessageDeduplicationId = aws.String(dlqDeduplicationID(c.queueName, aws.ToString(m.MessageId)))
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.CallTimeout)
	defer cancel()
	_, err := c.api.SendMessage(ctx, in)
	return err
}

// dlqDeduplicationID = sha256(fila de origem, MessageId do SQS) em hex (64
// caracteres; o limite do SQS é 128).
func dlqDeduplicationID(sourceQueue, sqsMessageID string) string {
	sum := sha256.Sum256([]byte(sourceQueue + "\n" + sqsMessageID))
	return hex.EncodeToString(sum[:])
}

func stringAttr(v string) types.MessageAttributeValue {
	return types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
}

func numberAttr(v int32) types.MessageAttributeValue {
	return types.MessageAttributeValue{DataType: aws.String("Number"), StringValue: aws.String(strconv.Itoa(int(v)))}
}

// attemptOf é a tentativa atual: o ApproximateReceiveCount menos os
// recebimentos que esta instância devolveu sem tentar processar (seguintes
// de um grupo interrompido, shutdown). Depois de um reinício, ou em outra
// instância, o desconto se perde e vale a contagem bruta (conservador: a
// mensagem pode ir à DLQ mais cedo, nunca ficar em retry sem fim).
func (c *Consumer) attemptOf(m types.Message, receiveCount int32) int32 {
	return max(1, receiveCount-c.released.count(aws.ToString(m.MessageId)))
}

// backoff = RetryBaseDelay·2^(attempt-1), com teto RetryMaxDelay.
func (c *Consumer) backoff(attempt int32) time.Duration {
	d := c.cfg.RetryBaseDelay
	for i := int32(1); i < attempt && d < c.cfg.RetryMaxDelay; i++ {
		d *= 2
	}
	return min(d, c.cfg.RetryMaxDelay)
}

// releaseTracker conta, por MessageId do SQS, os recebimentos devolvidos sem
// tentativa. É memória local e limitada: quando enche, entradas sem uso há
// mais de releaseTTL são podadas; se ainda estiver cheio, ele é zerado (volta
// a valer a contagem bruta, que é conservadora).
type releaseTracker struct {
	mu sync.Mutex
	m  map[string]releaseEntry
}

type releaseEntry struct {
	n  int32
	at time.Time
}

const (
	releaseTrackerMax = 10000
	releaseTTL        = time.Hour
)

func newReleaseTracker() *releaseTracker {
	return &releaseTracker{m: make(map[string]releaseEntry)}
}

func (r *releaseTracker) add(id string) {
	if id == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	if _, ok := r.m[id]; !ok && len(r.m) >= releaseTrackerMax {
		for k, e := range r.m {
			if now.Sub(e.at) > releaseTTL {
				delete(r.m, k)
			}
		}
		if len(r.m) >= releaseTrackerMax {
			clear(r.m)
		}
	}
	e := r.m[id]
	e.n++
	e.at = now
	r.m[id] = e
}

func (r *releaseTracker) count(id string) int32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.m[id].n
}

func (r *releaseTracker) forget(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.m, id)
}

func (c *Consumer) delete(m types.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.CallTimeout)
	defer cancel()
	_, err := c.api.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
		QueueUrl: aws.String(c.cfg.QueueURL), ReceiptHandle: m.ReceiptHandle,
	})
	return err
}

func (c *Consumer) changeVisibility(m types.Message, d time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.CallTimeout)
	defer cancel()
	secs := min(int32(d/time.Second), maxVisibilitySeconds)
	_, err := c.api.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(c.cfg.QueueURL), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: secs,
	})
	return err
}

func (c *Consumer) refreshDLQDepth(ctx context.Context) {
	callCtx, cancel := context.WithTimeout(ctx, c.cfg.CallTimeout)
	defer cancel()
	out, err := c.api.GetQueueAttributes(callCtx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(c.cfg.DLQURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
	})
	if err != nil {
		if ctx.Err() == nil {
			c.logger.Debug("não foi possível ler a profundidade da DLQ", logging.Err(err))
		}
		return
	}
	n, err := strconv.ParseInt(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)], 10, 64)
	if err == nil {
		c.metrics.SetDLQDepth(c.dlqName, n)
	}
}

func (c *Consumer) recordResult(cmd app.ProcessWagerCommand, res app.WagerResult, receiveCount int32) {
	if c.metrics == nil {
		return
	}
	if !res.IdempotentReplay {
		c.metrics.TransactionResult(metrics.SourceSQS, cmd.Kind.String(), res.Status.String())
		return
	}
	// Replay numa reentrega (receiveCount > 1) é, em regra, a inbox
	// absorvendo a mesma mensagem; na primeira entrega é a mesma operação
	// já feita por outra mensagem ou pelo HTTP.
	kind := metrics.DuplicateReplay
	if receiveCount > 1 {
		kind = metrics.DuplicateInbox
	}
	c.metrics.Duplicate(metrics.SourceSQS, kind)
}

func (c *Consumer) observe(outcome string, start time.Time) {
	if c.metrics != nil {
		c.metrics.ObserveProcessing(metrics.SourceSQS, metrics.OperationSQSConsume, outcome, time.Since(start))
	}
}

// msgAttrs monta os atributos de log da mensagem. Nunca inclui o corpo.
func (c *Consumer) msgAttrs(m types.Message, messageID string, extra ...any) []any {
	attrs := []any{slog.String("sqsMessageId", aws.ToString(m.MessageId))}
	if messageID != "" {
		attrs = append(attrs, logging.MessageID(messageID), logging.CorrelationID(messageID))
	}
	return append(attrs, extra...)
}

func receiveCountOf(m types.Message) int32 {
	n, err := strconv.ParseInt(m.Attributes[attrReceiveCount], 10, 32)
	if err != nil || n < 1 {
		return 1
	}
	return int32(n)
}
