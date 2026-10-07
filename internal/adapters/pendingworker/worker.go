// Package pendingworker é o adapter de entrada temporizado que retoma
// transações pendentes: referências ainda indisponíveis (PENDING_REFERENCE) e
// PENDING órfãos (ARCHITECTURE.md, ADR-023, T21).
//
// Decisões:
//
//   - O worker só orquestra: a cada ciclo gera um token, reivindica um lote
//     (app.ResumeWagerTransaction.Claim, lease persistido na linha) e reaplica
//     cada item (Resume) até Concurrency em paralelo. Toda a regra (lock da
//     carteira, decisão, backoff, expiração, eventos) está no caso de uso, então
//     o comportamento é o mesmo em qualquer instância e após reinício: não há
//     estado em memória que precise sobreviver.
//   - Lote cheio → novo ciclo imediato; senão espera PollInterval.
//   - Um item cujo lease já não cobre um processamento completo
//     (ProcessTimeout) não é iniciado: volta para outra instância quando o
//     lease expirar, em vez de ser processado em cima da expiração.
//   - Falhas de um item (transitórias ou não) não derrubam o ciclo: o lease
//     continua gravado e o item volta quando ele expirar. Falha no claim
//     (banco indisponível) espera PollInterval e tenta de novo.
//   - Shutdown (Stop): para de reivindicar e de iniciar itens; os itens em voo
//     terminam até DrainTimeout (ou o prazo do OnStop). Estourado, o contexto
//     do processamento é cancelado (rollback; o lease expira e outra instância
//     retoma) e Stop devolve ErrDrainTimeout. Stop sempre espera as goroutines
//     do worker, salvo se o próprio ctx do OnStop vencer.
package pendingworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/platform/failpoint"
	"github.com/junglegaming/backend-challenge-go/internal/platform/logging"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

// ErrDrainTimeout indica que itens em voo foram cancelados no shutdown.
var ErrDrainTimeout = errors.New("pendingworker: prazo de drenagem estourado; itens em voo cancelados")

// Resumer é o caso de uso consumido (app.ResumeWagerTransaction).
type Resumer interface {
	Claim(ctx context.Context, req app.ClaimRequest) ([]app.ClaimedWager, error)
	Resume(ctx context.Context, id, token uuid.UUID) (app.ResumeResult, error)
}

var _ Resumer = (*app.ResumeWagerTransaction)(nil)

// Config são os parâmetros do worker (config.PendingWorkerConfig).
type Config struct {
	PollInterval   time.Duration
	BatchSize      int
	Concurrency    int
	Lease          time.Duration
	ProcessTimeout time.Duration
	DrainTimeout   time.Duration
	OrphanGrace    time.Duration
}

// Validate valida a configuração.
func (c Config) Validate() error {
	switch {
	case c.PollInterval <= 0:
		return fmt.Errorf("pendingworker: poll interval %v must be positive", c.PollInterval)
	case c.BatchSize < 1:
		return fmt.Errorf("pendingworker: batch size %d must be >= 1", c.BatchSize)
	case c.Concurrency < 1:
		return fmt.Errorf("pendingworker: concurrency %d must be >= 1", c.Concurrency)
	case c.ProcessTimeout <= 0 || c.Lease <= c.ProcessTimeout:
		return fmt.Errorf("pendingworker: process timeout %v must be positive and below lease %v", c.ProcessTimeout, c.Lease)
	case c.DrainTimeout <= 0:
		return fmt.Errorf("pendingworker: drain timeout %v must be positive", c.DrainTimeout)
	case c.OrphanGrace < 0:
		return fmt.Errorf("pendingworker: orphan grace %v must not be negative", c.OrphanGrace)
	}
	return nil
}

// Option configura o Worker.
type Option func(*Worker)

// WithLogger define o logger (nil = descarta).
func WithLogger(l *slog.Logger) Option { return func(w *Worker) { w.logger = l } }

// WithMetrics define as métricas (nil = sem métricas).
func WithMetrics(m *metrics.Metrics) Option { return func(w *Worker) { w.metrics = m } }

// WithClock substitui o relógio usado para o prazo do lease (testes).
func WithClock(now func() time.Time) Option { return func(w *Worker) { w.now = now } }

// Worker retoma transações pendentes em ciclos.
type Worker struct {
	cfg     Config
	uc      Resumer
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

	inFlight atomic.Int64
}

// New cria o worker.
func New(cfg Config, uc Resumer, opts ...Option) (*Worker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if uc == nil {
		return nil, errors.New("pendingworker: nil resumer")
	}
	w := &Worker{cfg: cfg, uc: uc, now: time.Now}
	for _, o := range opts {
		o(w)
	}
	if w.logger == nil {
		w.logger = slog.New(slog.DiscardHandler)
	}
	w.logger = w.logger.With(slog.String("component", "pendingworker"))
	return w, nil
}

// Start inicia o laço em background. Não bloqueia.
func (w *Worker) Start(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started {
		return errors.New("pendingworker: já iniciado")
	}
	loopCtx, loopCancel := context.WithCancel(context.Background())
	procCtx, procCancel := context.WithCancel(context.Background())
	w.loopCancel, w.procCtx, w.procCancel = loopCancel, procCtx, procCancel
	w.loopDone = make(chan struct{})
	w.started = true
	go w.loop(loopCtx, w.loopDone)
	w.logger.InfoContext(ctx, "worker de pendências iniciado",
		slog.Int("batchSize", w.cfg.BatchSize), slog.Int("concurrency", w.cfg.Concurrency),
		slog.Duration("lease", w.cfg.Lease), slog.Duration("pollInterval", w.cfg.PollInterval))
	return nil
}

// Stop para de reivindicar, drena os itens em voo e espera as goroutines.
func (w *Worker) Stop(ctx context.Context) error {
	w.mu.Lock()
	if !w.started || w.stopped {
		w.mu.Unlock()
		return nil
	}
	w.stopped = true
	w.mu.Unlock()

	w.loopCancel()
	drainCtx, cancel := context.WithTimeout(ctx, w.cfg.DrainTimeout)
	defer cancel()
	var result error
	select {
	case <-w.loopDone:
	case <-drainCtx.Done():
		w.logger.WarnContext(context.WithoutCancel(ctx), "drenagem do worker de pendências estourou o prazo; cancelando itens em voo",
			slog.Int64("inFlight", w.inFlight.Load()))
		result = ErrDrainTimeout
		w.procCancel()
		select {
		case <-w.loopDone:
		case <-ctx.Done():
			return errors.Join(ErrDrainTimeout, ctx.Err())
		}
	}
	w.procCancel()
	if result == nil {
		w.logger.InfoContext(context.WithoutCancel(ctx), "worker de pendências parado")
	}
	return result
}

// loop executa ciclos até o cancelamento.
func (w *Worker) loop(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	for {
		full, err := w.cycle(ctx)
		if ctx.Err() != nil {
			return
		}
		wait := w.cfg.PollInterval
		if err == nil && full {
			continue // ainda há trabalho vencido
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// RunOnce executa um ciclo (claim + processamento do lote) e devolve quantos
// itens foram reivindicados. Usado por testes e ferramentas; o laço do Start
// chama o mesmo código.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	w.mu.Lock()
	procCtx := w.procCtx
	w.mu.Unlock()
	if procCtx == nil {
		procCtx = ctx
	}
	n, _, err := w.runCycle(ctx, procCtx)
	return n, err
}

func (w *Worker) cycle(ctx context.Context) (bool, error) {
	_, full, err := w.runCycle(ctx, w.procCtx)
	return full, err
}

// runCycle reivindica um lote com ctx (cancelado no Stop: para de reivindicar e
// de iniciar itens) e processa os itens com procCtx (só cancelado quando a
// drenagem estoura).
func (w *Worker) runCycle(ctx, procCtx context.Context) (int, bool, error) {
	token, err := uuid.NewV7()
	if err != nil {
		return 0, false, err
	}
	claimedAt := w.now()
	claimed, err := w.uc.Claim(ctx, app.ClaimRequest{
		Token: token, Lease: w.cfg.Lease, Limit: w.cfg.BatchSize, OrphanGrace: w.cfg.OrphanGrace,
	})
	if err != nil {
		if ctx.Err() == nil {
			w.logger.WarnContext(ctx, "falha ao reivindicar pendências; tentando no próximo ciclo", logging.Err(err))
		}
		return 0, false, err
	}
	if len(claimed) == 0 {
		return 0, false, nil
	}
	// Teste de recuperação (T26, ADR-028): crash com o lote reivindicado
	// (lease gravado) e nenhuma retomada iniciada. No build normal é vazia.
	failpoint.Hit(failpoint.PendingAfterClaim)
	// Prazo para iniciar um item: o lease precisa cobrir um processamento
	// completo depois do início.
	startBy := claimedAt.Add(w.cfg.Lease - w.cfg.ProcessTimeout)

	sem := make(chan struct{}, w.cfg.Concurrency)
	var wg sync.WaitGroup
dispatch:
	for _, c := range claimed {
		select {
		case <-ctx.Done():
			break dispatch
		case sem <- struct{}{}:
		}
		if ctx.Err() != nil {
			<-sem
			break
		}
		if w.now().After(startBy) {
			<-sem
			w.logger.WarnContext(ctx, "lease do lote perto do fim; item devolvido para nova reivindicação",
				slog.String("transactionId", c.ID.String()))
			continue
		}
		wg.Add(1)
		w.inFlight.Add(1)
		go func(c app.ClaimedWager) {
			defer func() {
				w.inFlight.Add(-1)
				<-sem
				wg.Done()
			}()
			w.process(procCtx, c, token)
		}(c)
	}
	wg.Wait()
	return len(claimed), len(claimed) == w.cfg.BatchSize, nil
}

func (w *Worker) process(ctx context.Context, c app.ClaimedWager, token uuid.UUID) {
	ctx, cancel := context.WithTimeout(ctx, w.cfg.ProcessTimeout)
	defer cancel()
	start := time.Now()
	res, err := w.uc.Resume(ctx, c.ID, token)
	elapsed := time.Since(start)
	if err != nil {
		outcome := metrics.OutcomeError
		if errors.Is(err, app.ErrUnavailable) || errors.Is(err, context.DeadlineExceeded) {
			outcome = metrics.OutcomeTransient
		}
		w.observe(outcome, elapsed)
		w.logger.WarnContext(ctx, "retomada falhou; o item volta quando o lease expirar",
			slog.String("transactionId", c.ID.String()), slog.String("status", string(c.Status)), logging.Err(err))
		return
	}
	switch res.Outcome {
	case app.ResumeResolved, app.ResumeExpired:
		outcome := metrics.OutcomeSuccess
		if res.Status == domain.WagerStatusRejected {
			outcome = metrics.OutcomeRejected
		}
		w.observe(outcome, elapsed)
		if w.metrics != nil {
			w.metrics.TransactionResult(metrics.SourceWorker, res.Kind.String(), res.Status.String())
		}
	case app.ResumeRescheduled:
		w.observe(metrics.OutcomeSuccess, elapsed)
		if w.metrics != nil && res.PreviousStatus != res.Status {
			// PENDING órfão que passou a aguardar a referência.
			w.metrics.TransactionResult(metrics.SourceWorker, res.Kind.String(), res.Status.String())
		}
	}
}

func (w *Worker) observe(outcome string, d time.Duration) {
	if w.metrics != nil {
		w.metrics.ObserveProcessing(metrics.SourceWorker, metrics.OperationPendingReference, outcome, d)
	}
}
