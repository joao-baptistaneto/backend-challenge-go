package pendingworker

import (
	"log/slog"

	"go.uber.org/fx"

	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

// ConfigFrom monta a configuração do worker a partir da configuração da
// aplicação.
func ConfigFrom(c config.Config) Config {
	p := c.Workers.PendingWorker
	return Config{
		PollInterval: p.PollInterval, BatchSize: p.BatchSize, Concurrency: p.Concurrency,
		Lease: p.Lease, ProcessTimeout: p.ProcessTimeout, DrainTimeout: p.DrainTimeout,
		OrphanGrace: p.OrphanGrace,
	}
}

// PolicyFrom monta a política de referências pendentes (limite, TTL e
// backoff) a partir da configuração da aplicação.
func PolicyFrom(c config.Config) app.PendingWagerPolicy {
	return app.PendingWagerPolicy{
		MaxAttempts:       c.Workers.PendingReferenceMaxAttempts,
		TTL:               c.Workers.PendingReferenceTTL,
		RetryBaseDelay:    c.Workers.PendingReferenceRetryBaseDelay,
		RetryMaxDelay:     c.Workers.PendingReferenceRetryMaxDelay,
		MaxResumeFailures: c.Workers.PendingWorker.MaxFailures,
	}
}

// ResumerParams são as dependências Fx do caso de uso de retomada.
type ResumerParams struct {
	fx.In

	Config config.Config
	UoW    app.UnitOfWork
	Logger *slog.Logger `optional:"true"`
}

// NewResumerFx cria o *app.ResumeWagerTransaction com a política da config.
func NewResumerFx(p ResumerParams) (*app.ResumeWagerTransaction, error) {
	return app.NewResumeWagerTransaction(p.UoW, PolicyFrom(p.Config), app.WithLogger(p.Logger))
}

// WorkerParams são as dependências Fx do worker.
type WorkerParams struct {
	fx.In

	Lifecycle fx.Lifecycle
	Config    config.Config
	Resumer   Resumer
	Metrics   *metrics.Metrics `optional:"true"`
	Logger    *slog.Logger     `optional:"true"`
}

// NewWorkerFx cria o worker e registra Start/Stop no ciclo de vida. Como
// depende do caso de uso (e, por ele, do pool), o Fx o para antes do pool.
func NewWorkerFx(p WorkerParams) (*Worker, error) {
	w, err := New(ConfigFrom(p.Config), p.Resumer, WithLogger(p.Logger), WithMetrics(p.Metrics))
	if err != nil {
		return nil, err
	}
	p.Lifecycle.Append(fx.Hook{OnStart: w.Start, OnStop: w.Stop})
	return w, nil
}

// Module provê *app.ResumeWagerTransaction (como Resumer) e o *Worker com seu
// ciclo de vida, e o instancia (fx.Invoke). Depende de config.Config e
// app.UnitOfWork (postgres.Module); métricas e logger são opcionais.
var Module = fx.Module("pending-worker",
	fx.Provide(
		NewResumerFx,
		func(r *app.ResumeWagerTransaction) Resumer { return r },
		NewWorkerFx,
	),
	fx.Invoke(func(*Worker) {}),
)
