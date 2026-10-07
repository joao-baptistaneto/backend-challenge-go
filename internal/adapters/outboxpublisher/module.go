package outboxpublisher

import (
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/adapters/sqs"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

// ConfigFrom monta a configuração do publisher a partir da configuração da
// aplicação.
func ConfigFrom(c config.Config) Config {
	w := c.Workers
	return Config{
		QueueURL: c.SQS.EventsQueueURL, PollInterval: w.OutboxPollInterval, BatchSize: w.OutboxBatchSize,
		Concurrency: w.OutboxConcurrency, Lease: w.OutboxLease, PublishTimeout: w.OutboxPublishTimeout,
		DrainTimeout: w.OutboxDrainTimeout,
	}
}

// BackoffFrom monta a política de backoff persistido da configuração.
func BackoffFrom(c config.Config) postgres.OutboxBackoff {
	return postgres.OutboxBackoff{Base: c.Workers.OutboxRetryBaseDelay, Max: c.Workers.OutboxRetryMaxDelay}
}

// StoreParams são as dependências Fx do store.
type StoreParams struct {
	fx.In

	Config config.Config
	Pool   *pgxpool.Pool
	Repo   *postgres.OutboxRepository
}

// NewStoreFx cria o PostgresStore sobre o pool (autocommit).
func NewStoreFx(p StoreParams) (*PostgresStore, error) {
	return NewPostgresStore(p.Pool, p.Repo, BackoffFrom(p.Config))
}

// PublisherParams são as dependências Fx do publisher.
type PublisherParams struct {
	fx.In

	Lifecycle fx.Lifecycle
	Config    config.Config
	Store     Store
	Sender    Sender
	Metrics   *metrics.Metrics `optional:"true"`
	Logger    *slog.Logger     `optional:"true"`
}

// NewPublisherFx cria o publisher e registra Start/Stop no ciclo de vida.
// Como depende do Store (pool) e do Sender (cliente SQS), construídos antes,
// o Fx o para antes de fechar o pool e o cliente.
func NewPublisherFx(p PublisherParams) (*Publisher, error) {
	pub, err := New(ConfigFrom(p.Config), p.Store, p.Sender, WithLogger(p.Logger), WithMetrics(p.Metrics))
	if err != nil {
		return nil, err
	}
	p.Lifecycle.Append(fx.Hook{OnStart: pub.Start, OnStop: pub.Stop})
	return pub, nil
}

// Module provê o Store (PostgresStore sobre o pool), o Sender (sqs.API) e o
// *Publisher com seu ciclo de vida, e o instancia (fx.Invoke). Depende de
// config.Config, de postgres.Module (*pgxpool.Pool, *OutboxRepository) e de
// sqs.ClientModule (sqs.API). Métricas e logger são opcionais.
var Module = fx.Module("outbox-publisher",
	fx.Provide(
		NewStoreFx,
		func(s *PostgresStore) Store { return s },
		func(api sqs.API) Sender { return api },
		NewPublisherFx,
	),
	fx.Invoke(func(*Publisher) {}),
)
