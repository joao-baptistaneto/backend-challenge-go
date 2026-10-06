package postgres

import (
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

// Params são as dependências de newDBWithLifecycle.
type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Config    config.Config
	Logger    *slog.Logger `optional:"true"`
}

// newDBWithLifecycle cria o DB e registra Start (ping com retry) e Stop
// (Close) no ciclo de vida do Fx.
func newDBWithLifecycle(p Params) (*DB, error) {
	db, err := NewDB(p.Config.Database, p.Logger)
	if err != nil {
		return nil, err
	}
	p.Lifecycle.Append(fx.Hook{OnStart: db.Start, OnStop: db.Stop})
	return db, nil
}

// TxManagerParams são as dependências de newTxManagerFx.
type TxManagerParams struct {
	fx.In

	DB      *DB
	Logger  *slog.Logger     `optional:"true"`
	Metrics *metrics.Metrics `optional:"true"`
}

func newTxManagerFx(p TxManagerParams) *TxManager {
	return NewTxManager(p.DB, p.Logger, p.Metrics)
}

// Module provê:
//   - *DB (pool com OnStart = ping com retry/backoff e OnStop = Close; Ping
//     para readiness);
//   - *pgxpool.Pool (para repositórios que leem fora de transação);
//   - *TxManager e Transactor (interface consumida pelos casos de uso);
//   - *WalletRepository, *LedgerRepository, *WagerTransactionRepository,
//     *InboxRepository e *OutboxRepository
//     (stateless; recebem o Querier/pgx.Tx em cada chamada).
//
// Depende de config.Config; *slog.Logger e *metrics.Metrics são opcionais.
var Module = fx.Module("postgres",
	fx.Provide(
		newDBWithLifecycle,
		func(db *DB) *pgxpool.Pool { return db.Pool() },
		fx.Annotate(newTxManagerFx, fx.As(fx.Self()), fx.As(new(Transactor))),
		NewWalletRepository,
		NewLedgerRepository,
		NewWagerTransactionRepository,
		NewInboxRepository,
		NewOutboxRepository,
	),
)
