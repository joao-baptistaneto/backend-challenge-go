package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
	"github.com/junglegaming/backend-challenge-go/internal/platform/logging"
	"github.com/junglegaming/backend-challenge-go/internal/platform/startbudget"
)

// Pool pgx (ARCHITECTURE.md, ADR-013).
//
// Decisões:
//
//   - NewDB só monta o pool (pgxpool.NewWithConfig não abre conexões quando
//     MinConns = 0); a primeira conexão é aberta em Start (OnStart do Fx),
//     com ping repetido e backoff limitado pelo ctx do start menos a reserva
//     de rollback (startbudget, ADR-025). Assim fx.New
//     continua sem I/O e a aplicação não fica "pronta" sem banco.
//   - O DSN só sai de config.Secret via Reveal, uma única vez, direto para
//     pgxpool.ParseConfig. Erros de parse NÃO são propagados textualmente: o
//     erro do pgx pode citar o DSN (url.Error inclui a URL inteira).
//   - Start só repete falhas transitórias (conexão recusada, 57P03 "starting
//     up", timeouts). Erros permanentes (28P01 senha, 3D000 banco inexistente)
//     falham imediatamente, com mensagem acionável.
//   - Stop fecha o pool (aguarda conexões emprestadas voltarem) limitado pelo
//     ctx do stop. No Fx, o pool é o primeiro a iniciar e o último a parar
//     entre seus dependentes, então servidor e workers já liberaram as
//     conexões quando Stop roda.

// Parâmetros de start.
const (
	startInitialBackoff = 100 * time.Millisecond
	startMaxBackoff     = 2 * time.Second
	applicationName     = "wallet"
)

// ErrPoolConfig indica DATABASE_URL/parâmetros que o pgx não aceitou.
var ErrPoolConfig = errors.New("postgres: configuração do pool inválida (detalhes omitidos por conter credenciais)")

// DB encapsula o pool de conexões.
type DB struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
	// startReserve é a parte final do prazo do start reservada ao rollback
	// do Fx (ADR-025): Start desiste antes dela.
	startReserve startbudget.Reserve
	// sleep é substituível em testes.
	sleep func(ctx context.Context, d time.Duration) error
}

// PoolConfig converte DatabaseConfig em *pgxpool.Config.
func PoolConfig(cfg config.DatabaseConfig) (*pgxpool.Config, error) {
	pc, err := pgxpool.ParseConfig(cfg.URL.Reveal())
	if err != nil {
		return nil, ErrPoolConfig
	}
	if cfg.MaxConns > 0 {
		pc.MaxConns = cfg.MaxConns
	}
	if cfg.ConnectTimeout > 0 {
		pc.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	}
	if _, ok := pc.ConnConfig.RuntimeParams["application_name"]; !ok {
		pc.ConnConfig.RuntimeParams["application_name"] = applicationName
	}
	return pc, nil
}

// NewDB cria o pool sem abrir conexões. logger pode ser nil.
func NewDB(cfg config.DatabaseConfig, logger *slog.Logger) (*DB, error) {
	pc, err := PoolConfig(cfg)
	if err != nil {
		return nil, err
	}
	return newDBFromConfig(pc, logger)
}

func newDBFromConfig(pc *pgxpool.Config, logger *slog.Logger) (*DB, error) {
	pool, err := pgxpool.NewWithConfig(context.Background(), pc)
	if err != nil {
		return nil, ErrPoolConfig
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &DB{pool: pool, logger: logger.With(slog.String("component", "postgres")), sleep: sleepCtx}, nil
}

// Pool devolve o pool subjacente (leituras fora de transação, coletores).
func (db *DB) Pool() *pgxpool.Pool { return db.pool }

// Ping verifica se uma conexão do pool responde, limitado por ctx. É o
// probe de readiness do PostgreSQL (/health/ready, ADR-020): quem chama deve
// passar um ctx com prazo curto.
func (db *DB) Ping(ctx context.Context) error {
	if err := db.pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres: ping: %w", err)
	}
	return nil
}

// SetStartReserve define quanto do final do prazo do ctx de Start fica
// reservado (ADR-025): Start repete só até prazo(ctx) − r, para que o Fx
// ainda tenha tempo de executar o rollback se o start falhar. Chame antes
// de Start.
func (db *DB) SetStartReserve(r startbudget.Reserve) { db.startReserve = r }

// Start faz ping até ter sucesso, até um erro permanente ou até o orçamento
// do start (prazo do ctx menos a reserva de rollback) expirar.
func (db *DB) Start(ctx context.Context) error {
	ctx, cancel := startbudget.Context(ctx, db.startReserve)
	defer cancel()
	backoff := startInitialBackoff
	for attempt := 1; ; attempt++ {
		err := db.Ping(ctx)
		if err == nil {
			db.logger.InfoContext(ctx, "conectado ao PostgreSQL",
				slog.Int("attempts", attempt), slog.Int("maxConns", int(db.pool.Config().MaxConns)))
			return nil
		}
		if !IsTransient(err) || ctx.Err() != nil {
			return fmt.Errorf("postgres: start após %d tentativa(s): %w", attempt, err)
		}
		db.logger.WarnContext(ctx, "PostgreSQL indisponível; nova tentativa",
			slog.Int("attempt", attempt), slog.Duration("backoff", backoff), logging.Err(err))
		if serr := db.sleep(ctx, backoff); serr != nil {
			return fmt.Errorf("postgres: start após %d tentativa(s): %w", attempt, errors.Join(err, serr))
		}
		backoff = min(backoff*2, startMaxBackoff)
	}
}

// Stop fecha o pool. pgxpool.Close bloqueia até as conexões emprestadas
// voltarem; se ctx expirar antes, Stop devolve o erro do ctx (o fechamento
// continua em segundo plano e termina quando as conexões forem devolvidas).
func (db *DB) Stop(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		db.pool.Close()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("postgres: close: %w", ctx.Err())
	}
}
