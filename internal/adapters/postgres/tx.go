package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/junglegaming/backend-challenge-go/internal/platform/logging"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

// Gerenciador de transações (ARCHITECTURE.md, ADR-013).
//
// Decisões:
//
//   - A fronteira transacional é explícita e pertence ao caso de uso: ele
//     chama WithinTx e passa o pgx.Tx recebido para cada repositório (T12/T13),
//     que aceitam um Querier. Não há transação implícita no context: o tx é um
//     parâmetro visível, o que torna auditável quais comandos SQL participam do
//     mesmo commit (wallet + transação + ledger + outbox + inbox).
//   - Isolamento padrão READ COMMITTED. A consistência por carteira vem do
//     lock pessimista (SELECT ... FOR UPDATE) somado ao UPDATE ... WHERE
//     version = $old (T12), não do nível de isolamento. SERIALIZABLE e
//     REPEATABLE READ (reconciliação, T16) ficam disponíveis por opção.
//   - Rollback em qualquer erro de fn, em panic e em runtime.Goexit
//     (t.FailNow dentro de fn): o defer não depende de recover, então o panic
//     original continua propagando sem ser engolido e a conexão volta ao pool.
//     O rollback usa um contexto derivado SEM cancelamento e com prazo curto,
//     para funcionar mesmo quando o ctx da requisição já foi cancelado.
//   - Retry: o MECANISMO fica aqui (é preciso reexecutar BEGIN..COMMIT
//     inteiro, e só este componente sabe se o commit foi ou não enviado), mas
//     ele é OPT-IN por chamada (WithRetry). Quem decide usar é o caso de uso
//     (T15/T14/T21), porque só ele sabe que fn é reexecutável: fn não pode ter
//     efeitos fora do banco (HTTP, SQS, mutação de estado em memória
//     compartilhado) e deve recalcular tudo a partir do que lê no tx.
//   - Só se reexecuta quando é garantido que nada foi confirmado
//     (IsRetryable): erro do servidor (SQLSTATE 40001/40P01/55P03/57014/08xxx)
//     durante fn ou no COMMIT (o servidor respondeu, logo não confirmou), ou
//     falha de conexão antes do COMMIT ser enviado. Commit ambíguo (COMMIT
//     enviado sem resposta) vira ErrCommitOutcomeUnknown e NUNCA é
//     reexecutado aqui; quem chama resolve via idempotência (replay).
//   - Tentativas limitadas (MaxAttempts) com backoff exponencial e jitter
//     ("equal jitter": metade fixa + metade aleatória), interrompido pelo ctx.
//     Cada nova tentativa incrementa wallet_retries_total{operation} e, para
//     conflitos de concorrência, wallet_concurrency_conflicts_total{kind}.

// Querier é o subconjunto comum a pgx.Tx, *pgx.Conn e *pgxpool.Pool usado
// pelos repositórios. Recebê-lo (em vez de *pgxpool.Pool) permite que o
// mesmo repositório rode dentro de WithinTx ou, para leituras isoladas, no
// pool.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// TxFunc é o corpo de uma transação. Deve usar apenas tx (e ctx) para acessar
// o banco. Se a chamada usar WithRetry, fn pode ser executada mais de uma vez
// e não pode ter efeitos colaterais fora do banco.
type TxFunc func(ctx context.Context, tx pgx.Tx) error

// Transactor é a interface pequena consumida pelos casos de uso.
type Transactor interface {
	WithinTx(ctx context.Context, fn TxFunc, opts ...TxOption) error
}

// beginner abstrai pool.BeginTx (permite testar o ciclo de vida da transação
// sem banco).
type beginner interface {
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
}

// RetryPolicy configura o retry opt-in de WithinTx.
type RetryPolicy struct {
	// MaxAttempts é o total de execuções, incluindo a primeira (<= 1 desliga).
	MaxAttempts int
	// BaseDelay é o atraso antes da 2ª tentativa; dobra a cada tentativa.
	BaseDelay time.Duration
	// MaxDelay limita o atraso de uma tentativa.
	MaxDelay time.Duration
	// Operation é o rótulo de métricas (metrics.Operation*).
	Operation string
	// RetryIf, se não nil, amplia os erros retentáveis além de IsRetryable.
	// Uso típico (T15): corrida resolvida por UNIQUE de idempotência, em que
	// a nova tentativa encontra a linha do vencedor e devolve o replay. Nunca
	// é consultado para ErrCommitOutcomeUnknown.
	RetryIf func(error) bool
}

// DefaultRetryPolicy devolve a política recomendada para casos de uso
// financeiros: 4 execuções no total, 10ms → 20ms → 40ms (+ jitter), teto de
// 250ms. Curta de propósito: conflitos por carteira se resolvem em
// milissegundos e o retry externo (HTTP do provedor, reentrega SQS) cobre o
// resto.
func DefaultRetryPolicy(operation string) RetryPolicy {
	return RetryPolicy{MaxAttempts: 4, BaseDelay: 10 * time.Millisecond, MaxDelay: 250 * time.Millisecond, Operation: operation}
}

// TxOption altera uma chamada de WithinTx.
type TxOption func(*txConfig)

type txConfig struct {
	opts  pgx.TxOptions
	retry RetryPolicy
}

// WithIsolation define o nível de isolamento (padrão READ COMMITTED).
func WithIsolation(level pgx.TxIsoLevel) TxOption {
	return func(c *txConfig) { c.opts.IsoLevel = level }
}

// ReadOnly abre a transação como READ ONLY.
func ReadOnly() TxOption {
	return func(c *txConfig) { c.opts.AccessMode = pgx.ReadOnly }
}

// WithRetry habilita o retry limitado (ver RetryPolicy e as decisões acima).
func WithRetry(p RetryPolicy) TxOption {
	return func(c *txConfig) { c.retry = p }
}

// rollbackTimeout limita o ROLLBACK, que roda desacoplado do ctx da chamada.
const rollbackTimeout = 5 * time.Second

// TxManager executa funções em transações do pool.
type TxManager struct {
	db      beginner
	logger  *slog.Logger
	metrics *metrics.Metrics
	// sleep é substituível em testes; respeita ctx.
	sleep func(ctx context.Context, d time.Duration) error
}

var _ Transactor = (*TxManager)(nil)

// NewTxManager cria o gerenciador. logger e m podem ser nil.
func NewTxManager(db *DB, logger *slog.Logger, m *metrics.Metrics) *TxManager {
	return newTxManager(db.Pool(), logger, m)
}

func newTxManager(b beginner, logger *slog.Logger, m *metrics.Metrics) *TxManager {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &TxManager{db: b, logger: logger.With(slog.String("component", "postgres.tx")), metrics: m, sleep: sleepCtx}
}

// WithinTx executa fn numa transação: COMMIT se fn devolver nil, ROLLBACK
// se fn devolver erro, entrar em panic ou encerrar a goroutine. O erro de fn é
// devolvido sem tradução (para que erros de domínio passem intactos); erros
// de BEGIN/COMMIT são embrulhados com contexto e, no commit ambíguo, com
// ErrCommitOutcomeUnknown. Use Translate/Is*Violation/IsTransient no erro
// devolvido.
func (m *TxManager) WithinTx(ctx context.Context, fn TxFunc, opts ...TxOption) error {
	cfg := txConfig{opts: pgx.TxOptions{IsoLevel: pgx.ReadCommitted}}
	for _, o := range opts {
		o(&cfg)
	}
	maxAttempts := max(cfg.retry.MaxAttempts, 1)

	for attempt := 1; ; attempt++ {
		err := m.runOnce(ctx, cfg.opts, fn)
		if err == nil {
			return nil
		}
		if attempt >= maxAttempts || !cfg.shouldRetry(err) {
			if attempt > 1 {
				return fmt.Errorf("postgres: transação falhou após %d tentativas: %w", attempt, err)
			}
			return err
		}
		if ctx.Err() != nil {
			return err
		}
		m.recordRetry(ctx, cfg.retry, attempt, err)
		if serr := m.sleep(ctx, cfg.retry.backoff(attempt)); serr != nil {
			// ctx encerrado durante a espera: devolve a falha real; o erro
			// de contexto fica na cadeia para errors.Is.
			return errors.Join(err, serr)
		}
	}
}

func (c txConfig) shouldRetry(err error) bool {
	if errors.Is(err, ErrCommitOutcomeUnknown) || errors.Is(err, context.Canceled) {
		return false
	}
	if IsRetryable(err) {
		return true
	}
	return c.retry.RetryIf != nil && c.retry.RetryIf(err)
}

// backoff devolve o atraso antes da tentativa attempt+1 (equal jitter).
func (p RetryPolicy) backoff(attempt int) time.Duration {
	base := p.BaseDelay
	if base <= 0 {
		return 0
	}
	d := base
	for i := 1; i < attempt && (p.MaxDelay <= 0 || d < p.MaxDelay); i++ {
		d *= 2
	}
	if p.MaxDelay > 0 && d > p.MaxDelay {
		d = p.MaxDelay
	}
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(d-half)+1))
}

// runOnce executa uma tentativa completa (BEGIN, fn, COMMIT/ROLLBACK).
func (m *TxManager) runOnce(ctx context.Context, opts pgx.TxOptions, fn TxFunc) error {
	tx, err := m.db.BeginTx(ctx, opts)
	if err != nil {
		// Nada foi executado: a classificação de IsRetryable vale como está.
		return fmt.Errorf("postgres: begin: %w", err)
	}

	finished := false
	defer func() {
		if !finished {
			// panic ou runtime.Goexit dentro de fn: desfaz e deixa o panic
			// seguir (sem recover).
			m.rollback(ctx, tx, "panic")
		}
	}()

	if ferr := fn(ctx, tx); ferr != nil {
		finished = true
		m.rollback(ctx, tx, "error")
		return ferr
	}
	finished = true
	if cerr := tx.Commit(ctx); cerr != nil {
		return commitError(cerr)
	}
	return nil
}

// commitError separa falha definitiva de commit ambíguo.
func commitError(err error) error {
	var pgErr *pgconn.PgError
	switch {
	case errors.As(err, &pgErr):
		// O servidor respondeu ao COMMIT com erro (ex.: 40001 em
		// SERIALIZABLE, FK DEFERRABLE): a transação foi desfeita.
		return fmt.Errorf("postgres: commit: %w", err)
	case errors.Is(err, pgx.ErrTxCommitRollback):
		// Transação já abortada (fn engoliu um erro SQL): nada confirmado.
		return fmt.Errorf("postgres: commit: %w", err)
	case pgconn.SafeToRetry(err):
		// O COMMIT nem chegou a ser enviado (ex.: ctx já encerrado).
		return fmt.Errorf("postgres: commit não enviado: %w", err)
	default:
		return fmt.Errorf("%w: %w", ErrCommitOutcomeUnknown, err)
	}
}

func (m *TxManager) rollback(ctx context.Context, tx pgx.Tx, cause string) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
	defer cancel()
	if err := tx.Rollback(rctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		// O pgx descarta a conexão quando o ROLLBACK falha; o servidor
		// desfaz a transação ao perder a sessão. Só registramos.
		m.logger.WarnContext(ctx, "falha no rollback; conexão descartada",
			slog.String("cause", cause), logging.Err(err))
	}
}

func (m *TxManager) recordRetry(ctx context.Context, p RetryPolicy, attempt int, err error) {
	reason, _ := TransientReasonOf(err)
	if reason == "" && IsUniqueViolation(err, "") {
		reason = TransientReason(metrics.ConflictUniqueViolation)
	}
	if m.metrics != nil {
		m.metrics.Retry(p.Operation)
		switch reason {
		case ReasonSerialization:
			m.metrics.ConcurrencyConflict(metrics.ConflictSerialization)
		case ReasonDeadlock:
			m.metrics.ConcurrencyConflict(metrics.ConflictDeadlock)
		case ReasonLockTimeout:
			m.metrics.ConcurrencyConflict(metrics.ConflictLockTimeout)
		case TransientReason(metrics.ConflictUniqueViolation):
			m.metrics.ConcurrencyConflict(metrics.ConflictUniqueViolation)
		}
	}
	m.logger.WarnContext(ctx, "transação será repetida",
		slog.String("operation", p.Operation),
		slog.Int("attempt", attempt),
		slog.String("reason", string(reason)),
		slog.String("sqlstate", SQLState(err)),
		logging.Err(err),
	)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
