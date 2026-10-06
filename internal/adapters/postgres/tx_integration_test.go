//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/fx"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

func newTxManager(t *testing.T, db *postgres.DB) (*postgres.TxManager, *metrics.Metrics) {
	t.Helper()
	m, err := metrics.New(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	return postgres.NewTxManager(db, nil, m), m
}

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

const insertWalletSQL = `INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at)
VALUES ($1, $2, $3, $4, 1, now(), now())`

func insertWallet(ctx context.Context, q postgres.Querier, id, player uuid.UUID, balance int64) error {
	_, err := q.Exec(ctx, insertWalletSQL, id, player, "BRL", balance)
	return err
}

func walletExists(t *testing.T, db *postgres.DB, id uuid.UUID) bool {
	t.Helper()
	var n int
	if err := db.Pool().QueryRow(t.Context(), `SELECT count(*) FROM wallets WHERE id = $1`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n == 1
}

func walletBalance(t *testing.T, db *postgres.DB, id uuid.UUID) (balance, version int64) {
	t.Helper()
	if err := db.Pool().QueryRow(t.Context(), `SELECT balance, version FROM wallets WHERE id = $1`, id).Scan(&balance, &version); err != nil {
		t.Fatal(err)
	}
	return balance, version
}

// requireNoAcquiredConns garante que nenhuma conexão ficou presa a uma
// transação esquecida.
func requireNoAcquiredConns(t *testing.T, db *postgres.DB) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for db.Pool().Stat().AcquiredConns() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("acquired conns = %d", db.Pool().Stat().AcquiredConns())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestIntegrationCommit(t *testing.T) {
	t.Parallel()
	db := startedDB(t)
	tm, _ := newTxManager(t, db)
	id := uuid.New()
	err := tm.WithinTx(t.Context(), func(ctx context.Context, tx pgx.Tx) error {
		if err := insertWallet(ctx, tx, id, uuid.New(), 100); err != nil {
			return err
		}
		// Visível dentro da transação.
		var n int
		return tx.QueryRow(ctx, `SELECT count(*) FROM wallets WHERE id = $1`, id).Scan(&n)
	})
	if err != nil {
		t.Fatal(err)
	}
	if !walletExists(t, db, id) {
		t.Fatal("committed row not visible")
	}
	requireNoAcquiredConns(t, db)
}

func TestIntegrationRollbackOnError(t *testing.T) {
	t.Parallel()
	db := startedDB(t)
	tm, _ := newTxManager(t, db)
	id := uuid.New()
	sentinel := errors.New("regra de negócio")
	err := tm.WithinTx(t.Context(), func(ctx context.Context, tx pgx.Tx) error {
		if err := insertWallet(ctx, tx, id, uuid.New(), 100); err != nil {
			return err
		}
		return sentinel
	})
	if err != sentinel {
		t.Fatalf("err = %v, want sentinel unchanged", err)
	}
	if walletExists(t, db, id) {
		t.Fatal("row persisted after rollback")
	}
	requireNoAcquiredConns(t, db)
}

func TestIntegrationRollbackOnPanic(t *testing.T) {
	t.Parallel()
	db := startedDB(t)
	tm, _ := newTxManager(t, db)
	id := uuid.New()
	func() {
		defer func() {
			if p := recover(); p != "boom" {
				t.Fatalf("recovered %v", p)
			}
		}()
		_ = tm.WithinTx(t.Context(), func(ctx context.Context, tx pgx.Tx) error {
			if err := insertWallet(ctx, tx, id, uuid.New(), 100); err != nil {
				return err
			}
			panic("boom")
		})
	}()
	if walletExists(t, db, id) {
		t.Fatal("row persisted after panic")
	}
	requireNoAcquiredConns(t, db)
	// O pool continua utilizável.
	if err := tm.WithinTx(t.Context(), func(ctx context.Context, tx pgx.Tx) error {
		return insertWallet(ctx, tx, id, uuid.New(), 1)
	}); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationContextCancellation(t *testing.T) {
	t.Parallel()
	db := startedDB(t)
	tm, _ := newTxManager(t, db)
	id := uuid.New()

	t.Run("during statement", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		started := time.Now()
		go func() {
			time.Sleep(200 * time.Millisecond)
			cancel()
		}()
		err := tm.WithinTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
			if err := insertWallet(ctx, tx, id, uuid.New(), 100); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `SELECT pg_sleep(30)`)
			return err
		}, postgres.WithRetry(postgres.DefaultRetryPolicy(metrics.OperationProcessTransaction)))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		if postgres.IsTransient(err) || postgres.IsRetryable(err) {
			t.Fatalf("caller cancellation classified as transient: %v", err)
		}
		if elapsed := time.Since(started); elapsed > 10*time.Second {
			t.Fatalf("cancellation took %v", elapsed)
		}
	})

	t.Run("deadline during statement", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		err := tm.WithinTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `SELECT pg_sleep(30)`)
			return err
		})
		if !errors.Is(err, context.DeadlineExceeded) || !postgres.IsTransient(err) {
			t.Fatalf("err = %v, want transient DeadlineExceeded", err)
		}
	})

	t.Run("already canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		called := false
		err := tm.WithinTx(ctx, func(context.Context, pgx.Tx) error { called = true; return nil })
		if !errors.Is(err, context.Canceled) || called {
			t.Fatalf("err=%v called=%v", err, called)
		}
	})

	if walletExists(t, db, id) {
		t.Fatal("row persisted after cancellation")
	}
	requireNoAcquiredConns(t, db)
	pingCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := db.Ping(pingCtx); err != nil {
		t.Fatalf("pool unusable after cancellation: %v", err)
	}
}

func TestIntegrationErrorClassification(t *testing.T) {
	t.Parallel()
	db := startedDB(t)
	tm, _ := newTxManager(t, db)
	ctx := t.Context()
	player := uuid.New()
	walletID := uuid.New()
	if err := tm.WithinTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return insertWallet(ctx, tx, walletID, player, 100)
	}); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name       string
		fn         postgres.TxFunc
		kind       postgres.ConstraintKind
		constraint string
		sentinel   error
		is         func(error, string) bool
	}{
		{
			name: "unique",
			fn: func(ctx context.Context, tx pgx.Tx) error {
				return insertWallet(ctx, tx, uuid.New(), player, 0)
			},
			kind: postgres.ConstraintUnique, constraint: "wallets_player_currency_key",
			sentinel: postgres.ErrUniqueViolation, is: postgres.IsUniqueViolation,
		},
		{
			name: "check",
			fn: func(ctx context.Context, tx pgx.Tx) error {
				return insertWallet(ctx, tx, uuid.New(), uuid.New(), -1)
			},
			kind: postgres.ConstraintCheck, constraint: "wallets_balance_non_negative",
			sentinel: postgres.ErrCheckViolation, is: postgres.IsCheckViolation,
		},
		{
			name: "trigger restrict_violation",
			fn: func(ctx context.Context, tx pgx.Tx) error {
				// Muda o saldo sem incrementar a versão: barrado pelo
				// trigger wallets_guard_update.
				_, err := tx.Exec(ctx, `UPDATE wallets SET balance = balance + 1 WHERE id = $1`, walletID)
				return err
			},
			kind: postgres.ConstraintRestrict, constraint: "wallets_version_increment",
			sentinel: postgres.ErrRestrictViolation, is: postgres.IsRestrictViolation,
		},
		{
			name: "foreign key",
			fn: func(ctx context.Context, tx pgx.Tx) error {
				_, err := tx.Exec(ctx, `INSERT INTO inbox (consumer_name, message_id, request_hash, transaction_id, received_at)
VALUES ('c', 'm', $1, $2, now())`, strings.Repeat("a", 64), uuid.New())
				return err
			},
			kind: postgres.ConstraintForeignKey, constraint: "inbox_transaction_fkey",
			sentinel: postgres.ErrForeignKeyViolation, is: postgres.IsForeignKeyViolation,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tm.WithinTx(t.Context(), tc.fn, postgres.WithRetry(postgres.DefaultRetryPolicy(metrics.OperationOpenWallet)))
			if err == nil {
				t.Fatal("expected violation")
			}
			cv, ok := postgres.AsConstraintViolation(err)
			if !ok || cv.Kind != tc.kind || cv.Constraint != tc.constraint {
				t.Fatalf("violation = %+v (ok=%v) from %v", cv, ok, err)
			}
			if !tc.is(err, tc.constraint) || tc.is(err, "other") {
				t.Fatal("Is* by constraint name mismatch")
			}
			if tr := postgres.Translate(err); !errors.Is(tr, tc.sentinel) || !errors.Is(tr, postgres.ErrConstraintViolation) {
				t.Fatalf("Translate(%v) lacks sentinel", err)
			}
			if postgres.IsTransient(err) {
				t.Fatal("constraint violation classified transient")
			}
		})
	}
	if b, v := walletBalance(t, db, walletID); b != 100 || v != 1 {
		t.Fatalf("wallet changed: balance=%d version=%d", b, v)
	}
	requireNoAcquiredConns(t, db)
}

const bumpSQL = `UPDATE wallets SET balance = $2, version = version + 1, updated_at = clock_timestamp() WHERE id = $1`

func TestIntegrationSerializationRetry(t *testing.T) {
	t.Parallel()
	db := startedDB(t)
	tm, m := newTxManager(t, db)
	ctx := t.Context()
	walletID := uuid.New()
	if err := insertWallet(ctx, db.Pool(), walletID, uuid.New(), 100); err != nil {
		t.Fatal(err)
	}

	tx2Read := make(chan struct{})
	tx1Done := make(chan struct{})
	var tx1Err error
	go func() {
		defer close(tx1Done)
		tx1Err = tm.WithinTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
			var bal int64
			if err := tx.QueryRow(ctx, `SELECT balance FROM wallets WHERE id = $1`, walletID).Scan(&bal); err != nil {
				return err
			}
			select {
			case <-tx2Read:
			case <-ctx.Done():
				return ctx.Err()
			}
			_, err := tx.Exec(ctx, bumpSQL, walletID, bal+10)
			return err
		}, postgres.WithIsolation(pgx.Serializable))
	}()

	var attempts atomic.Int32
	var firstErrSQLState string
	err := tm.WithinTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		n := attempts.Add(1)
		var bal int64
		if err := tx.QueryRow(ctx, `SELECT balance FROM wallets WHERE id = $1`, walletID).Scan(&bal); err != nil {
			return err
		}
		if n == 1 {
			close(tx2Read)
			<-tx1Done // tx1 confirmou depois do nosso snapshot
		}
		_, err := tx.Exec(ctx, bumpSQL, walletID, bal+5)
		if n == 1 {
			firstErrSQLState = postgres.SQLState(err)
		}
		return err
	}, postgres.WithIsolation(pgx.Serializable),
		postgres.WithRetry(postgres.RetryPolicy{MaxAttempts: 3, BaseDelay: 5 * time.Millisecond, MaxDelay: 20 * time.Millisecond, Operation: metrics.OperationProcessTransaction}))
	if err != nil {
		t.Fatalf("tx2: %v", err)
	}
	if tx1Err != nil {
		t.Fatalf("tx1: %v", tx1Err)
	}
	if firstErrSQLState != postgres.CodeSerializationFailure {
		t.Fatalf("first attempt SQLSTATE = %q, want 40001", firstErrSQLState)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want 2", attempts.Load())
	}
	if b, v := walletBalance(t, db, walletID); b != 115 || v != 3 {
		t.Fatalf("balance=%d version=%d, want 115/3 (no lost update)", b, v)
	}
	if got := counterValue(t, m.RetriesTotal.WithLabelValues(metrics.OperationProcessTransaction)); got != 1 {
		t.Fatalf("retries_total = %v", got)
	}
	if got := counterValue(t, m.ConcurrencyConflictsTotal.WithLabelValues(metrics.ConflictSerialization)); got != 1 {
		t.Fatalf("serialization conflicts = %v", got)
	}
	requireNoAcquiredConns(t, db)
}

func TestIntegrationSerializationWithoutRetry(t *testing.T) {
	t.Parallel()
	db := startedDB(t)
	tm, _ := newTxManager(t, db)
	ctx := t.Context()
	walletID := uuid.New()
	if err := insertWallet(ctx, db.Pool(), walletID, uuid.New(), 100); err != nil {
		t.Fatal(err)
	}
	read := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		<-read
		done <- tm.WithinTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, bumpSQL, walletID, 1)
			return err
		})
	}()
	err := tm.WithinTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var bal int64
		if err := tx.QueryRow(ctx, `SELECT balance FROM wallets WHERE id = $1`, walletID).Scan(&bal); err != nil {
			return err
		}
		close(read)
		if err := <-done; err != nil {
			return err
		}
		_, err := tx.Exec(ctx, bumpSQL, walletID, bal-1)
		return err
	}, postgres.WithIsolation(pgx.RepeatableRead))
	if postgres.SQLState(err) != postgres.CodeSerializationFailure || !postgres.IsRetryable(err) {
		t.Fatalf("err = %v, want retryable 40001", err)
	}
}

func TestIntegrationDeadlockRetry(t *testing.T) {
	t.Parallel()
	db := startedDB(t)
	tm, m := newTxManager(t, db)
	ctx := t.Context()
	a, b := uuid.New(), uuid.New()
	for _, id := range []uuid.UUID{a, b} {
		if err := insertWallet(ctx, db.Pool(), id, uuid.New(), 100); err != nil {
			t.Fatal(err)
		}
	}
	// Cada transação trava uma carteira e espera a outra travar a sua na
	// primeira tentativa, garantindo o ciclo; o PostgreSQL aborta uma delas
	// com 40P01 e o retry a completa.
	var barrier sync.WaitGroup
	barrier.Add(2)
	lockBoth := func(first, second uuid.UUID, attempts *atomic.Int32) postgres.TxFunc {
		return func(ctx context.Context, tx pgx.Tx) error {
			n := attempts.Add(1)
			if _, err := tx.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, first); err != nil {
				return err
			}
			if n == 1 {
				barrier.Done()
				barrier.Wait()
			}
			_, err := tx.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, second)
			return err
		}
	}
	policy := postgres.WithRetry(postgres.RetryPolicy{MaxAttempts: 3, BaseDelay: 5 * time.Millisecond, Operation: metrics.OperationProcessTransaction})
	var n1, n2 atomic.Int32
	errs := make(chan error, 2)
	go func() { errs <- tm.WithinTx(ctx, lockBoth(a, b, &n1), policy) }()
	go func() { errs <- tm.WithinTx(ctx, lockBoth(b, a, &n2), policy) }()
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("tx: %v", err)
		}
	}
	if total := n1.Load() + n2.Load(); total != 3 {
		t.Fatalf("total attempts = %d, want 3 (exactly one deadlock victim retried)", total)
	}
	if got := counterValue(t, m.ConcurrencyConflictsTotal.WithLabelValues(metrics.ConflictDeadlock)); got != 1 {
		t.Fatalf("deadlock conflicts = %v", got)
	}
	requireNoAcquiredConns(t, db)
}

func TestIntegrationStartFailsFastOnPermanentError(t *testing.T) {
	t.Parallel()
	u, err := url.Parse(dsnFor(t, "pgtx_does_not_exist_"+randomHex(t, 4)))
	if err != nil {
		t.Fatal(err)
	}
	// Garante uma senha no DSN para checar o vazamento: usa a do admin ou,
	// se não houver (auth trust), injeta uma que o servidor ignora.
	password, ok := u.User.Password()
	if !ok || password == "" {
		password = "n0t-l0gg3d"
		u.User = url.UserPassword(u.User.Username(), password)
	}
	db, err := postgres.NewDB(dbConfig(u.String()), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Stop(context.Background())
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	started := time.Now()
	err = db.Start(ctx)
	if err == nil {
		t.Fatal("start succeeded on missing database")
	}
	if postgres.SQLState(err) != "3D000" {
		t.Fatalf("SQLSTATE = %q (%v), want 3D000", postgres.SQLState(err), err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("permanent error retried for %v", elapsed)
	}
	if strings.Contains(err.Error(), password) {
		t.Fatalf("error leaked password: %v", err)
	}
}

// TestIntegrationFxModuleLifecycle não é paralelo: mede goroutines e roda
// antes dos testes paralelos (que só são liberados após os sequenciais).
func TestIntegrationFxModuleLifecycle(t *testing.T) {
	dsn := migratedDSN(t)
	baseline := runtime.NumGoroutine()

	var db *postgres.DB
	var txr postgres.Transactor
	app := fx.New(
		fx.NopLogger,
		fx.Supply(config.Config{Database: dbConfig(dsn)}),
		metrics.Module,
		postgres.Module,
		fx.Populate(&db, &txr),
	)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}
	startCtx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := app.Start(startCtx); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := db.Ping(startCtx); err != nil {
		t.Fatalf("ping after start: %v", err)
	}
	if db.Pool().Stat().TotalConns() < 1 {
		t.Fatal("OnStart did not open a connection")
	}
	if err := txr.WithinTx(startCtx, func(ctx context.Context, tx pgx.Tx) error {
		return insertWallet(ctx, tx, uuid.New(), uuid.New(), 1)
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Stop(startCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := db.Ping(startCtx); err == nil {
		t.Fatal("ping succeeded after stop (pool not closed)")
	}

	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > baseline {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			n := runtime.Stack(buf, true)
			t.Fatalf("goroutines: %d > baseline %d\n%s", runtime.NumGoroutine(), baseline, buf[:n])
		}
		time.Sleep(20 * time.Millisecond)
	}
}
