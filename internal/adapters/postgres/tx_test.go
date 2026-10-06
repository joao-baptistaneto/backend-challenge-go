package postgres

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

// fakeTx implementa só o que o TxManager usa; o restante de pgx.Tx fica
// nil (chamá-lo derruba o teste com panic, o que é desejável).
type fakeTx struct {
	pgx.Tx
	commitErr   error
	rollbackErr error
	commits     int
	rollbacks   int
	// Estado do ctx no momento do Rollback.
	rollbackCtxErr      error
	rollbackHasDeadline bool
}

func (f *fakeTx) Commit(context.Context) error { f.commits++; return f.commitErr }

func (f *fakeTx) Rollback(ctx context.Context) error {
	f.rollbacks++
	f.rollbackCtxErr = ctx.Err()
	_, f.rollbackHasDeadline = ctx.Deadline()
	return f.rollbackErr
}

type fakeBeginner struct {
	mu       sync.Mutex
	txs      []*fakeTx
	beginErr []error // consumidos em ordem; nil = sucesso
	opts     []pgx.TxOptions
	next     func() *fakeTx
}

func (b *fakeBeginner) BeginTx(_ context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.opts = append(b.opts, opts)
	if len(b.beginErr) > 0 {
		err := b.beginErr[0]
		b.beginErr = b.beginErr[1:]
		if err != nil {
			return nil, err
		}
	}
	tx := &fakeTx{}
	if b.next != nil {
		tx = b.next()
	}
	b.txs = append(b.txs, tx)
	return tx, nil
}

func newTestManager(t *testing.T, b beginner) (*TxManager, *metrics.Metrics, *[]time.Duration) {
	t.Helper()
	m, err := metrics.New(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	tm := newTxManager(b, nil, m)
	var sleeps []time.Duration
	tm.sleep = func(ctx context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		return ctx.Err()
	}
	return tm, m, &sleeps
}

func TestWithinTxCommit(t *testing.T) {
	t.Parallel()
	b := &fakeBeginner{}
	tm, _, _ := newTestManager(t, b)
	called := 0
	if err := tm.WithinTx(t.Context(), func(_ context.Context, tx pgx.Tx) error {
		called++
		if tx == nil {
			t.Error("nil tx")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if called != 1 || b.txs[0].commits != 1 || b.txs[0].rollbacks != 0 {
		t.Fatalf("called=%d commits=%d rollbacks=%d", called, b.txs[0].commits, b.txs[0].rollbacks)
	}
	if b.opts[0].IsoLevel != pgx.ReadCommitted || b.opts[0].AccessMode != "" {
		t.Fatalf("default options = %+v", b.opts[0])
	}
}

func TestWithinTxOptions(t *testing.T) {
	t.Parallel()
	b := &fakeBeginner{}
	tm, _, _ := newTestManager(t, b)
	_ = tm.WithinTx(t.Context(), func(context.Context, pgx.Tx) error { return nil },
		WithIsolation(pgx.RepeatableRead), ReadOnly())
	if b.opts[0].IsoLevel != pgx.RepeatableRead || b.opts[0].AccessMode != pgx.ReadOnly {
		t.Fatalf("options = %+v", b.opts[0])
	}
}

func TestWithinTxRollbackOnError(t *testing.T) {
	t.Parallel()
	b := &fakeBeginner{}
	tm, _, _ := newTestManager(t, b)
	domainErr := errors.New("insufficient funds")
	err := tm.WithinTx(t.Context(), func(context.Context, pgx.Tx) error { return domainErr })
	if err != domainErr {
		t.Fatalf("err = %v, want fn error unchanged", err)
	}
	if b.txs[0].commits != 0 || b.txs[0].rollbacks != 1 {
		t.Fatalf("commits=%d rollbacks=%d", b.txs[0].commits, b.txs[0].rollbacks)
	}
}

func TestWithinTxRollbackUsesUncanceledContext(t *testing.T) {
	t.Parallel()
	b := &fakeBeginner{}
	tm, _, _ := newTestManager(t, b)
	ctx, cancel := context.WithCancel(t.Context())
	err := tm.WithinTx(ctx, func(context.Context, pgx.Tx) error {
		cancel()
		return context.Canceled
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if b.txs[0].rollbacks != 1 || b.txs[0].rollbackCtxErr != nil {
		t.Fatalf("rollback ran with a canceled context: %v", b.txs[0].rollbackCtxErr)
	}
	if !b.txs[0].rollbackHasDeadline {
		t.Fatal("rollback context has no deadline")
	}
}

func TestWithinTxRollbackOnPanic(t *testing.T) {
	t.Parallel()
	b := &fakeBeginner{}
	tm, _, _ := newTestManager(t, b)
	defer func() {
		p := recover()
		if p != "boom" {
			t.Fatalf("recovered %v, want original panic value", p)
		}
		if b.txs[0].rollbacks != 1 || b.txs[0].commits != 0 {
			t.Fatalf("commits=%d rollbacks=%d", b.txs[0].commits, b.txs[0].rollbacks)
		}
	}()
	_ = tm.WithinTx(t.Context(), func(context.Context, pgx.Tx) error { panic("boom") })
	t.Fatal("panic was swallowed")
}

func TestWithinTxRollbackOnGoexit(t *testing.T) {
	t.Parallel()
	b := &fakeBeginner{}
	tm, _, _ := newTestManager(t, b)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = tm.WithinTx(context.Background(), func(context.Context, pgx.Tx) error {
			runtime.Goexit()
			return nil
		})
	}()
	<-done
	if b.txs[0].rollbacks != 1 || b.txs[0].commits != 0 {
		t.Fatalf("commits=%d rollbacks=%d", b.txs[0].commits, b.txs[0].rollbacks)
	}
}

func TestWithinTxRollbackFailureIsNotReturned(t *testing.T) {
	t.Parallel()
	b := &fakeBeginner{next: func() *fakeTx { return &fakeTx{rollbackErr: io.EOF} }}
	tm, _, _ := newTestManager(t, b)
	fnErr := errors.New("fn")
	if err := tm.WithinTx(t.Context(), func(context.Context, pgx.Tx) error { return fnErr }); err != fnErr {
		t.Fatalf("err = %v", err)
	}
}

func TestWithinTxBeginError(t *testing.T) {
	t.Parallel()
	b := &fakeBeginner{beginErr: []error{&pgconn.ConnectError{Config: &pgconn.Config{}}}}
	tm, _, _ := newTestManager(t, b)
	called := false
	err := tm.WithinTx(t.Context(), func(context.Context, pgx.Tx) error { called = true; return nil })
	if err == nil || called {
		t.Fatalf("err=%v called=%v", err, called)
	}
	if !IsRetryable(err) {
		t.Fatalf("begin connection error should be retryable: %v", err)
	}
}

func TestCommitErrorClassification(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		commitErr error
		unknown   bool
		retryable bool
	}{
		{"server serialization at commit", pgErr(CodeSerializationFailure, ""), false, true},
		{"server fk at commit", pgErr(CodeForeignKeyViolation, "fk"), false, false},
		{"aborted tx", pgx.ErrTxCommitRollback, false, false},
		{"not sent", safeErr{}, false, true},
		{"connection lost after send", io.ErrUnexpectedEOF, true, false},
		{"ctx deadline during commit", context.DeadlineExceeded, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := &fakeBeginner{next: func() *fakeTx { return &fakeTx{commitErr: tc.commitErr} }}
			tm, _, _ := newTestManager(t, b)
			calls := 0
			err := tm.WithinTx(t.Context(), func(context.Context, pgx.Tx) error { calls++; return nil },
				WithRetry(RetryPolicy{MaxAttempts: 1}))
			if err == nil {
				t.Fatal("expected error")
			}
			if got := errors.Is(err, ErrCommitOutcomeUnknown); got != tc.unknown {
				t.Errorf("unknown = %v, want %v (%v)", got, tc.unknown, err)
			}
			if got := IsRetryable(err); got != tc.retryable {
				t.Errorf("retryable = %v, want %v (%v)", got, tc.retryable, err)
			}
			if !errors.Is(err, tc.commitErr) {
				t.Errorf("cause lost: %v", err)
			}
			if b.txs[0].rollbacks != 0 {
				t.Error("rollback after commit attempt")
			}
		})
	}
}

func TestWithinTxNeverRetriesAmbiguousCommit(t *testing.T) {
	t.Parallel()
	b := &fakeBeginner{next: func() *fakeTx { return &fakeTx{commitErr: io.ErrUnexpectedEOF} }}
	tm, _, _ := newTestManager(t, b)
	calls := 0
	err := tm.WithinTx(t.Context(), func(context.Context, pgx.Tx) error { calls++; return nil },
		WithRetry(RetryPolicy{MaxAttempts: 5, RetryIf: func(error) bool { return true }}))
	if !errors.Is(err, ErrCommitOutcomeUnknown) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	if !IsTransient(err) {
		t.Fatal("ambiguous commit should be transient for the caller")
	}
}

func TestWithinTxRetry(t *testing.T) {
	t.Parallel()
	b := &fakeBeginner{}
	tm, m, sleeps := newTestManager(t, b)
	calls := 0
	err := tm.WithinTx(t.Context(), func(context.Context, pgx.Tx) error {
		calls++
		switch calls {
		case 1:
			return pgErr(CodeSerializationFailure, "")
		case 2:
			return pgErr(CodeDeadlockDetected, "")
		}
		return nil
	}, WithRetry(RetryPolicy{MaxAttempts: 4, BaseDelay: 10 * time.Millisecond, MaxDelay: 15 * time.Millisecond, Operation: metrics.OperationProcessTransaction}))
	if err != nil {
		t.Fatal(err)
	}
	if calls != 3 || len(b.txs) != 3 {
		t.Fatalf("calls=%d txs=%d", calls, len(b.txs))
	}
	for i, tx := range b.txs[:2] {
		if tx.rollbacks != 1 || tx.commits != 0 {
			t.Fatalf("tx %d: commits=%d rollbacks=%d", i, tx.commits, tx.rollbacks)
		}
	}
	if b.txs[2].commits != 1 {
		t.Fatal("last attempt not committed")
	}
	if len(*sleeps) != 2 {
		t.Fatalf("sleeps = %v", *sleeps)
	}
	if d := (*sleeps)[0]; d < 5*time.Millisecond || d > 10*time.Millisecond {
		t.Fatalf("first backoff %v outside [5ms,10ms]", d)
	}
	if d := (*sleeps)[1]; d < 7*time.Millisecond || d > 15*time.Millisecond {
		t.Fatalf("second backoff %v outside [7.5ms,15ms] (capped)", d)
	}
	if got := counterValue(t, m.RetriesTotal.WithLabelValues(metrics.OperationProcessTransaction)); got != 2 {
		t.Fatalf("retries_total = %v", got)
	}
	if got := counterValue(t, m.ConcurrencyConflictsTotal.WithLabelValues(metrics.ConflictSerialization)); got != 1 {
		t.Fatalf("serialization conflicts = %v", got)
	}
	if got := counterValue(t, m.ConcurrencyConflictsTotal.WithLabelValues(metrics.ConflictDeadlock)); got != 1 {
		t.Fatalf("deadlock conflicts = %v", got)
	}
}

func TestWithinTxRetryExhausted(t *testing.T) {
	t.Parallel()
	b := &fakeBeginner{}
	tm, _, _ := newTestManager(t, b)
	calls := 0
	err := tm.WithinTx(t.Context(), func(context.Context, pgx.Tx) error {
		calls++
		return pgErr(CodeSerializationFailure, "")
	}, WithRetry(RetryPolicy{MaxAttempts: 3}))
	if calls != 3 {
		t.Fatalf("calls = %d", calls)
	}
	if SQLState(err) != CodeSerializationFailure || !IsTransient(err) {
		t.Fatalf("err = %v", err)
	}
}

func TestWithinTxNoRetryByDefault(t *testing.T) {
	t.Parallel()
	b := &fakeBeginner{}
	tm, _, _ := newTestManager(t, b)
	calls := 0
	err := tm.WithinTx(t.Context(), func(context.Context, pgx.Tx) error {
		calls++
		return pgErr(CodeSerializationFailure, "")
	})
	if calls != 1 || SQLState(err) != CodeSerializationFailure {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestWithinTxNoRetryOnPermanent(t *testing.T) {
	t.Parallel()
	for _, perm := range []error{pgErr(CodeUniqueViolation, "x"), errors.New("domain"), context.Canceled} {
		b := &fakeBeginner{}
		tm, _, _ := newTestManager(t, b)
		calls := 0
		_ = tm.WithinTx(t.Context(), func(context.Context, pgx.Tx) error { calls++; return perm },
			WithRetry(DefaultRetryPolicy(metrics.OperationProcessTransaction)))
		if calls != 1 {
			t.Fatalf("%v: calls = %d", perm, calls)
		}
	}
}

func TestWithinTxRetryIf(t *testing.T) {
	t.Parallel()
	b := &fakeBeginner{}
	tm, m, _ := newTestManager(t, b)
	calls := 0
	err := tm.WithinTx(t.Context(), func(context.Context, pgx.Tx) error {
		calls++
		if calls == 1 {
			return pgErr(CodeUniqueViolation, "wager_transactions_provider_idempotency_key")
		}
		return nil
	}, WithRetry(RetryPolicy{MaxAttempts: 2, Operation: metrics.OperationProcessTransaction, RetryIf: func(err error) bool {
		return IsUniqueViolation(err, "wager_transactions_provider_idempotency_key")
	}}))
	if err != nil || calls != 2 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	if got := counterValue(t, m.ConcurrencyConflictsTotal.WithLabelValues(metrics.ConflictUniqueViolation)); got != 1 {
		t.Fatalf("unique conflicts = %v", got)
	}
}

func TestWithinTxRetryStopsOnContext(t *testing.T) {
	t.Parallel()
	b := &fakeBeginner{}
	tm, _, _ := newTestManager(t, b)
	ctx, cancel := context.WithCancel(t.Context())
	tm.sleep = func(ctx context.Context, _ time.Duration) error { cancel(); return ctx.Err() }
	calls := 0
	err := tm.WithinTx(ctx, func(context.Context, pgx.Tx) error {
		calls++
		return pgErr(CodeDeadlockDetected, "")
	}, WithRetry(RetryPolicy{MaxAttempts: 10, BaseDelay: time.Second}))
	if calls != 1 {
		t.Fatalf("calls = %d", calls)
	}
	if !errors.Is(err, context.Canceled) || SQLState(err) != CodeDeadlockDetected {
		t.Fatalf("err = %v", err)
	}
}

func TestBackoff(t *testing.T) {
	t.Parallel()
	p := RetryPolicy{BaseDelay: 10 * time.Millisecond, MaxDelay: 50 * time.Millisecond}
	bounds := map[int][2]time.Duration{
		1:  {5 * time.Millisecond, 10 * time.Millisecond},
		2:  {10 * time.Millisecond, 20 * time.Millisecond},
		3:  {20 * time.Millisecond, 40 * time.Millisecond},
		4:  {25 * time.Millisecond, 50 * time.Millisecond},
		60: {25 * time.Millisecond, 50 * time.Millisecond}, // sem overflow
	}
	for attempt, b := range bounds {
		for range 200 {
			d := p.backoff(attempt)
			if d < b[0] || d > b[1] {
				t.Fatalf("attempt %d: %v outside %v", attempt, d, b)
			}
		}
	}
	if (RetryPolicy{}).backoff(3) != 0 {
		t.Fatal("zero policy should not wait")
	}
}

func TestSleepCtx(t *testing.T) {
	t.Parallel()
	if err := sleepCtx(t.Context(), time.Millisecond); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := sleepCtx(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := sleepCtx(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// counterValue lê um contador sem prometheus/testutil (evita dependência extra).
func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}
