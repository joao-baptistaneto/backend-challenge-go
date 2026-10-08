package pendingworker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

// fakeResumer simula o caso de uso: devolve lotes de uma fila de ids e
// executa resume (bloqueante, se configurado).
type fakeResumer struct {
	mu       sync.Mutex
	queue    []uuid.UUID
	claims   []app.ClaimRequest
	resumed  []uuid.UUID
	claimErr error
	resume   func(ctx context.Context, id uuid.UUID) (app.ResumeResult, error)
}

func (f *fakeResumer) Claim(ctx context.Context, req app.ClaimRequest) ([]app.ClaimedWager, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims = append(f.claims, req)
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	n := min(req.Limit, len(f.queue))
	var out []app.ClaimedWager
	for _, id := range f.queue[:n] {
		out = append(out, app.ClaimedWager{ID: id, Status: domain.WagerStatusPendingReference})
	}
	f.queue = f.queue[n:]
	return out, nil
}

func (f *fakeResumer) Resume(ctx context.Context, id, token uuid.UUID) (app.ResumeResult, error) {
	if token == uuid.Nil {
		return app.ResumeResult{}, errors.New("nil token")
	}
	var (
		r   = app.ResumeResult{Outcome: app.ResumeResolved, TransactionID: id, Kind: domain.WagerKindRefund, Status: domain.WagerStatusProcessed}
		err error
	)
	if f.resume != nil {
		r, err = f.resume(ctx, id)
	}
	f.mu.Lock()
	f.resumed = append(f.resumed, id)
	f.mu.Unlock()
	return r, err
}

func (f *fakeResumer) push(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for range n {
		f.queue = append(f.queue, uuid.New())
	}
}

func (f *fakeResumer) resumedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.resumed)
}

func testConfig() Config {
	return Config{
		PollInterval: 10 * time.Millisecond, BatchSize: 3, Concurrency: 2, Lease: time.Minute,
		ProcessTimeout: time.Second, DrainTimeout: time.Second, OrphanGrace: time.Second,
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestConfigValidate(t *testing.T) {
	if err := testConfig().Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Config){
		"poll":          func(c *Config) { c.PollInterval = 0 },
		"batch":         func(c *Config) { c.BatchSize = 0 },
		"concurrency":   func(c *Config) { c.Concurrency = 0 },
		"process zero":  func(c *Config) { c.ProcessTimeout = 0 },
		"process>lease": func(c *Config) { c.ProcessTimeout = c.Lease },
		"drain":         func(c *Config) { c.DrainTimeout = 0 },
		"grace":         func(c *Config) { c.OrphanGrace = -1 },
	} {
		c := testConfig()
		mutate(&c)
		if _, err := New(c, &fakeResumer{}); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := New(testConfig(), nil); err == nil {
		t.Fatal("nil resumer accepted")
	}
}

func TestWorker_ProcessesAllBatchesAndPassesClaimParams(t *testing.T) {
	f := &fakeResumer{}
	f.push(7)
	m := newMetrics(t)
	w, err := New(testConfig(), f, WithMetrics(m))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := w.Start(t.Context()); err == nil {
		t.Fatal("second Start accepted")
	}
	waitFor(t, func() bool { return f.resumedCount() == 7 })
	if err := w.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := w.Stop(t.Context()); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	// Lotes cheios (3, 3) emendam sem esperar; cada ciclo tem token próprio.
	if len(f.claims) < 3 {
		t.Fatalf("claims = %d", len(f.claims))
	}
	tokens := map[uuid.UUID]bool{}
	for _, c := range f.claims {
		if c.Limit != 3 || c.Lease != time.Minute || c.OrphanGrace != time.Second || c.Token == uuid.Nil || tokens[c.Token] {
			t.Fatalf("claim request %+v", c)
		}
		tokens[c.Token] = true
	}
	if got := testutil.ToFloat64(m.TransactionsTotal.WithLabelValues(metrics.SourceWorker, "REFUND", "PROCESSED")); got != 7 {
		t.Fatalf("metric = %v", got)
	}
}

func TestWorker_RespectsConcurrency(t *testing.T) {
	var cur, peak atomic.Int64
	f := &fakeResumer{resume: func(context.Context, uuid.UUID) (app.ResumeResult, error) {
		n := cur.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		cur.Add(-1)
		return app.ResumeResult{Outcome: app.ResumeRescheduled}, nil
	}}
	f.push(9)
	cfg := testConfig()
	cfg.BatchSize = 9
	cfg.Concurrency = 3
	w, err := New(cfg, f)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := w.RunOnce(t.Context()); err != nil || n != 9 {
		t.Fatalf("RunOnce = %d, %v", n, err)
	}
	if p := peak.Load(); p != 3 {
		t.Fatalf("peak concurrency %d, want 3", p)
	}
}

func TestWorker_ItemErrorsDoNotStopTheCycle(t *testing.T) {
	calls := 0
	f := &fakeResumer{resume: func(context.Context, uuid.UUID) (app.ResumeResult, error) {
		calls++
		if calls == 1 {
			return app.ResumeResult{}, &app.Error{Kind: app.KindUnavailable, Code: app.CodeTemporarilyUnavailable}
		}
		return app.ResumeResult{Outcome: app.ResumeExpired, Status: domain.WagerStatusRejected, Kind: domain.WagerKindRollback}, nil
	}}
	f.push(3)
	cfg := testConfig()
	cfg.Concurrency = 1
	m := newMetrics(t)
	w, _ := New(cfg, f, WithMetrics(m))
	if n, err := w.RunOnce(t.Context()); err != nil || n != 3 || f.resumedCount() != 3 {
		t.Fatalf("RunOnce = %d, %v (resumed %d)", n, err, f.resumedCount())
	}
	if got := testutil.ToFloat64(m.TransactionsTotal.WithLabelValues(metrics.SourceWorker, "ROLLBACK", "REJECTED")); got != 2 {
		t.Fatalf("rejected metric = %v", got)
	}
}

// ADR-031: falha contada (DEFERRED, com erro) e item encerrado (FAILED, sem
// erro) não param o ciclo; FAILED entra em wager_transactions_total.
func TestWorker_DeferredAndFailedOutcomes(t *testing.T) {
	calls := 0
	f := &fakeResumer{resume: func(context.Context, uuid.UUID) (app.ResumeResult, error) {
		calls++
		if calls == 1 {
			return app.ResumeResult{Outcome: app.ResumeDeferred, ResumeFailures: 1, HeldUntil: time.Now().Add(time.Second)},
				errors.New("corrupted row")
		}
		return app.ResumeResult{Outcome: app.ResumeFailed, Kind: domain.WagerKindRollback, Status: domain.WagerStatusFailed,
			FailureCode: domain.FailureCodeRetriesExhausted, ResumeFailures: 5, Cause: errors.New("corrupted row")}, nil
	}}
	f.push(2)
	cfg := testConfig()
	cfg.Concurrency = 1
	m := newMetrics(t)
	w, _ := New(cfg, f, WithMetrics(m))
	if n, err := w.RunOnce(t.Context()); err != nil || n != 2 || f.resumedCount() != 2 {
		t.Fatalf("RunOnce = %d, %v (resumed %d)", n, err, f.resumedCount())
	}
	if got := testutil.ToFloat64(m.TransactionsTotal.WithLabelValues(metrics.SourceWorker, "ROLLBACK", "FAILED")); got != 1 {
		t.Fatalf("failed metric = %v", got)
	}
}

func TestWorker_ClaimErrorRetriesNextCycle(t *testing.T) {
	f := &fakeResumer{claimErr: errors.New("db down")}
	w, _ := New(testConfig(), f)
	if _, err := w.RunOnce(t.Context()); err == nil {
		t.Fatal("claim error swallowed by RunOnce")
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return len(f.claims) >= 3 })
	f.mu.Lock()
	f.claimErr = nil
	f.mu.Unlock()
	f.push(1)
	waitFor(t, func() bool { return f.resumedCount() == 1 })
	if err := w.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// Um item cujo lease não cobre mais um processamento não é iniciado.
func TestWorker_SkipsItemsWhenLeaseIsAboutToExpire(t *testing.T) {
	var mu sync.Mutex
	now := time.Now()
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	f := &fakeResumer{}
	f.resume = func(context.Context, uuid.UUID) (app.ResumeResult, error) {
		mu.Lock()
		now = now.Add(50 * time.Second) // o primeiro item "demora" 50s
		mu.Unlock()
		return app.ResumeResult{Outcome: app.ResumeRescheduled}, nil
	}
	f.push(3)
	cfg := testConfig()
	cfg.Concurrency = 1
	cfg.Lease = time.Minute
	cfg.ProcessTimeout = 20 * time.Second
	w, _ := New(cfg, f, WithClock(clock))
	if n, err := w.RunOnce(t.Context()); err != nil || n != 3 {
		t.Fatalf("RunOnce = %d, %v", n, err)
	}
	if got := f.resumedCount(); got != 1 {
		t.Fatalf("resumed %d items after the start deadline, want 1", got)
	}
}

func TestWorker_StopDrainsInFlight(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	var finished atomic.Bool
	f := &fakeResumer{resume: func(ctx context.Context, _ uuid.UUID) (app.ResumeResult, error) {
		started <- struct{}{}
		<-release
		finished.Store(ctx.Err() == nil)
		return app.ResumeResult{Outcome: app.ResumeResolved}, nil
	}}
	f.push(1)
	w, _ := New(testConfig(), f)
	if err := w.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-started
	stopped := make(chan error, 1)
	go func() { stopped <- w.Stop(context.Background()) }()
	select {
	case err := <-stopped:
		t.Fatalf("Stop returned with an item in flight: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if !finished.Load() {
		t.Fatal("in-flight item was cancelled instead of drained")
	}
	f.push(2)
	time.Sleep(30 * time.Millisecond)
	if got := f.resumedCount(); got != 1 {
		t.Fatalf("worker kept claiming after Stop: %d", got)
	}
}

func TestWorker_StopDrainTimeoutCancelsInFlight(t *testing.T) {
	cancelled := make(chan struct{})
	started := make(chan struct{})
	f := &fakeResumer{resume: func(ctx context.Context, _ uuid.UUID) (app.ResumeResult, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return app.ResumeResult{}, ctx.Err()
	}}
	f.push(1)
	cfg := testConfig()
	cfg.DrainTimeout = 30 * time.Millisecond
	cfg.ProcessTimeout = 10 * time.Second
	w, _ := New(cfg, f)
	if err := w.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := w.Stop(context.Background()); !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("Stop = %v, want ErrDrainTimeout", err)
	}
	<-cancelled
}

func TestWorker_StopBeforeStartIsNoop(t *testing.T) {
	w, _ := New(testConfig(), &fakeResumer{})
	if err := w.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func newMetrics(t *testing.T) *metrics.Metrics {
	t.Helper()
	m, err := metrics.New(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	return m
}
