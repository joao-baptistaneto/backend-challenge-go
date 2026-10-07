package app_test

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// Testes unitários de ResumeWagerTransaction (T21) sobre a UnitOfWork em
// memória (process_wager_test.go), que espelha claim, fencing e Update.

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func testPolicy() app.PendingWagerPolicy {
	return app.PendingWagerPolicy{MaxAttempts: 4, TTL: time.Hour, RetryBaseDelay: 2 * time.Second, RetryMaxDelay: 10 * time.Second, MaxResumeFailures: 3}
}

func newResume(t *testing.T, u app.UnitOfWork, clock *testClock, policy app.PendingWagerPolicy) *app.ResumeWagerTransaction {
	t.Helper()
	uc, err := app.NewResumeWagerTransaction(u, policy, app.WithClock(clock.Now))
	if err != nil {
		t.Fatal(err)
	}
	return uc
}

// claimAndResume reivindica tudo o que venceu e retoma cada item.
func claimAndResume(t *testing.T, uc *app.ResumeWagerTransaction) []app.ResumeResult {
	t.Helper()
	token := uuid.New()
	claimed, err := uc.Claim(t.Context(), app.ClaimRequest{Token: token, Lease: time.Minute, Limit: 10, OrphanGrace: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var out []app.ResumeResult
	for _, c := range claimed {
		r, err := uc.Resume(t.Context(), c.ID, token)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}

// pendingRollback aceita um ROLLBACK de "bet-1" antes da BET (PENDING_REFERENCE,
// attempts = 1, nova busca em fixedNow + 2s).
func pendingRollback(t *testing.T, u *memUoW) uuid.UUID {
	t.Helper()
	res, err := newProcess(t, u, app.WithReferenceRetryDelay(2*time.Second)).
		Execute(t.Context(), wagerCmd(t, domain.WagerKindRollback, "rb-1", "10.00", "bet-1"))
	if err != nil || res.Status != domain.WagerStatusPendingReference {
		t.Fatalf("accept rollback: %+v, %v", res, err)
	}
	u.state.events = nil
	return res.TransactionID
}

func wagerByID(t *testing.T, u *memUoW, id uuid.UUID) *domain.WagerTransaction {
	t.Helper()
	for _, w := range u.state.wagers {
		if w.ID() == id {
			return w
		}
	}
	t.Fatalf("transaction %s not found", id)
	return nil
}

func TestPendingWagerPolicy_DelayAndValidate(t *testing.T) {
	p := app.PendingWagerPolicy{MaxAttempts: 3, TTL: time.Minute, RetryBaseDelay: time.Second, RetryMaxDelay: 10 * time.Second, MaxResumeFailures: 3}
	want := map[int32]time.Duration{0: time.Second, 1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 4: 8 * time.Second, 5: 10 * time.Second, 1 << 30: 10 * time.Second}
	for attempts, d := range want {
		if got := p.Delay(attempts); got != d {
			t.Errorf("Delay(%d) = %v, want %v", attempts, got, d)
		}
	}
	huge := app.PendingWagerPolicy{MaxAttempts: 1, TTL: time.Hour, RetryBaseDelay: time.Hour, RetryMaxDelay: 1<<63 - 1}
	if got := huge.Delay(1 << 30); got <= 0 {
		t.Fatalf("overflow: %v", got)
	}
	if err := app.DefaultPendingWagerPolicy().Validate(); err != nil {
		t.Fatal(err)
	}
	for name, bad := range map[string]app.PendingWagerPolicy{
		"zero attempts": {MaxAttempts: 0, TTL: time.Minute, RetryBaseDelay: time.Second, RetryMaxDelay: time.Second, MaxResumeFailures: 1},
		"zero ttl":      {MaxAttempts: 1, RetryBaseDelay: time.Second, RetryMaxDelay: time.Second, MaxResumeFailures: 1},
		"zero base":     {MaxAttempts: 1, TTL: time.Minute, RetryMaxDelay: time.Second, MaxResumeFailures: 1},
		"max < base":    {MaxAttempts: 1, TTL: time.Minute, RetryBaseDelay: time.Minute, RetryMaxDelay: time.Second, MaxResumeFailures: 1},
		"zero failures": {MaxAttempts: 1, TTL: time.Minute, RetryBaseDelay: time.Second, RetryMaxDelay: time.Second},
	} {
		if bad.Validate() == nil {
			t.Errorf("%s: accepted", name)
		}
		if _, err := app.NewResumeWagerTransaction(newMemUoW(), bad); err == nil {
			t.Errorf("%s: constructor accepted", name)
		}
	}
	if _, err := app.NewResumeWagerTransaction(nil, app.DefaultPendingWagerPolicy()); err == nil {
		t.Fatal("nil UnitOfWork accepted")
	}
}

func TestResumeWager_NotDueIsNotClaimed(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	pendingRollback(t, u)
	clock := &testClock{now: fixedNow.Add(time.Second)}
	if got := claimAndResume(t, newResume(t, u, clock, testPolicy())); len(got) != 0 {
		t.Fatalf("claimed before next_attempt_at: %+v", got)
	}
}

func TestResumeWager_RescheduleWithExponentialBackoff(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	id := pendingRollback(t, u)
	clock := &testClock{now: fixedNow.Add(2 * time.Second)}
	uc := newResume(t, u, clock, testPolicy())

	// Busca 2 (attempts 1 → 2): espera base·2 = 4s; busca 3: 8s.
	for i, wantDelay := range []time.Duration{4 * time.Second, 8 * time.Second} {
		got := claimAndResume(t, uc)
		if len(got) != 1 || got[0].Outcome != app.ResumeRescheduled || got[0].Attempts != int32(i+2) ||
			!got[0].NextAttemptAt.Equal(clock.Now().Add(wantDelay)) {
			t.Fatalf("step %d: %+v", i, got)
		}
		w := wagerByID(t, u, id)
		if w.Status() != domain.WagerStatusPendingReference || w.Attempts() != int32(i+2) {
			t.Fatalf("step %d: persisted %s attempts %d", i, w.Status(), w.Attempts())
		}
		if len(u.state.events) != 0 || len(u.state.ledger) != 0 {
			t.Fatalf("reschedule produced effects: %s", eventTypes(u.state.events))
		}
		if len(u.state.claims) != 0 {
			t.Fatal("lease kept after Update")
		}
		clock.Advance(wantDelay)
	}
}

func TestResumeWager_ResolvesAfterReferenceArrives(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	id := pendingRollback(t, u)
	// A BET referenciada chega depois (saldo 100 → 90).
	if _, err := newProcess(t, u).Execute(t.Context(), wagerCmd(t, domain.WagerKindBet, "bet-1", "10.00", "")); err != nil {
		t.Fatal(err)
	}
	bet := u.state.wagers[1]
	u.state.events = nil

	clock := &testClock{now: fixedNow.Add(2 * time.Second)}
	got := claimAndResume(t, newResume(t, u, clock, testPolicy()))
	if len(got) != 1 || got[0].Outcome != app.ResumeResolved || got[0].Status != domain.WagerStatusProcessed ||
		got[0].PreviousStatus != domain.WagerStatusPendingReference || got[0].TransactionID != id {
		t.Fatalf("result = %+v", got)
	}
	w := wagerByID(t, u, id)
	if w.ReferenceTransactionID() != bet.ID() || w.BalanceAfter() != brl(t, "100.00") || !w.NextAttemptAt().IsZero() {
		t.Fatalf("persisted: ref %s balance %s", w.ReferenceTransactionID(), w.BalanceAfter())
	}
	if bal := u.state.wallets[unitWallet].Balance(); bal != brl(t, "100.00") {
		t.Fatalf("wallet balance %s", bal)
	}
	if n := len(u.state.ledger); n != 2 || u.state.ledger[1].TransactionID() != id ||
		u.state.ledger[1].Direction() != domain.LedgerDirectionCredit {
		t.Fatalf("ledger = %d entries", n)
	}
	if got := eventTypes(u.state.events); got != "WagerTransactionProcessed,WalletBalanceChanged" {
		t.Fatalf("events = %s", got)
	}
	for _, e := range u.state.events {
		if e.CorrelationID() != id.String() || e.CausationID() == "" {
			t.Fatalf("event metadata %q/%q", e.CorrelationID(), e.CausationID())
		}
	}
	// Resolvida, não é mais reivindicada.
	clock.Advance(time.Hour)
	if again := claimAndResume(t, newResume(t, u, clock, testPolicy())); len(again) != 0 {
		t.Fatalf("terminal claimed again: %+v", again)
	}
}

func TestResumeWager_ExpiresByMaxAttempts(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	id := pendingRollback(t, u)
	clock := &testClock{now: fixedNow.Add(2 * time.Second)}
	policy := testPolicy()
	policy.MaxAttempts = 3
	uc := newResume(t, u, clock, policy)

	// Busca 2: reagenda. Busca 3 (= MaxAttempts): expira.
	if got := claimAndResume(t, uc); len(got) != 1 || got[0].Outcome != app.ResumeRescheduled {
		t.Fatalf("lookup 2: %+v", got)
	}
	clock.Advance(4 * time.Second)
	got := claimAndResume(t, uc)
	if len(got) != 1 || got[0].Outcome != app.ResumeExpired || got[0].Status != domain.WagerStatusRejected ||
		got[0].FailureCode != domain.FailureCodeReferenceNotFound {
		t.Fatalf("lookup 3: %+v", got)
	}
	w := wagerByID(t, u, id)
	if w.Status() != domain.WagerStatusRejected || w.FailureCode() != domain.FailureCodeReferenceNotFound ||
		w.ReferenceTransactionID() != uuid.Nil || w.Attempts() != 2 || w.BalanceAfter() != brl(t, "100.00") {
		t.Fatalf("persisted %s %s attempts %d", w.Status(), w.FailureCode(), w.Attempts())
	}
	if got := eventTypes(u.state.events); got != "WagerTransactionRejected" {
		t.Fatalf("events = %s", got)
	}
	if len(u.state.ledger) != 0 || u.state.wallets[unitWallet].Version() != 1 {
		t.Fatal("expiry moved the wallet")
	}
}

func TestResumeWager_ExpiresByTTLAndCapsBackoffAtDeadline(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	id := pendingRollback(t, u)
	policy := app.PendingWagerPolicy{MaxAttempts: 100, TTL: 5 * time.Second, RetryBaseDelay: 2 * time.Second, RetryMaxDelay: time.Minute, MaxResumeFailures: 3}
	clock := &testClock{now: fixedNow.Add(2 * time.Second)}
	uc := newResume(t, u, clock, policy)

	// Backoff seria 4s (fixedNow+6s), mas o TTL vence em fixedNow+5s.
	got := claimAndResume(t, uc)
	if len(got) != 1 || got[0].Outcome != app.ResumeRescheduled || !got[0].NextAttemptAt.Equal(fixedNow.Add(5*time.Second)) {
		t.Fatalf("capped reschedule: %+v", got)
	}
	clock.Advance(3 * time.Second)
	got = claimAndResume(t, uc)
	if len(got) != 1 || got[0].Outcome != app.ResumeExpired || got[0].FailureCode != domain.FailureCodeReferenceNotFound {
		t.Fatalf("ttl expiry: %+v", got)
	}
	if wagerByID(t, u, id).Status() != domain.WagerStatusRejected {
		t.Fatal("not rejected")
	}
}

// A referência que chega na última tentativa ainda é aplicada.
func TestResumeWager_LastAttemptStillResolves(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	pendingRollback(t, u)
	if _, err := newProcess(t, u).Execute(t.Context(), wagerCmd(t, domain.WagerKindBet, "bet-1", "10.00", "")); err != nil {
		t.Fatal(err)
	}
	policy := testPolicy()
	policy.MaxAttempts = 2
	clock := &testClock{now: fixedNow.Add(time.Hour)} // TTL também vencido
	got := claimAndResume(t, newResume(t, u, clock, policy))
	if len(got) != 1 || got[0].Status != domain.WagerStatusProcessed {
		t.Fatalf("result = %+v", got)
	}
}

// Referência existente mas rejeitada: rejeição definitiva, sem esperar.
func TestResumeWager_ReferenceNotProcessed(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "5.00")
	id := pendingRollback(t, u)
	// A BET chega e é rejeitada por saldo.
	res, err := newProcess(t, u).Execute(t.Context(), wagerCmd(t, domain.WagerKindBet, "bet-1", "10.00", ""))
	if err != nil || res.FailureCode != domain.FailureCodeInsufficientFunds {
		t.Fatalf("bet: %+v %v", res, err)
	}
	u.state.events = nil
	clock := &testClock{now: fixedNow.Add(2 * time.Second)}
	got := claimAndResume(t, newResume(t, u, clock, testPolicy()))
	if len(got) != 1 || got[0].Outcome != app.ResumeResolved || got[0].FailureCode != domain.FailureCodeReferenceNotProcessed {
		t.Fatalf("result = %+v", got)
	}
	if wagerByID(t, u, id).Status() != domain.WagerStatusRejected || eventTypes(u.state.events) != "WagerTransactionRejected" {
		t.Fatal("rejection not persisted with event")
	}
}

// Fencing: lease reassumido por outro token → nada é gravado.
func TestResumeWager_ClaimLostIsSkipped(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	id := pendingRollback(t, u)
	clock := &testClock{now: fixedNow.Add(2 * time.Second)}
	uc := newResume(t, u, clock, testPolicy())
	old := uuid.New()
	if c, err := uc.Claim(t.Context(), app.ClaimRequest{Token: old, Lease: time.Second, Limit: 5}); err != nil || len(c) != 1 {
		t.Fatalf("claim: %v %v", c, err)
	}
	// O worker "morre"; o lease expira e outra instância reassume.
	clock.Advance(2 * time.Second)
	newer := uuid.New()
	if c, err := uc.Claim(t.Context(), app.ClaimRequest{Token: newer, Lease: time.Minute, Limit: 5}); err != nil || len(c) != 1 {
		t.Fatalf("reclaim: %v %v", c, err)
	}
	r, err := uc.Resume(t.Context(), id, old)
	if err != nil || r.Outcome != app.ResumeSkipped {
		t.Fatalf("stale owner: %+v %v", r, err)
	}
	if w := wagerByID(t, u, id); w.Attempts() != 1 {
		t.Fatalf("stale owner wrote: attempts %d", w.Attempts())
	}
	r, err = uc.Resume(t.Context(), id, newer)
	if err != nil || r.Outcome != app.ResumeRescheduled || r.Attempts != 2 {
		t.Fatalf("new owner: %+v %v", r, err)
	}
	// Inexistente e inválidos.
	if r, err := uc.Resume(t.Context(), uuid.New(), newer); err != nil || r.Outcome != app.ResumeSkipped {
		t.Fatalf("missing: %+v %v", r, err)
	}
	if _, err := uc.Resume(t.Context(), uuid.Nil, newer); !errors.Is(err, app.ErrInvalidInput) {
		t.Fatalf("nil id: %v", err)
	}
	if _, err := uc.Claim(t.Context(), app.ClaimRequest{Lease: time.Second, Limit: 1}); !errors.Is(err, app.ErrInvalidInput) {
		t.Fatalf("nil token: %v", err)
	}
}

// insertOrphan grava um PENDING confirmado (aceite interrompido).
func insertOrphan(t *testing.T, u *memUoW, kind domain.WagerKind, ext, amount, ref string) uuid.UUID {
	t.Helper()
	tx, err := domain.NewExternalWagerTransaction(domain.NewExternalWagerTransactionParams{
		ID: uuid.New(), ProviderID: "provider-a", ExternalTransactionID: ext, IdempotencyKey: "provider-a:" + ext,
		RequestHash: strings.Repeat("a", 64), WalletID: unitWallet, PlayerID: unitPlayer,
		RoundID: "round-1", GameID: "game-1", Kind: kind, Money: brl(t, amount),
		ReferenceExternalTransactionID: ref, Now: fixedNow,
	})
	if err != nil {
		t.Fatal(err)
	}
	u.state.wagers = append(u.state.wagers, tx)
	return tx.ID()
}

func TestResumeWager_OrphanPending(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	bet := insertOrphan(t, u, domain.WagerKindBet, "orphan-bet", "30.00", "")
	refund := insertOrphan(t, u, domain.WagerKindRefund, "orphan-refund", "5.00", "missing-bet")
	clock := &testClock{now: fixedNow.Add(500 * time.Millisecond)}
	uc := newResume(t, u, clock, testPolicy())
	if got := claimAndResume(t, uc); len(got) != 0 {
		t.Fatalf("claimed inside the grace period: %+v", got)
	}
	clock.Advance(time.Second)
	got := claimAndResume(t, uc)
	if len(got) != 2 {
		t.Fatalf("results = %+v", got)
	}
	byID := map[uuid.UUID]app.ResumeResult{got[0].TransactionID: got[0], got[1].TransactionID: got[1]}
	if r := byID[bet]; r.Outcome != app.ResumeResolved || r.Status != domain.WagerStatusProcessed || r.PreviousStatus != domain.WagerStatusPending {
		t.Fatalf("orphan bet: %+v", r)
	}
	if r := byID[refund]; r.Outcome != app.ResumeRescheduled || r.Status != domain.WagerStatusPendingReference || r.Attempts != 1 {
		t.Fatalf("orphan refund: %+v", r)
	}
	if bal := u.state.wallets[unitWallet].Balance(); bal != brl(t, "70.00") {
		t.Fatalf("balance %s", bal)
	}
	// PENDING → PENDING_REFERENCE emite o evento de pendência (ADR-012).
	if got := eventTypes(u.state.events); got != "WagerTransactionProcessed,WalletBalanceChanged,WagerTransactionPendingReference" {
		t.Fatalf("events = %s", got)
	}
}
