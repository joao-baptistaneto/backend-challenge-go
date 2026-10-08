package app_test

import (
	"slices"
	"testing"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// Wake-up de pendências quando a referência chega (ADR-032), sobre a
// UnitOfWork em memória (process_wager_test.go), que espelha o adapter.

// A referência gravada num estado terminal acorda, na mesma transação, a
// reversão PENDING_REFERENCE do mesmo provedor: o worker a resolve no ciclo
// seguinte, sem esperar o backoff de 1h. A do outro provedor continua
// agendada.
func TestWakePending_ReferenceArrivalWakesSameProviderOnly(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	slow := newProcess(t, u, app.WithReferenceRetryDelay(time.Hour))
	rb, err := slow.Execute(t.Context(), wagerCmd(t, domain.WagerKindRollback, "rb-1", "10.00", "bet-1"))
	if err != nil || rb.Status != domain.WagerStatusPendingReference {
		t.Fatalf("rollback = %+v, %v", rb, err)
	}
	other := wagerCmd(t, domain.WagerKindRollback, "rb-b", "10.00", "bet-1")
	other.ProviderID, other.IdempotencyKey = "provider-b", "provider-b:rb-b"
	rbB, err := slow.Execute(t.Context(), other)
	if err != nil || rbB.Status != domain.WagerStatusPendingReference {
		t.Fatalf("other provider rollback = %+v, %v", rbB, err)
	}
	if len(u.state.wakeCalls) != 0 {
		t.Fatalf("pending outcome woke dependents: %v", u.state.wakeCalls)
	}

	clock := &testClock{now: fixedNow}
	resume := newResume(t, u, clock, app.PendingWagerPolicy{
		MaxAttempts: 10, TTL: 24 * time.Hour, RetryBaseDelay: time.Hour, RetryMaxDelay: time.Hour, MaxResumeFailures: 3,
	})
	if got := claimAndResume(t, resume); len(got) != 0 {
		t.Fatalf("claimed before the reference: %+v", got)
	}

	if _, err := newProcess(t, u).Execute(t.Context(), wagerCmd(t, domain.WagerKindBet, "bet-1", "10.00", "")); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(u.state.wakeCalls, []string{"provider-a/bet-1"}) {
		t.Fatalf("wake calls = %v", u.state.wakeCalls)
	}
	if w := wagerByID(t, u, rb.TransactionID); w.Attempts() != 1 || w.Status() != domain.WagerStatusPendingReference {
		t.Fatalf("wake changed the aggregate: attempts %d status %s", w.Attempts(), w.Status())
	}

	got := claimAndResume(t, resume) // ainda em fixedNow: nada do backoff passou
	if len(got) != 1 || got[0].TransactionID != rb.TransactionID || got[0].Outcome != app.ResumeResolved ||
		got[0].Status != domain.WagerStatusProcessed {
		t.Fatalf("resume after wake = %+v", got)
	}
	if w := wagerByID(t, u, rbB.TransactionID); w.Status() != domain.WagerStatusPendingReference ||
		!w.NextAttemptAt().Equal(fixedNow.Add(time.Hour)) {
		t.Fatalf("other provider disturbed: %s %v", w.Status(), w.NextAttemptAt())
	}
	if _, ok := u.state.woken[rbB.TransactionID]; ok {
		t.Fatal("other provider woken")
	}
	// A resolução terminal da reversão também chama o wake (pela referência
	// dela), sem efeito porque ninguém a cita.
	if !slices.Equal(u.state.wakeCalls, []string{"provider-a/bet-1", "provider-a/rb-1"}) {
		t.Fatalf("wake calls = %v", u.state.wakeCalls)
	}
}

// Referência ainda pendente não resolve a dependente: não acorda. Quando ela
// é resolvida pelo worker, a dependente é acordada no mesmo commit.
func TestWakePending_PendingReferenceDoesNotWakeUntilResolved(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	slow := newProcess(t, u, app.WithReferenceRetryDelay(time.Hour))
	// ROLLBACK do WIN; o WIN cita uma BET que ainda não chegou.
	rb, err := slow.Execute(t.Context(), wagerCmd(t, domain.WagerKindRollback, "rb-w", "5.00", "win-1"))
	if err != nil || rb.Status != domain.WagerStatusPendingReference {
		t.Fatalf("rollback = %+v, %v", rb, err)
	}
	win, err := newProcess(t, u, app.WithReferenceRetryDelay(time.Second)).
		Execute(t.Context(), wagerCmd(t, domain.WagerKindWin, "win-1", "5.00", "bet-x"))
	if err != nil || win.Status != domain.WagerStatusPendingReference {
		t.Fatalf("win = %+v, %v", win, err)
	}
	if len(u.state.wakeCalls) != 0 || len(u.state.woken) != 0 {
		t.Fatalf("pending reference woke dependents: %v", u.state.wakeCalls)
	}
	if _, err := newProcess(t, u).Execute(t.Context(), wagerCmd(t, domain.WagerKindBet, "bet-x", "1.00", "")); err != nil {
		t.Fatal(err)
	}

	// O worker resolve o WIN (vencido em 1s) e, no mesmo commit, acorda o
	// ROLLBACK; o ciclo seguinte o aplica, muito antes de 1h.
	clock := &testClock{now: fixedNow.Add(time.Second)}
	resume := newResume(t, u, clock, app.PendingWagerPolicy{
		MaxAttempts: 10, TTL: 24 * time.Hour, RetryBaseDelay: time.Hour, RetryMaxDelay: time.Hour, MaxResumeFailures: 3,
	})
	got := claimAndResume(t, resume)
	if len(got) != 1 || got[0].TransactionID != win.TransactionID || got[0].Status != domain.WagerStatusProcessed {
		t.Fatalf("win resume = %+v", got)
	}
	if woke, ok := u.state.woken[rb.TransactionID]; !ok || !woke.Equal(clock.Now()) {
		t.Fatalf("rollback not woken by the resolved win: %v %v", woke, ok)
	}
	got = claimAndResume(t, resume)
	if len(got) != 1 || got[0].TransactionID != rb.TransactionID || got[0].Status != domain.WagerStatusProcessed {
		t.Fatalf("rollback resume = %+v", got)
	}
}
