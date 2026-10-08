package app_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// Testes unitários do limite de falhas da retomada (ADR-031): item
// envenenado → FAILED RETRIES_EXHAUSTED, transitórias não contam, sucesso
// zera o contador, TTL encerra, fencing e replay do estado final.

var errCorruptedRow = fmt.Errorf("mem: %w: round id has control characters", domain.ErrCorruptedWagerTransaction)

func failurePolicy(maxFailures int) app.PendingWagerPolicy {
	return app.PendingWagerPolicy{MaxAttempts: 100, TTL: time.Hour, RetryBaseDelay: 2 * time.Second,
		RetryMaxDelay: 10 * time.Second, MaxResumeFailures: maxFailures}
}

// claimOne reivindica o que venceu (lease de 1 min) e devolve o token e os ids.
func claimOne(t *testing.T, uc *app.ResumeWagerTransaction) (uuid.UUID, []uuid.UUID) {
	t.Helper()
	token := uuid.New()
	claimed, err := uc.Claim(t.Context(), app.ClaimRequest{Token: token, Lease: time.Minute, Limit: 10, OrphanGrace: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]uuid.UUID, len(claimed))
	for i, c := range claimed {
		ids[i] = c.ID
	}
	return token, ids
}

func assertNoEffects(t *testing.T, u *memUoW) {
	t.Helper()
	if len(u.state.events) != 0 || len(u.state.ledger) != 0 || u.state.wallets[unitWallet].Version() != 1 ||
		u.state.wallets[unitWallet].Balance() != brl(t, "100.00") {
		t.Fatalf("failure path produced effects: events %s, ledger %d", eventTypes(u.state.events), len(u.state.ledger))
	}
}

// A própria linha não reidrata: falhas contadas com backoff exponencial
// (2s, 4s) e, na 3ª, FAILED pelo caminho sem agregado. Depois disso o item
// nunca mais é reivindicado (sem loop infinito) e o replay devolve FAILED.
func TestResumeWager_UnreadableRowEndsFailed(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	id := pendingRollback(t, u)
	u.poison[id] = errCorruptedRow
	clock := &testClock{now: fixedNow.Add(2 * time.Second)}
	uc := newResume(t, u, clock, failurePolicy(3))

	for i, hold := range []time.Duration{2 * time.Second, 4 * time.Second} {
		token, ids := claimOne(t, uc)
		if len(ids) != 1 || ids[0] != id {
			t.Fatalf("step %d: claimed %v", i, ids)
		}
		res, err := uc.Resume(t.Context(), id, token)
		if !errors.Is(err, domain.ErrCorruptedWagerTransaction) || res.Outcome != app.ResumeDeferred ||
			res.ResumeFailures != int32(i+1) || !res.HeldUntil.Equal(clock.Now().Add(hold)) {
			t.Fatalf("step %d: %+v, %v", i, res, err)
		}
		if got := u.state.failures[id]; got != int32(i+1) {
			t.Fatalf("step %d: persisted failures %d", i, got)
		}
		// Retido pelo backoff: não volta antes do prazo.
		clock.Advance(hold - time.Millisecond)
		if _, again := claimOne(t, uc); len(again) != 0 {
			t.Fatalf("step %d: claimed during backoff", i)
		}
		clock.Advance(time.Millisecond)
	}

	token, ids := claimOne(t, uc)
	if len(ids) != 1 {
		t.Fatalf("last claim: %v", ids)
	}
	res, err := uc.Resume(t.Context(), id, token)
	if err != nil || res.Outcome != app.ResumeFailed || res.Status != domain.WagerStatusFailed ||
		res.FailureCode != domain.FailureCodeRetriesExhausted || res.ResumeFailures != 3 ||
		res.PreviousStatus != domain.WagerStatusPendingReference || !errors.Is(res.Cause, domain.ErrCorruptedWagerTransaction) {
		t.Fatalf("final: %+v, %v", res, err)
	}
	if u.unreadableFailed != 1 {
		t.Fatalf("unreadable path used %d times", u.unreadableFailed)
	}
	w := wagerByID(t, u, id)
	if w.Status() != domain.WagerStatusFailed || w.FailureCode() != domain.FailureCodeRetriesExhausted ||
		!w.NextAttemptAt().IsZero() || w.CompletedAt().IsZero() || w.BalanceAfter() != (domain.Money{}) {
		t.Fatalf("persisted %s %s", w.Status(), w.FailureCode())
	}
	if u.state.failures[id] != 3 || len(u.state.claims) != 0 {
		t.Fatalf("failures %d (kept for audit), claims %d", u.state.failures[id], len(u.state.claims))
	}
	assertNoEffects(t, u)

	// Terminal: nunca mais reivindicado.
	clock.Advance(24 * time.Hour)
	if _, again := claimOne(t, uc); len(again) != 0 {
		t.Fatal("FAILED claimed again")
	}

	// Replay devolve o estado final persistido.
	delete(u.poison, id)
	replay, err := newProcess(t, u).Execute(t.Context(), wagerCmd(t, domain.WagerKindRollback, "rb-1", "10.00", "bet-1"))
	if err != nil || !replay.IdempotentReplay || replay.Status != domain.WagerStatusFailed ||
		replay.FailureCode != domain.FailureCodeRetriesExhausted || replay.TransactionID != id {
		t.Fatalf("replay = %+v, %v", replay, err)
	}
	assertNoEffects(t, u)
}

// A linha reidrata, mas a retomada falha sempre (carteira corrompida): FAILED
// pelo domínio (WagerTransaction.Fail + Update), sem lock de carteira.
func TestResumeWager_PermanentErrorEndsFailedViaDomain(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	id := pendingRollback(t, u)
	u.walletErr = fmt.Errorf("mem: %w: negative balance", domain.ErrInvalidWallet)
	clock := &testClock{now: fixedNow.Add(2 * time.Second)}
	uc := newResume(t, u, clock, failurePolicy(2))

	token, _ := claimOne(t, uc)
	if res, err := uc.Resume(t.Context(), id, token); err == nil || res.Outcome != app.ResumeDeferred {
		t.Fatalf("first: %+v, %v", res, err)
	}
	clock.Advance(2 * time.Second)
	token, _ = claimOne(t, uc)
	res, err := uc.Resume(t.Context(), id, token)
	if err != nil || res.Outcome != app.ResumeFailed || res.Kind != domain.WagerKindRollback ||
		res.WalletID != unitWallet || res.Attempts != 1 || res.FailureCode != domain.FailureCodeRetriesExhausted {
		t.Fatalf("second: %+v, %v", res, err)
	}
	if u.unreadableFailed != 0 {
		t.Fatal("unreadable path used for a readable row")
	}
	if w := wagerByID(t, u, id); w.Status() != domain.WagerStatusFailed || w.Attempts() != 1 {
		t.Fatalf("persisted %s attempts %d", w.Status(), w.Attempts())
	}
	assertNoEffects(t, u)
}

// Falhas transitórias (banco indisponível) e cancelamento não contam: o item
// continua pendente e volta pelo lease; quando a infraestrutura volta, é
// resolvido normalmente.
func TestResumeWager_TransientErrorsDoNotCount(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	id := pendingRollback(t, u)
	u.walletErr = fmt.Errorf("%w: connection refused", app.ErrUnavailable)
	clock := &testClock{now: fixedNow.Add(2 * time.Second)}
	uc := newResume(t, u, clock, failurePolicy(1))

	for i := range 5 {
		token, ids := claimOne(t, uc)
		if len(ids) != 1 {
			t.Fatalf("round %d: claimed %v", i, ids)
		}
		res, err := uc.Resume(t.Context(), id, token)
		if !errors.Is(err, app.ErrUnavailable) || res.Outcome != "" {
			t.Fatalf("round %d: %+v, %v", i, res, err)
		}
		if len(u.state.failures) != 0 {
			t.Fatalf("round %d: transient failure counted", i)
		}
		clock.Advance(time.Minute) // o lease expira
	}
	// Cancelamento (shutdown) também não conta.
	u.walletErr = errors.New("mem: boom")
	token, _ := claimOne(t, uc)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := uc.Resume(ctx, id, token); err == nil {
		t.Fatal("canceled resume succeeded")
	}
	if len(u.state.failures) != 0 || wagerByID(t, u, id).Status() != domain.WagerStatusPendingReference {
		t.Fatal("canceled resume counted")
	}

	u.walletErr = nil
	clock.Advance(time.Minute)
	token, _ = claimOne(t, uc)
	if res, err := uc.Resume(t.Context(), id, token); err != nil || res.Outcome != app.ResumeRescheduled {
		t.Fatalf("after recovery: %+v, %v", res, err)
	}
}

// Uma retomada bem-sucedida zera o contador: só falhas CONSECUTIVAS contam.
func TestResumeWager_SuccessResetsConsecutiveFailures(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	id := pendingRollback(t, u)
	clock := &testClock{now: fixedNow.Add(2 * time.Second)}
	uc := newResume(t, u, clock, failurePolicy(2))

	u.walletErr = errors.New("mem: boom")
	token, _ := claimOne(t, uc)
	if res, err := uc.Resume(t.Context(), id, token); err == nil || res.ResumeFailures != 1 {
		t.Fatalf("failure: %+v, %v", res, err)
	}
	u.walletErr = nil
	clock.Advance(2 * time.Second)
	token, _ = claimOne(t, uc)
	if res, err := uc.Resume(t.Context(), id, token); err != nil || res.Outcome != app.ResumeRescheduled {
		t.Fatalf("success: %+v, %v", res, err)
	}
	if _, ok := u.state.failures[id]; ok {
		t.Fatal("counter not reset by a successful resume")
	}
	// Nova falha recomeça em 1 (com MaxResumeFailures 2 ainda não encerra).
	u.walletErr = errors.New("mem: boom")
	clock.Advance(time.Minute)
	token, _ = claimOne(t, uc)
	if res, err := uc.Resume(t.Context(), id, token); err == nil || res.Outcome != app.ResumeDeferred || res.ResumeFailures != 1 {
		t.Fatalf("after reset: %+v, %v", res, err)
	}
}

// Depois do TTL, a primeira falha não transitória já encerra.
func TestResumeWager_FailureAfterTTLEndsImmediately(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	id := pendingRollback(t, u)
	u.walletErr = errors.New("mem: boom")
	policy := failurePolicy(100)
	policy.TTL = 5 * time.Second
	clock := &testClock{now: fixedNow.Add(5 * time.Second)}
	uc := newResume(t, u, clock, policy)
	token, _ := claimOne(t, uc)
	res, err := uc.Resume(t.Context(), id, token)
	if err != nil || res.Outcome != app.ResumeFailed || res.ResumeFailures != 1 {
		t.Fatalf("%+v, %v", res, err)
	}
	if wagerByID(t, u, id).Status() != domain.WagerStatusFailed {
		t.Fatal("not FAILED")
	}
}

// Fencing: quem perdeu o lease não registra a falha (o dono atual decide).
func TestResumeWager_FailureNotRecordedAfterLeaseLost(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	id := pendingRollback(t, u)
	u.poison[id] = errCorruptedRow
	clock := &testClock{now: fixedNow.Add(2 * time.Second)}
	uc := newResume(t, u, clock, failurePolicy(1))
	stale, _ := claimOne(t, uc)
	clock.Advance(2 * time.Minute)
	if _, ids := claimOne(t, uc); len(ids) != 1 { // outra instância reassume
		t.Fatal("not reclaimed")
	}
	res, err := uc.Resume(t.Context(), id, stale)
	if !errors.Is(err, domain.ErrCorruptedWagerTransaction) || res.Outcome != "" {
		t.Fatalf("stale: %+v, %v", res, err)
	}
	if len(u.state.failures) != 0 || wagerByID(t, u, id).Status() != domain.WagerStatusPendingReference {
		t.Fatal("stale owner recorded the failure")
	}
}

// resumeRound espera o backoff, reivindica o item e o retoma.
func resumeRound(t *testing.T, uc *app.ResumeWagerTransaction, clock *testClock, id uuid.UUID, wait time.Duration) (app.ResumeResult, error) {
	t.Helper()
	clock.Advance(wait)
	token, ids := claimOne(t, uc)
	if len(ids) != 1 || ids[0] != id {
		t.Fatalf("claimed %v, want %s", ids, id)
	}
	return uc.Resume(t.Context(), id, token)
}

// A linha não reidrata por um erro que NÃO é ErrCorruptedWagerTransaction
// (ex.: Money inválido de um adapter que não marca o erro). Antes da correção,
// a gravação de FAILED devolvia esse erro, desfazia o contador junto e o item
// ficava preso em MaxResumeFailures−1 para sempre. Agora a contagem é durável
// e FAILED sai pelo caminho sem agregado.
func TestResumeWager_AnyUnreadableErrorEndsFailed(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	id := pendingRollback(t, u)
	u.poison[id] = errors.New("mem: amount: unsupported currency XYZ")
	clock := &testClock{now: fixedNow.Add(2 * time.Second)}
	uc := newResume(t, u, clock, failurePolicy(3))

	for i, wait := range []time.Duration{0, 2 * time.Second, 4 * time.Second} {
		res, err := resumeRound(t, uc, clock, id, wait)
		if i < 2 {
			if err == nil || res.Outcome != app.ResumeDeferred || res.ResumeFailures != int32(i+1) || u.state.failures[id] != int32(i+1) {
				t.Fatalf("round %d: %+v, %v (persisted %d)", i, res, err, u.state.failures[id])
			}
			continue
		}
		if err != nil || res.Outcome != app.ResumeFailed || res.FailureCode != domain.FailureCodeRetriesExhausted ||
			res.ResumeFailures != 3 || res.PreviousStatus != domain.WagerStatusPendingReference {
			t.Fatalf("final: %+v, %v", res, err)
		}
	}
	if u.unreadableFailed != 1 || wagerByID(t, u, id).Status() != domain.WagerStatusFailed || len(u.state.claims) != 0 {
		t.Fatalf("unreadable path %d, status %s", u.unreadableFailed, wagerByID(t, u, id).Status())
	}
	assertNoEffects(t, u)
	clock.Advance(24 * time.Hour)
	if _, again := claimOne(t, uc); len(again) != 0 {
		t.Fatal("FAILED claimed again")
	}
}

// A linha reidrata, mas gravar FAILED pelo agregado falha de forma não
// transitória (ex.: constraint): FAILED sai pelo caminho sem agregado numa
// transação nova, sem efeitos.
func TestResumeWager_AggregateFailWriteErrorFallsBack(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	id := pendingRollback(t, u)
	u.walletErr = errors.New("mem: boom")
	u.failUpdateErr = errors.New("mem: check constraint violated")
	clock := &testClock{now: fixedNow.Add(2 * time.Second)}
	uc := newResume(t, u, clock, failurePolicy(1))

	res, err := resumeRound(t, uc, clock, id, 0)
	if err != nil || res.Outcome != app.ResumeFailed || res.ResumeFailures != 1 || res.FailureCode != domain.FailureCodeRetriesExhausted {
		t.Fatalf("%+v, %v", res, err)
	}
	if u.unreadableFailed != 1 || wagerByID(t, u, id).Status() != domain.WagerStatusFailed {
		t.Fatal("fallback not used")
	}
	assertNoEffects(t, u)
}

// Se nem o caminho sem agregado consegue gravar FAILED (erro permanente ou
// transitório), a contagem da rodada continua durável: cada rodada avança o
// contador e o item é retido pelo backoff (no máximo RetryMaxDelay); quando a
// gravação volta a funcionar, a rodada seguinte encerra em FAILED.
func TestResumeWager_FailWriteErrorKeepsCountDurable(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	id := pendingRollback(t, u)
	u.poison[id] = errCorruptedRow
	clock := &testClock{now: fixedNow.Add(2 * time.Second)}
	uc := newResume(t, u, clock, failurePolicy(2))

	if res, err := resumeRound(t, uc, clock, id, 0); err == nil || res.ResumeFailures != 1 {
		t.Fatalf("round 1: %+v, %v", res, err)
	}
	for i, writeErr := range []error{errors.New("mem: trigger rejected"), fmt.Errorf("%w: connection reset", app.ErrUnavailable)} {
		u.failUnreadableErr = writeErr
		res, err := resumeRound(t, uc, clock, id, 10*time.Second)
		want := int32(i + 2)
		if !errors.Is(err, domain.ErrCorruptedWagerTransaction) || res.Outcome != app.ResumeDeferred || res.ResumeFailures != want ||
			u.state.failures[id] != want || !res.HeldUntil.Equal(clock.Now().Add(failurePolicy(2).Delay(want))) {
			t.Fatalf("round %d: %+v, %v (persisted %d)", i+2, res, err, u.state.failures[id])
		}
		if wagerByID(t, u, id).Status() != domain.WagerStatusPendingReference {
			t.Fatalf("round %d: status changed", i+2)
		}
		// Retido: não volta antes do backoff.
		if _, again := claimOne(t, uc); len(again) != 0 {
			t.Fatalf("round %d: claimed during backoff", i+2)
		}
	}
	u.failUnreadableErr = nil
	res, err := resumeRound(t, uc, clock, id, 10*time.Second)
	if err != nil || res.Outcome != app.ResumeFailed || res.ResumeFailures != 4 {
		t.Fatalf("final: %+v, %v", res, err)
	}
	if u.state.failures[id] != 4 || wagerByID(t, u, id).Status() != domain.WagerStatusFailed {
		t.Fatal("not FAILED")
	}
	assertNoEffects(t, u)
}
