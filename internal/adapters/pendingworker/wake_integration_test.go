//go:build integration

package pendingworker

// Integração do wake-up de pendências quando a referência chega (ADR-032,
// ADR-032) contra PostgreSQL real: a reversão aceita antes da referência
// fica PENDING_REFERENCE com backoff de 1h; quando a referência é gravada, a
// mesma transação antecipa o next_attempt_at e o worker a resolve no ciclo
// seguinte. Itens de outro provedor não são acordados, um item reivindicado
// por um worker não tem lease, attempts nem contador de falhas alterados, e
// uma linha travada por outra transação é pulada sem bloquear o caminho
// quente.

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// longBackoff é o atraso de nova busca usado aqui: muito maior que qualquer
// timeout do teste, então só o wake-up explica uma resolução.
const longBackoff = time.Hour

func slowPolicy() app.PendingWagerPolicy {
	return app.PendingWagerPolicy{MaxAttempts: 1000, TTL: 24 * time.Hour, RetryBaseDelay: longBackoff, RetryMaxDelay: longBackoff, MaxResumeFailures: 3}
}

// slowProcess aceita operações com a primeira nova busca em longBackoff.
func (i instance) slowProcess(t *testing.T) *app.ProcessWagerTransaction {
	t.Helper()
	p, err := app.NewProcessWagerTransaction(i.uow, app.WithReferenceRetryDelay(longBackoff))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func (i instance) wagerAs(t *testing.T, p *app.ProcessWagerTransaction, provider string, w walletRef, kind domain.WagerKind, ext, amount, ref string) app.WagerResult {
	t.Helper()
	res, err := p.Execute(t.Context(), app.ProcessWagerCommand{
		ProviderID: provider, ExternalTransactionID: ext, IdempotencyKey: provider + ":" + ext,
		PlayerID: w.player, WalletID: w.id, RoundID: "round-1", GameID: "game-1",
		Kind: kind, Money: brlMoney(t, amount), ReferenceExternalTransactionID: ref, CorrelationID: "corr-" + ext,
	})
	if err != nil {
		t.Fatalf("%s %s %s: %v", provider, kind, ext, err)
	}
	return res
}

type scheduleRow struct {
	status        string
	attempts      int32
	nextAttemptAt *time.Time
	claimedBy     *uuid.UUID
	claimedUntil  *time.Time
	failures      int32
	updatedAt     time.Time
}

func (i instance) schedule(t *testing.T, id uuid.UUID) scheduleRow {
	t.Helper()
	var r scheduleRow
	if err := i.db.Pool().QueryRow(t.Context(), `SELECT status, attempts, next_attempt_at, claimed_by, claimed_until,
  resume_failures, updated_at FROM wager_transactions WHERE id = $1`, id).
		Scan(&r.status, &r.attempts, &r.nextAttemptAt, &r.claimedBy, &r.claimedUntil, &r.failures, &r.updatedAt); err != nil {
		t.Fatal(err)
	}
	return r
}

// A referência chega e a reversão é resolvida bem antes do backoff de 1h;
// uma referência REJECTED também resolve (REFERENCE_NOT_PROCESSED). A
// reversão de outro provedor que cita o mesmo externalId não é acordada.
func TestIntegrationWorker_WakesPendingWhenReferenceArrives(t *testing.T) {
	inst := newInstance(t, migratedDSN(t), slowPolicy())
	slow := inst.slowProcess(t)
	w := inst.openWallet(t, "100.00")
	wB := inst.openWallet(t, "100.00")

	rb := inst.wagerAs(t, slow, "provider-a", w, domain.WagerKindRollback, "rb-1", "25.00", "bet-1")
	rbRejected := inst.wagerAs(t, slow, "provider-a", w, domain.WagerKindRefund, "rf-2", "500.00", "bet-2")
	rbOther := inst.wagerAs(t, slow, "provider-b", wB, domain.WagerKindRollback, "rb-b", "25.00", "bet-1")
	for _, r := range []app.WagerResult{rb, rbRejected, rbOther} {
		if r.Status != domain.WagerStatusPendingReference {
			t.Fatalf("accept = %+v", r)
		}
	}
	otherBefore := inst.schedule(t, rbOther.TransactionID)

	inst.startWorker(t, fastConfig())
	// Sem a referência, nada vence: o worker não toca nas pendências.
	time.Sleep(300 * time.Millisecond)
	for _, id := range []uuid.UUID{rb.TransactionID, rbRejected.TransactionID} {
		if r := inst.schedule(t, id); r.status != "PENDING_REFERENCE" || r.attempts != 1 ||
			r.nextAttemptAt == nil || time.Until(*r.nextAttemptAt) < 50*time.Minute {
			t.Fatalf("before reference: %+v", r)
		}
	}

	start := time.Now()
	inst.wagerAs(t, inst.process, "provider-a", w, domain.WagerKindBet, "bet-1", "25.00", "")
	bet2 := inst.wagerAs(t, inst.process, "provider-a", w, domain.WagerKindBet, "bet-2", "500.00", "")
	if bet2.Status != domain.WagerStatusRejected || bet2.FailureCode != domain.FailureCodeInsufficientFunds {
		t.Fatalf("bet-2 = %+v", bet2)
	}
	eventually(t, "wake-up resolution", 5*time.Second, func() bool {
		return inst.row(t, rb.TransactionID).status == "PROCESSED" && inst.row(t, rbRejected.TransactionID).status == "REJECTED"
	})
	if elapsed := time.Since(start); elapsed >= longBackoff/10 {
		t.Fatalf("resolution took %v", elapsed)
	}
	if r := inst.row(t, rb.TransactionID); r.attempts != 1 || r.claimed || r.failure != "" {
		t.Fatalf("resolved rollback = %+v", r)
	}
	if r := inst.row(t, rbRejected.TransactionID); r.failure != string(domain.FailureCodeReferenceNotProcessed) || r.attempts != 1 {
		t.Fatalf("rejected refund = %+v", r)
	}
	if n := inst.ledgerEntries(t, rb.TransactionID); n != 1 {
		t.Fatalf("rollback ledger entries = %d", n)
	}
	if inst.events(t, rb.TransactionID, "WagerTransactionProcessed") != 1 ||
		inst.events(t, rbRejected.TransactionID, "WagerTransactionRejected") != 1 {
		t.Fatal("missing resolution events")
	}
	inst.assertReconciled(t, w.id, 10000)

	// Outro provedor: nada mudou (nem agendamento, nem attempts, nem lease).
	if after := inst.schedule(t, rbOther.TransactionID); after.status != "PENDING_REFERENCE" ||
		!after.nextAttemptAt.Equal(*otherBefore.nextAttemptAt) || after.attempts != otherBefore.attempts ||
		after.claimedBy != nil || !after.updatedAt.Equal(otherBefore.updatedAt) {
		t.Fatalf("other provider disturbed: before %+v after %+v", otherBefore, after)
	}
}

// Um item reivindicado por um worker (lease vigente e contador de falhas da
// retomada, ADR-031) é acordado só no agendamento: lease, attempts, resume_failures e
// updated_at ficam intactos, outro worker não o reivindica enquanto o lease
// vale, e o dono do lease o resolve com o mesmo token.
func TestIntegrationWake_ClaimedItemUndisturbed(t *testing.T) {
	inst := newInstance(t, migratedDSN(t), slowPolicy())
	slow := inst.slowProcess(t)
	w := inst.openWallet(t, "100.00")
	rb := inst.wagerAs(t, slow, "provider-a", w, domain.WagerKindRollback, "rb-1", "25.00", "bet-1")

	token := uuid.New()
	until := time.Now().Add(time.Minute).UTC().Truncate(time.Microsecond)
	if _, err := inst.db.Pool().Exec(t.Context(), `UPDATE wager_transactions
SET claimed_by = $2, claimed_until = $3, resume_failures = 2 WHERE id = $1`, rb.TransactionID, token, until); err != nil {
		t.Fatal(err)
	}
	before := inst.schedule(t, rb.TransactionID)

	inst.wagerAs(t, inst.process, "provider-a", w, domain.WagerKindBet, "bet-1", "25.00", "")
	after := inst.schedule(t, rb.TransactionID)
	if after.status != "PENDING_REFERENCE" || after.nextAttemptAt == nil || after.nextAttemptAt.After(time.Now()) {
		t.Fatalf("not woken: %+v", after)
	}
	if after.claimedBy == nil || *after.claimedBy != token || !after.claimedUntil.Equal(until) ||
		after.failures != 2 || after.attempts != before.attempts || !after.updatedAt.Equal(before.updatedAt) {
		t.Fatalf("claim disturbed: before %+v after %+v", before, after)
	}
	// O lease ainda vale: outro worker não pega o item.
	if got, err := inst.resume.Claim(t.Context(), app.ClaimRequest{Token: uuid.New(), Lease: time.Minute, Limit: 10, OrphanGrace: time.Hour}); err != nil || len(got) != 0 {
		t.Fatalf("claimed under a live lease: %+v, %v", got, err)
	}
	res, err := inst.resume.Resume(t.Context(), rb.TransactionID, token)
	if err != nil || res.Outcome != app.ResumeResolved || res.Status != domain.WagerStatusProcessed {
		t.Fatalf("resume by the lease owner = %+v, %v", res, err)
	}
	if r := inst.schedule(t, rb.TransactionID); r.claimedBy != nil || r.failures != 0 {
		t.Fatalf("after resolution = %+v", r)
	}
	inst.assertReconciled(t, w.id, 10000)
}

// Linha travada por outra transação (ex.: o worker no meio da retomada dela):
// o wake-up a pula (SKIP LOCKED) e a operação que chegou não espera o lock.
func TestIntegrationWake_SkipsLockedRowWithoutBlocking(t *testing.T) {
	inst := newInstance(t, migratedDSN(t), slowPolicy())
	slow := inst.slowProcess(t)
	w := inst.openWallet(t, "100.00")
	rb := inst.wagerAs(t, slow, "provider-a", w, domain.WagerKindRollback, "rb-1", "25.00", "bet-1")
	before := inst.schedule(t, rb.TransactionID)

	locker, err := inst.db.Pool().Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = locker.Rollback(context.Background()) }()
	if _, err := locker.Exec(t.Context(), `SELECT id FROM wager_transactions WHERE id = $1 FOR NO KEY UPDATE`, rb.TransactionID); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	start := time.Now()
	bet, err := inst.process.Execute(ctx, app.ProcessWagerCommand{
		ProviderID: "provider-a", ExternalTransactionID: "bet-1", IdempotencyKey: "provider-a:bet-1",
		PlayerID: w.player, WalletID: w.id, RoundID: "round-1", GameID: "game-1",
		Kind: domain.WagerKindBet, Money: brlMoney(t, "25.00"), CorrelationID: "corr-bet-1",
	})
	if err != nil || bet.Status != domain.WagerStatusProcessed {
		t.Fatalf("bet = %+v, %v", bet, err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("bet waited %v for the locked dependent", elapsed)
	}
	if err := locker.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Pulada: mantém o backoff que já tinha (no pior caso a resolução espera
	// por ele, como antes do wake-up do ADR-032).
	if after := inst.schedule(t, rb.TransactionID); !after.nextAttemptAt.Equal(*before.nextAttemptAt) {
		t.Fatalf("locked row was updated: before %v after %v", before.nextAttemptAt, after.nextAttemptAt)
	}
}

// Uma referência que fica PENDING_REFERENCE (WIN que cita BET ausente) não
// resolve a dependente e não a acorda.
func TestIntegrationWake_PendingReferenceDoesNotWake(t *testing.T) {
	inst := newInstance(t, migratedDSN(t), slowPolicy())
	slow := inst.slowProcess(t)
	w := inst.openWallet(t, "100.00")
	rb := inst.wagerAs(t, slow, "provider-a", w, domain.WagerKindRollback, "rb-w", "5.00", "win-1")
	before := inst.schedule(t, rb.TransactionID)
	if win := inst.wagerAs(t, slow, "provider-a", w, domain.WagerKindWin, "win-1", "5.00", "bet-x"); win.Status != domain.WagerStatusPendingReference {
		t.Fatalf("win = %+v", win)
	}
	if after := inst.schedule(t, rb.TransactionID); !after.nextAttemptAt.Equal(*before.nextAttemptAt) {
		t.Fatalf("woken by a pending reference: before %v after %v", before.nextAttemptAt, after.nextAttemptAt)
	}
}
