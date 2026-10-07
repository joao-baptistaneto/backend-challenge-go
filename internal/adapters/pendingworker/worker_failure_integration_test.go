//go:build integration

package pendingworker

// Integração do limite de falhas da retomada (T30, ADR-031) contra
// PostgreSQL real: itens envenenados terminam FAILED RETRIES_EXHAUSTED (sem
// loop infinito, sem mexer em saldo, ledger ou outbox), um item saudável do
// mesmo lote é resolvido normalmente, o replay devolve o estado final e a
// reconciliação continua íntegra.

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

type failureRow struct {
	status, failure string
	failures        int32
	claimed         bool
	completed       bool
	updatedAt       time.Time
}

func (i instance) failureRow(t *testing.T, id uuid.UUID) failureRow {
	t.Helper()
	var (
		r       failureRow
		failure *string
	)
	if err := i.db.Pool().QueryRow(t.Context(), `SELECT status, failure_code, resume_failures,
  claimed_by IS NOT NULL, completed_at IS NOT NULL, updated_at
FROM wager_transactions WHERE id = $1`, id).Scan(&r.status, &failure, &r.failures, &r.claimed, &r.completed, &r.updatedAt); err != nil {
		t.Fatal(err)
	}
	if failure != nil {
		r.failure = *failure
	}
	return r
}

// corruption descreve como insertPoisonedWager estraga a linha: valores que
// passam nos CHECKs do banco mas não reidratam no domínio.
type corruption struct {
	round    string // round_id (ex.: com caractere de controle)
	currency string // currency (ex.: ISO bem formado, mas não suportado)
}

var (
	corruptRound        = corruption{round: "round-1\x01", currency: "BRL"}
	unsupportedCurrency = corruption{round: "round-1", currency: "XYZ"}
)

// insertCorruptWager grava, por SQL, uma linha que passa nos CHECKs do banco
// mas não reidrata no domínio (round_id com caractere de controle): o
// "item envenenado" típico (dado legado, escrita fora da aplicação).
func (i instance) insertCorruptWager(t *testing.T, w walletRef, kind domain.WagerKind, status domain.WagerStatus, ext, ref string) uuid.UUID {
	t.Helper()
	return i.insertPoisonedWager(t, w, kind, status, ext, ref, corruptRound)
}

// insertPoisonedWager grava, por SQL, uma linha estragada conforme c.
func (i instance) insertPoisonedWager(t *testing.T, w walletRef, kind domain.WagerKind, status domain.WagerStatus, ext, ref string, c corruption) uuid.UUID {
	t.Helper()
	id := uuid.Must(uuid.NewV7())
	now := time.Now().UTC()
	var (
		refArg      any
		attempts    int32
		nextAttempt any
	)
	if ref != "" {
		refArg = ref
	}
	if status == domain.WagerStatusPendingReference {
		attempts, nextAttempt = 1, now
	}
	_, err := i.db.Pool().Exec(t.Context(), `INSERT INTO wager_transactions (
  id, origin, kind, status, wallet_id, player_id, amount, currency,
  provider_id, external_transaction_id, idempotency_key, request_hash, round_id, game_id,
  reference_external_transaction_id, attempts, next_attempt_at, created_at, updated_at)
VALUES ($1, 'EXTERNAL', $2, $3, $4, $5, 1000, $14, 'provider-a', $6, $7, $8, $9, 'game-1', $10, $11, $12, $13, $13)`,
		id, kind.String(), status.String(), w.id, w.player, ext, "provider-a:"+ext, strings.Repeat("ab", 32),
		c.round, refArg, attempts, nextAttempt, now, c.currency)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func failurePolicy() app.PendingWagerPolicy {
	p := fastPolicy()
	p.MaxResumeFailures = 3
	return p
}

// A própria linha não reidrata: o worker conta as falhas com backoff e, na
// terceira, grava FAILED sem o agregado (domain.ValidateFailTransition +
// FailClaimedUnreadable). Um ROLLBACK saudável do mesmo ciclo é resolvido.
// Cobre as formas de reidratação inválida, inclusive a do Money (moeda bem
// formada para o CHECK do banco, mas não suportada pelo domínio): sem
// ErrCorruptedWagerTransaction, a gravação de FAILED desfazia o contador e o
// item ficava em loop para sempre.
func TestIntegrationWorker_UnreadableRowEndsFailed(t *testing.T) {
	for name, c := range map[string]corruption{"control char in round": corruptRound, "unsupported currency": unsupportedCurrency} {
		t.Run(name, func(t *testing.T) { testUnreadableRowEndsFailed(t, c) })
	}
}

func testUnreadableRowEndsFailed(t *testing.T, c corruption) {
	inst := newInstance(t, migratedDSN(t), failurePolicy())
	w := inst.openWallet(t, "100.00")
	poisoned := inst.insertPoisonedWager(t, w, domain.WagerKindRollback, domain.WagerStatusPendingReference, "rb-bad", "bet-x", c)
	healthy := inst.wager(t, w, domain.WagerKindRollback, "rb-1", "25.00", "bet-1")
	inst.wager(t, w, domain.WagerKindBet, "bet-1", "25.00", "") // 100 → 75

	inst.startWorker(t, fastConfig())

	eventually(t, "poisoned row FAILED", 10*time.Second, func() bool { return inst.failureRow(t, poisoned).status == "FAILED" })
	eventually(t, "healthy rollback", 10*time.Second, func() bool { return inst.row(t, healthy.TransactionID).status == "PROCESSED" })
	r := inst.failureRow(t, poisoned)
	if r.failure != "RETRIES_EXHAUSTED" || r.failures != 3 || r.claimed || !r.completed {
		t.Fatalf("poisoned row = %+v", r)
	}
	// Sem loop: terminal, não é mais reivindicada nem alterada.
	time.Sleep(300 * time.Millisecond)
	if again := inst.failureRow(t, poisoned); again != r {
		t.Fatalf("FAILED row changed: %+v -> %+v", r, again)
	}
	if inst.ledgerEntries(t, poisoned) != 0 || inst.count(t, `SELECT count(*) FROM outbox WHERE aggregate_id = $1`, poisoned.String()) != 0 {
		t.Fatal("FAILED must not create ledger entries or events")
	}
	// Saldo 100 → 75 (BET) → 100 (ROLLBACK saudável); ledger confere.
	inst.assertReconciled(t, w.id, 10000)
	rec, err := app.NewReconcileWallet(inst.uow)
	if err != nil {
		t.Fatal(err)
	}
	res, err := rec.Execute(t.Context(), app.ReconcileCommand{WalletID: w.id})
	if err != nil || !res.Consistent {
		t.Fatalf("reconcile = %+v, %v", res, err)
	}
}

// A linha reidrata, mas a referência que ela cita não (resolver falha
// sempre): FAILED pelo domínio (WagerTransaction.Fail + Update), mantendo
// attempts; o replay HTTP/SQS (ProcessWagerTransaction) devolve FAILED.
func TestIntegrationWorker_PermanentResumeErrorEndsFailedAndReplays(t *testing.T) {
	inst := newInstance(t, migratedDSN(t), failurePolicy())
	w := inst.openWallet(t, "100.00")
	pending := inst.wager(t, w, domain.WagerKindRefund, "refund-1", "10.00", "bet-1")
	if pending.Status != domain.WagerStatusPendingReference {
		t.Fatalf("accept = %+v", pending)
	}
	// A "referência" chega corrompida (não reidrata): toda retomada falha.
	inst.insertCorruptWager(t, w, domain.WagerKindBet, domain.WagerStatusPending, "bet-1", "")

	inst.startWorker(t, fastConfig())
	eventually(t, "FAILED", 10*time.Second, func() bool { return inst.failureRow(t, pending.TransactionID).status == "FAILED" })
	r := inst.failureRow(t, pending.TransactionID)
	if r.failure != "RETRIES_EXHAUSTED" || r.failures != 3 || r.claimed || !r.completed {
		t.Fatalf("failed row = %+v", r)
	}
	if got := inst.row(t, pending.TransactionID); got.attempts != 1 {
		t.Fatalf("attempts changed by failures: %+v", got)
	}
	if inst.ledgerEntries(t, pending.TransactionID) != 0 ||
		inst.events(t, pending.TransactionID, "WagerTransactionPendingReference") != 1 ||
		inst.count(t, `SELECT count(*) FROM outbox WHERE aggregate_id = $1`, pending.TransactionID.String()) != 1 {
		t.Fatal("FAILED must not create ledger entries or new events")
	}
	inst.assertReconciled(t, w.id, 10000)

	replay := inst.wager(t, w, domain.WagerKindRefund, "refund-1", "10.00", "bet-1")
	if !replay.IdempotentReplay || replay.Status != domain.WagerStatusFailed ||
		replay.FailureCode != domain.FailureCodeRetriesExhausted || replay.TransactionID != pending.TransactionID {
		t.Fatalf("replay = %+v", replay)
	}
	if r2 := inst.failureRow(t, pending.TransactionID); r2 != r {
		t.Fatalf("replay changed the row: %+v -> %+v", r, r2)
	}
}

// Uma retomada bem-sucedida zera o contador; o adapter respeita o fencing
// (token errado não registra nada) e só aceita transição válida.
func TestIntegrationResume_FailureCounterFencingAndReset(t *testing.T) {
	inst := newInstance(t, migratedDSN(t), failurePolicy())
	w := inst.openWallet(t, "100.00")
	pending := inst.wager(t, w, domain.WagerKindRollback, "rb-1", "10.00", "bet-1")
	corrupt := inst.insertCorruptWager(t, w, domain.WagerKindBet, domain.WagerStatusPending, "bet-1", "")

	token := uuid.New()
	time.Sleep(2 * testReferenceDelay)
	claimed, err := inst.resume.Claim(t.Context(), app.ClaimRequest{Token: token, Lease: time.Minute, Limit: 10, OrphanGrace: time.Hour})
	if err != nil || len(claimed) != 1 || claimed[0].ID != pending.TransactionID {
		t.Fatalf("claim = %+v, %v", claimed, err)
	}
	// Token alheio: falha não registrada.
	if res, err := inst.resume.Resume(t.Context(), pending.TransactionID, uuid.New()); err != nil || res.Outcome != app.ResumeSkipped {
		t.Fatalf("foreign token = %+v, %v", res, err)
	}
	res, err := inst.resume.Resume(t.Context(), pending.TransactionID, token)
	if err == nil || res.Outcome != app.ResumeDeferred || res.ResumeFailures != 1 {
		t.Fatalf("first failure = %+v, %v", res, err)
	}
	if r := inst.failureRow(t, pending.TransactionID); r.failures != 1 || !r.claimed || r.status != "PENDING_REFERENCE" {
		t.Fatalf("after failure = %+v", r)
	}
	// Retido pelo backoff: um claim imediato não o devolve.
	if again, err := inst.resume.Claim(t.Context(), app.ClaimRequest{Token: uuid.New(), Lease: time.Minute, Limit: 10, OrphanGrace: time.Hour}); err != nil || len(again) != 0 {
		t.Fatalf("claimed during backoff: %+v, %v", again, err)
	}

	// A "referência" corrompida é removida (correção operacional): a próxima
	// retomada funciona e zera o contador.
	if _, err := inst.db.Pool().Exec(t.Context(), `DELETE FROM wager_transactions WHERE id = $1`, corrupt); err != nil {
		t.Fatal(err)
	}
	time.Sleep(fastPolicy().RetryMaxDelay)
	token2 := uuid.New()
	if claimed, err := inst.resume.Claim(t.Context(), app.ClaimRequest{Token: token2, Lease: time.Minute, Limit: 10, OrphanGrace: time.Hour}); err != nil || len(claimed) != 1 {
		t.Fatalf("reclaim = %+v, %v", claimed, err)
	}
	if res, err := inst.resume.Resume(t.Context(), pending.TransactionID, token2); err != nil || res.Outcome != app.ResumeRescheduled {
		t.Fatalf("resume after fix = %+v, %v", res, err)
	}
	if r := inst.failureRow(t, pending.TransactionID); r.failures != 0 || r.claimed {
		t.Fatalf("counter not reset = %+v", r)
	}
}
