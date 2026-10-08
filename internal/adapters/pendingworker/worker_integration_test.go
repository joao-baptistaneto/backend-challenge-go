//go:build integration

package pendingworker

// Integração do worker de pendências (ADR-023) contra PostgreSQL real:
// resolução após a chegada da referência, expiração, sobrevivência a
// restart (inclusive crash com lease gravado e PENDING órfão) e várias
// instâncias concorrentes sem duplicidade. Cada "instância" tem pool,
// UnitOfWork, casos de uso e worker próprios.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

type instance struct {
	db      *postgres.DB
	stopDB  func()
	uow     *postgres.UnitOfWork
	open    *app.OpenWallet
	process *app.ProcessWagerTransaction
	resume  *app.ResumeWagerTransaction
}

const testReferenceDelay = 50 * time.Millisecond

func fastPolicy() app.PendingWagerPolicy {
	return app.PendingWagerPolicy{MaxAttempts: 1000, TTL: time.Minute, RetryBaseDelay: testReferenceDelay, RetryMaxDelay: 200 * time.Millisecond, MaxResumeFailures: 3}
}

func fastConfig() Config {
	return Config{
		PollInterval: 20 * time.Millisecond, BatchSize: 5, Concurrency: 3, Lease: 3 * time.Second,
		ProcessTimeout: 2 * time.Second, DrainTimeout: 5 * time.Second, OrphanGrace: 100 * time.Millisecond,
	}
}

func newInstance(t *testing.T, dsn string, policy app.PendingWagerPolicy) instance {
	t.Helper()
	db, stop := startDB(t, dsn)
	uow := postgres.NewUnitOfWork(postgres.NewTxManager(db, nil, nil), postgres.NewWalletRepository(),
		postgres.NewLedgerRepository(), postgres.NewWagerTransactionRepository(),
		postgres.NewOutboxRepository(), postgres.NewInboxRepository())
	open, err := app.NewOpenWallet(uow)
	if err != nil {
		t.Fatal(err)
	}
	process, err := app.NewProcessWagerTransaction(uow, app.WithReferenceRetryDelay(testReferenceDelay))
	if err != nil {
		t.Fatal(err)
	}
	resume, err := app.NewResumeWagerTransaction(uow, policy)
	if err != nil {
		t.Fatal(err)
	}
	return instance{db: db, stopDB: stop, uow: uow, open: open, process: process, resume: resume}
}

// startWorker inicia um worker da instância; o Stop roda no cleanup (antes
// do fechamento do pool, que foi registrado antes).
func (i instance) startWorker(t *testing.T, cfg Config) *Worker {
	t.Helper()
	w, err := New(cfg, i.resume)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Stop(context.Background()); err != nil {
			t.Errorf("stop worker: %v", err)
		}
	})
	return w
}

type walletRef struct{ id, player uuid.UUID }

func (i instance) openWallet(t *testing.T, balance string) walletRef {
	t.Helper()
	m, err := domain.ParseNonNegative(balance, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	player := uuid.New()
	res, err := i.open.Execute(t.Context(), app.OpenWalletCommand{PlayerID: player, InitialBalance: m, CorrelationID: "open-" + player.String()})
	if err != nil {
		t.Fatal(err)
	}
	return walletRef{id: res.Wallet.ID, player: player}
}

func (i instance) wager(t *testing.T, w walletRef, kind domain.WagerKind, ext, amount, ref string) app.WagerResult {
	t.Helper()
	res, err := i.tryWager(t.Context(), w, kind, ext, amount, ref)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// tryWager não chama t.Fatal (pode rodar fora da goroutine do teste).
func (i instance) tryWager(ctx context.Context, w walletRef, kind domain.WagerKind, ext, amount, ref string) (app.WagerResult, error) {
	m, err := domain.ParseNonNegative(amount, "BRL")
	if err != nil {
		return app.WagerResult{}, err
	}
	res, err := i.process.Execute(ctx, app.ProcessWagerCommand{
		ProviderID: "provider-a", ExternalTransactionID: ext, IdempotencyKey: "provider-a:" + ext,
		PlayerID: w.player, WalletID: w.id, RoundID: "round-1", GameID: "game-1",
		Kind: kind, Money: m, ReferenceExternalTransactionID: ref, CorrelationID: "corr-" + ext,
	})
	if err != nil {
		return app.WagerResult{}, fmt.Errorf("%s %s: %w", kind, ext, err)
	}
	return res, nil
}

type txRow struct {
	status, failure string
	attempts        int32
	claimed         bool
}

func (i instance) row(t *testing.T, id uuid.UUID) txRow {
	t.Helper()
	var r txRow
	var failure *string
	if err := i.db.Pool().QueryRow(t.Context(), `SELECT status, failure_code, attempts, claimed_by IS NOT NULL
FROM wager_transactions WHERE id = $1`, id).Scan(&r.status, &failure, &r.attempts, &r.claimed); err != nil {
		t.Fatal(err)
	}
	if failure != nil {
		r.failure = *failure
	}
	return r
}

func (i instance) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := i.db.Pool().QueryRow(t.Context(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (i instance) events(t *testing.T, aggregate uuid.UUID, eventType string) int {
	return i.count(t, `SELECT count(*) FROM outbox WHERE aggregate_id = $1 AND event_type = $2`, aggregate.String(), eventType)
}

func (i instance) ledgerEntries(t *testing.T, txID uuid.UUID) int {
	return i.count(t, `SELECT count(*) FROM wallet_ledger_entries WHERE transaction_id = $1`, txID)
}

// assertReconciled confere saldo armazenado == créditos − débitos do ledger.
func (i instance) assertReconciled(t *testing.T, wallet uuid.UUID, wantBalance int64) {
	t.Helper()
	var stored, calculated int64
	if err := i.db.Pool().QueryRow(t.Context(), `SELECT w.balance,
  COALESCE((SELECT SUM(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END)
            FROM wallet_ledger_entries WHERE wallet_id = w.id), 0)
FROM wallets w WHERE w.id = $1`, wallet).Scan(&stored, &calculated); err != nil {
		t.Fatal(err)
	}
	if stored != calculated || stored != wantBalance {
		t.Fatalf("wallet %s: stored %d, ledger %d, want %d", wallet, stored, calculated, wantBalance)
	}
}

func eventually(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ROLLBACK chega antes da BET: o worker reagenda com backoff e, quando a BET
// chega, aplica o ROLLBACK uma única vez, com eventos no mesmo commit.
func TestIntegrationWorker_ResolvesAfterReferenceArrives(t *testing.T) {
	inst := newInstance(t, migratedDSN(t), fastPolicy())
	w := inst.openWallet(t, "100.00")
	pending := inst.wager(t, w, domain.WagerKindRollback, "rb-1", "25.00", "bet-1")
	if pending.Status != domain.WagerStatusPendingReference {
		t.Fatalf("accept = %+v", pending)
	}
	inst.startWorker(t, fastConfig())

	// Sem a referência, o worker só reagenda (attempts cresce, nada se move).
	eventually(t, "reschedules", 5*time.Second, func() bool { return inst.row(t, pending.TransactionID).attempts >= 3 })
	if r := inst.row(t, pending.TransactionID); r.status != "PENDING_REFERENCE" {
		t.Fatalf("status %s", r.status)
	}
	if n := inst.ledgerEntries(t, pending.TransactionID); n != 0 {
		t.Fatalf("ledger before reference: %d", n)
	}

	// A BET chega (100 → 75); o ROLLBACK é aplicado (75 → 100).
	bet := inst.wager(t, w, domain.WagerKindBet, "bet-1", "25.00", "")
	eventually(t, "resolution", 5*time.Second, func() bool { return inst.row(t, pending.TransactionID).status == "PROCESSED" })
	r := inst.row(t, pending.TransactionID)
	if r.claimed || r.failure != "" {
		t.Fatalf("resolved row = %+v", r)
	}
	if n := inst.ledgerEntries(t, pending.TransactionID); n != 1 {
		t.Fatalf("rollback ledger entries = %d", n)
	}
	if inst.events(t, pending.TransactionID, "WagerTransactionProcessed") != 1 ||
		inst.events(t, pending.TransactionID, "WagerTransactionPendingReference") != 1 ||
		inst.events(t, pending.TransactionID, "WagerTransactionRejected") != 0 {
		t.Fatal("unexpected events for the resolved rollback")
	}
	if n := inst.count(t, `SELECT count(*) FROM outbox WHERE event_type = 'WalletBalanceChanged' AND aggregate_id = $1`, w.id.String()); n != 3 {
		t.Fatalf("balance changed events = %d (opening, bet, rollback)", n)
	}
	inst.assertReconciled(t, w.id, 10000)

	// O replay da operação resolvida devolve o estado final, com o saldo
	// observado na resolução.
	replay := inst.wager(t, w, domain.WagerKindRollback, "rb-1", "25.00", "bet-1")
	if !replay.IdempotentReplay || replay.Status != domain.WagerStatusProcessed ||
		replay.ReferenceTransactionID != bet.TransactionID || replay.Balance != brlMoney(t, "100.00") {
		t.Fatalf("replay = %+v", replay)
	}
}

// Referência que nunca chega: REJECTED REFERENCE_NOT_FOUND + evento, sem
// movimento.
func TestIntegrationWorker_ExpiresAfterMaxAttempts(t *testing.T) {
	policy := fastPolicy()
	policy.MaxAttempts = 3
	inst := newInstance(t, migratedDSN(t), policy)
	w := inst.openWallet(t, "100.00")
	pending := inst.wager(t, w, domain.WagerKindRefund, "refund-1", "10.00", "never")
	ttlPending := inst.wager(t, w, domain.WagerKindRollback, "rb-ttl", "10.00", "never-2")

	// Uma segunda instância com TTL curto expira a outra pelo prazo.
	ttlPolicy := fastPolicy()
	ttlPolicy.TTL = 300 * time.Millisecond
	ttlInst := newInstance(t, migratedDSN(t), ttlPolicy)
	tw := ttlInst.openWallet(t, "50.00")
	byTTL := ttlInst.wager(t, tw, domain.WagerKindRollback, "rb-ttl", "10.00", "never-2")

	inst.startWorker(t, fastConfig())
	ttlInst.startWorker(t, fastConfig())

	for _, c := range []struct {
		inst     instance
		id       uuid.UUID
		attempts int32
	}{{inst, pending.TransactionID, 2}, {inst, ttlPending.TransactionID, 2}, {ttlInst, byTTL.TransactionID, -1}} {
		eventually(t, "expiry", 5*time.Second, func() bool { return c.inst.row(t, c.id).status == "REJECTED" })
		r := c.inst.row(t, c.id)
		if r.failure != "REFERENCE_NOT_FOUND" || r.claimed || (c.attempts >= 0 && r.attempts != c.attempts) {
			t.Fatalf("expired row = %+v", r)
		}
		if c.inst.events(t, c.id, "WagerTransactionRejected") != 1 || c.inst.ledgerEntries(t, c.id) != 0 {
			t.Fatal("expiry must emit exactly one rejection event and no ledger entry")
		}
	}
	inst.assertReconciled(t, w.id, 10000)
	ttlInst.assertReconciled(t, tw.id, 5000)

	// Terminal: a referência que chega depois não reabre a operação.
	inst.wager(t, w, domain.WagerKindBet, "never", "10.00", "")
	time.Sleep(200 * time.Millisecond)
	if r := inst.row(t, pending.TransactionID); r.status != "REJECTED" {
		t.Fatalf("expired operation reopened: %+v", r)
	}
	replay := inst.wager(t, w, domain.WagerKindRefund, "refund-1", "10.00", "never")
	if !replay.IdempotentReplay || replay.FailureCode != domain.FailureCodeReferenceNotFound {
		t.Fatalf("replay = %+v", replay)
	}
}

// O estado é todo persistido: parar e recriar o worker (e o pool) não perde
// pendências, attempts nem leases; um worker que morre com o lease gravado
// tem o item retomado por outra instância quando o lease expira; um PENDING
// órfão (aceite interrompido) é concluído.
func TestIntegrationWorker_SurvivesRestart(t *testing.T) {
	dsn := migratedDSN(t)
	policy := fastPolicy()
	first := newInstance(t, dsn, policy)
	w := first.openWallet(t, "100.00")
	pending := first.wager(t, w, domain.WagerKindRollback, "rb-1", "30.00", "bet-1")
	crashed := first.wager(t, w, domain.WagerKindRefund, "refund-2", "5.00", "bet-2")

	// Instância 1 roda um pouco (reagenda) e é parada.
	w1, err := New(fastConfig(), first.resume)
	if err != nil {
		t.Fatal(err)
	}
	if err := w1.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	eventually(t, "first instance reschedules", 5*time.Second, func() bool { return first.row(t, pending.TransactionID).attempts >= 2 })
	if err := w1.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	attemptsBefore := first.row(t, pending.TransactionID).attempts

	// Crash: a instância 1 reivindica refund-2 com lease e "morre" sem
	// processar (o pool é fechado).
	time.Sleep(policy.RetryMaxDelay + 50*time.Millisecond) // garante que venceu
	crashAt := time.Now()
	claimed, err := first.resume.Claim(t.Context(), app.ClaimRequest{Token: uuid.New(), Lease: 2 * time.Second, Limit: 10})
	if err != nil || len(claimed) == 0 {
		t.Fatalf("crash claim = %v, %v", claimed, err)
	}
	if !first.row(t, crashed.TransactionID).claimed {
		t.Fatal("crashed item not leased")
	}

	// PENDING órfão: aceite confirmado sem conclusão.
	orphan, err := domain.NewExternalWagerTransaction(domain.NewExternalWagerTransactionParams{
		ID: uuid.New(), ProviderID: "provider-a", ExternalTransactionID: "orphan-bet", IdempotencyKey: "provider-a:orphan-bet",
		RequestHash: fmt.Sprintf("%064x", 1), WalletID: w.id, PlayerID: w.player, RoundID: "round-1", GameID: "game-1",
		Kind: domain.WagerKindBet, Money: brlMoney(t, "10.00"), Now: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.NewWagerTransactionRepository().Insert(t.Context(), first.db.Pool(), orphan); err != nil {
		t.Fatal(err)
	}
	first.stopDB()

	// As referências chegam (outra instância), com nenhum worker rodando.
	second := newInstance(t, dsn, policy)
	second.wager(t, w, domain.WagerKindBet, "bet-1", "30.00", "")
	second.wager(t, w, domain.WagerKindBet, "bet-2", "5.00", "")
	if r := second.row(t, pending.TransactionID); r.status != "PENDING_REFERENCE" || r.attempts != attemptsBefore {
		t.Fatalf("persisted state lost across restart: %+v (attempts before %d)", r, attemptsBefore)
	}

	// Nova instância do worker: resolve tudo; o item do crash só depois do
	// lease.
	second.startWorker(t, fastConfig())
	for _, id := range []uuid.UUID{pending.TransactionID, crashed.TransactionID, orphan.ID()} {
		eventually(t, "resolution after restart", 10*time.Second, func() bool { return second.row(t, id).status == "PROCESSED" })
		if n := second.ledgerEntries(t, id); n != 1 {
			t.Fatalf("%s ledger entries = %d", id, n)
		}
		if second.events(t, id, "WagerTransactionProcessed") != 1 {
			t.Fatalf("%s processed events != 1", id)
		}
	}
	var crashedUpdated time.Time
	if err := second.db.Pool().QueryRow(t.Context(), `SELECT updated_at FROM wager_transactions WHERE id = $1`,
		crashed.TransactionID).Scan(&crashedUpdated); err != nil {
		t.Fatal(err)
	}
	if crashedUpdated.Before(crashAt.Add(2 * time.Second)) {
		t.Fatalf("leased item resumed before the lease expired (%v after crash)", crashedUpdated.Sub(crashAt))
	}
	// 100 − 30 + 30 − 5 + 5 − 10 = 90.
	second.assertReconciled(t, w.id, 9000)
}

// Várias instâncias (pool e worker próprios) disputando as mesmas
// pendências: cada uma é aplicada exatamente uma vez (ledger e outbox sem
// duplicidade) e os saldos fecham com o ledger.
func TestIntegrationWorker_MultipleInstancesNoDuplicates(t *testing.T) {
	dsn := migratedDSN(t)
	const (
		instances = 4
		wallets   = 5
		perWallet = 8
	)
	cfg := fastConfig()
	cfg.BatchSize = 3
	var insts []instance
	for range instances {
		insts = append(insts, newInstance(t, dsn, fastPolicy()))
	}
	setup := insts[0]

	type item struct {
		wallet walletRef
		id     uuid.UUID
	}
	var items []item
	var ws []walletRef
	for wi := range wallets {
		w := setup.openWallet(t, "1000.00")
		ws = append(ws, w)
		for j := range perWallet {
			ext := fmt.Sprintf("w%d-rb-%d", wi, j)
			res := setup.wager(t, w, domain.WagerKindRollback, ext, "1.00", fmt.Sprintf("w%d-bet-%d", wi, j))
			items = append(items, item{wallet: w, id: res.TransactionID})
		}
	}
	// PENDING órfãos também disputados.
	repo := postgres.NewWagerTransactionRepository()
	var orphans []uuid.UUID
	for wi, w := range ws {
		o, err := domain.NewExternalWagerTransaction(domain.NewExternalWagerTransactionParams{
			ID: uuid.New(), ProviderID: "provider-a", ExternalTransactionID: fmt.Sprintf("orphan-%d", wi),
			IdempotencyKey: fmt.Sprintf("provider-a:orphan-%d", wi), RequestHash: fmt.Sprintf("%064x", wi+1),
			WalletID: w.id, PlayerID: w.player, RoundID: "round-1", GameID: "game-1",
			Kind: domain.WagerKindBet, Money: brlMoney(t, "2.00"), Now: time.Now().UTC(),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.Insert(t.Context(), setup.db.Pool(), o); err != nil {
			t.Fatal(err)
		}
		orphans = append(orphans, o.ID())
	}

	for _, inst := range insts {
		inst.startWorker(t, cfg)
	}
	// As referências chegam em paralelo, por instâncias diferentes, enquanto
	// os workers disputam as pendências.
	var wg sync.WaitGroup
	errCh := make(chan error, wallets*perWallet)
	for wi, w := range ws {
		wg.Add(1)
		go func(wi int, w walletRef) {
			defer wg.Done()
			inst := insts[wi%instances]
			for j := range perWallet {
				if _, err := inst.tryWager(t.Context(), w, domain.WagerKindBet, fmt.Sprintf("w%d-bet-%d", wi, j), "1.00", ""); err != nil {
					errCh <- err
				}
			}
		}(wi, w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	all := append([]uuid.UUID{}, orphans...)
	for _, it := range items {
		all = append(all, it.id)
	}
	eventually(t, "all resolved", 20*time.Second, func() bool {
		return setup.count(t, `SELECT count(*) FROM wager_transactions WHERE id = ANY($1) AND status = 'PROCESSED'`, all) == len(all)
	})
	for _, id := range all {
		if n := setup.ledgerEntries(t, id); n != 1 {
			t.Fatalf("%s ledger entries = %d", id, n)
		}
		if n := setup.events(t, id, "WagerTransactionProcessed"); n != 1 {
			t.Fatalf("%s processed events = %d", id, n)
		}
	}
	if n := setup.count(t, `SELECT count(*) FROM (SELECT event_id FROM outbox GROUP BY event_id HAVING count(*) > 1) d`); n != 0 {
		t.Fatalf("duplicated outbox events: %d", n)
	}
	if n := setup.count(t, `SELECT count(*) FROM wager_transactions WHERE claimed_by IS NOT NULL`); n != 0 {
		t.Fatalf("leases left behind: %d", n)
	}
	for _, w := range ws {
		// 1000 − 8 + 8 − 2 = 998.
		setup.assertReconciled(t, w.id, 99800)
	}
}

func brlMoney(t *testing.T, amount string) domain.Money {
	t.Helper()
	m, err := domain.ParseNonNegative(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}
