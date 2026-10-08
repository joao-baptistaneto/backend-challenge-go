//go:build integration

package app_test

// Integração do caso de uso ProcessWagerTransaction (ADR-017) com o adapter
// postgres real: cada tipo de operação, replay, conflitos, concorrência
// (duplicatas e débitos na mesma carteira), rejeições persistidas, referência
// ausente, atomicidade e inbox no mesmo commit.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

const provider = "provider-a"

func (e env) processor(t *testing.T, uow app.UnitOfWork) *app.ProcessWagerTransaction {
	t.Helper()
	if uow == nil {
		uow = e.uow
	}
	uc, err := app.NewProcessWagerTransaction(uow)
	if err != nil {
		t.Fatal(err)
	}
	return uc
}

type testWallet struct {
	id, player uuid.UUID
}

func (e env) openWallet(t *testing.T, amount string) testWallet {
	t.Helper()
	player := uuid.New()
	res, err := e.useCase(t, nil).Execute(t.Context(), openCmd(t, player, amount, "BRL"))
	if err != nil {
		t.Fatal(err)
	}
	return testWallet{id: res.Wallet.ID, player: player}
}

func (w testWallet) cmd(t *testing.T, kind domain.WagerKind, ext, amount, ref string) app.ProcessWagerCommand {
	t.Helper()
	m, err := domain.ParseNonNegative(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return app.ProcessWagerCommand{
		ProviderID: provider, ExternalTransactionID: ext, IdempotencyKey: provider + ":" + ext,
		PlayerID: w.player, WalletID: w.id, RoundID: "round-1", GameID: "fortune-chimp",
		Kind: kind, Money: m, ReferenceExternalTransactionID: ref, CorrelationID: "corr-" + ext,
	}
}

// walletState devolve saldo (centavos), versão e número de lançamentos.
func walletState(t *testing.T, p *pgxpool.Pool, id uuid.UUID) (balance, version int64, entries int) {
	t.Helper()
	err := p.QueryRow(t.Context(), `SELECT w.balance, w.version,
 (SELECT count(*) FROM wallet_ledger_entries l WHERE l.wallet_id = w.id)
 FROM wallets w WHERE w.id = $1`, id).Scan(&balance, &version, &entries)
	if err != nil {
		t.Fatal(err)
	}
	return balance, version, entries
}

// assertReconciled confere saldo armazenado = soma do ledger (créditos −
// débitos) e o encadeamento before/after.
func assertReconciled(t *testing.T, p *pgxpool.Pool, id uuid.UUID) {
	t.Helper()
	var stored, calculated int64
	err := p.QueryRow(t.Context(), `SELECT w.balance,
 COALESCE((SELECT sum(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END)
           FROM wallet_ledger_entries WHERE wallet_id = w.id), 0)
 FROM wallets w WHERE w.id = $1`, id).Scan(&stored, &calculated)
	if err != nil {
		t.Fatal(err)
	}
	if stored != calculated {
		t.Fatalf("wallet %s: stored %d != ledger %d", id, stored, calculated)
	}
	var broken int
	err = p.QueryRow(t.Context(), `SELECT count(*) FROM (
 SELECT balance_before, lag(balance_after) OVER (ORDER BY seq) AS prev
 FROM wallet_ledger_entries WHERE wallet_id = $1) x
 WHERE prev IS NOT NULL AND prev <> balance_before`, id).Scan(&broken)
	if err != nil {
		t.Fatal(err)
	}
	if broken != 0 {
		t.Fatalf("ledger chain broken in %d entries", broken)
	}
}

type wagerRow struct {
	status, failure string
	balanceAfter    *int64
	refID           *uuid.UUID
	attempts        int32
	nextAttempt     *time.Time
	xmin            string
}

func readWager(t *testing.T, p *pgxpool.Pool, id uuid.UUID) wagerRow {
	t.Helper()
	var r wagerRow
	var failure *string
	err := p.QueryRow(t.Context(), `SELECT status, failure_code, balance_after, reference_transaction_id,
 attempts, next_attempt_at, xmin::text FROM wager_transactions WHERE id = $1`, id).
		Scan(&r.status, &failure, &r.balanceAfter, &r.refID, &r.attempts, &r.nextAttempt, &r.xmin)
	if err != nil {
		t.Fatal(err)
	}
	if failure != nil {
		r.failure = *failure
	}
	return r
}

// outboxFor devolve os tipos de evento (ordem de seq) cujo agregado é a
// transação ou que citam a transação (WalletBalanceChanged).
func outboxFor(t *testing.T, p *pgxpool.Pool, txID uuid.UUID) []string {
	t.Helper()
	rows, err := p.Query(t.Context(), `SELECT event_type FROM outbox
 WHERE aggregate_id = $1 OR payload::jsonb -> 'data' ->> 'transactionId' = $2 ORDER BY seq`, txID, txID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func process(t *testing.T, uc *app.ProcessWagerTransaction, cmd app.ProcessWagerCommand) app.WagerResult {
	t.Helper()
	res, err := uc.Execute(t.Context(), cmd)
	if err != nil {
		t.Fatalf("%s %s: %v", cmd.Kind, cmd.ExternalTransactionID, err)
	}
	return res
}

func expectResult(t *testing.T, res app.WagerResult, status domain.WagerStatus, balance string, code domain.FailureCode) {
	t.Helper()
	got := ""
	if res.Balance.IsValid() {
		got = res.Balance.Amount()
	}
	if res.Status != status || got != balance || res.FailureCode != code {
		t.Fatalf("result = %s balance %q code %q, want %s %q %q", res.Status, got, res.FailureCode, status, balance, code)
	}
}

// Cada tipo: BET, WIN com e sem referência, LOSS, REFUND e ROLLBACK de BET,
// WIN e REFUND, com saldo, versão, ledger e eventos conferidos.
func TestIntegrationProcessWager_AllKinds(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	p := e.pool()
	uc := e.processor(t, nil)
	w := e.openWallet(t, "100.00")

	type step struct {
		kind      domain.WagerKind
		ext       string
		amount    string
		ref       string
		balance   string
		version   int64
		entries   int
		direction string // "" sem lançamento
	}
	steps := []step{
		{domain.WagerKindBet, "b1", "30.00", "", "70.00", 2, 2, "DEBIT"},
		{domain.WagerKindWin, "w1", "20.00", "b1", "90.00", 3, 3, "CREDIT"},
		{domain.WagerKindWin, "w2", "5.00", "", "95.00", 4, 4, "CREDIT"},
		{domain.WagerKindLoss, "l1", "0.00", "", "95.00", 4, 4, ""},
		{domain.WagerKindBet, "b2", "10.00", "", "85.00", 5, 5, "DEBIT"},
		{domain.WagerKindRefund, "r1", "10.00", "b2", "95.00", 6, 6, "CREDIT"},
		{domain.WagerKindBet, "b3", "15.00", "", "80.00", 7, 7, "DEBIT"},
		{domain.WagerKindRollback, "rb-bet", "15.00", "b3", "95.00", 8, 8, "CREDIT"},
		{domain.WagerKindRollback, "rb-win", "5.00", "w2", "90.00", 9, 9, "DEBIT"},
		{domain.WagerKindRollback, "rb-refund", "10.00", "r1", "80.00", 10, 10, "DEBIT"},
	}
	ids := map[string]uuid.UUID{}
	for _, s := range steps {
		res := process(t, uc, w.cmd(t, s.kind, s.ext, s.amount, s.ref))
		expectResult(t, res, domain.WagerStatusProcessed, s.balance, "")
		if res.IdempotentReplay {
			t.Fatalf("%s: replay on first call", s.ext)
		}
		ids[s.ext] = res.TransactionID
		if s.ref != "" && res.ReferenceTransactionID != ids[s.ref] {
			t.Fatalf("%s: reference %s, want %s", s.ext, res.ReferenceTransactionID, ids[s.ref])
		}
		bal, ver, n := walletState(t, p, w.id)
		if fmt.Sprintf("%d.%02d", bal/100, bal%100) != s.balance || ver != s.version || n != s.entries {
			t.Fatalf("%s: wallet balance %d version %d entries %d", s.ext, bal, ver, n)
		}
		row := readWager(t, p, res.TransactionID)
		if row.status != "PROCESSED" || row.balanceAfter == nil || *row.balanceAfter != bal {
			t.Fatalf("%s: row %+v", s.ext, row)
		}
		wantEvents := "[WagerTransactionProcessed WalletBalanceChanged]"
		if s.direction == "" {
			wantEvents = "[WagerTransactionProcessed]"
		}
		if got := fmt.Sprint(outboxFor(t, p, res.TransactionID)); got != wantEvents {
			t.Fatalf("%s: events %s, want %s", s.ext, got, wantEvents)
		}
		var dir string
		err := p.QueryRow(t.Context(), `SELECT direction FROM wallet_ledger_entries WHERE transaction_id = $1`, res.TransactionID).Scan(&dir)
		switch {
		case s.direction == "" && err == nil:
			t.Fatalf("%s: unexpected ledger entry", s.ext)
		case s.direction != "" && (err != nil || dir != s.direction):
			t.Fatalf("%s: ledger direction %q err %v", s.ext, dir, err)
		}
	}
	assertReconciled(t, p, w.id)

	// Mesmo commit: carteira, transação, lançamento e eventos de uma BET.
	res := process(t, uc, w.cmd(t, domain.WagerKindBet, "b-commit", "1.00", ""))
	var walletXmin, ledgerXmin string
	if err := p.QueryRow(t.Context(), `SELECT xmin::text FROM wallets WHERE id = $1`, w.id).Scan(&walletXmin); err != nil {
		t.Fatal(err)
	}
	if err := p.QueryRow(t.Context(), `SELECT xmin::text FROM wallet_ledger_entries WHERE transaction_id = $1`, res.TransactionID).Scan(&ledgerXmin); err != nil {
		t.Fatal(err)
	}
	var outboxXmins []string
	rows, err := p.Query(t.Context(), `SELECT DISTINCT xmin::text FROM outbox
 WHERE aggregate_id = $1 OR payload::jsonb -> 'data' ->> 'transactionId' = $2`, res.TransactionID, res.TransactionID.String())
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var x string
		if err := rows.Scan(&x); err != nil {
			t.Fatal(err)
		}
		outboxXmins = append(outboxXmins, x)
	}
	rows.Close()
	if wx := readWager(t, p, res.TransactionID).xmin; wx != walletXmin || ledgerXmin != walletXmin ||
		len(outboxXmins) != 1 || outboxXmins[0] != walletXmin {
		t.Fatalf("not the same commit: wallet %s wager %s ledger %s outbox %v", walletXmin, wx, ledgerXmin, outboxXmins)
	}
}

// LOSS: sem ledger, sem incremento de versão, só WagerTransactionProcessed.
func TestIntegrationProcessWager_LossKeepsVersion(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := e.openWallet(t, "40.00")
	var updatedBefore time.Time
	if err := e.pool().QueryRow(t.Context(), `SELECT updated_at FROM wallets WHERE id = $1`, w.id).Scan(&updatedBefore); err != nil {
		t.Fatal(err)
	}
	res := process(t, e.processor(t, nil), w.cmd(t, domain.WagerKindLoss, "l1", "0.00", ""))
	expectResult(t, res, domain.WagerStatusProcessed, "40.00", "")
	bal, ver, n := walletState(t, e.pool(), w.id)
	if bal != 4000 || ver != 1 || n != 1 {
		t.Fatalf("wallet %d v%d entries %d", bal, ver, n)
	}
	var updatedAfter time.Time
	if err := e.pool().QueryRow(t.Context(), `SELECT updated_at FROM wallets WHERE id = $1`, w.id).Scan(&updatedAfter); err != nil {
		t.Fatal(err)
	}
	if !updatedAfter.Equal(updatedBefore) {
		t.Fatal("LOSS touched the wallet row")
	}
	if got := fmt.Sprint(outboxFor(t, e.pool(), res.TransactionID)); got != "[WagerTransactionProcessed]" {
		t.Fatalf("events = %s", got)
	}
}

// Replay após outras movimentações devolve o saldo original e
// idempotentReplay=true, sem novos efeitos.
func TestIntegrationProcessWager_ReplayAfterOtherMovements(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	uc := e.processor(t, nil)
	w := e.openWallet(t, "1000.00")
	bet := w.cmd(t, domain.WagerKindBet, "transaction-123", "25.00", "")
	first := process(t, uc, bet)
	expectResult(t, first, domain.WagerStatusProcessed, "975.00", "")
	process(t, uc, w.cmd(t, domain.WagerKindWin, "win-1", "100.00", ""))
	process(t, uc, w.cmd(t, domain.WagerKindBet, "bet-2", "50.00", ""))
	rejected := process(t, uc, w.cmd(t, domain.WagerKindBet, "bet-big", "5000.00", ""))
	expectResult(t, rejected, domain.WagerStatusRejected, "1025.00", domain.FailureCodeInsufficientFunds)

	before := counts(t, e.pool())
	bal0, ver0, _ := walletState(t, e.pool(), w.id)
	for range 3 {
		again := process(t, uc, bet)
		if !again.IdempotentReplay || again.TransactionID != first.TransactionID {
			t.Fatalf("replay = %+v", again)
		}
		expectResult(t, again, domain.WagerStatusProcessed, "975.00", "")
		rej := process(t, uc, w.cmd(t, domain.WagerKindBet, "bet-big", "5000.00", ""))
		if !rej.IdempotentReplay || rej.TransactionID != rejected.TransactionID {
			t.Fatalf("rejected replay = %+v", rej)
		}
		expectResult(t, rej, domain.WagerStatusRejected, "1025.00", domain.FailureCodeInsufficientFunds)
	}
	if after := counts(t, e.pool()); after != before {
		t.Fatalf("replay had side effects: %+v -> %+v", before, after)
	}
	if bal, ver, _ := walletState(t, e.pool(), w.id); bal != bal0 || ver != ver0 {
		t.Fatal("replay moved the wallet")
	}
}

// Conflitos: hash diferente com a mesma key; outra key para a mesma
// (provider, externalId). Nenhum efeito.
func TestIntegrationProcessWager_Conflicts(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	uc := e.processor(t, nil)
	w := e.openWallet(t, "100.00")
	process(t, uc, w.cmd(t, domain.WagerKindBet, "b1", "25.00", ""))
	before := counts(t, e.pool())

	changed := w.cmd(t, domain.WagerKindBet, "b1", "25.01", "")
	_, err := uc.Execute(t.Context(), changed)
	wantAppError(t, err, app.KindConflict, app.CodeIdempotencyKeyConflict)

	otherRound := w.cmd(t, domain.WagerKindBet, "b1", "25.00", "")
	otherRound.RoundID = "round-2"
	_, err = uc.Execute(t.Context(), otherRound)
	wantAppError(t, err, app.KindConflict, app.CodeIdempotencyKeyConflict)

	otherKey := w.cmd(t, domain.WagerKindBet, "b1", "25.00", "")
	otherKey.IdempotencyKey = "custom-key-1"
	_, err = uc.Execute(t.Context(), otherKey)
	wantAppError(t, err, app.KindConflict, app.CodeExternalTransactionConflict)

	// A mesma key de outro provedor é outra operação (escopo do provedor).
	otherProvider := w.cmd(t, domain.WagerKindBet, "b1", "25.00", "")
	otherProvider.ProviderID = "provider-b"
	if after := counts(t, e.pool()); after != before {
		t.Fatalf("conflict had side effects: %+v -> %+v", before, after)
	}
	res := process(t, uc, otherProvider)
	if res.IdempotentReplay {
		t.Fatal("other provider replayed provider-a's transaction")
	}
	if bal, _, _ := walletState(t, e.pool(), w.id); bal != 5000 {
		t.Fatalf("balance = %d", bal)
	}
}

// N goroutines com a mesma operação: exatamente um efeito; as demais
// recebem o replay. Com carteira existente (serializa no lock) e inexistente
// (corrida resolvida pelo UNIQUE + retry).
func TestIntegrationProcessWager_ConcurrentDuplicates(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	uc := e.processor(t, nil)
	w := e.openWallet(t, "100.00")
	ghost := testWallet{id: uuid.New(), player: uuid.New()}

	for _, tc := range []struct {
		name   string
		cmd    app.ProcessWagerCommand
		status domain.WagerStatus
	}{
		{"existing wallet", w.cmd(t, domain.WagerKindBet, "dup-bet", "10.00", ""), domain.WagerStatusProcessed},
		{"missing wallet", ghost.cmd(t, domain.WagerKindBet, "dup-ghost", "10.00", ""), domain.WagerStatusRejected},
	} {
		const n = 50
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			fresh   int
			ids     = map[uuid.UUID]bool{}
			errs    []error
			start   = make(chan struct{})
			results []app.WagerResult
		)
		for range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				res, err := uc.Execute(context.Background(), tc.cmd)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					errs = append(errs, err)
					return
				}
				results = append(results, res)
				ids[res.TransactionID] = true
				if !res.IdempotentReplay {
					fresh++
				}
			}()
		}
		close(start)
		wg.Wait()
		if len(errs) != 0 || fresh != 1 || len(ids) != 1 || len(results) != n {
			t.Fatalf("%s: fresh %d ids %d errs %v", tc.name, fresh, len(ids), errs)
		}
		for _, r := range results {
			if r.Status != tc.status {
				t.Fatalf("%s: status %s", tc.name, r.Status)
			}
		}
		var rows int
		if err := e.pool().QueryRow(t.Context(), `SELECT count(*) FROM wager_transactions
 WHERE provider_id = $1 AND external_transaction_id = $2`, provider, tc.cmd.ExternalTransactionID).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != 1 {
			t.Fatalf("%s: %d rows", tc.name, rows)
		}
	}
	bal, ver, entries := walletState(t, e.pool(), w.id)
	if bal != 9000 || ver != 2 || entries != 2 {
		t.Fatalf("wallet %d v%d entries %d (want one debit)", bal, ver, entries)
	}
	assertReconciled(t, e.pool(), w.id)

	// Mesma operação com keys diferentes em paralelo: uma vence, as demais
	// são conflito; nunca dois efeitos.
	const n = 10
	var (
		wg              sync.WaitGroup
		mu              sync.Mutex
		ok, conflict    int
		other           []error
		start           = make(chan struct{})
		base            = w.cmd(t, domain.WagerKindBet, "multi-key", "1.00", "")
		balanceBefore   = bal
		versionBefore   = ver
		entriesExpected = entries + 1
	)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd := base
			cmd.IdempotencyKey = fmt.Sprintf("key-%d", i)
			<-start
			_, err := uc.Execute(context.Background(), cmd)
			mu.Lock()
			defer mu.Unlock()
			switch ae, isApp := app.AsError(err); {
			case err == nil:
				ok++
			case isApp && ae.Code == app.CodeExternalTransactionConflict:
				conflict++
			default:
				other = append(other, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if ok != 1 || conflict != n-1 || len(other) != 0 {
		t.Fatalf("multi-key: ok %d conflict %d other %v", ok, conflict, other)
	}
	if bal, ver, entries := walletState(t, e.pool(), w.id); bal != balanceBefore-100 || ver != versionBefore+1 || entries != entriesExpected {
		t.Fatalf("multi-key wallet %d v%d entries %d", bal, ver, entries)
	}
}

// Rejeições persistidas com failureCode e WagerTransactionRejected, sem
// movimentar saldo.
func TestIntegrationProcessWager_RejectionsPersisted(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	p := e.pool()
	uc := e.processor(t, nil)
	w := e.openWallet(t, "10.00")
	process(t, uc, w.cmd(t, domain.WagerKindWin, "win-50", "50.00", ""))        // 60
	process(t, uc, w.cmd(t, domain.WagerKindBet, "bet-r1", "5.00", ""))         // 55
	process(t, uc, w.cmd(t, domain.WagerKindBet, "bet-big", "45.00", ""))       // 10
	process(t, uc, w.cmd(t, domain.WagerKindRefund, "ref-1", "5.00", "bet-r1")) // 15
	bal0, ver0, n0 := walletState(t, p, w.id)

	otherRound := w.cmd(t, domain.WagerKindRefund, "ref-mismatch", "45.00", "bet-big")
	otherRound.RoundID = "round-2"
	other := e.openWallet(t, "0.00")
	wrongPlayer := w.cmd(t, domain.WagerKindBet, "bet-wrong-player", "1.00", "")
	wrongPlayer.PlayerID = other.player

	cases := []struct {
		name    string
		cmd     app.ProcessWagerCommand
		code    domain.FailureCode
		balance string // "" = escondido
	}{
		{"insufficient funds", w.cmd(t, domain.WagerKindBet, "bet-too-big", "15.01", ""), domain.FailureCodeInsufficientFunds, "15.00"},
		{"reference mismatch", otherRound, domain.FailureCodeReferenceMismatch, "15.00"},
		{"reversal insufficient funds", w.cmd(t, domain.WagerKindRollback, "rb-win", "50.00", "win-50"), domain.FailureCodeReversalInsufficientFunds, "15.00"},
		{"already reversed", w.cmd(t, domain.WagerKindRollback, "rb-bet-r1", "5.00", "bet-r1"), domain.FailureCodeAlreadyReversed, "15.00"},
		{"amount mismatch", w.cmd(t, domain.WagerKindRefund, "ref-amount", "1.00", "bet-big"), domain.FailureCodeAmountMismatch, "15.00"},
		{"not reversible", w.cmd(t, domain.WagerKindRefund, "ref-of-win", "50.00", "win-50"), domain.FailureCodeReferenceNotReversible, "15.00"},
		{"wallet player mismatch", wrongPlayer, domain.FailureCodeWalletPlayerMismatch, ""},
		{"wallet not found", testWallet{id: uuid.New(), player: w.player}.cmd(t, domain.WagerKindBet, "bet-no-wallet", "1.00", ""), domain.FailureCodeWalletNotFound, ""},
	}
	for _, tc := range cases {
		res := process(t, uc, tc.cmd)
		expectResult(t, res, domain.WagerStatusRejected, tc.balance, tc.code)
		if res.ReferenceTransactionID != uuid.Nil {
			t.Fatalf("%s: rejection exposes reference id", tc.name)
		}
		row := readWager(t, p, res.TransactionID)
		if row.status != "REJECTED" || row.failure != string(tc.code) {
			t.Fatalf("%s: row %+v", tc.name, row)
		}
		if got := fmt.Sprint(outboxFor(t, p, res.TransactionID)); got != "[WagerTransactionRejected]" {
			t.Fatalf("%s: events %s", tc.name, got)
		}
		var payload []byte
		if err := p.QueryRow(t.Context(), `SELECT payload FROM outbox WHERE aggregate_id = $1`, res.TransactionID).Scan(&payload); err != nil {
			t.Fatal(err)
		}
		var ev struct {
			Data struct {
				FailureCode            string     `json:"failureCode"`
				ReferenceTransactionID *uuid.UUID `json:"referenceTransactionId"`
			} `json:"data"`
		}
		if err := json.Unmarshal(payload, &ev); err != nil {
			t.Fatal(err)
		}
		if ev.Data.FailureCode != string(tc.code) || ev.Data.ReferenceTransactionID != nil {
			t.Fatalf("%s: event payload %s", tc.name, payload)
		}
	}
	if bal, ver, n := walletState(t, p, w.id); bal != bal0 || ver != ver0 || n != n0 {
		t.Fatalf("rejections moved the wallet: %d v%d n%d", bal, ver, n)
	}
	assertReconciled(t, p, w.id)
}

// ROLLBACK (e REFUND) antes da transação original: PENDING_REFERENCE + evento,
// sem movimento. A referência chegando depois é processada normalmente; a
// pendência continua para o worker (ADR-023), e o replay a devolve.
func TestIntegrationProcessWager_ReversalBeforeReference(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	p := e.pool()
	uc := e.processor(t, nil)
	w := e.openWallet(t, "100.00")
	before := time.Now()

	rb := w.cmd(t, domain.WagerKindRollback, "rb-early", "30.00", "bet-late")
	res := process(t, uc, rb)
	expectResult(t, res, domain.WagerStatusPendingReference, "", "")
	row := readWager(t, p, res.TransactionID)
	if row.status != "PENDING_REFERENCE" || row.attempts != 1 || row.nextAttempt == nil ||
		row.nextAttempt.Before(before) || row.balanceAfter != nil || row.refID != nil {
		t.Fatalf("row = %+v", row)
	}
	if got := fmt.Sprint(outboxFor(t, p, res.TransactionID)); got != "[WagerTransactionPendingReference]" {
		t.Fatalf("events = %s", got)
	}
	if bal, ver, n := walletState(t, p, w.id); bal != 10000 || ver != 1 || n != 1 {
		t.Fatalf("pending moved the wallet: %d v%d n%d", bal, ver, n)
	}

	// A referência chega depois e é processada normalmente.
	bet := process(t, uc, w.cmd(t, domain.WagerKindBet, "bet-late", "30.00", ""))
	expectResult(t, bet, domain.WagerStatusProcessed, "70.00", "")

	again := process(t, uc, rb)
	if !again.IdempotentReplay || again.TransactionID != res.TransactionID || again.Status != domain.WagerStatusPendingReference {
		t.Fatalf("pending replay = %+v", again)
	}
	if got := fmt.Sprint(outboxFor(t, p, res.TransactionID)); got != "[WagerTransactionPendingReference]" {
		t.Fatalf("replay emitted events: %s", got)
	}
}

// Débitos concorrentes na mesma carteira: nunca saldo negativo nem
// atualização perdida. Inclui o cenário obrigatório 80 + 80 sobre 100.
func TestIntegrationProcessWager_ConcurrentDebitsSameWallet(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	uc := e.processor(t, nil)

	run := func(w testWallet, amounts []string, prefix string) (processed, rejected int) {
		var (
			wg    sync.WaitGroup
			mu    sync.Mutex
			start = make(chan struct{})
			errs  []error
		)
		for i, amount := range amounts {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				res, err := uc.Execute(context.Background(), w.cmd(t, domain.WagerKindBet, fmt.Sprintf("%s-%d", prefix, i), amount, ""))
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err != nil:
					errs = append(errs, err)
				case res.Status == domain.WagerStatusProcessed:
					processed++
				case res.FailureCode == domain.FailureCodeInsufficientFunds:
					rejected++
				default:
					errs = append(errs, fmt.Errorf("unexpected %+v", res))
				}
			}()
		}
		close(start)
		wg.Wait()
		if len(errs) != 0 {
			t.Fatalf("errors: %v", errs)
		}
		return processed, rejected
	}

	// 80 + 80 sobre 100.
	w := e.openWallet(t, "100.00")
	processed, rejected := run(w, []string{"80.00", "80.00"}, "bet80")
	if processed != 1 || rejected != 1 {
		t.Fatalf("80/80: processed %d rejected %d", processed, rejected)
	}
	if bal, ver, n := walletState(t, e.pool(), w.id); bal != 2000 || ver != 2 || n != 2 {
		t.Fatalf("80/80 wallet %d v%d entries %d", bal, ver, n)
	}
	// Reenvios não alteram o resultado.
	for i := range 2 {
		res := process(t, uc, w.cmd(t, domain.WagerKindBet, fmt.Sprintf("bet80-%d", i), "80.00", ""))
		if !res.IdempotentReplay {
			t.Fatal("resend not replayed")
		}
	}
	if bal, _, n := walletState(t, e.pool(), w.id); bal != 2000 || n != 2 {
		t.Fatal("resend changed the result")
	}

	// 30 débitos de 7.00 sobre 100.00: 14 passam, 16 são rejeitados.
	w2 := e.openWallet(t, "100.00")
	amounts := make([]string, 30)
	for i := range amounts {
		amounts[i] = "7.00"
	}
	processed, rejected = run(w2, amounts, "bet7")
	if processed != 14 || rejected != 16 {
		t.Fatalf("7.00 x30: processed %d rejected %d", processed, rejected)
	}
	if bal, ver, n := walletState(t, e.pool(), w2.id); bal != 200 || ver != 15 || n != 15 {
		t.Fatalf("7.00 x30 wallet %d v%d entries %d", bal, ver, n)
	}
	assertReconciled(t, e.pool(), w.id)
	assertReconciled(t, e.pool(), w2.id)
}

// Falha injetada depois de cada escrita: rollback total (nem a inbox fica).
func TestIntegrationProcessWager_AtomicRollback(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	boom := errors.New("injected failure")
	w := e.openWallet(t, "100.00")
	before := counts(t, e.pool())
	cmd := w.cmd(t, domain.WagerKindBet, "atomic-1", "40.00", "")
	cmd.Inbox = &app.InboxMessageRef{ConsumerName: "wager-consumer", MessageID: "msg-atomic"}
	for _, step := range []string{"wallet_update", "wager", "ledger", "outbox", "inbox_complete"} {
		_, err := e.processor(t, failingUoW{inner: e.uow, step: step, err: boom}).Execute(t.Context(), cmd)
		if !errors.Is(err, boom) {
			t.Fatalf("step %s: err = %v", step, err)
		}
		if c := counts(t, e.pool()); c != before {
			t.Fatalf("step %s: partial commit %+v -> %+v", step, before, c)
		}
		var inbox int
		if err := e.pool().QueryRow(t.Context(), `SELECT count(*) FROM inbox`).Scan(&inbox); err != nil {
			t.Fatal(err)
		}
		if inbox != 0 {
			t.Fatalf("step %s: inbox row survived the rollback", step)
		}
		if bal, ver, _ := walletState(t, e.pool(), w.id); bal != 10000 || ver != 1 {
			t.Fatalf("step %s: wallet moved %d v%d", step, bal, ver)
		}
	}
	res := process(t, e.processor(t, nil), cmd)
	expectResult(t, res, domain.WagerStatusProcessed, "60.00", "")
	if res.IdempotentReplay {
		t.Fatal("first effective execution reported as replay")
	}
}

// Inbox: registro e conclusão no MESMO commit dos efeitos; reentrega é
// replay; conteúdo divergente é conflito; entregas concorrentes da mesma
// mensagem produzem um único efeito.
func TestIntegrationProcessWager_InboxSameCommit(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	p := e.pool()
	uc := e.processor(t, nil)
	w := e.openWallet(t, "100.00")
	inbox := &app.InboxMessageRef{ConsumerName: "wager-consumer", MessageID: "msg-123"}
	cmd := w.cmd(t, domain.WagerKindBet, "sqs-1", "25.00", "")
	cmd.Inbox, cmd.CausationID = inbox, inbox.MessageID

	const n = 10
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		fresh int
		errs  []error
		start = make(chan struct{})
	)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := uc.Execute(context.Background(), cmd)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
			} else if !res.IdempotentReplay {
				fresh++
			}
		}()
	}
	close(start)
	wg.Wait()
	if fresh != 1 || len(errs) != 0 {
		t.Fatalf("fresh %d errs %v", fresh, errs)
	}

	var (
		txID                 uuid.UUID
		completed            *time.Time
		inboxXmin, wagerXmin string
		hash, wagerHash      string
	)
	if err := p.QueryRow(t.Context(), `SELECT transaction_id, completed_at, xmin::text, request_hash FROM inbox
 WHERE consumer_name = $1 AND message_id = $2`, inbox.ConsumerName, inbox.MessageID).Scan(&txID, &completed, &inboxXmin, &hash); err != nil {
		t.Fatal(err)
	}
	if err := p.QueryRow(t.Context(), `SELECT xmin::text, request_hash FROM wager_transactions WHERE id = $1`, txID).Scan(&wagerXmin, &wagerHash); err != nil {
		t.Fatal(err)
	}
	if completed == nil || inboxXmin != wagerXmin || hash != wagerHash {
		t.Fatalf("inbox not completed in the same commit: completed %v xmin %s/%s hash %s/%s", completed, inboxXmin, wagerXmin, hash, wagerHash)
	}
	if bal, _, entries := walletState(t, p, w.id); bal != 7500 || entries != 2 {
		t.Fatalf("wallet %d entries %d", bal, entries)
	}
	var causation string
	if err := p.QueryRow(t.Context(), `SELECT payload::jsonb ->> 'causationId' FROM outbox WHERE aggregate_id = $1`, txID).Scan(&causation); err != nil {
		t.Fatal(err)
	}
	if causation != inbox.MessageID {
		t.Fatalf("causation = %q", causation)
	}

	// Mesmo messageId com outro conteúdo.
	changed := w.cmd(t, domain.WagerKindBet, "sqs-1", "26.00", "")
	changed.Inbox = inbox
	_, err := uc.Execute(t.Context(), changed)
	wantAppError(t, err, app.KindConflict, app.CodeMessageConflict)

	// A mesma operação recebida por HTTP (sem inbox) é replay.
	httpCmd := w.cmd(t, domain.WagerKindBet, "sqs-1", "25.00", "")
	if res := process(t, uc, httpCmd); !res.IdempotentReplay || res.TransactionID != txID {
		t.Fatalf("http replay = %+v", res)
	}
	// Operação vinda de HTTP e depois por SQS (mensagem nova): replay, e a
	// mensagem é concluída apontando para a transação original.
	first := process(t, uc, w.cmd(t, domain.WagerKindBet, "http-first", "5.00", ""))
	viaSQS := w.cmd(t, domain.WagerKindBet, "http-first", "5.00", "")
	viaSQS.Inbox = &app.InboxMessageRef{ConsumerName: "wager-consumer", MessageID: "msg-456"}
	if res := process(t, uc, viaSQS); !res.IdempotentReplay || res.TransactionID != first.TransactionID {
		t.Fatalf("sqs replay = %+v", res)
	}
	var linked uuid.UUID
	if err := p.QueryRow(t.Context(), `SELECT transaction_id FROM inbox WHERE message_id = 'msg-456' AND completed_at IS NOT NULL`).Scan(&linked); err != nil || linked != first.TransactionID {
		t.Fatalf("msg-456 linked to %s (%v)", linked, err)
	}
	assertReconciled(t, p, w.id)
}

// Composição Fx: app.Module provê *ProcessWagerTransaction ligado ao adapter.
func TestIntegrationProcessWager_FxComposition(t *testing.T) {
	t.Parallel()
	dsn := migratedDSN(t)
	var (
		open *app.OpenWallet
		uc   *app.ProcessWagerTransaction
	)
	fxApp := fx.New(
		fx.NopLogger,
		fx.Supply(config.Config{Database: dbConfig(dsn)}),
		metrics.Module,
		postgres.Module,
		app.Module,
		fx.Populate(&open, &uc),
	)
	if err := fxApp.Err(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := fxApp.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := fxApp.Stop(context.Background()); err != nil {
			t.Errorf("stop: %v", err)
		}
	}()
	player := uuid.New()
	opened, err := open.Execute(ctx, openCmd(t, player, "10.00", "BRL"))
	if err != nil {
		t.Fatal(err)
	}
	w := testWallet{id: opened.Wallet.ID, player: player}
	res, err := uc.Execute(ctx, w.cmd(t, domain.WagerKindBet, "fx-bet", "2.50", ""))
	if err != nil {
		t.Fatal(err)
	}
	expectResult(t, res, domain.WagerStatusProcessed, "7.50", "")
}
