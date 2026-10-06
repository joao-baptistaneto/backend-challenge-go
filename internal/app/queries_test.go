package app_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/fx"

	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// Testes unitários de ReconcileWallet e Queries (T16) sobre a memUoW.

// --- leituras da memUoW (T16) -------------------------------------------------

func (s memWallets) Get(_ context.Context, id uuid.UUID) (*domain.Wallet, error) {
	w, ok := s.s.wallets[id]
	if !ok {
		return nil, app.ErrRecordNotFound
	}
	return cloneWallet(w), nil
}

// List usa como cursor "w:<walletId>:<índice>" (opaco para o caso de uso).
func (s memLedger) List(_ context.Context, walletID uuid.UUID, cursor string, limit int) (app.LedgerPage, error) {
	if limit < 1 || limit > app.MaxLedgerPageLimit {
		return app.LedgerPage{}, errors.New("mem: invalid limit")
	}
	var entries []domain.WalletLedgerEntry
	for _, e := range s.s.ledger {
		if e.WalletID() == walletID {
			entries = append(entries, e)
		}
	}
	start := 0
	if cursor != "" {
		prefix := "w:" + walletID.String() + ":"
		n, err := strconv.Atoi(strings.TrimPrefix(cursor, prefix))
		if !strings.HasPrefix(cursor, prefix) || err != nil || n < 1 || n > len(entries) {
			return app.LedgerPage{}, app.ErrInvalidCursor
		}
		start = n
	}
	end := min(start+limit, len(entries))
	p := app.LedgerPage{Entries: entries[start:end]}
	if end < len(entries) {
		p.NextCursor = "w:" + walletID.String() + ":" + strconv.Itoa(end)
	}
	return p, nil
}

func (s memLedger) Summarize(_ context.Context, walletID uuid.UUID) (app.LedgerSummary, error) {
	var sum app.LedgerSummary
	for _, e := range s.s.ledger {
		if e.WalletID() != walletID {
			continue
		}
		sum.Entries++
		if e.Direction() == domain.LedgerDirectionCredit {
			sum.NetMinorUnits += e.Amount().MinorUnits()
		} else {
			sum.NetMinorUnits -= e.Amount().MinorUnits()
		}
	}
	return sum, nil
}

// --- helpers -----------------------------------------------------------------

type recordedReconciliation struct {
	currency  string
	divergent bool
}

type fakeRecorder struct{ calls []recordedReconciliation }

func (r *fakeRecorder) Reconciliation(currency string, divergent bool) {
	r.calls = append(r.calls, recordedReconciliation{currency, divergent})
}

// openedWallet abre uma carteira com saldo 100.00 (OPENING + ledger) e
// processa uma BET de 25.00 do provider-a; devolve a carteira e a BET.
func openedWallet(t *testing.T, u *memUoW) (uuid.UUID, app.WagerResult) {
	t.Helper()
	ow, err := app.NewOpenWallet(u, app.WithClock(func() time.Time { return fixedNow }))
	if err != nil {
		t.Fatal(err)
	}
	player := uuid.Must(uuid.NewV7())
	res, err := ow.Execute(t.Context(), app.OpenWalletCommand{PlayerID: player, InitialBalance: brl(t, "100.00"), CorrelationID: "corr-open"})
	if err != nil {
		t.Fatal(err)
	}
	cmd := wagerCmd(t, domain.WagerKindBet, "bet-1", "25.00", "")
	cmd.WalletID, cmd.PlayerID = res.Wallet.ID, player
	bet, err := newProcess(t, u).Execute(t.Context(), cmd)
	if err != nil || bet.Status != domain.WagerStatusProcessed {
		t.Fatalf("bet = %+v, %v", bet, err)
	}
	return res.Wallet.ID, bet
}

func setStoredBalance(t *testing.T, u *memUoW, id uuid.UUID, amount string) {
	t.Helper()
	w := u.state.wallets[id]
	c, err := domain.RehydrateWallet(domain.RehydrateWalletParams{
		ID: w.ID(), PlayerID: w.PlayerID(), Balance: brl(t, amount), Version: w.Version(),
		CreatedAt: w.CreatedAt(), UpdatedAt: w.UpdatedAt(),
	})
	if err != nil {
		t.Fatal(err)
	}
	u.state.wallets[id] = c
}

func newReconcile(t *testing.T, u app.UnitOfWork, opts ...app.Option) *app.ReconcileWallet {
	t.Helper()
	uc, err := app.NewReconcileWallet(u, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return uc
}

func newQueries(t *testing.T, u app.UnitOfWork) *app.Queries {
	t.Helper()
	q, err := app.NewQueries(u)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

// --- reconciliação -------------------------------------------------------------

func TestReconcile_Consistent(t *testing.T) {
	u := newMemUoW()
	walletID, _ := openedWallet(t, u)
	rec := &fakeRecorder{}
	var logs bytes.Buffer
	before := u.state.clone()

	rep, err := newReconcile(t, u, app.WithReconciliationRecorder(rec),
		app.WithLogger(jsonLogger(&logs))).Execute(t.Context(), app.ReconcileCommand{WalletID: walletID, CorrelationID: "corr-r"})
	if err != nil {
		t.Fatal(err)
	}
	if rep.WalletID != walletID || rep.StoredBalance.Amount() != "75.00" || rep.CalculatedBalance.Amount() != "75.00" ||
		rep.Difference.Amount() != "0.00" || rep.Difference.Currency().String() != "BRL" ||
		!rep.Consistent || rep.CheckedEntries != 2 || rep.WalletVersion != 2 {
		t.Fatalf("report = %+v", rep)
	}
	if len(rec.calls) != 1 || rec.calls[0] != (recordedReconciliation{"BRL", false}) {
		t.Fatalf("recorder = %+v", rec.calls)
	}
	if strings.Contains(logs.String(), "divergência") {
		t.Fatalf("divergence logged on consistent wallet: %s", logs.String())
	}
	assertStateUnchanged(t, before, u.state)

	// Opções da transação: REPEATABLE READ, somente leitura, reexecutável.
	last := u.opts[len(u.opts)-1]
	if last.Isolation != app.IsolationRepeatableRead || !last.ReadOnly || !last.Retry || last.Operation != app.OperationReconcile {
		t.Fatalf("tx options = %+v", last)
	}
}

func TestReconcile_DivergentReportsLogsAndRecords(t *testing.T) {
	u := newMemUoW()
	walletID, _ := openedWallet(t, u)
	setStoredBalance(t, u, walletID, "80.50")
	before := u.state.clone()
	rec := &fakeRecorder{}
	var logs bytes.Buffer

	rep, err := newReconcile(t, u, app.WithReconciliationRecorder(rec),
		app.WithLogger(jsonLogger(&logs))).Execute(t.Context(), app.ReconcileCommand{WalletID: walletID, CorrelationID: "corr-d"})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Consistent || rep.StoredBalance.Amount() != "80.50" || rep.CalculatedBalance.Amount() != "75.00" ||
		rep.Difference.Amount() != "5.50" || rep.CheckedEntries != 2 {
		t.Fatalf("report = %+v", rep)
	}
	if len(rec.calls) != 1 || rec.calls[0] != (recordedReconciliation{"BRL", true}) {
		t.Fatalf("recorder = %+v", rec.calls)
	}
	line := findLog(t, &logs, "divergência de reconciliação")
	want := map[string]any{
		"level": "WARN", "walletId": walletID.String(), "currency": "BRL", "storedBalance": "80.50",
		"calculatedBalance": "75.00", "difference": "5.50", "checkedEntries": float64(2), "correlationId": "corr-d",
	}
	for k, v := range want {
		if line[k] != v {
			t.Fatalf("log[%s] = %v, want %v (%v)", k, line[k], v, line)
		}
	}
	assertStateUnchanged(t, before, u.state)

	// Saldo gravado menor que o ledger: diferença negativa.
	setStoredBalance(t, u, walletID, "70.00")
	rep, err = newReconcile(t, u).Execute(t.Context(), app.ReconcileCommand{WalletID: walletID})
	if err != nil || rep.Difference.Amount() != "-5.00" || rep.Consistent {
		t.Fatalf("report = %+v, %v", rep, err)
	}
}

func TestReconcile_WalletWithoutLedger(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "0.00")
	rep, err := newReconcile(t, u).Execute(t.Context(), app.ReconcileCommand{WalletID: unitWallet})
	if err != nil || !rep.Consistent || rep.CheckedEntries != 0 || rep.CalculatedBalance.Amount() != "0.00" {
		t.Fatalf("report = %+v, %v", rep, err)
	}
	// Saldo sem nenhum lançamento: divergente.
	seedWallet(t, u, "10.00")
	rep, err = newReconcile(t, u).Execute(t.Context(), app.ReconcileCommand{WalletID: unitWallet})
	if err != nil || rep.Consistent || rep.Difference.Amount() != "10.00" {
		t.Fatalf("report = %+v, %v", rep, err)
	}
}

func TestReconcile_Errors(t *testing.T) {
	u := newMemUoW()
	_, err := newReconcile(t, u).Execute(t.Context(), app.ReconcileCommand{WalletID: uuid.Nil})
	wantAppError(t, err, app.KindInvalidInput, app.CodeInvalidWalletID)
	if len(u.opts) != 0 {
		t.Fatal("tx opened for invalid input")
	}

	_, err = newReconcile(t, u).Execute(t.Context(), app.ReconcileCommand{WalletID: unitWallet})
	wantAppError(t, err, app.KindNotFound, app.CodeWalletNotFound)
	if !errors.Is(err, app.ErrNotFound) {
		t.Fatal("not found kind")
	}

	u.txErr = errors.Join(app.ErrUnavailable, errors.New("conn reset"))
	_, err = newReconcile(t, u).Execute(t.Context(), app.ReconcileCommand{WalletID: unitWallet})
	wantAppError(t, err, app.KindUnavailable, app.CodeTemporarilyUnavailable)

	u.txErr = errors.New("boom")
	_, err = newReconcile(t, u).Execute(t.Context(), app.ReconcileCommand{WalletID: unitWallet})
	if _, ok := app.AsError(err); ok || err == nil {
		t.Fatalf("internal error typed: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := newReconcile(t, u).Execute(ctx, app.ReconcileCommand{WalletID: unitWallet}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
	if _, err := app.NewReconcileWallet(nil); err == nil {
		t.Fatal("nil uow accepted")
	}
}

// --- consultas ---------------------------------------------------------------

func TestQueries_GetWallet(t *testing.T) {
	u := newMemUoW()
	walletID, _ := openedWallet(t, u)
	q := newQueries(t, u)

	w, err := q.GetWallet(t.Context(), walletID)
	if err != nil || w.ID != walletID || w.Balance.Amount() != "75.00" || w.Version != 2 {
		t.Fatalf("wallet = %+v, %v", w, err)
	}
	last := u.opts[len(u.opts)-1]
	if !last.ReadOnly || last.Operation != app.OperationQuery {
		t.Fatalf("tx options = %+v", last)
	}

	_, err = q.GetWallet(t.Context(), uuid.Must(uuid.NewV7()))
	wantAppError(t, err, app.KindNotFound, app.CodeWalletNotFound)
	_, err = q.GetWallet(t.Context(), uuid.Nil)
	wantAppError(t, err, app.KindInvalidInput, app.CodeInvalidWalletID)
}

func TestQueries_ListLedgerPagination(t *testing.T) {
	u := newMemUoW()
	walletID, bet := openedWallet(t, u)
	// Mais 3 lançamentos: total 5.
	for i := 2; i <= 4; i++ {
		cmd := wagerCmd(t, domain.WagerKindBet, "bet-"+strconv.Itoa(i), "1.00", "")
		cmd.WalletID = walletID
		cmd.PlayerID = u.state.wallets[walletID].PlayerID()
		if _, err := newProcess(t, u).Execute(t.Context(), cmd); err != nil {
			t.Fatal(err)
		}
	}
	q := newQueries(t, u)

	var all []app.LedgerEntryView
	cursor, pages := "", 0
	for {
		p, err := q.ListLedger(t.Context(), app.ListLedgerQuery{WalletID: walletID, Cursor: cursor, Limit: 2})
		if err != nil {
			t.Fatal(err)
		}
		pages++
		all = append(all, p.Entries...)
		if p.NextCursor == "" {
			break
		}
		cursor = p.NextCursor
	}
	if pages != 3 || len(all) != 5 {
		t.Fatalf("pages %d entries %d", pages, len(all))
	}
	if all[0].Direction != domain.LedgerDirectionCredit || all[0].BalanceAfter.Amount() != "100.00" ||
		all[1].TransactionID != bet.TransactionID || all[1].BalanceAfter.Amount() != "75.00" ||
		all[4].BalanceAfter.Amount() != "72.00" {
		t.Fatalf("entries = %+v", all)
	}

	// Limit 0 usa o padrão (uma página só).
	p, err := q.ListLedger(t.Context(), app.ListLedgerQuery{WalletID: walletID})
	if err != nil || len(p.Entries) != 5 || p.NextCursor != "" {
		t.Fatalf("default page = %+v, %v", p, err)
	}

	for _, limit := range []int{-1, app.MaxLedgerPageLimit + 1} {
		_, err := q.ListLedger(t.Context(), app.ListLedgerQuery{WalletID: walletID, Limit: limit})
		wantAppError(t, err, app.KindInvalidInput, app.CodeInvalidPageLimit)
	}
	_, err = q.ListLedger(t.Context(), app.ListLedgerQuery{WalletID: walletID, Cursor: "garbage"})
	wantAppError(t, err, app.KindInvalidInput, app.CodeInvalidCursor)
	_, err = q.ListLedger(t.Context(), app.ListLedgerQuery{WalletID: uuid.Must(uuid.NewV7())})
	wantAppError(t, err, app.KindNotFound, app.CodeWalletNotFound)
	_, err = q.ListLedger(t.Context(), app.ListLedgerQuery{})
	wantAppError(t, err, app.KindInvalidInput, app.CodeInvalidWalletID)
}

func TestQueries_GetTransactionRespectsProvider(t *testing.T) {
	u := newMemUoW()
	walletID, bet := openedWallet(t, u)
	q := newQueries(t, u)
	ctx := t.Context()

	v, err := q.GetTransaction(ctx, app.GetTransactionQuery{TransactionID: bet.TransactionID, ProviderID: "provider-a"})
	if err != nil {
		t.Fatal(err)
	}
	if v.TransactionID != bet.TransactionID || v.ProviderID != "provider-a" || v.ExternalTransactionID != "bet-1" ||
		v.WalletID != walletID || v.Kind != domain.WagerKindBet || v.Status != domain.WagerStatusProcessed ||
		v.Money.Amount() != "25.00" || v.Balance.Amount() != "75.00" || v.RoundID != "round-1" {
		t.Fatalf("view = %+v", v)
	}

	v2, err := q.GetTransactionByProvider(ctx, app.GetTransactionByProviderQuery{ProviderID: "provider-a", ExternalTransactionID: "bet-1"})
	if err != nil || v2 != v {
		t.Fatalf("by provider = %+v, %v", v2, err)
	}

	// Outro provedor: exatamente como inexistente.
	_, err = q.GetTransaction(ctx, app.GetTransactionQuery{TransactionID: bet.TransactionID, ProviderID: "provider-b"})
	wantAppError(t, err, app.KindNotFound, app.CodeTransactionNotFound)
	_, err = q.GetTransactionByProvider(ctx, app.GetTransactionByProviderQuery{ProviderID: "provider-b", ExternalTransactionID: "bet-1"})
	wantAppError(t, err, app.KindNotFound, app.CodeTransactionNotFound)

	// Inexistente.
	_, err = q.GetTransaction(ctx, app.GetTransactionQuery{TransactionID: uuid.Must(uuid.NewV7()), ProviderID: "provider-a"})
	wantAppError(t, err, app.KindNotFound, app.CodeTransactionNotFound)
	_, err = q.GetTransactionByProvider(ctx, app.GetTransactionByProviderQuery{ProviderID: "provider-a", ExternalTransactionID: "nope"})
	wantAppError(t, err, app.KindNotFound, app.CodeTransactionNotFound)

	// OPENING é interna: nenhum provedor a vê.
	var openingID uuid.UUID
	for _, w := range u.state.wagers {
		if w.Kind() == domain.WagerKindOpening {
			openingID = w.ID()
		}
	}
	_, err = q.GetTransaction(ctx, app.GetTransactionQuery{TransactionID: openingID, ProviderID: "provider-a"})
	wantAppError(t, err, app.KindNotFound, app.CodeTransactionNotFound)

	// Entradas inválidas não abrem transação.
	n := len(u.opts)
	_, err = q.GetTransaction(ctx, app.GetTransactionQuery{TransactionID: bet.TransactionID})
	wantAppError(t, err, app.KindInvalidInput, app.CodeInvalidProviderID)
	_, err = q.GetTransaction(ctx, app.GetTransactionQuery{ProviderID: "provider-a"})
	wantAppError(t, err, app.KindInvalidInput, app.CodeInvalidTransactionID)
	for _, bad := range []string{"", strings.Repeat("x", 256), "a\x00b", "\xff"} {
		_, err = q.GetTransactionByProvider(ctx, app.GetTransactionByProviderQuery{ProviderID: "provider-a", ExternalTransactionID: bad})
		wantAppError(t, err, app.KindInvalidInput, app.CodeInvalidExternalTransactionID)
		_, err = q.GetTransactionByProvider(ctx, app.GetTransactionByProviderQuery{ProviderID: bad, ExternalTransactionID: "bet-1"})
		wantAppError(t, err, app.KindInvalidInput, app.CodeInvalidProviderID)
	}
	if len(u.opts) != n {
		t.Fatal("tx opened for invalid input")
	}
}

func TestQueries_TransactionViewHidesForeignData(t *testing.T) {
	u := newMemUoW()
	walletID, _ := openedWallet(t, u)
	// Rejeição por carteira de outro jogador: sem saldo nem referência.
	cmd := wagerCmd(t, domain.WagerKindBet, "bet-x", "1.00", "")
	cmd.WalletID = walletID
	res, err := newProcess(t, u).Execute(t.Context(), cmd)
	if err != nil || res.FailureCode != domain.FailureCodeWalletPlayerMismatch {
		t.Fatalf("res = %+v, %v", res, err)
	}
	v, err := newQueries(t, u).GetTransaction(t.Context(), app.GetTransactionQuery{TransactionID: res.TransactionID, ProviderID: "provider-a"})
	if err != nil || v.Status != domain.WagerStatusRejected || v.FailureCode != domain.FailureCodeWalletPlayerMismatch ||
		v.Balance.IsValid() || v.ReferenceTransactionID != uuid.Nil {
		t.Fatalf("view = %+v, %v", v, err)
	}
}

func TestQueries_NilUoW(t *testing.T) {
	if _, err := app.NewQueries(nil); err == nil {
		t.Fatal("nil uow accepted")
	}
}

func TestModule_ProvidesQueriesAndReconcile(t *testing.T) {
	err := fx.ValidateApp(
		fx.Supply(fx.Annotate(&fakeUoW{}, fx.As(new(app.UnitOfWork)))),
		fx.Supply(fx.Annotate(&fakeRecorder{}, fx.As(new(app.ReconciliationRecorder)))),
		app.Module,
		fx.Invoke(func(*app.ReconcileWallet, *app.Queries) {}),
	)
	if err != nil {
		t.Fatal(err)
	}
	// Sem recorder (opcional).
	err = fx.ValidateApp(
		fx.Supply(fx.Annotate(&fakeUoW{}, fx.As(new(app.UnitOfWork)))),
		app.Module,
		fx.Invoke(func(*app.ReconcileWallet, *app.Queries) {}),
	)
	if err != nil {
		t.Fatal(err)
	}
}

// --- utilitários ---------------------------------------------------------------

func assertStateUnchanged(t *testing.T, before, after memState) {
	t.Helper()
	if len(before.wallets) != len(after.wallets) || len(before.wagers) != len(after.wagers) ||
		len(before.ledger) != len(after.ledger) || len(before.events) != len(after.events) {
		t.Fatalf("state changed: before %d/%d/%d/%d after %d/%d/%d/%d",
			len(before.wallets), len(before.wagers), len(before.ledger), len(before.events),
			len(after.wallets), len(after.wagers), len(after.ledger), len(after.events))
	}
	for id, w := range before.wallets {
		a := after.wallets[id]
		if a == nil || a.Balance() != w.Balance() || a.Version() != w.Version() || !a.UpdatedAt().Equal(w.UpdatedAt()) {
			t.Fatalf("wallet %s changed", id)
		}
	}
}

func findLog(t *testing.T, buf *bytes.Buffer, msg string) map[string]any {
	t.Helper()
	for _, l := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("log line %q: %v", l, err)
		}
		if m["msg"] == msg {
			return m
		}
	}
	t.Fatalf("log %q not found in %s", msg, buf.String())
	return nil
}

func jsonLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug}))
}
