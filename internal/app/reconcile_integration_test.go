//go:build integration

package app_test

// Integração da reconciliação e das consultas (T16) com o adapter postgres
// real: caso consistente e divergente sem alteração de dados, transação
// REPEATABLE READ + READ ONLY de fato (escrita recusada, snapshot estável sob
// commit concorrente), métrica e log de divergência, consultas com
// isolamento por provedor e paginação do extrato.

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/fx"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

// dbFingerprint resume TODO o estado de negócio: o conteúdo de cada linha e o
// xmin (a transação que gravou a versão atual da linha). Um UPDATE que grave
// os mesmos valores ainda muda o xmin, então igualdade prova que nada foi
// escrito.
func dbFingerprint(t *testing.T, p *pgxpool.Pool) string {
	t.Helper()
	var parts []string
	for _, table := range []string{"wallets", "wager_transactions", "wallet_ledger_entries", "outbox", "inbox"} {
		var s string
		err := p.QueryRow(t.Context(), `SELECT count(*)::text || ':' ||
 coalesce(string_agg(r::text || '@' || r.xmin::text, '|' ORDER BY r::text), '') FROM `+table+` r`).Scan(&s)
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, table+"="+s)
	}
	return strings.Join(parts, "\n")
}

func newMetrics(t *testing.T) *metrics.Metrics {
	t.Helper()
	m, err := metrics.New(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

func (e env) reconciler(t *testing.T, uow app.UnitOfWork, opts ...app.Option) *app.ReconcileWallet {
	t.Helper()
	if uow == nil {
		uow = e.uow
	}
	uc, err := app.NewReconcileWallet(uow, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return uc
}

func (e env) queries(t *testing.T) *app.Queries {
	t.Helper()
	q, err := app.NewQueries(e.uow)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func TestIntegrationReconcile_Consistent(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := e.openWallet(t, "1000.00")
	uc := e.processor(t, nil)
	process(t, uc, w.cmd(t, domain.WagerKindBet, "c-bet", "25.00", ""))
	process(t, uc, w.cmd(t, domain.WagerKindWin, "c-win", "10.00", "c-bet"))
	process(t, uc, w.cmd(t, domain.WagerKindLoss, "c-loss", "0.00", ""))

	m := newMetrics(t)
	var logs bytes.Buffer
	before := dbFingerprint(t, e.pool())
	rep, err := e.reconciler(t, nil, app.WithReconciliationRecorder(m), app.WithLogger(jsonLogger(&logs))).
		Execute(t.Context(), app.ReconcileCommand{WalletID: w.id, CorrelationID: "corr-rec"})
	if err != nil {
		t.Fatal(err)
	}
	if rep.WalletID != w.id || rep.StoredBalance.Amount() != "985.00" || rep.CalculatedBalance.Amount() != "985.00" ||
		rep.Difference.Amount() != "0.00" || !rep.Consistent || rep.CheckedEntries != 3 || rep.WalletVersion != 3 {
		t.Fatalf("report = %+v", rep)
	}
	if after := dbFingerprint(t, e.pool()); after != before {
		t.Fatalf("reconciliation changed data:\nbefore %s\nafter  %s", before, after)
	}
	if v := counterValue(t, m.ReconciliationsTotal.WithLabelValues(metrics.ReconciliationConsistent)); v != 1 {
		t.Fatalf("consistent counter = %v", v)
	}
	if v := counterValue(t, m.ReconciliationDivergencesTotal.WithLabelValues("BRL")); v != 0 {
		t.Fatalf("divergence counter = %v", v)
	}
	if strings.Contains(logs.String(), "divergência") {
		t.Fatalf("divergence logged: %s", logs.String())
	}

	// Carteira aberta com saldo zero: sem lançamentos e consistente.
	z := e.openWallet(t, "0.00")
	rep, err = e.reconciler(t, nil).Execute(t.Context(), app.ReconcileCommand{WalletID: z.id})
	if err != nil || !rep.Consistent || rep.CheckedEntries != 0 || rep.StoredBalance.Amount() != "0.00" {
		t.Fatalf("zero wallet = %+v, %v", rep, err)
	}

	_, err = e.reconciler(t, nil).Execute(t.Context(), app.ReconcileCommand{WalletID: uuid.New()})
	wantAppError(t, err, app.KindNotFound, app.CodeWalletNotFound)
}

func TestIntegrationReconcile_DivergentReportsWithoutChangingData(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := e.openWallet(t, "1000.00")
	process(t, e.processor(t, nil), w.cmd(t, domain.WagerKindBet, "d-bet", "25.00", ""))

	// Corrompe o saldo gravado fora da aplicação. O guard de UPDATE
	// (wallets_guard_update) e o trigger de versão são desabilitados só nesta
	// sessão (session_replication_role = replica, papel de superusuário do
	// banco de teste), sem tocar no ledger.
	ctx := t.Context()
	conn, err := e.pool().Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE wallets SET balance = balance + 550 WHERE id = $1`, w.id); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	conn.Release()
	if bal, ver, entries := walletState(t, e.pool(), w.id); bal != 98050 || ver != 2 || entries != 2 {
		t.Fatalf("tampered wallet = %d v%d entries %d", bal, ver, entries)
	}

	m := newMetrics(t)
	var logs bytes.Buffer
	before := dbFingerprint(t, e.pool())
	rep, err := e.reconciler(t, nil, app.WithReconciliationRecorder(m), app.WithLogger(jsonLogger(&logs))).
		Execute(ctx, app.ReconcileCommand{WalletID: w.id, CorrelationID: "corr-div"})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Consistent || rep.StoredBalance.Amount() != "980.50" || rep.CalculatedBalance.Amount() != "975.00" ||
		rep.Difference.Amount() != "5.50" || rep.CheckedEntries != 2 || rep.WalletVersion != 2 {
		t.Fatalf("report = %+v", rep)
	}
	// Nada foi alterado: nem o saldo divergente foi "corrigido".
	if after := dbFingerprint(t, e.pool()); after != before {
		t.Fatalf("reconciliation changed data:\nbefore %s\nafter  %s", before, after)
	}
	if bal, _, _ := walletState(t, e.pool(), w.id); bal != 98050 {
		t.Fatalf("balance changed to %d", bal)
	}
	// Métrica e log.
	if v := counterValue(t, m.ReconciliationDivergencesTotal.WithLabelValues("BRL")); v != 1 {
		t.Fatalf("divergence counter = %v", v)
	}
	if v := counterValue(t, m.ReconciliationsTotal.WithLabelValues(metrics.ReconciliationDivergent)); v != 1 {
		t.Fatalf("divergent counter = %v", v)
	}
	line := findLog(t, &logs, "divergência de reconciliação")
	for k, v := range map[string]any{
		"level": "WARN", "walletId": w.id.String(), "storedBalance": "980.50", "calculatedBalance": "975.00",
		"difference": "5.50", "currency": "BRL", "correlationId": "corr-div", "checkedEntries": float64(2),
	} {
		if line[k] != v {
			t.Fatalf("log[%s] = %v, want %v", k, line[k], v)
		}
	}

	// Repetir não muda nada e conta de novo.
	if _, err := e.reconciler(t, nil, app.WithReconciliationRecorder(m)).Execute(ctx, app.ReconcileCommand{WalletID: w.id}); err != nil {
		t.Fatal(err)
	}
	if v := counterValue(t, m.ReconciliationDivergencesTotal.WithLabelValues("BRL")); v != 2 {
		t.Fatalf("divergence counter = %v", v)
	}
	if after := dbFingerprint(t, e.pool()); after != before {
		t.Fatal("second reconciliation changed data")
	}
}

// probeUoW envolve a UoW real: registra as opções pedidas pelo caso de uso,
// pode trocar o isolamento (controle negativo) e injeta ações nos stores da
// MESMA transação aberta pelo caso de uso.
type probeUoW struct {
	inner           app.UnitOfWork
	opts            *[]app.TxOptions
	forceIsolation  *app.IsolationLevel
	afterWalletGet  func(ctx context.Context, tx app.Tx, w *domain.Wallet) error
	beforeSummarize func(ctx context.Context)
}

func (p probeUoW) WithinTx(ctx context.Context, opts app.TxOptions, fn app.TxFunc) error {
	*p.opts = append(*p.opts, opts)
	if p.forceIsolation != nil {
		opts.Isolation = *p.forceIsolation
	}
	return p.inner.WithinTx(ctx, opts, func(ctx context.Context, tx app.Tx) error {
		return fn(ctx, probeTx{Tx: tx, p: p})
	})
}

type probeTx struct {
	app.Tx
	p probeUoW
}

func (t probeTx) Wallets() app.WalletStore { return probeWallets{WalletStore: t.Tx.Wallets(), t: t} }
func (t probeTx) Ledger() app.LedgerStore  { return probeLedger{LedgerStore: t.Tx.Ledger(), t: t} }

type probeWallets struct {
	app.WalletStore
	t probeTx
}

func (s probeWallets) Get(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	w, err := s.WalletStore.Get(ctx, id)
	if err == nil && s.t.p.afterWalletGet != nil {
		if herr := s.t.p.afterWalletGet(ctx, s.t.Tx, w); herr != nil {
			return nil, herr
		}
	}
	return w, err
}

type probeLedger struct {
	app.LedgerStore
	t probeTx
}

func (s probeLedger) Summarize(ctx context.Context, id uuid.UUID) (app.LedgerSummary, error) {
	if s.t.p.beforeSummarize != nil {
		s.t.p.beforeSummarize(ctx)
	}
	return s.LedgerStore.Summarize(ctx, id)
}

// A transação da reconciliação é READ ONLY: uma escrita feita DENTRO dela
// (pelos stores do mesmo app.Tx) é recusada pelo PostgreSQL com 25006.
func TestIntegrationReconcile_TransactionIsReadOnly(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := e.openWallet(t, "100.00")
	before := dbFingerprint(t, e.pool())

	var (
		opts     []app.TxOptions
		writeErr error
	)
	probe := probeUoW{inner: e.uow, opts: &opts,
		afterWalletGet: func(ctx context.Context, tx app.Tx, wallet *domain.Wallet) error {
			m, _ := domain.ParseNonNegative("1.00", "BRL")
			if _, err := wallet.Credit(m, uuid.New(), time.Now()); err != nil {
				return err
			}
			writeErr = tx.Wallets().Update(ctx, wallet)
			return writeErr
		}}
	_, err := e.reconciler(t, probe).Execute(t.Context(), app.ReconcileCommand{WalletID: w.id})
	if err == nil {
		t.Fatal("reconciliation succeeded after a write inside a read-only transaction")
	}
	var pgErr *pgconn.PgError
	if !errors.As(writeErr, &pgErr) || pgErr.Code != "25006" { // read_only_sql_transaction
		t.Fatalf("write inside reconciliation tx: %v", writeErr)
	}
	if len(opts) != 1 || opts[0].Isolation != app.IsolationRepeatableRead || !opts[0].ReadOnly {
		t.Fatalf("tx options = %+v", opts)
	}
	if after := dbFingerprint(t, e.pool()); after != before {
		t.Fatal("data changed")
	}
}

// A transação da reconciliação é REPEATABLE READ: uma BET confirmada por
// outra transação ENTRE a leitura da carteira e a soma do ledger não aparece
// na soma (mesmo snapshot), então o resultado continua consistente. O
// controle negativo (mesmo roteiro forçado para READ COMMITTED) mostra a
// falsa divergência que o snapshot evita.
func TestIntegrationReconcile_RepeatableReadSnapshot(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := e.openWallet(t, "1000.00")
	uc := e.processor(t, nil)

	run := func(ext string, force *app.IsolationLevel) app.ReconciliationReport {
		t.Helper()
		var opts []app.TxOptions
		fired := false
		probe := probeUoW{inner: e.uow, opts: &opts, forceIsolation: force,
			beforeSummarize: func(context.Context) {
				if fired {
					return // retry da transação: não repete a BET
				}
				fired = true
				// Outra transação (outra conexão do pool) confirma uma BET.
				process(t, uc, w.cmd(t, domain.WagerKindBet, ext, "25.00", ""))
			}}
		rep, err := e.reconciler(t, probe).Execute(t.Context(), app.ReconcileCommand{WalletID: w.id})
		if err != nil {
			t.Fatal(err)
		}
		if !fired {
			t.Fatal("concurrent commit not injected")
		}
		return rep
	}

	rep := run("rr-bet-1", nil)
	if !rep.Consistent || rep.StoredBalance.Amount() != "1000.00" || rep.CalculatedBalance.Amount() != "1000.00" ||
		rep.CheckedEntries != 1 {
		t.Fatalf("repeatable read report = %+v", rep)
	}
	// O commit concorrente existe e a próxima reconciliação o vê.
	rep, err := e.reconciler(t, nil).Execute(t.Context(), app.ReconcileCommand{WalletID: w.id})
	if err != nil || !rep.Consistent || rep.StoredBalance.Amount() != "975.00" || rep.CheckedEntries != 2 {
		t.Fatalf("after commit = %+v, %v", rep, err)
	}

	rc := app.IsolationReadCommitted
	rep = run("rc-bet-2", &rc)
	if rep.Consistent || rep.StoredBalance.Amount() != "975.00" || rep.CalculatedBalance.Amount() != "950.00" {
		t.Fatalf("read committed control = %+v (expected a false divergence)", rep)
	}
}

func TestIntegrationQueries_WalletAndLedger(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := e.openWallet(t, "100.00")
	other := e.openWallet(t, "5.00")
	uc := e.processor(t, nil)
	for _, ext := range []string{"l1", "l2", "l3", "l4", "l5", "l6"} {
		process(t, uc, w.cmd(t, domain.WagerKindBet, ext, "1.00", ""))
	}
	q := e.queries(t)
	ctx := t.Context()

	snap, err := q.GetWallet(ctx, w.id)
	if err != nil || snap.ID != w.id || snap.PlayerID != w.player || snap.Balance.Amount() != "94.00" || snap.Version != 7 {
		t.Fatalf("wallet = %+v, %v", snap, err)
	}
	_, err = q.GetWallet(ctx, uuid.New())
	wantAppError(t, err, app.KindNotFound, app.CodeWalletNotFound)

	// Paginação de 3 em 3 sobre 7 lançamentos; um lançamento confirmado no
	// meio da leitura aparece no fim, sem duplicar nem pular.
	var (
		got    []app.LedgerEntryView
		cursor string
		pages  int
	)
	for {
		p, err := q.ListLedger(ctx, app.ListLedgerQuery{WalletID: w.id, Cursor: cursor, Limit: 3})
		if err != nil {
			t.Fatal(err)
		}
		pages++
		got = append(got, p.Entries...)
		if pages == 1 {
			process(t, uc, w.cmd(t, domain.WagerKindBet, "l7", "1.00", ""))
		}
		if p.NextCursor == "" {
			break
		}
		cursor = p.NextCursor
	}
	if pages != 3 || len(got) != 8 {
		t.Fatalf("pages %d entries %d", pages, len(got))
	}
	full, err := q.ListLedger(ctx, app.ListLedgerQuery{WalletID: w.id, Limit: app.MaxLedgerPageLimit})
	if err != nil || len(full.Entries) != 8 || full.NextCursor != "" {
		t.Fatalf("full = %d %q %v", len(full.Entries), full.NextCursor, err)
	}
	for i := range got {
		if got[i] != full.Entries[i] {
			t.Fatalf("entry %d differs: %+v vs %+v", i, got[i], full.Entries[i])
		}
		if i > 0 && got[i].BalanceBefore != got[i-1].BalanceAfter {
			t.Fatalf("chain broken at %d", i)
		}
	}
	if got[0].Direction != domain.LedgerDirectionCredit || got[7].BalanceAfter.Amount() != "93.00" {
		t.Fatalf("first/last = %+v / %+v", got[0], got[7])
	}

	// Cursor de outra carteira, adulterado, limite inválido, carteira inexistente.
	p1, err := q.ListLedger(ctx, app.ListLedgerQuery{WalletID: w.id, Limit: 1})
	if err != nil || p1.NextCursor == "" {
		t.Fatal(err)
	}
	_, err = q.ListLedger(ctx, app.ListLedgerQuery{WalletID: other.id, Cursor: p1.NextCursor})
	wantAppError(t, err, app.KindInvalidInput, app.CodeInvalidCursor)
	_, err = q.ListLedger(ctx, app.ListLedgerQuery{WalletID: w.id, Cursor: "%%%"})
	wantAppError(t, err, app.KindInvalidInput, app.CodeInvalidCursor)
	_, err = q.ListLedger(ctx, app.ListLedgerQuery{WalletID: w.id, Limit: app.MaxLedgerPageLimit + 1})
	wantAppError(t, err, app.KindInvalidInput, app.CodeInvalidPageLimit)
	_, err = q.ListLedger(ctx, app.ListLedgerQuery{WalletID: uuid.New()})
	wantAppError(t, err, app.KindNotFound, app.CodeWalletNotFound)
}

func TestIntegrationQueries_TransactionsIsolatedByProvider(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := e.openWallet(t, "100.00")
	uc := e.processor(t, nil)
	bet := process(t, uc, w.cmd(t, domain.WagerKindBet, "q-bet", "30.00", ""))
	win := process(t, uc, w.cmd(t, domain.WagerKindWin, "q-win", "5.00", "q-bet"))
	pending := process(t, uc, w.cmd(t, domain.WagerKindRollback, "q-rb", "1.00", "missing-ref"))
	// O provider-b usa o MESMO externalId para a sua própria operação.
	cmdB := w.cmd(t, domain.WagerKindBet, "q-bet", "2.00", "")
	cmdB.ProviderID, cmdB.IdempotencyKey = "provider-b", "provider-b:q-bet"
	betB := process(t, uc, cmdB)

	q := e.queries(t)
	ctx := t.Context()
	before := dbFingerprint(t, e.pool())

	v, err := q.GetTransaction(ctx, app.GetTransactionQuery{TransactionID: win.TransactionID, ProviderID: provider})
	if err != nil {
		t.Fatal(err)
	}
	if v.TransactionID != win.TransactionID || v.Kind != domain.WagerKindWin || v.Status != domain.WagerStatusProcessed ||
		v.ProviderID != provider || v.ExternalTransactionID != "q-win" || v.WalletID != w.id || v.PlayerID != w.player ||
		v.Money.Amount() != "5.00" || v.Balance.Amount() != "75.00" || v.ReferenceExternalTransactionID != "q-bet" ||
		v.ReferenceTransactionID != bet.TransactionID || v.GameID != "fortune-chimp" || v.CompletedAt.IsZero() {
		t.Fatalf("view = %+v", v)
	}
	byExt, err := q.GetTransactionByProvider(ctx, app.GetTransactionByProviderQuery{ProviderID: provider, ExternalTransactionID: "q-win"})
	if err != nil || byExt != v {
		t.Fatalf("by provider = %+v, %v", byExt, err)
	}

	// Pendência acompanhável.
	pv, err := q.GetTransactionByProvider(ctx, app.GetTransactionByProviderQuery{ProviderID: provider, ExternalTransactionID: "q-rb"})
	if err != nil || pv.TransactionID != pending.TransactionID || pv.Status != domain.WagerStatusPendingReference ||
		pv.Attempts != 1 || pv.NextAttemptAt.IsZero() || pv.Balance.IsValid() {
		t.Fatalf("pending = %+v, %v", pv, err)
	}

	// Cada provedor vê só a sua operação com o mesmo externalId.
	a, err := q.GetTransactionByProvider(ctx, app.GetTransactionByProviderQuery{ProviderID: provider, ExternalTransactionID: "q-bet"})
	if err != nil || a.TransactionID != bet.TransactionID {
		t.Fatalf("provider-a q-bet = %+v, %v", a, err)
	}
	b, err := q.GetTransactionByProvider(ctx, app.GetTransactionByProviderQuery{ProviderID: "provider-b", ExternalTransactionID: "q-bet"})
	if err != nil || b.TransactionID != betB.TransactionID || b.Money.Amount() != "2.00" {
		t.Fatalf("provider-b q-bet = %+v, %v", b, err)
	}

	// Provedor diferente → não encontrado (sem revelar existência).
	_, err = q.GetTransaction(ctx, app.GetTransactionQuery{TransactionID: win.TransactionID, ProviderID: "provider-b"})
	wantAppError(t, err, app.KindNotFound, app.CodeTransactionNotFound)
	_, err = q.GetTransactionByProvider(ctx, app.GetTransactionByProviderQuery{ProviderID: "provider-b", ExternalTransactionID: "q-win"})
	wantAppError(t, err, app.KindNotFound, app.CodeTransactionNotFound)
	_, err = q.GetTransaction(ctx, app.GetTransactionQuery{TransactionID: betB.TransactionID, ProviderID: provider})
	wantAppError(t, err, app.KindNotFound, app.CodeTransactionNotFound)

	// Inexistente.
	_, err = q.GetTransaction(ctx, app.GetTransactionQuery{TransactionID: uuid.New(), ProviderID: provider})
	wantAppError(t, err, app.KindNotFound, app.CodeTransactionNotFound)
	_, err = q.GetTransactionByProvider(ctx, app.GetTransactionByProviderQuery{ProviderID: provider, ExternalTransactionID: "nope"})
	wantAppError(t, err, app.KindNotFound, app.CodeTransactionNotFound)

	// OPENING (interna) não é visível a provedor algum.
	var openingID uuid.UUID
	if err := e.pool().QueryRow(ctx, `SELECT id FROM wager_transactions WHERE wallet_id = $1 AND kind = 'OPENING'`, w.id).Scan(&openingID); err != nil {
		t.Fatal(err)
	}
	_, err = q.GetTransaction(ctx, app.GetTransactionQuery{TransactionID: openingID, ProviderID: provider})
	wantAppError(t, err, app.KindNotFound, app.CodeTransactionNotFound)

	if after := dbFingerprint(t, e.pool()); after != before {
		t.Fatal("queries changed data")
	}
}

// Composição Fx: o metrics.Module fornece *metrics.Metrics como
// ReconciliationRecorder (fiação prevista para cmd/server, T23).
func TestIntegrationReconcile_FxComposition(t *testing.T) {
	t.Parallel()
	dsn := migratedDSN(t)
	var (
		open *app.OpenWallet
		rec  *app.ReconcileWallet
		q    *app.Queries
		m    *metrics.Metrics
	)
	fxApp := fx.New(
		fx.NopLogger,
		fx.Supply(config.Config{Database: dbConfig(dsn)}),
		metrics.Module,
		postgres.Module,
		app.Module,
		fx.Provide(func(m *metrics.Metrics) app.ReconciliationRecorder { return m }),
		fx.Populate(&open, &rec, &q, &m),
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
	opened, err := open.Execute(ctx, openCmd(t, uuid.New(), "10.00", "BRL"))
	if err != nil {
		t.Fatal(err)
	}
	rep, err := rec.Execute(ctx, app.ReconcileCommand{WalletID: opened.Wallet.ID})
	if err != nil || !rep.Consistent || rep.CheckedEntries != 1 {
		t.Fatalf("report = %+v, %v", rep, err)
	}
	if v := counterValue(t, m.ReconciliationsTotal.WithLabelValues(metrics.ReconciliationConsistent)); v != 1 {
		t.Fatalf("metric = %v", v)
	}
	if snap, err := q.GetWallet(ctx, opened.Wallet.ID); err != nil || snap.Balance.Amount() != "10.00" {
		t.Fatalf("wallet = %+v, %v", snap, err)
	}
}
