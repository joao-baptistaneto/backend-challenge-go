//go:build integration

package app_test

// Integração do caso de uso OpenWallet (T14) com o adapter postgres real:
// carteira, OPENING, ledger e outbox no mesmo commit; saldo zero; conflito;
// atomicidade (rollback em falha de qualquer passo).

import (
	"context"
	"encoding/json"
	"errors"
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

type env struct {
	db  *postgres.DB
	uow *postgres.UnitOfWork
}

func newEnv(t *testing.T) env {
	t.Helper()
	db := startedDB(t)
	tm := postgres.NewTxManager(db, nil, nil)
	uow := postgres.NewUnitOfWork(tm, postgres.NewWalletRepository(), postgres.NewLedgerRepository(),
		postgres.NewWagerTransactionRepository(), postgres.NewOutboxRepository(), postgres.NewInboxRepository())
	return env{db: db, uow: uow}
}

func (e env) useCase(t *testing.T, uow app.UnitOfWork) *app.OpenWallet {
	t.Helper()
	if uow == nil {
		uow = e.uow
	}
	uc, err := app.NewOpenWallet(uow)
	if err != nil {
		t.Fatal(err)
	}
	return uc
}

func (e env) pool() *pgxpool.Pool { return e.db.Pool() }

type tableCounts struct{ wallets, wagers, ledger, outbox int }

func counts(t *testing.T, p *pgxpool.Pool) tableCounts {
	t.Helper()
	var c tableCounts
	err := p.QueryRow(t.Context(), `SELECT
 (SELECT count(*) FROM wallets), (SELECT count(*) FROM wager_transactions),
 (SELECT count(*) FROM wallet_ledger_entries), (SELECT count(*) FROM outbox)`).
		Scan(&c.wallets, &c.wagers, &c.ledger, &c.outbox)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func openCmd(t *testing.T, player uuid.UUID, amount, currency string) app.OpenWalletCommand {
	t.Helper()
	m, err := domain.ParseNonNegative(amount, currency)
	if err != nil {
		t.Fatal(err)
	}
	return app.OpenWalletCommand{PlayerID: player, InitialBalance: m, CorrelationID: "corr-" + player.String()[:8]}
}

func TestIntegrationOpenWallet_PositiveBalance(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	player := uuid.New()
	cmd := openCmd(t, player, "1000.00", "BRL")

	res, err := e.useCase(t, nil).Execute(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if res.Wallet.Version != 1 || res.Wallet.Balance.Amount() != "1000.00" || res.OpeningTransactionID == uuid.Nil {
		t.Fatalf("result = %+v", res)
	}
	walletID, openingID := res.Wallet.ID, res.OpeningTransactionID

	if c := counts(t, e.pool()); c != (tableCounts{1, 1, 1, 2}) {
		t.Fatalf("counts = %+v", c)
	}

	// Carteira: versão 1, saldo exato.
	var (
		balance, version int64
		currency         string
		walletXmin       string
	)
	if err := e.pool().QueryRow(ctx, `SELECT balance, version, currency, xmin::text FROM wallets WHERE id = $1 AND player_id = $2`,
		walletID, player).Scan(&balance, &version, &currency, &walletXmin); err != nil {
		t.Fatal(err)
	}
	if balance != 100000 || version != 1 || currency != "BRL" {
		t.Fatalf("wallet row = %d %d %s", balance, version, currency)
	}

	// OPENING PROCESSED interno, sem metadados externos.
	var (
		origin, kind, status string
		amount, balAfter     int64
		providerNull         bool
		wagerXmin            string
	)
	if err := e.pool().QueryRow(ctx, `SELECT origin, kind, status, amount, balance_after,
 provider_id IS NULL AND external_transaction_id IS NULL AND idempotency_key IS NULL AND round_id IS NULL,
 xmin::text FROM wager_transactions WHERE id = $1 AND wallet_id = $2`, openingID, walletID).
		Scan(&origin, &kind, &status, &amount, &balAfter, &providerNull, &wagerXmin); err != nil {
		t.Fatal(err)
	}
	if origin != "INTERNAL" || kind != "OPENING" || status != "PROCESSED" || amount != 100000 || balAfter != 100000 || !providerNull {
		t.Fatalf("opening row = %s %s %s %d %d %v", origin, kind, status, amount, balAfter, providerNull)
	}

	// Um único lançamento CREDIT 0 → 1000.00.
	var (
		direction            string
		lAmount, before, aft int64
		ledgerXmin           string
	)
	if err := e.pool().QueryRow(ctx, `SELECT direction, amount, balance_before, balance_after, xmin::text
 FROM wallet_ledger_entries WHERE wallet_id = $1 AND transaction_id = $2`, walletID, openingID).
		Scan(&direction, &lAmount, &before, &aft, &ledgerXmin); err != nil {
		t.Fatal(err)
	}
	if direction != "CREDIT" || lAmount != 100000 || before != 0 || aft != 100000 {
		t.Fatalf("ledger row = %s %d %d %d", direction, lAmount, before, aft)
	}

	// Dois eventos na outbox, em ordem: Processed (agregado = OPENING) e
	// BalanceChanged (agregado = carteira, walletVersion 1).
	rows, err := e.pool().Query(ctx, `SELECT event_type, aggregate_id, correlation_id, payload::text, xmin::text
 FROM outbox ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	type outboxRow struct{ typ, agg, corr, payload, xmin string }
	var events []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.typ, &r.agg, &r.corr, &r.payload, &r.xmin); err != nil {
			t.Fatal(err)
		}
		events = append(events, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 ||
		events[0].typ != "WagerTransactionProcessed" || events[0].agg != openingID.String() ||
		events[1].typ != "WalletBalanceChanged" || events[1].agg != walletID.String() {
		t.Fatalf("outbox = %+v", events)
	}
	var processed struct {
		Data struct {
			Kind         string          `json:"kind"`
			Origin       string          `json:"origin"`
			ProviderID   *string         `json:"providerId"`
			BalanceAfter json.RawMessage `json:"balanceAfter"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(events[0].payload), &processed); err != nil {
		t.Fatal(err)
	}
	if processed.Data.Kind != "OPENING" || processed.Data.Origin != "INTERNAL" || processed.Data.ProviderID != nil ||
		string(processed.Data.BalanceAfter) != `{"amount":"1000.00","currency":"BRL"}` {
		t.Fatalf("processed payload = %s", events[0].payload)
	}
	var changed struct {
		Data struct {
			WalletVersion int64           `json:"walletVersion"`
			Direction     string          `json:"direction"`
			BalanceBefore json.RawMessage `json:"balanceBefore"`
			BalanceAfter  json.RawMessage `json:"balanceAfter"`
			TransactionID uuid.UUID       `json:"transactionId"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(events[1].payload), &changed); err != nil {
		t.Fatal(err)
	}
	if changed.Data.WalletVersion != 1 || changed.Data.Direction != "CREDIT" || changed.Data.TransactionID != openingID ||
		string(changed.Data.BalanceBefore) != `{"amount":"0.00","currency":"BRL"}` ||
		string(changed.Data.BalanceAfter) != `{"amount":"1000.00","currency":"BRL"}` {
		t.Fatalf("balance changed payload = %s", events[1].payload)
	}
	for _, ev := range events {
		if ev.corr != cmd.CorrelationID {
			t.Fatalf("correlation = %q", ev.corr)
		}
	}

	// Mesmo commit: todas as linhas foram criadas pela mesma transação (xmin).
	for name, x := range map[string]string{
		"wager": wagerXmin, "ledger": ledgerXmin, "outbox0": events[0].xmin, "outbox1": events[1].xmin,
	} {
		if x != walletXmin {
			t.Fatalf("%s xmin %s != wallet xmin %s (not the same commit)", name, x, walletXmin)
		}
	}
}

func TestIntegrationOpenWallet_ZeroBalance(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	res, err := e.useCase(t, nil).Execute(t.Context(), openCmd(t, uuid.New(), "0.00", "BRL"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Wallet.Version != 1 || !res.Wallet.Balance.IsZero() || res.OpeningTransactionID != uuid.Nil {
		t.Fatalf("result = %+v", res)
	}
	if c := counts(t, e.pool()); c != (tableCounts{wallets: 1}) {
		t.Fatalf("counts = %+v, want only the wallet", c)
	}
	var balance, version int64
	if err := e.pool().QueryRow(t.Context(), `SELECT balance, version FROM wallets WHERE id = $1`, res.Wallet.ID).
		Scan(&balance, &version); err != nil {
		t.Fatal(err)
	}
	if balance != 0 || version != 1 {
		t.Fatalf("wallet row = %d %d", balance, version)
	}
}

func TestIntegrationOpenWallet_Conflict(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	uc := e.useCase(t, nil)
	player := uuid.New()
	first, err := uc.Execute(t.Context(), openCmd(t, player, "50.00", "BRL"))
	if err != nil {
		t.Fatal(err)
	}
	before := counts(t, e.pool())

	for _, amount := range []string{"50.00", "0.00", "7.00"} {
		_, err := uc.Execute(t.Context(), openCmd(t, player, amount, "BRL"))
		ae, ok := app.AsError(err)
		if !ok || ae.Kind != app.KindConflict || ae.Code != app.CodeWalletAlreadyExists ||
			!errors.Is(err, app.ErrConflict) || !errors.Is(err, app.ErrWalletAlreadyExists) {
			t.Fatalf("amount %s: err = %v, want conflict", amount, err)
		}
	}
	if after := counts(t, e.pool()); after != before {
		t.Fatalf("conflict had side effects: before %+v after %+v", before, after)
	}
	var balance int64
	if err := e.pool().QueryRow(t.Context(), `SELECT balance FROM wallets WHERE id = $1`, first.Wallet.ID).Scan(&balance); err != nil {
		t.Fatal(err)
	}
	if balance != 5000 {
		t.Fatalf("original wallet balance changed: %d", balance)
	}

	// Mesmo jogador, outra moeda: carteira independente.
	if _, err := uc.Execute(t.Context(), openCmd(t, player, "1.00", "USD")); err != nil {
		t.Fatalf("other currency: %v", err)
	}
}

// Aberturas concorrentes do mesmo (jogador, moeda): o UNIQUE do banco decide;
// exatamente uma vence e as demais recebem conflito sem efeitos.
func TestIntegrationOpenWallet_ConcurrentConflict(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	uc := e.useCase(t, nil)
	player := uuid.New()
	const n = 10
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		ok, confl int
		other     []error
	)
	cmd := openCmd(t, player, "25.00", "BRL")
	start := make(chan struct{})
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := uc.Execute(context.Background(), cmd)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				ok++
			case errors.Is(err, app.ErrWalletAlreadyExists):
				confl++
			default:
				other = append(other, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if ok != 1 || confl != n-1 || len(other) != 0 {
		t.Fatalf("ok %d conflicts %d other %v", ok, confl, other)
	}
	if c := counts(t, e.pool()); c != (tableCounts{1, 1, 1, 2}) {
		t.Fatalf("counts = %+v", c)
	}
}

// failingUoW executa os stores reais e injeta uma falha DEPOIS do passo
// escolhido, para provar que o que já foi gravado na transação é desfeito.
type failingUoW struct {
	inner app.UnitOfWork
	step  string
	err   error
}

func (f failingUoW) WithinTx(ctx context.Context, opts app.TxOptions, fn app.TxFunc) error {
	return f.inner.WithinTx(ctx, opts, func(ctx context.Context, tx app.Tx) error {
		return fn(ctx, failingTx{Tx: tx, f: f})
	})
}

type failingTx struct {
	app.Tx
	f failingUoW
}

func (t failingTx) after(step string, err error) error {
	if err != nil {
		return err
	}
	if t.f.step == step {
		return t.f.err
	}
	return nil
}

// Os stores embutem o store real (métodos sem falha injetada passam direto).
type failWallets struct {
	failingTx
	app.WalletStore
}

func (s failWallets) Insert(ctx context.Context, w *domain.Wallet) error {
	return s.after("wallet", s.WalletStore.Insert(ctx, w))
}

func (s failWallets) Update(ctx context.Context, w *domain.Wallet) error {
	return s.after("wallet_update", s.WalletStore.Update(ctx, w))
}

type failWagers struct {
	failingTx
	app.WagerTransactionStore
}

func (s failWagers) Insert(ctx context.Context, w *domain.WagerTransaction) error {
	return s.after("wager", s.WagerTransactionStore.Insert(ctx, w))
}

type failInbox struct {
	failingTx
	app.InboxStore
}

func (s failInbox) Complete(ctx context.Context, c, m, h string, txID uuid.UUID, now time.Time) error {
	return s.after("inbox_complete", s.InboxStore.Complete(ctx, c, m, h, txID, now))
}

type failLedger struct {
	failingTx
	app.LedgerStore
}

func (s failLedger) Append(ctx context.Context, en domain.WalletLedgerEntry) error {
	return s.after("ledger", s.LedgerStore.Append(ctx, en))
}

type failOutbox struct{ failingTx }

func (s failOutbox) Enqueue(ctx context.Context, events ...domain.IntegrationEvent) error {
	return s.after("outbox", s.Tx.Outbox().Enqueue(ctx, events...))
}

func (t failingTx) Wallets() app.WalletStore { return failWallets{t, t.Tx.Wallets()} }
func (t failingTx) Transactions() app.WagerTransactionStore {
	return failWagers{t, t.Tx.Transactions()}
}
func (t failingTx) Ledger() app.LedgerStore { return failLedger{t, t.Tx.Ledger()} }
func (t failingTx) Outbox() app.OutboxStore { return failOutbox{t} }
func (t failingTx) Inbox() app.InboxStore   { return failInbox{t, t.Tx.Inbox()} }

func TestIntegrationOpenWallet_AtomicRollback(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	boom := errors.New("injected failure")
	player := uuid.New()
	for _, step := range []string{"wallet", "wager", "ledger", "outbox"} {
		_, err := e.useCase(t, failingUoW{inner: e.uow, step: step, err: boom}).
			Execute(t.Context(), openCmd(t, player, "300.00", "BRL"))
		if !errors.Is(err, boom) {
			t.Fatalf("step %s: err = %v", step, err)
		}
		if c := counts(t, e.pool()); c != (tableCounts{}) {
			t.Fatalf("step %s: partial commit %+v", step, c)
		}
	}
	// Nada ficou para trás: o mesmo jogador abre normalmente.
	if _, err := e.useCase(t, nil).Execute(t.Context(), openCmd(t, player, "300.00", "BRL")); err != nil {
		t.Fatalf("open after rollbacks: %v", err)
	}
	if c := counts(t, e.pool()); c != (tableCounts{1, 1, 1, 2}) {
		t.Fatalf("counts = %+v", c)
	}
}

// Falha real do PostgreSQL no último passo (INSERT do 2º evento na outbox,
// barrado por trigger): carteira, OPENING, ledger e o 1º evento, já gravados
// na transação, são desfeitos.
func TestIntegrationOpenWallet_OutboxDatabaseFailureRollsBack(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	if _, err := e.pool().Exec(ctx, `
CREATE FUNCTION test_fail_balance_changed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.event_type = 'WalletBalanceChanged' THEN
        RAISE EXCEPTION 'injected outbox failure';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER test_fail_balance_changed BEFORE INSERT ON outbox
    FOR EACH ROW EXECUTE FUNCTION test_fail_balance_changed();`); err != nil {
		t.Fatal(err)
	}
	player := uuid.New()
	_, err := e.useCase(t, nil).Execute(ctx, openCmd(t, player, "80.00", "BRL"))
	if err == nil {
		t.Fatal("want error")
	}
	if _, ok := app.AsError(err); ok {
		t.Fatalf("database failure classified as app error: %v", err)
	}
	if c := counts(t, e.pool()); c != (tableCounts{}) {
		t.Fatalf("partial commit %+v", c)
	}
	if _, err := e.pool().Exec(ctx, `DROP TRIGGER test_fail_balance_changed ON outbox`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.useCase(t, nil).Execute(ctx, openCmd(t, player, "80.00", "BRL")); err != nil {
		t.Fatalf("open after rollback: %v", err)
	}
}

// Composição Fx: postgres.Module liga app.UnitOfWork ao adapter e app.Module
// provê o caso de uso.
func TestIntegrationOpenWallet_FxComposition(t *testing.T) {
	t.Parallel()
	dsn := migratedDSN(t)
	var uc *app.OpenWallet
	fxApp := fx.New(
		fx.NopLogger,
		fx.Supply(config.Config{Database: dbConfig(dsn)}),
		metrics.Module,
		postgres.Module,
		app.Module,
		fx.Populate(&uc),
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
	res, err := uc.Execute(ctx, openCmd(t, uuid.New(), "12.34", "BRL"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Wallet.Version != 1 || res.Wallet.Balance.Amount() != "12.34" {
		t.Fatalf("result = %+v", res)
	}
}
