package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

func TestOperationLabelsMatchMetrics(t *testing.T) {
	if app.OperationProcessTransaction != metrics.OperationProcessTransaction {
		t.Fatalf("app.OperationProcessTransaction = %q, metrics = %q", app.OperationProcessTransaction, metrics.OperationProcessTransaction)
	}
	if app.OperationOpenWallet != metrics.OperationOpenWallet {
		t.Fatalf("app.OperationOpenWallet = %q, metrics = %q", app.OperationOpenWallet, metrics.OperationOpenWallet)
	}
}

func TestPortError(t *testing.T) {
	transient := &pgconn.PgError{Code: "40001"}
	if err := portError(transient); !errors.Is(err, app.ErrUnavailable) || !errors.Is(err, transient) {
		t.Fatalf("transient: %v", err)
	}
	unknown := fmt.Errorf("%w: eof", ErrCommitOutcomeUnknown)
	if err := portError(unknown); !errors.Is(err, app.ErrUnavailable) || !errors.Is(err, ErrCommitOutcomeUnknown) {
		t.Fatalf("commit unknown: %v", err)
	}
	unique := &pgconn.PgError{Code: "23505", ConstraintName: "wallets_player_currency_key"}
	if err := portError(unique); errors.Is(err, app.ErrUnavailable) {
		t.Fatalf("unique marked unavailable: %v", err)
	}
	if err := portError(context.Canceled); errors.Is(err, app.ErrUnavailable) {
		t.Fatal("canceled marked unavailable")
	}
	if portError(nil) != nil {
		t.Fatal("nil")
	}
}

// recordingTransactor captura as opções e devolve um erro fixo sem banco.
type recordingTransactor struct {
	opts []TxOption
	err  error
}

func (r *recordingTransactor) WithinTx(ctx context.Context, fn TxFunc, opts ...TxOption) error {
	r.opts = opts
	if r.err != nil {
		return r.err
	}
	return fn(ctx, nil)
}

func TestUnitOfWork_RetryOptionAndErrors(t *testing.T) {
	rt := &recordingTransactor{}
	u := NewUnitOfWork(rt, NewWalletRepository(), NewLedgerRepository(), NewWagerTransactionRepository(), NewOutboxRepository(), NewInboxRepository())

	called := false
	err := u.WithinTx(t.Context(), app.TxOptions{Operation: app.OperationOpenWallet, Retry: true},
		func(ctx context.Context, tx app.Tx) error {
			called = true
			if tx.Wallets() == nil || tx.Transactions() == nil || tx.Ledger() == nil || tx.Outbox() == nil {
				t.Fatal("nil store")
			}
			return nil
		})
	if err != nil || !called {
		t.Fatalf("err %v called %v", err, called)
	}
	cfg := txConfig{}
	for _, o := range rt.opts {
		o(&cfg)
	}
	if cfg.retry.MaxAttempts <= 1 || cfg.retry.Operation != app.OperationOpenWallet {
		t.Fatalf("retry policy = %+v", cfg.retry)
	}

	rt.opts = nil
	_ = u.WithinTx(t.Context(), app.TxOptions{}, func(context.Context, app.Tx) error { return nil })
	if len(rt.opts) != 0 {
		t.Fatal("retry enabled without Retry")
	}

	rt.err = fmt.Errorf("postgres: begin: %w", &pgconn.PgError{Code: "57P03"})
	if err := u.WithinTx(t.Context(), app.TxOptions{}, func(context.Context, app.Tx) error { return nil }); !errors.Is(err, app.ErrUnavailable) {
		t.Fatalf("transient begin: %v", err)
	}
}

// fakeExecTx devolve um erro fixo em Exec (para o mapeamento do store).
type fakeExecTx struct {
	pgx.Tx
	err error
}

func (f fakeExecTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, f.err
}

func TestWalletStore_MapsDuplicateToAppConflict(t *testing.T) {
	u := NewUnitOfWork(nil, NewWalletRepository(), nil, nil, nil, nil)
	w := newTestWallet(t)
	s := txStores{u: u, q: fakeExecTx{err: &pgconn.PgError{Code: "23505", ConstraintName: "wallets_player_currency_key"}}}
	err := s.Wallets().Insert(t.Context(), w)
	if !errors.Is(err, app.ErrWalletAlreadyExists) || !errors.Is(err, app.ErrConflict) || !errors.Is(err, ErrWalletAlreadyExists) {
		t.Fatalf("err = %v", err)
	}
	s.q = fakeExecTx{err: &pgconn.PgError{Code: "23505", ConstraintName: "wallets_pkey"}}
	if err := s.Wallets().Insert(t.Context(), w); errors.Is(err, app.ErrWalletAlreadyExists) {
		t.Fatalf("pkey collision mapped to wallet conflict: %v", err)
	}
}

func newTestWallet(t *testing.T) *domain.Wallet {
	t.Helper()
	zero, err := domain.Zero("BRL")
	if err != nil {
		t.Fatal(err)
	}
	w, _, err := domain.NewWallet(domain.NewWalletParams{ID: unitWalletID, PlayerID: unitPlayerID, InitialBalance: zero, Now: unitT0})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestUnitOfWork_RetryIfIsForwarded(t *testing.T) {
	rt := &recordingTransactor{}
	u := NewUnitOfWork(rt, nil, nil, nil, nil, nil)
	sentinel := errors.New("retry me")
	_ = u.WithinTx(t.Context(), app.TxOptions{
		Operation: app.OperationProcessTransaction, Retry: true,
		RetryIf: func(err error) bool { return errors.Is(err, sentinel) },
	}, func(context.Context, app.Tx) error { return nil })
	cfg := txConfig{}
	for _, o := range rt.opts {
		o(&cfg)
	}
	if cfg.retry.RetryIf == nil || !cfg.retry.RetryIf(sentinel) || cfg.retry.RetryIf(errors.New("other")) {
		t.Fatal("RetryIf not forwarded to the retry policy")
	}
	if cfg.retry.Operation != app.OperationProcessTransaction {
		t.Fatalf("operation = %q", cfg.retry.Operation)
	}
}

func TestWagerStore_MapsDuplicatesToApp(t *testing.T) {
	u := NewUnitOfWork(nil, nil, nil, NewWagerTransactionRepository(), nil, nil)
	key := strings.Repeat("a", 64)
	tx, err := domain.NewExternalWagerTransaction(domain.NewExternalWagerTransactionParams{
		ID: uuid.New(), ProviderID: "p", ExternalTransactionID: "e", IdempotencyKey: "k", RequestHash: key,
		WalletID: unitWalletID, PlayerID: unitPlayerID, RoundID: "r", GameID: "g",
		Kind: domain.WagerKindBet, Money: brlMoney(t, "1.00"), Now: unitT0,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []string{
		"wager_transactions_provider_external_key",
		"wager_transactions_provider_idempotency_key",
		"wager_transactions_reversal_once_key",
	} {
		s := txStores{u: u, q: fakeExecTx{err: &pgconn.PgError{Code: "23505", ConstraintName: c}}}
		err := s.Transactions().Insert(t.Context(), tx)
		if !errors.Is(err, app.ErrDuplicateWagerTransaction) || !errors.Is(err, app.ErrConflict) {
			t.Fatalf("%s: err = %v", c, err)
		}
	}
	s := txStores{u: u, q: fakeExecTx{err: &pgconn.PgError{Code: "23505", ConstraintName: "wager_transactions_pkey"}}}
	if err := s.Transactions().Insert(t.Context(), tx); errors.Is(err, app.ErrDuplicateWagerTransaction) {
		t.Fatalf("pkey collision mapped to duplicate operation: %v", err)
	}
}

func TestStoreErrorMapping(t *testing.T) {
	nf := notFound(fmt.Errorf("x: %w", ErrNotFound))
	if !errors.Is(nf, app.ErrRecordNotFound) || !errors.Is(nf, app.ErrNotFound) || !errors.Is(nf, ErrNotFound) {
		t.Fatalf("notFound = %v", nf)
	}
	other := errors.New("other")
	if notFound(other) != other || notFound(nil) != nil {
		t.Fatal("notFound changed unrelated error")
	}
	hm := inboxError(fmt.Errorf("x: %w", ErrInboxHashMismatch))
	if !errors.Is(hm, app.ErrInboxHashMismatch) || !errors.Is(hm, app.ErrConflict) {
		t.Fatalf("inboxError = %v", hm)
	}
}

func TestStores_LockingMethodsRequireTx(t *testing.T) {
	u := NewUnitOfWork(nil, NewWalletRepository(), nil, nil, nil, NewInboxRepository())
	s := txStores{u: u, q: noDBQuerier{t: t}}
	if _, err := s.Wallets().GetForUpdate(t.Context(), unitWalletID); !errors.Is(err, ErrTxRequired) {
		t.Fatalf("wallet lock: %v", err)
	}
	if _, err := s.Inbox().Register(t.Context(), "c", "m", strings.Repeat("a", 64), unitT0); !errors.Is(err, ErrTxRequired) {
		t.Fatalf("inbox register: %v", err)
	}
}

func TestOperationLabelsMatchMetrics_T16(t *testing.T) {
	if app.OperationPendingReference != metrics.OperationPendingReference {
		t.Fatalf("pending label: %q vs %q", app.OperationPendingReference, metrics.OperationPendingReference)
	}
	if app.OperationReconcile != metrics.OperationReconcile || app.OperationQuery != metrics.OperationQuery {
		t.Fatalf("labels: %q/%q vs %q/%q", app.OperationReconcile, app.OperationQuery,
			metrics.OperationReconcile, metrics.OperationQuery)
	}
}

func TestTxOptionsFor_IsolationAndReadOnly(t *testing.T) {
	apply := func(opts []TxOption) txConfig {
		cfg := txConfig{opts: pgx.TxOptions{IsoLevel: pgx.ReadCommitted}}
		for _, o := range opts {
			o(&cfg)
		}
		return cfg
	}
	cases := []struct {
		in       app.TxOptions
		iso      pgx.TxIsoLevel
		access   pgx.TxAccessMode
		retrying bool
	}{
		{app.TxOptions{}, pgx.ReadCommitted, "", false},
		{app.TxOptions{Isolation: app.IsolationRepeatableRead, ReadOnly: true, Retry: true, Operation: app.OperationReconcile},
			pgx.RepeatableRead, pgx.ReadOnly, true},
		{app.TxOptions{Isolation: app.IsolationSerializable}, pgx.Serializable, "", false},
		{app.TxOptions{ReadOnly: true}, pgx.ReadCommitted, pgx.ReadOnly, false},
	}
	for _, c := range cases {
		opts, err := txOptionsFor(c.in)
		if err != nil {
			t.Fatal(err)
		}
		cfg := apply(opts)
		if cfg.opts.IsoLevel != c.iso || cfg.opts.AccessMode != c.access || (cfg.retry.MaxAttempts > 1) != c.retrying {
			t.Fatalf("%+v → %+v / %+v", c.in, cfg.opts, cfg.retry)
		}
	}
	if _, err := txOptionsFor(app.TxOptions{Isolation: "READ_UNCOMMITTED"}); err == nil {
		t.Fatal("unknown isolation accepted")
	}

	// Isolamento desconhecido não chega ao Transactor.
	rt := &recordingTransactor{}
	u := NewUnitOfWork(rt, nil, nil, nil, nil, nil)
	called := false
	err := u.WithinTx(t.Context(), app.TxOptions{Isolation: "bogus"}, func(context.Context, app.Tx) error {
		called = true
		return nil
	})
	if err == nil || called {
		t.Fatalf("err %v called %v", err, called)
	}
}

func TestLedgerStore_MapsInvalidCursor(t *testing.T) {
	u := NewUnitOfWork(nil, nil, NewLedgerRepository(), nil, nil, nil)
	s := txStores{u: u, q: noDBQuerier{t: t}}
	_, err := s.Ledger().List(t.Context(), unitWalletID, "not-a-cursor", 10)
	if !errors.Is(err, app.ErrInvalidCursor) || !errors.Is(err, app.ErrInvalidInput) || !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("err = %v", err)
	}
	if _, err := s.Ledger().List(t.Context(), unitWalletID, "", 0); !errors.Is(err, ErrInvalidPageLimit) || errors.Is(err, app.ErrInvalidCursor) {
		t.Fatalf("limit err = %v", err)
	}
}

// textRow devolve (count, texto) para Summarize sem banco.
type textRow struct {
	count int64
	net   string
}

func (r textRow) Scan(dest ...any) error {
	*(dest[0].(*int64)) = r.count
	*(dest[1].(*string)) = r.net
	return nil
}

type rowQuerier struct {
	noDBQuerier
	row pgx.Row
}

func (q rowQuerier) QueryRow(context.Context, string, ...any) pgx.Row { return q.row }

func TestLedgerSummarize_ParsesAndRejectsOutOfRange(t *testing.T) {
	r := NewLedgerRepository()
	s, err := r.Summarize(t.Context(), rowQuerier{noDBQuerier{t}, textRow{3, "-1250"}}, unitWalletID)
	if err != nil || s.NetMinorUnits != -1250 || s.Entries != 3 {
		t.Fatalf("summary = %+v, %v", s, err)
	}
	_, err = r.Summarize(t.Context(), rowQuerier{noDBQuerier{t}, textRow{2, "18446744073709551614"}}, unitWalletID)
	if !errors.Is(err, ErrLedgerSumOutOfRange) {
		t.Fatalf("overflow: %v", err)
	}
}
