package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

func TestOperationLabelsMatchMetrics(t *testing.T) {
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
	u := NewUnitOfWork(rt, NewWalletRepository(), NewLedgerRepository(), NewWagerTransactionRepository(), NewOutboxRepository())

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
	u := NewUnitOfWork(nil, NewWalletRepository(), nil, nil, nil)
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
