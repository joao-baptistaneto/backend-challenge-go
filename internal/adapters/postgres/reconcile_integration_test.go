//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/app"
)

// Summarize: Σ créditos − Σ débitos e contagem, por carteira.
func TestIntegrationLedgerRepository_Summarize(t *testing.T) {
	t.Parallel()
	r := newRepos(t)
	now := baseTime()
	w := r.openWallet(t, "100.00", now)
	for i, amount := range []string{"1.00", "2.50", "3.25"} {
		if err := r.within(t, func(ctx context.Context, tx pgx.Tx) error {
			_, err := r.bet(ctx, tx, t, w.ID(), "sum-"+string(rune('a'+i)), amount, now)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	other := r.openWallet(t, "7.00", now)
	empty := r.openWallet(t, "0.00", now)

	pool := r.db.Pool()
	for _, c := range []struct {
		name    string
		got     func() (postgres.LedgerSummary, error)
		net     int64
		entries int64
	}{
		{"wallet", func() (postgres.LedgerSummary, error) { return r.ledger.Summarize(t.Context(), pool, w.ID()) }, 9325, 4},
		{"other", func() (postgres.LedgerSummary, error) { return r.ledger.Summarize(t.Context(), pool, other.ID()) }, 700, 1},
		{"empty", func() (postgres.LedgerSummary, error) { return r.ledger.Summarize(t.Context(), pool, empty.ID()) }, 0, 0},
		{"missing", func() (postgres.LedgerSummary, error) { return r.ledger.Summarize(t.Context(), pool, newID(t)) }, 0, 0},
	} {
		s, err := c.got()
		if err != nil || s.NetMinorUnits != c.net || s.Entries != c.entries {
			t.Fatalf("%s: %+v, %v", c.name, s, err)
		}
	}
}

// As opções da UnitOfWork chegam ao BEGIN: REPEATABLE READ + READ ONLY de
// fato (consulta às variáveis da transação e escrita recusada com 25006).
func TestIntegrationUnitOfWork_IsolationAndReadOnly(t *testing.T) {
	t.Parallel()
	r := newRepos(t)
	uow := postgres.NewUnitOfWork(r.tm, r.wallets, r.ledger, r.wagers, postgres.NewOutboxRepository(), postgres.NewInboxRepository())
	w := r.openWallet(t, "10.00", baseTime())

	show := func(ctx context.Context, q postgres.Querier) (iso, ro string) {
		t.Helper()
		if err := q.QueryRow(ctx, `SELECT current_setting('transaction_isolation'), current_setting('transaction_read_only')`).
			Scan(&iso, &ro); err != nil {
			t.Fatal(err)
		}
		return iso, ro
	}

	var writeErr error
	err := uow.WithinTx(t.Context(), app.TxOptions{
		Operation: app.OperationReconcile, Isolation: app.IsolationRepeatableRead, ReadOnly: true, Retry: true,
	}, func(ctx context.Context, tx app.Tx) error {
		q := postgres.QuerierOf(tx)
		if iso, ro := show(ctx, q); iso != "repeatable read" || ro != "on" {
			t.Fatalf("isolation %q read_only %q", iso, ro)
		}
		// Leitura pelos stores funciona.
		if _, err := tx.Wallets().Get(ctx, w.ID()); err != nil {
			t.Fatal(err)
		}
		_, writeErr = q.Exec(ctx, `UPDATE wallets SET updated_at = updated_at WHERE id = $1`, w.ID())
		return writeErr
	})
	var pgErr *pgconn.PgError
	if !errors.As(writeErr, &pgErr) || pgErr.Code != "25006" || !errors.Is(err, writeErr) {
		t.Fatalf("write in read-only tx: %v / %v", writeErr, err)
	}

	// Padrão: READ COMMITTED, leitura e escrita.
	err = uow.WithinTx(t.Context(), app.TxOptions{}, func(ctx context.Context, tx app.Tx) error {
		if iso, ro := show(ctx, postgres.QuerierOf(tx)); iso != "read committed" || ro != "off" {
			t.Fatalf("default isolation %q read_only %q", iso, ro)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = uow.WithinTx(t.Context(), app.TxOptions{Isolation: app.IsolationSerializable}, func(ctx context.Context, tx app.Tx) error {
		if iso, _ := show(ctx, postgres.QuerierOf(tx)); iso != "serializable" {
			t.Fatalf("isolation %q", iso)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
