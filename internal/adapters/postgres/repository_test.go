package postgres

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// noDBQuerier falha o teste se algum SQL for executado: as validações
// precisam barrar a escrita antes do banco.
type noDBQuerier struct{ t *testing.T }

func (q noDBQuerier) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	q.t.Fatal("unexpected Exec")
	return pgconn.CommandTag{}, nil
}

func (q noDBQuerier) Query(context.Context, string, ...any) (pgx.Rows, error) {
	q.t.Fatal("unexpected Query")
	return nil, nil
}

func (q noDBQuerier) QueryRow(context.Context, string, ...any) pgx.Row {
	q.t.Fatal("unexpected QueryRow")
	return nil
}

var (
	unitWalletID = uuid.MustParse("0192f291-27dd-7d3f-8071-5f8685deef37")
	unitPlayerID = uuid.MustParse("0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1")
	unitT0       = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
)

func brlMoney(t *testing.T, s string) domain.Money {
	t.Helper()
	m, err := domain.Parse(s, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func rehydratedWallet(t *testing.T, balance string, version int64) *domain.Wallet {
	t.Helper()
	w, err := domain.RehydrateWallet(domain.RehydrateWalletParams{
		ID: unitWalletID, PlayerID: unitPlayerID, Balance: brlMoney(t, balance), Version: version,
		CreatedAt: unitT0, UpdatedAt: unitT0,
	})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestLedgerCursor_RoundTrip(t *testing.T) {
	t.Parallel()
	for _, seq := range []int64{1, 42, 1 << 40, 1<<63 - 1} {
		c := encodeLedgerCursor(unitWalletID, seq)
		got, err := decodeLedgerCursor(c, unitWalletID)
		if err != nil || got != seq {
			t.Fatalf("seq %d: got %d, err %v", seq, got, err)
		}
	}
}

func TestLedgerCursor_Rejects(t *testing.T) {
	t.Parallel()
	valid := encodeLedgerCursor(unitWalletID, 7)
	raw, _ := base64.RawURLEncoding.DecodeString(valid)
	withVersion := func(v byte) string {
		b := append([]byte(nil), raw...)
		b[0] = v
		return base64.RawURLEncoding.EncodeToString(b)
	}
	cases := map[string]struct {
		cursor string
		wallet uuid.UUID
	}{
		"other wallet":  {valid, uuid.MustParse("0192f291-27dd-7d3f-8071-000000000001")},
		"not base64":    {"!!!", unitWalletID},
		"std padding":   {base64.StdEncoding.EncodeToString(raw) + "=", unitWalletID},
		"truncated":     {valid[:len(valid)-2], unitWalletID},
		"too long":      {base64.RawURLEncoding.EncodeToString(append(raw, 0)), unitWalletID},
		"wrong version": {withVersion(2), unitWalletID},
		"zero seq":      {encodeLedgerCursor(unitWalletID, 0), unitWalletID},
		"negative seq":  {encodeLedgerCursor(unitWalletID, -1), unitWalletID},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeLedgerCursor(c.cursor, c.wallet); !errors.Is(err, ErrInvalidCursor) {
				t.Fatalf("err = %v, want ErrInvalidCursor", err)
			}
		})
	}
}

func TestMoneyColumns_RejectsZeroValue(t *testing.T) {
	t.Parallel()
	if _, _, err := moneyColumns("amount", domain.Money{}); !errors.Is(err, ErrInvalidAggregate) || !errors.Is(err, domain.ErrUninitializedMoney) {
		t.Fatalf("err = %v", err)
	}
	minor, cur, err := moneyColumns("amount", brlMoney(t, "25.50"))
	if err != nil || minor != 2550 || cur != "BRL" {
		t.Fatalf("got %d %q %v", minor, cur, err)
	}
	m, err := moneyFromColumns("amount", 2550, "BRL")
	if err != nil || m != brlMoney(t, "25.50") {
		t.Fatalf("round-trip: %v %v", m, err)
	}
	if _, err := moneyFromColumns("amount", 1, "XXX"); !errors.Is(err, domain.ErrInvalidCurrency) {
		t.Fatalf("invalid currency err = %v", err)
	}
}

func TestMapWriteError_ByConstraintName(t *testing.T) {
	t.Parallel()
	for name, want := range constraintErrors {
		pgErr := &pgconn.PgError{Code: CodeUniqueViolation, ConstraintName: name}
		err := mapWriteError("op", fmt.Errorf("exec: %w", pgErr))
		if !errors.Is(err, want) {
			t.Errorf("%s: err = %v, want %v", name, err, want)
		}
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || !errors.Is(err, ErrConstraintViolation) {
			t.Errorf("%s: cause lost: %v", name, err)
		}
	}
	unknown := mapWriteError("op", &pgconn.PgError{Code: CodeCheckViolation, ConstraintName: "wallets_balance_non_negative"})
	if !errors.Is(unknown, ErrCheckViolation) || errors.Is(unknown, ErrStaleWallet) {
		t.Fatalf("unknown constraint err = %v", unknown)
	}
	if mapWriteError("op", nil) != nil {
		t.Fatal("nil must stay nil")
	}
	if err := mapReadError("op", pgx.ErrNoRows); !errors.Is(err, ErrNotFound) {
		t.Fatalf("no rows err = %v", err)
	}
}

func TestRepositories_ValidateBeforeSQL(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	q := noDBQuerier{t}
	wallets, ledger, wagers := NewWalletRepository(), NewLedgerRepository(), NewWagerTransactionRepository()

	fresh, _, err := domain.NewWallet(domain.NewWalletParams{
		ID: unitWalletID, PlayerID: unitPlayerID, InitialBalance: brlMoney(t, "0.00"), Now: unitT0,
	})
	if err != nil {
		t.Fatal(err)
	}
	twice := rehydratedWallet(t, "10.00", 3)
	for range 2 {
		if _, err := twice.Debit(brlMoney(t, "1.00"), uuid.New(), unitT0); err != nil {
			t.Fatal(err)
		}
	}

	claimErr := func(p ClaimParams) error {
		_, err := wagers.ClaimDue(ctx, q, p)
		return err
	}
	cases := map[string]struct {
		err  error
		want error
	}{
		"insert nil wallet":        {wallets.Insert(ctx, q, nil), ErrInvalidAggregate},
		"insert zero wallet":       {wallets.Insert(ctx, q, &domain.Wallet{}), domain.ErrUninitializedWallet},
		"insert rehydrated wallet": {wallets.Insert(ctx, q, rehydratedWallet(t, "1.00", 1)), ErrInvalidAggregate},
		"update new wallet":        {wallets.Update(ctx, q, fresh), ErrInvalidAggregate},
		"update two movements":     {wallets.Update(ctx, q, twice), ErrInvalidAggregate},
		"update zero wallet":       {wallets.Update(ctx, q, &domain.Wallet{}), ErrInvalidAggregate},
		"append zero entry":        {ledger.Append(ctx, q, domain.WalletLedgerEntry{}), ErrInvalidAggregate},
		"insert nil transaction":   {wagers.Insert(ctx, q, nil), ErrInvalidAggregate},
		"insert zero transaction":  {wagers.Insert(ctx, q, &domain.WagerTransaction{}), domain.ErrUninitializedWagerTransaction},
		"update zero transaction":  {wagers.Update(ctx, q, &domain.WagerTransaction{}), ErrInvalidAggregate},
		"wallet lock outside tx":   {func() error { _, err := wallets.GetForUpdate(ctx, q, unitWalletID); return err }(), ErrTxRequired},
		"transaction lock outside": {func() error { _, err := wagers.GetByIDForUpdate(ctx, q, unitWalletID); return err }(), ErrTxRequired},
		"claimed lock outside tx": {func() error {
			_, err := wagers.GetClaimedForUpdate(ctx, q, unitWalletID, uuid.New())
			return err
		}(), ErrTxRequired},
		"claim zero now":    {claimErr(ClaimParams{Token: uuid.New(), Lease: time.Second, Limit: 1}), ErrInvalidClaim},
		"claim nil token":   {claimErr(ClaimParams{Now: unitT0, Lease: time.Second, Limit: 1}), ErrInvalidClaim},
		"claim short lease": {claimErr(ClaimParams{Now: unitT0, Token: uuid.New(), Lease: time.Microsecond, Limit: 1}), ErrInvalidClaim},
		"claim long lease":  {claimErr(ClaimParams{Now: unitT0, Token: uuid.New(), Lease: MaxClaimLease + 1, Limit: 1}), ErrInvalidClaim},
		"claim batch zero":  {claimErr(ClaimParams{Now: unitT0, Token: uuid.New(), Lease: time.Second}), ErrInvalidClaim},
		"claim batch high":  {claimErr(ClaimParams{Now: unitT0, Token: uuid.New(), Lease: time.Second, Limit: MaxClaimBatch + 1}), ErrInvalidClaim},
		"claim neg grace": {claimErr(ClaimParams{Now: unitT0, Token: uuid.New(), Lease: time.Second, Limit: 1,
			OrphanGrace: -time.Second}), ErrInvalidClaim},
		"ledger page limit zero":     {func() error { _, err := ledger.ListByWallet(ctx, q, unitWalletID, "", 0); return err }(), ErrInvalidPageLimit},
		"ledger page limit over max": {func() error { _, err := ledger.ListByWallet(ctx, q, unitWalletID, "", MaxLedgerPageSize+1); return err }(), ErrInvalidPageLimit},
		"ledger bad cursor":          {func() error { _, err := ledger.ListByWallet(ctx, q, unitWalletID, "x", 10); return err }(), ErrInvalidCursor},
	}
	for name, c := range cases {
		if !errors.Is(c.err, c.want) {
			t.Errorf("%s: err = %v, want %v", name, c.err, c.want)
		}
	}

	// Sem movimento (LOSS): Update não executa SQL.
	if err := wallets.Update(ctx, q, rehydratedWallet(t, "5.00", 2)); err != nil {
		t.Fatalf("unchanged wallet: %v", err)
	}
}
