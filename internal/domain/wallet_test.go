package domain

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
)

var (
	testWalletID = uuid.MustParse("0192f291-27dd-7d3f-8071-5f8685deef37")
	testPlayerID = uuid.MustParse("0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1")
	testOpenTxID = uuid.MustParse("0192f298-0000-7e38-af88-e43f851a8190")
	testTxID     = uuid.MustParse("0192f298-345e-7e38-af88-e43f851a819d")
	testT0       = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
)

func assertErrs(t *testing.T, err error, want ...error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error matching %v, got nil", want)
	}
	for _, w := range want {
		if !errors.Is(err, w) {
			t.Fatalf("error %q does not match %v", err, w)
		}
	}
}

func rehydrated(t *testing.T, amount string, version int64) *Wallet {
	t.Helper()
	w, err := RehydrateWallet(RehydrateWalletParams{
		ID:        testWalletID,
		PlayerID:  testPlayerID,
		Balance:   mustParse(t, amount, "BRL"),
		Version:   version,
		CreatedAt: testT0,
		UpdatedAt: testT0.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("RehydrateWallet: %v", err)
	}
	return w
}

type walletState struct {
	balance   Money
	version   int64
	persisted int64
	updatedAt time.Time
}

func stateOf(w *Wallet) walletState {
	return walletState{w.Balance(), w.Version(), w.PersistedVersion(), w.UpdatedAt()}
}

func TestNewWallet_PositiveInitialBalance(t *testing.T) {
	local := testT0.In(time.FixedZone("BRT", -3*3600))
	w, entry, err := NewWallet(NewWalletParams{
		ID: testWalletID, PlayerID: testPlayerID,
		InitialBalance:       mustParse(t, "1000.00", "BRL"),
		OpeningTransactionID: testOpenTxID,
		Now:                  local,
	})
	if err != nil {
		t.Fatalf("NewWallet: %v", err)
	}
	if w.Version() != 1 {
		t.Fatalf("version = %d, want 1 (opening credit must not bump version)", w.Version())
	}
	if w.PersistedVersion() != 0 {
		t.Fatalf("persisted version = %d, want 0 for new wallet", w.PersistedVersion())
	}
	if w.Balance() != mustParse(t, "1000.00", "BRL") || w.Currency() != "BRL" {
		t.Fatalf("balance = %s", w.Balance())
	}
	if w.ID() != testWalletID || w.PlayerID() != testPlayerID {
		t.Fatal("identity not preserved")
	}
	if w.CreatedAt() != testT0 || w.UpdatedAt() != testT0 || w.CreatedAt().Location() != time.UTC {
		t.Fatalf("timestamps not normalized to UTC: %v %v", w.CreatedAt(), w.UpdatedAt())
	}
	if entry == nil {
		t.Fatal("expected opening entry")
	}
	if entry.Direction() != LedgerDirectionCredit || entry.TransactionID() != testOpenTxID ||
		entry.WalletID() != testWalletID || entry.ID() == uuid.Nil {
		t.Fatalf("unexpected opening entry: %+v", entry)
	}
	if !entry.BalanceBefore().IsZero() || entry.BalanceAfter() != w.Balance() || entry.Amount() != w.Balance() {
		t.Fatalf("opening entry arithmetic: %s -> %s (%s)", entry.BalanceBefore(), entry.BalanceAfter(), entry.Amount())
	}
	if entry.CreatedAt() != testT0 {
		t.Fatalf("entry createdAt = %v", entry.CreatedAt())
	}
}

func TestNewWallet_ZeroInitialBalance(t *testing.T) {
	zero, _ := Zero("BRL")
	// OpeningTransactionID é ignorado quando o saldo é zero.
	w, entry, err := NewWallet(NewWalletParams{ID: testWalletID, PlayerID: testPlayerID, InitialBalance: zero, Now: testT0})
	if err != nil {
		t.Fatalf("NewWallet: %v", err)
	}
	if entry != nil {
		t.Fatalf("zero opening must not produce ledger entry, got %+v", entry)
	}
	if w.Version() != 1 || !w.Balance().IsZero() {
		t.Fatalf("version=%d balance=%s", w.Version(), w.Balance())
	}
}

func TestNewWallet_Invalid(t *testing.T) {
	ok := func() NewWalletParams {
		m, _ := Parse("10.00", "BRL")
		return NewWalletParams{ID: testWalletID, PlayerID: testPlayerID, InitialBalance: m, OpeningTransactionID: testOpenTxID, Now: testT0}
	}
	neg, _ := Parse("-0.01", "BRL")
	tests := []struct {
		name   string
		mutate func(*NewWalletParams)
		want   []error
	}{
		{"nil id", func(p *NewWalletParams) { p.ID = uuid.Nil }, []error{ErrInvalidWallet}},
		{"nil player", func(p *NewWalletParams) { p.PlayerID = uuid.Nil }, []error{ErrInvalidWallet}},
		{"uninitialized balance", func(p *NewWalletParams) { p.InitialBalance = Money{} }, []error{ErrInvalidWallet, ErrUninitializedMoney}},
		{"negative balance", func(p *NewWalletParams) { p.InitialBalance = neg }, []error{ErrInvalidWallet, ErrNegativeAmount}},
		{"zero time", func(p *NewWalletParams) { p.Now = time.Time{} }, []error{ErrInvalidWallet}},
		{"positive without opening tx", func(p *NewWalletParams) { p.OpeningTransactionID = uuid.Nil }, []error{ErrInvalidWallet}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := ok()
			tt.mutate(&p)
			w, e, err := NewWallet(p)
			assertErrs(t, err, tt.want...)
			if w != nil || e != nil {
				t.Fatal("expected nil results on error")
			}
		})
	}
}

func TestNewWallet_EntryIDGeneratorFailure(t *testing.T) {
	orig := newLedgerEntryID
	t.Cleanup(func() { newLedgerEntryID = orig })
	newLedgerEntryID = func() (uuid.UUID, error) { return uuid.Nil, errors.New("entropy") }

	_, _, err := NewWallet(NewWalletParams{ID: testWalletID, PlayerID: testPlayerID,
		InitialBalance: mustParse(t, "1.00", "BRL"), OpeningTransactionID: testOpenTxID, Now: testT0})
	assertErrs(t, err, ErrInvalidLedgerEntry)

	w := rehydrated(t, "10.00", 3)
	before := stateOf(w)
	_, err = w.Credit(mustParse(t, "1.00", "BRL"), testTxID, testT0.Add(2*time.Hour))
	assertErrs(t, err, ErrInvalidLedgerEntry)
	if stateOf(w) != before {
		t.Fatal("wallet mutated on id generation failure")
	}
}

func TestRehydrate_NoSideEffects(t *testing.T) {
	created := testT0
	updated := testT0.Add(time.Hour).In(time.FixedZone("X", 7200))
	w, err := RehydrateWallet(RehydrateWalletParams{
		ID: testWalletID, PlayerID: testPlayerID,
		Balance: mustParse(t, "975.00", "BRL"), Version: 7,
		CreatedAt: created, UpdatedAt: updated,
	})
	if err != nil {
		t.Fatalf("RehydrateWallet: %v", err)
	}
	if w.Version() != 7 || w.PersistedVersion() != 7 {
		t.Fatalf("version=%d persisted=%d, want 7/7", w.Version(), w.PersistedVersion())
	}
	if w.Balance() != mustParse(t, "975.00", "BRL") {
		t.Fatalf("balance = %s", w.Balance())
	}
	if !w.CreatedAt().Equal(created) || !w.UpdatedAt().Equal(updated) || w.UpdatedAt().Location() != time.UTC {
		t.Fatalf("timestamps changed: %v %v", w.CreatedAt(), w.UpdatedAt())
	}
	// Reidratar duas vezes o mesmo estado produz agregados iguais.
	w2 := rehydrated(t, "975.00", 7)
	w3 := rehydrated(t, "975.00", 7)
	if *w2 != *w3 {
		t.Fatal("rehydration is not deterministic")
	}
}

func TestRehydrate_ZeroBalanceAllowed(t *testing.T) {
	w := rehydrated(t, "0.00", 1)
	if !w.Balance().IsZero() {
		t.Fatal("expected zero balance")
	}
}

func TestRehydrate_Invalid(t *testing.T) {
	ok := func() RehydrateWalletParams {
		m, _ := Parse("10.00", "BRL")
		return RehydrateWalletParams{ID: testWalletID, PlayerID: testPlayerID, Balance: m, Version: 2, CreatedAt: testT0, UpdatedAt: testT0}
	}
	neg, _ := Parse("-5.00", "BRL")
	tests := []struct {
		name   string
		mutate func(*RehydrateWalletParams)
		want   []error
	}{
		{"nil id", func(p *RehydrateWalletParams) { p.ID = uuid.Nil }, []error{ErrInvalidWallet}},
		{"nil player", func(p *RehydrateWalletParams) { p.PlayerID = uuid.Nil }, []error{ErrInvalidWallet}},
		{"uninitialized balance", func(p *RehydrateWalletParams) { p.Balance = Money{} }, []error{ErrInvalidWallet, ErrUninitializedMoney}},
		{"negative balance", func(p *RehydrateWalletParams) { p.Balance = neg }, []error{ErrInvalidWallet, ErrNegativeAmount}},
		{"version zero", func(p *RehydrateWalletParams) { p.Version = 0 }, []error{ErrInvalidWallet}},
		{"version negative", func(p *RehydrateWalletParams) { p.Version = -1 }, []error{ErrInvalidWallet}},
		{"zero createdAt", func(p *RehydrateWalletParams) { p.CreatedAt = time.Time{} }, []error{ErrInvalidWallet}},
		{"zero updatedAt", func(p *RehydrateWalletParams) { p.UpdatedAt = time.Time{} }, []error{ErrInvalidWallet}},
		{"updated before created", func(p *RehydrateWalletParams) { p.UpdatedAt = testT0.Add(-time.Second) }, []error{ErrInvalidWallet}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := ok()
			tt.mutate(&p)
			w, err := RehydrateWallet(p)
			assertErrs(t, err, tt.want...)
			if w != nil {
				t.Fatal("expected nil wallet on error")
			}
		})
	}
}

func TestWallet_DebitCredit_Success(t *testing.T) {
	w := rehydrated(t, "100.00", 4)
	now := testT0.Add(2 * time.Hour)

	e, err := w.Debit(mustParse(t, "80.00", "BRL"), testTxID, now)
	if err != nil {
		t.Fatalf("Debit: %v", err)
	}
	if w.Balance() != mustParse(t, "20.00", "BRL") || w.Version() != 5 || w.PersistedVersion() != 4 {
		t.Fatalf("after debit: balance=%s version=%d persisted=%d", w.Balance(), w.Version(), w.PersistedVersion())
	}
	if e.Direction() != LedgerDirectionDebit || e.BalanceBefore() != mustParse(t, "100.00", "BRL") ||
		e.BalanceAfter() != mustParse(t, "20.00", "BRL") || e.Amount() != mustParse(t, "80.00", "BRL") ||
		e.WalletID() != testWalletID || e.TransactionID() != testTxID || e.ID() == uuid.Nil || e.CreatedAt() != now {
		t.Fatalf("unexpected debit entry: %+v", e)
	}
	if w.UpdatedAt() != now {
		t.Fatalf("updatedAt = %v, want %v", w.UpdatedAt(), now)
	}

	tx2 := uuid.MustParse("0192f298-345e-7e38-af88-e43f851a8200")
	e2, err := w.Credit(mustParse(t, "5.50", "BRL"), tx2, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("Credit: %v", err)
	}
	if w.Balance() != mustParse(t, "25.50", "BRL") || w.Version() != 6 {
		t.Fatalf("after credit: balance=%s version=%d", w.Balance(), w.Version())
	}
	if e2.Direction() != LedgerDirectionCredit || e2.BalanceBefore() != e.BalanceAfter() || e2.BalanceAfter() != w.Balance() {
		t.Fatalf("unexpected credit entry: %+v", e2)
	}
	if e2.ID() == e.ID() {
		t.Fatal("entry ids must be unique")
	}
}

func TestWallet_DebitExactBalanceToZero(t *testing.T) {
	w := rehydrated(t, "0.01", 1)
	if _, err := w.Debit(mustParse(t, "0.01", "BRL"), testTxID, testT0.Add(2*time.Hour)); err != nil {
		t.Fatalf("Debit: %v", err)
	}
	if !w.Balance().IsZero() || w.Version() != 2 {
		t.Fatalf("balance=%s version=%d", w.Balance(), w.Version())
	}
}

// Cenário 80/80 sobre 100 em sequência: apenas o primeiro débito passa e o
// saldo nunca fica negativo.
func TestWallet_BalanceNeverNegative(t *testing.T) {
	w := rehydrated(t, "100.00", 1)
	now := testT0.Add(2 * time.Hour)
	if _, err := w.Debit(mustParse(t, "80.00", "BRL"), testTxID, now); err != nil {
		t.Fatalf("first debit: %v", err)
	}
	before := stateOf(w)
	_, err := w.Debit(mustParse(t, "80.00", "BRL"), uuid.MustParse("0192f298-345e-7e38-af88-e43f851a8201"), now.Add(time.Second))
	assertErrs(t, err, ErrInsufficientFunds)
	if stateOf(w) != before {
		t.Fatalf("wallet mutated on insufficient funds: %+v vs %+v", stateOf(w), before)
	}
	if w.Balance() != mustParse(t, "20.00", "BRL") || w.Version() != 2 {
		t.Fatalf("balance=%s version=%d", w.Balance(), w.Version())
	}

	// Saldo zero: qualquer débito positivo falha.
	z := rehydrated(t, "0.00", 1)
	_, err = z.Debit(mustParse(t, "0.01", "BRL"), testTxID, now)
	assertErrs(t, err, ErrInsufficientFunds)
	if !z.Balance().IsZero() || z.Version() != 1 {
		t.Fatal("zero wallet mutated")
	}
}

func TestWallet_MovementValidation(t *testing.T) {
	now := testT0.Add(2 * time.Hour)
	usd := mustParse(t, "1.00", "USD")
	neg := mustParse(t, "-1.00", "BRL")
	zero := mustParse(t, "0.00", "BRL")
	one := mustParse(t, "1.00", "BRL")
	tests := []struct {
		name   string
		amount Money
		txID   uuid.UUID
		now    time.Time
		want   []error
	}{
		{"currency mismatch", usd, testTxID, now, []error{ErrCurrencyMismatch}},
		{"uninitialized money", Money{}, testTxID, now, []error{ErrInvalidAmount, ErrUninitializedMoney}},
		{"zero amount", zero, testTxID, now, []error{ErrInvalidAmount}},
		{"negative amount", neg, testTxID, now, []error{ErrInvalidAmount, ErrNegativeAmount}},
		{"nil transaction id", one, uuid.Nil, now, []error{ErrInvalidMovement}},
		{"zero time", one, testTxID, time.Time{}, []error{ErrInvalidMovement}},
	}
	ops := map[string]func(*Wallet, Money, uuid.UUID, time.Time) (WalletLedgerEntry, error){
		"Debit":  (*Wallet).Debit,
		"Credit": (*Wallet).Credit,
	}
	for opName, op := range ops {
		for _, tt := range tests {
			t.Run(opName+"/"+tt.name, func(t *testing.T) {
				w := rehydrated(t, "100.00", 3)
				before := stateOf(w)
				e, err := op(w, tt.amount, tt.txID, tt.now)
				assertErrs(t, err, tt.want...)
				if e != (WalletLedgerEntry{}) {
					t.Fatal("expected zero entry on error")
				}
				if stateOf(w) != before {
					t.Fatal("wallet mutated on validation error")
				}
			})
		}
	}
}

func TestWallet_UninitializedRejected(t *testing.T) {
	one := mustParse(t, "1.00", "BRL")
	var zero Wallet
	var nilW *Wallet
	for name, w := range map[string]*Wallet{"zero-value": &zero, "nil": nilW} {
		t.Run(name, func(t *testing.T) {
			if w.IsInitialized() {
				t.Fatal("expected uninitialized")
			}
			_, err := w.Debit(one, testTxID, testT0)
			assertErrs(t, err, ErrUninitializedWallet)
			_, err = w.Credit(one, testTxID, testT0)
			assertErrs(t, err, ErrUninitializedWallet)
		})
	}
	if zero != (Wallet{}) {
		t.Fatal("zero-value wallet mutated")
	}
}

func TestWallet_CreditOverflow(t *testing.T) {
	w, err := RehydrateWallet(RehydrateWalletParams{
		ID: testWalletID, PlayerID: testPlayerID,
		Balance: mustMinor(t, math.MaxInt64, "BRL"), Version: 1,
		CreatedAt: testT0, UpdatedAt: testT0,
	})
	if err != nil {
		t.Fatal(err)
	}
	before := stateOf(w)
	_, err = w.Credit(mustParse(t, "0.01", "BRL"), testTxID, testT0.Add(time.Hour))
	assertErrs(t, err, ErrMoneyOverflow)
	if stateOf(w) != before {
		t.Fatal("wallet mutated on overflow")
	}
}

func TestWallet_VersionOverflow(t *testing.T) {
	w := rehydrated(t, "10.00", math.MaxInt64)
	_, err := w.Credit(mustParse(t, "1.00", "BRL"), testTxID, testT0.Add(2*time.Hour))
	assertErrs(t, err, ErrInvalidWallet)
	if w.Version() != math.MaxInt64 || w.Balance() != mustParse(t, "10.00", "BRL") {
		t.Fatal("wallet mutated on version overflow")
	}
}

// A versão só muda com alteração de saldo: falhas não incrementam e cada
// movimento bem-sucedido incrementa exatamente 1.
func TestWallet_Versioning(t *testing.T) {
	w := rehydrated(t, "50.00", 1)
	now := testT0.Add(2 * time.Hour)
	steps := []struct {
		debit   bool
		amount  string
		wantErr bool
		wantVer int64
	}{
		{debit: true, amount: "10.00", wantVer: 2},
		{debit: true, amount: "100.00", wantErr: true, wantVer: 2},
		{debit: false, amount: "0.00", wantErr: true, wantVer: 2},
		{debit: false, amount: "5.00", wantVer: 3},
		{debit: true, amount: "45.00", wantVer: 4},
		{debit: true, amount: "0.01", wantErr: true, wantVer: 4},
	}
	for i, s := range steps {
		op := w.Credit
		if s.debit {
			op = w.Debit
		}
		txID := uuid.MustParse("0192f298-345e-7e38-af88-e43f851a8300")
		txID[15] = byte(i)
		_, err := op(mustParse(t, s.amount, "BRL"), txID, now)
		if (err != nil) != s.wantErr {
			t.Fatalf("step %d: err = %v, wantErr %v", i, err, s.wantErr)
		}
		if w.Version() != s.wantVer {
			t.Fatalf("step %d: version = %d, want %d", i, w.Version(), s.wantVer)
		}
		if w.PersistedVersion() != 1 {
			t.Fatalf("step %d: persisted version changed to %d", i, w.PersistedVersion())
		}
	}
	if !w.Balance().IsZero() {
		t.Fatalf("final balance = %s, want 0.00", w.Balance())
	}
}

func TestWallet_UpdatedAtMonotonic(t *testing.T) {
	w := rehydrated(t, "10.00", 1) // updatedAt = T0+1h
	past := testT0.Add(30 * time.Minute)
	e, err := w.Credit(mustParse(t, "1.00", "BRL"), testTxID, past)
	if err != nil {
		t.Fatal(err)
	}
	if w.UpdatedAt() != testT0.Add(time.Hour) {
		t.Fatalf("updatedAt went backwards: %v", w.UpdatedAt())
	}
	if e.CreatedAt() != past {
		t.Fatalf("entry createdAt = %v, want %v", e.CreatedAt(), past)
	}
}
