package domain

import (
	"errors"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

var testEntryID = uuid.MustParse("0192f298-345e-7e38-af88-e43f851a8400")

func brl(t *testing.T, a string) Money {
	t.Helper()
	return mustParse(t, a, "BRL")
}

// validEntryParams devolve um débito consistente (3.00 - 1.00 = 2.00).
func validEntryParams(t *testing.T) NewWalletLedgerEntryParams {
	t.Helper()
	return NewWalletLedgerEntryParams{
		ID:            testEntryID,
		WalletID:      testWalletID,
		TransactionID: testTxID,
		Direction:     LedgerDirectionDebit,
		Amount:        brl(t, "1.00"),
		BalanceBefore: brl(t, "3.00"),
		BalanceAfter:  brl(t, "2.00"),
		CreatedAt:     testT0,
	}
}

// ledgerCases é compartilhado por New e Rehydrate: ambas aplicam as mesmas
// invariantes.
func ledgerCases(t *testing.T) []struct {
	name   string
	mutate func(p *NewWalletLedgerEntryParams)
	want   []error
} {
	maxBRL, err := FromMinorUnits(math.MaxInt64, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	usd := func(a string) Money { return mustParse(t, a, "USD") }
	return []struct {
		name   string
		mutate func(p *NewWalletLedgerEntryParams)
		want   []error
	}{
		// Direção inválida.
		{"empty direction", func(p *NewWalletLedgerEntryParams) { p.Direction = "" }, []error{ErrInvalidLedgerEntry}},
		{"unknown direction", func(p *NewWalletLedgerEntryParams) { p.Direction = "TRANSFER" }, []error{ErrInvalidLedgerEntry}},
		{"lowercase direction", func(p *NewWalletLedgerEntryParams) { p.Direction = "debit" }, []error{ErrInvalidLedgerEntry}},
		// Aritmética inconsistente.
		{"debit with credit arithmetic", func(p *NewWalletLedgerEntryParams) { p.BalanceAfter = brl(t, "4.00") }, []error{ErrInvalidLedgerEntry}},
		{"credit with debit arithmetic", func(p *NewWalletLedgerEntryParams) { p.Direction = LedgerDirectionCredit }, []error{ErrInvalidLedgerEntry}},
		{"off by one cent", func(p *NewWalletLedgerEntryParams) { p.BalanceAfter = brl(t, "2.01") }, []error{ErrInvalidLedgerEntry}},
		{"after equals before", func(p *NewWalletLedgerEntryParams) { p.BalanceAfter = brl(t, "3.00") }, []error{ErrInvalidLedgerEntry}},
		{"credit overflow", func(p *NewWalletLedgerEntryParams) {
			p.Direction = LedgerDirectionCredit
			p.BalanceBefore = maxBRL
			p.BalanceAfter = maxBRL
		}, []error{ErrInvalidLedgerEntry, ErrMoneyOverflow}},
		// Moeda divergente.
		{"amount currency differs", func(p *NewWalletLedgerEntryParams) { p.Amount = usd("1.00") }, []error{ErrInvalidLedgerEntry, ErrCurrencyMismatch}},
		{"before currency differs", func(p *NewWalletLedgerEntryParams) { p.BalanceBefore = usd("3.00") }, []error{ErrInvalidLedgerEntry, ErrCurrencyMismatch}},
		{"after currency differs", func(p *NewWalletLedgerEntryParams) { p.BalanceAfter = usd("2.00") }, []error{ErrInvalidLedgerEntry, ErrCurrencyMismatch}},
		// Valor não positivo.
		{"zero amount", func(p *NewWalletLedgerEntryParams) {
			p.Amount = brl(t, "0.00")
			p.BalanceAfter = brl(t, "3.00")
		}, []error{ErrInvalidLedgerEntry, ErrInvalidAmount}},
		{"negative amount", func(p *NewWalletLedgerEntryParams) {
			p.Amount = brl(t, "-1.00")
			p.BalanceAfter = brl(t, "4.00")
		}, []error{ErrInvalidLedgerEntry, ErrInvalidAmount, ErrNegativeAmount}},
		// Saldos negativos.
		{"negative after", func(p *NewWalletLedgerEntryParams) {
			p.Amount = brl(t, "4.00")
			p.BalanceAfter = brl(t, "-1.00")
		}, []error{ErrInvalidLedgerEntry, ErrNegativeAmount}},
		{"negative before", func(p *NewWalletLedgerEntryParams) {
			p.Direction = LedgerDirectionCredit
			p.BalanceBefore = brl(t, "-1.00")
			p.BalanceAfter = brl(t, "0.00")
		}, []error{ErrInvalidLedgerEntry, ErrNegativeAmount}},
		// Money não inicializado.
		{"uninitialized amount", func(p *NewWalletLedgerEntryParams) { p.Amount = Money{} }, []error{ErrInvalidLedgerEntry, ErrUninitializedMoney}},
		{"uninitialized before", func(p *NewWalletLedgerEntryParams) { p.BalanceBefore = Money{} }, []error{ErrInvalidLedgerEntry, ErrUninitializedMoney}},
		{"uninitialized after", func(p *NewWalletLedgerEntryParams) { p.BalanceAfter = Money{} }, []error{ErrInvalidLedgerEntry, ErrUninitializedMoney}},
		// Ids nulos.
		{"nil entry id", func(p *NewWalletLedgerEntryParams) { p.ID = uuid.Nil }, []error{ErrInvalidLedgerEntry}},
		{"nil wallet id", func(p *NewWalletLedgerEntryParams) { p.WalletID = uuid.Nil }, []error{ErrInvalidLedgerEntry}},
		{"nil transaction id", func(p *NewWalletLedgerEntryParams) { p.TransactionID = uuid.Nil }, []error{ErrInvalidLedgerEntry}},
		// Instante.
		{"zero time", func(p *NewWalletLedgerEntryParams) { p.CreatedAt = time.Time{} }, []error{ErrInvalidLedgerEntry}},
	}
}

func toRehydrate(p NewWalletLedgerEntryParams) RehydrateWalletLedgerEntryParams {
	return RehydrateWalletLedgerEntryParams(p)
}

func TestNewWalletLedgerEntry_Valid(t *testing.T) {
	local := testT0.In(time.FixedZone("BRT", -3*3600))
	tests := []struct {
		name                  string
		dir                   LedgerDirection
		amount, before, after string
	}{
		{"debit", LedgerDirectionDebit, "1.00", "3.00", "2.00"},
		{"debit to zero", LedgerDirectionDebit, "3.00", "3.00", "0.00"},
		{"credit", LedgerDirectionCredit, "1.00", "3.00", "4.00"},
		{"credit from zero (opening)", LedgerDirectionCredit, "100.00", "0.00", "100.00"},
		{"one cent", LedgerDirectionCredit, "0.01", "0.00", "0.01"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewWalletLedgerEntryParams{
				ID: testEntryID, WalletID: testWalletID, TransactionID: testTxID,
				Direction: tt.dir, Amount: brl(t, tt.amount),
				BalanceBefore: brl(t, tt.before), BalanceAfter: brl(t, tt.after),
				CreatedAt: local,
			}
			e, err := NewWalletLedgerEntry(p)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !e.IsInitialized() {
				t.Fatal("entry should be initialized")
			}
			if e.ID() != testEntryID || e.WalletID() != testWalletID || e.TransactionID() != testTxID {
				t.Fatalf("ids mismatch: %v %v %v", e.ID(), e.WalletID(), e.TransactionID())
			}
			if e.Direction() != tt.dir || e.Amount() != p.Amount || e.BalanceBefore() != p.BalanceBefore || e.BalanceAfter() != p.BalanceAfter {
				t.Fatalf("values mismatch: %s %s %s %s", e.Direction(), e.Amount(), e.BalanceBefore(), e.BalanceAfter())
			}
			if e.CreatedAt().Location() != time.UTC || !e.CreatedAt().Equal(testT0) {
				t.Fatalf("createdAt = %v, want %v in UTC", e.CreatedAt(), testT0)
			}
		})
	}
}

func TestNewWalletLedgerEntry_Invalid(t *testing.T) {
	for _, tt := range ledgerCases(t) {
		t.Run(tt.name, func(t *testing.T) {
			p := validEntryParams(t)
			tt.mutate(&p)
			e, err := NewWalletLedgerEntry(p)
			assertErrs(t, err, tt.want...)
			if e != (WalletLedgerEntry{}) {
				t.Fatalf("expected zero-value entry on error, got %+v", e)
			}
		})
	}
}

func TestRehydrateWalletLedgerEntry_Valid(t *testing.T) {
	p := validEntryParams(t)
	p.CreatedAt = testT0.In(time.FixedZone("BRT", -3*3600))
	e, err := RehydrateWalletLedgerEntry(toRehydrate(p))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	created, err := NewWalletLedgerEntry(p)
	if err != nil {
		t.Fatal(err)
	}
	// Reidratação e criação com os mesmos dados produzem o mesmo valor.
	if e != created {
		t.Fatalf("rehydrated %+v != created %+v", e, created)
	}
	if e.CreatedAt().Location() != time.UTC {
		t.Fatalf("createdAt not UTC: %v", e.CreatedAt())
	}
}

func TestRehydrateWalletLedgerEntry_InvalidRejected(t *testing.T) {
	for _, tt := range ledgerCases(t) {
		t.Run(tt.name, func(t *testing.T) {
			p := validEntryParams(t)
			tt.mutate(&p)
			e, err := RehydrateWalletLedgerEntry(toRehydrate(p))
			assertErrs(t, err, tt.want...)
			if !strings.Contains(err.Error(), "rehydrate") {
				t.Fatalf("error should identify rehydration: %q", err)
			}
			if e.IsInitialized() {
				t.Fatalf("expected zero-value entry on error, got %+v", e)
			}
		})
	}
}

func TestLedgerDirection(t *testing.T) {
	if !LedgerDirectionDebit.IsValid() || !LedgerDirectionCredit.IsValid() {
		t.Fatal("DEBIT/CREDIT must be valid")
	}
	for _, d := range []LedgerDirection{"", "debit", "Credit", "TRANSFER", " DEBIT"} {
		if d.IsValid() {
			t.Fatalf("%q must be invalid", d)
		}
	}
	if LedgerDirectionDebit.String() != "DEBIT" || LedgerDirectionCredit.String() != "CREDIT" {
		t.Fatal("unexpected String()")
	}
}

func TestWalletLedgerEntry_ZeroValueNotInitialized(t *testing.T) {
	if (WalletLedgerEntry{}).IsInitialized() {
		t.Fatal("zero-value must not be initialized")
	}
}

// TestWalletLedgerEntry_Immutability garante, por reflexão, que o tipo não
// expõe campos nem métodos de escrita, e que alterações nos parâmetros ou em
// valores devolvidos não afetam o lançamento.
func TestWalletLedgerEntry_Immutability(t *testing.T) {
	typ := reflect.TypeOf(WalletLedgerEntry{})
	for i := 0; i < typ.NumField(); i++ {
		if f := typ.Field(i); f.IsExported() {
			t.Fatalf("field %s must be unexported", f.Name)
		}
	}

	// Conjunto de métodos fechado: apenas getters (receptor por valor) e
	// IsInitialized. Um novo método exige revisão consciente deste teste.
	want := []string{"Amount", "BalanceAfter", "BalanceBefore", "CreatedAt", "Direction", "ID", "IsInitialized", "TransactionID", "WalletID"}
	var got []string
	ptr := reflect.TypeOf(&WalletLedgerEntry{})
	for i := 0; i < ptr.NumMethod(); i++ {
		m := ptr.Method(i)
		got = append(got, m.Name)
		if m.Type.NumIn() != 1 {
			t.Fatalf("method %s takes arguments; ledger entry must expose only getters", m.Name)
		}
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("method set = %v, want %v", got, want)
	}
	if ptr.NumMethod() != typ.NumMethod() {
		t.Fatal("all methods must have value receivers")
	}

	p := validEntryParams(t)
	e, err := NewWalletLedgerEntry(p)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := e

	// Mutar os parâmetros após a construção não afeta o lançamento.
	p.ID = uuid.New()
	p.Direction = LedgerDirectionCredit
	p.Amount = brl(t, "999.00")
	p.BalanceAfter = brl(t, "0.00")
	p.CreatedAt = p.CreatedAt.Add(time.Hour)
	if e != snapshot {
		t.Fatal("entry changed after mutating params")
	}

	// Mutar valores devolvidos pelos getters não afeta o lançamento.
	id := e.ID()
	id[0] ^= 0xFF
	if id == e.ID() {
		t.Fatal("getter must return a copy of the id")
	}
	if e.ID() != testEntryID || !e.CreatedAt().Equal(testT0) {
		t.Fatal("entry changed after mutating getter results")
	}

	// Uma cópia é independente (tipo-valor sem ponteiros/slices internos).
	cp := e
	if cp != e {
		t.Fatal("copy must be equal")
	}
}

// TestWallet_UsesLedgerConstructor garante que os lançamentos devolvidos pela
// Wallet passam pelas mesmas invariantes (round-trip via reidratação).
func TestWallet_UsesLedgerConstructor(t *testing.T) {
	w := rehydrated(t, "10.00", 3)
	for _, op := range []func() (WalletLedgerEntry, error){
		func() (WalletLedgerEntry, error) { return w.Debit(brl(t, "2.50"), testTxID, testT0) },
		func() (WalletLedgerEntry, error) { return w.Credit(brl(t, "1.25"), testOpenTxID, testT0) },
	} {
		e, err := op()
		if err != nil {
			t.Fatal(err)
		}
		r, err := RehydrateWalletLedgerEntry(RehydrateWalletLedgerEntryParams{
			ID: e.ID(), WalletID: e.WalletID(), TransactionID: e.TransactionID(),
			Direction: e.Direction(), Amount: e.Amount(),
			BalanceBefore: e.BalanceBefore(), BalanceAfter: e.BalanceAfter(),
			CreatedAt: e.CreatedAt(),
		})
		if err != nil {
			t.Fatalf("round-trip: %v", err)
		}
		if r != e {
			t.Fatalf("round-trip mismatch: %+v != %+v", r, e)
		}
	}
	if !errors.Is(func() error { _, err := w.Debit(brl(t, "100.00"), testTxID, testT0); return err }(), ErrInsufficientFunds) {
		t.Fatal("expected insufficient funds")
	}
}
