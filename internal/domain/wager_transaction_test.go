package domain

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

var (
	testWagerID    = uuid.MustParse("0192f298-345e-7e38-af88-e43f851a8200")
	testRefWagerID = uuid.MustParse("0192f298-345e-7e38-af88-e43f851a8201")
	testHash       = strings.Repeat("ab", 32)
)

func wagerMoneyFor(t *testing.T, k WagerKind) Money {
	t.Helper()
	if k == WagerKindLoss {
		return mustParse(t, "0.00", "BRL")
	}
	return mustParse(t, "25.00", "BRL")
}

func externalParams(t *testing.T, k WagerKind) NewExternalWagerTransactionParams {
	t.Helper()
	p := NewExternalWagerTransactionParams{
		ID:                    testWagerID,
		ProviderID:            "provider-a",
		ExternalTransactionID: "transaction-123",
		IdempotencyKey:        "provider-a:transaction-123",
		RequestHash:           testHash,
		WalletID:              testWalletID,
		PlayerID:              testPlayerID,
		RoundID:               "round-987",
		GameID:                "fortune-chimp",
		Kind:                  k,
		Money:                 wagerMoneyFor(t, k),
		Now:                   testT0,
	}
	if k.RequiresReference() {
		p.ReferenceExternalTransactionID = "transaction-100"
	}
	return p
}

func newExternal(t *testing.T, k WagerKind) *WagerTransaction {
	t.Helper()
	tx, err := NewExternalWagerTransaction(externalParams(t, k))
	if err != nil {
		t.Fatalf("NewExternalWagerTransaction(%s): %v", k, err)
	}
	return tx
}

func rehydrateParamsOf(tx *WagerTransaction) RehydrateWagerTransactionParams {
	return RehydrateWagerTransactionParams{
		ID: tx.ID(), Origin: tx.Origin(), Kind: tx.Kind(), Status: tx.Status(),
		WalletID: tx.WalletID(), PlayerID: tx.PlayerID(), Money: tx.Money(),
		ProviderID: tx.ProviderID(), ExternalTransactionID: tx.ExternalTransactionID(),
		IdempotencyKey: tx.IdempotencyKey(), RequestHash: tx.RequestHash(),
		RoundID: tx.RoundID(), GameID: tx.GameID(),
		ReferenceExternalTransactionID: tx.ReferenceExternalTransactionID(),
		ReferenceTransactionID:         tx.ReferenceTransactionID(),
		FailureCode:                    tx.FailureCode(), BalanceAfter: tx.BalanceAfter(),
		Attempts: tx.Attempts(), NextAttemptAt: tx.NextAttemptAt(),
		CreatedAt: tx.CreatedAt(), UpdatedAt: tx.UpdatedAt(), CompletedAt: tx.CompletedAt(),
	}
}

// --- Tipos, estados e códigos ---------------------------------------------------

func TestWagerKind_Predicates(t *testing.T) {
	cases := []struct {
		k                                     WagerKind
		valid, external, reversal, req, accep bool
		origin                                WagerOrigin
	}{
		{WagerKindOpening, true, false, false, false, false, WagerOriginInternal},
		{WagerKindBet, true, true, false, false, false, WagerOriginExternal},
		{WagerKindWin, true, true, false, false, true, WagerOriginExternal},
		{WagerKindLoss, true, true, false, false, false, WagerOriginExternal},
		{WagerKindRefund, true, true, true, true, true, WagerOriginExternal},
		{WagerKindRollback, true, true, true, true, true, WagerOriginExternal},
		{"bet", false, false, false, false, false, ""},
		{"", false, false, false, false, false, ""},
	}
	for _, c := range cases {
		if c.k.IsValid() != c.valid || c.k.IsExternal() != c.external || c.k.IsReversal() != c.reversal ||
			c.k.RequiresReference() != c.req || c.k.AcceptsReference() != c.accep || c.k.Origin() != c.origin {
			t.Errorf("predicates mismatch for %q", c.k)
		}
	}
}

func TestWagerOrigin(t *testing.T) {
	if !WagerOriginInternal.IsValid() || !WagerOriginExternal.IsValid() || WagerOrigin("").IsValid() || WagerOrigin("internal").IsValid() {
		t.Fatal("origin validity mismatch")
	}
	if WagerOriginInternal.String() != "INTERNAL" || WagerOriginExternal.String() != "EXTERNAL" {
		t.Fatal("origin names changed")
	}
}

func TestParseExternalWagerKind(t *testing.T) {
	for _, s := range []string{"BET", "WIN", "LOSS", "REFUND", "ROLLBACK"} {
		k, err := ParseExternalWagerKind(s)
		if err != nil || string(k) != s {
			t.Errorf("ParseExternalWagerKind(%q) = %q, %v", s, k, err)
		}
	}
	k, err := ParseExternalWagerKind("OPENING")
	assertErrs(t, err, ErrInvalidWagerTransaction, ErrInvalidWagerKind, ErrOpeningNotExternal)
	if k != "" {
		t.Fatalf("kind = %q on error", k)
	}
	if code, ok := ValidationFailureCode(err); !ok || code != FailureCodeInvalidKind {
		t.Fatalf("code = %q, %v", code, ok)
	}
	for _, s := range []string{"", "bet", "Bet", "CASHOUT", " BET"} {
		_, err := ParseExternalWagerKind(s)
		assertErrs(t, err, ErrInvalidWagerKind)
		if errors.Is(err, ErrOpeningNotExternal) {
			t.Errorf("%q must not be reported as OPENING", s)
		}
	}
}

func TestWagerStatus_Predicates(t *testing.T) {
	terminal := map[WagerStatus]bool{
		WagerStatusPending: false, WagerStatusPendingReference: false,
		WagerStatusProcessed: true, WagerStatusRejected: true, WagerStatusFailed: true,
	}
	for s, term := range terminal {
		if !s.IsValid() || s.IsTerminal() != term {
			t.Errorf("%s: valid=%v terminal=%v", s, s.IsValid(), s.IsTerminal())
		}
	}
	if WagerStatus("DONE").IsValid() || WagerStatus("").IsTerminal() {
		t.Fatal("unknown status accepted")
	}
}

var allWagerStatuses = []WagerStatus{
	WagerStatusPending, WagerStatusPendingReference, WagerStatusProcessed, WagerStatusRejected, WagerStatusFailed,
}

func TestCanTransitionWagerStatus_Table(t *testing.T) {
	allowed := map[[2]WagerStatus]bool{
		{WagerStatusPending, WagerStatusPendingReference}:          true,
		{WagerStatusPending, WagerStatusProcessed}:                 true,
		{WagerStatusPending, WagerStatusRejected}:                  true,
		{WagerStatusPending, WagerStatusFailed}:                    true,
		{WagerStatusPendingReference, WagerStatusPendingReference}: true,
		{WagerStatusPendingReference, WagerStatusProcessed}:        true,
		{WagerStatusPendingReference, WagerStatusRejected}:         true,
		{WagerStatusPendingReference, WagerStatusFailed}:           true,
	}
	for _, from := range append(allWagerStatuses, "UNKNOWN") {
		for _, to := range append(allWagerStatuses, "UNKNOWN") {
			want := allowed[[2]WagerStatus{from, to}]
			if got := CanTransitionWagerStatus(from, to); got != want {
				t.Errorf("%s -> %s = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestFailureCodes(t *testing.T) {
	all := AllFailureCodes()
	if len(all) != len(failureCodeCategories) {
		t.Fatalf("AllFailureCodes has %d codes, categories map has %d", len(all), len(failureCodeCategories))
	}
	seen := map[FailureCode]bool{}
	for _, c := range all {
		if seen[c] || !c.IsValid() || c.Category() == "" {
			t.Errorf("code %q duplicated or invalid", c)
		}
		seen[c] = true
		if c.String() != string(c) || c.String() != strings.ToUpper(c.String()) {
			t.Errorf("code %q is not a stable upper-case identifier", c)
		}
	}
	// Contrato público exigido pelo README/plano.
	want := map[FailureCode]FailureCategory{
		"INSUFFICIENT_FUNDS":          FailureCategoryBusiness,
		"REVERSAL_INSUFFICIENT_FUNDS": FailureCategoryBusiness,
		"REFERENCE_NOT_FOUND":         FailureCategoryBusiness,
		"REFERENCE_NOT_REVERSIBLE":    FailureCategoryBusiness,
		"REFERENCE_NOT_PROCESSED":     FailureCategoryBusiness,
		"ALREADY_REVERSED":            FailureCategoryBusiness,
		"REFERENCE_MISMATCH":          FailureCategoryInput,
		"AMOUNT_MISMATCH":             FailureCategoryInput,
		"CURRENCY_MISMATCH":           FailureCategoryInput,
		"INVALID_KIND":                FailureCategoryInput,
		"INVALID_AMOUNT":              FailureCategoryInput,
		"INVALID_CURRENCY":            FailureCategoryInput,
		"INVALID_REQUEST":             FailureCategoryInput,
		"REFERENCE_REQUIRED":          FailureCategoryInput,
		"REFERENCE_NOT_ALLOWED":       FailureCategoryInput,
		"WALLET_NOT_FOUND":            FailureCategoryInput,
		"WALLET_PLAYER_MISMATCH":      FailureCategoryInput,
		"PROCESSING_FAILED":           FailureCategoryInfrastructure,
		"RETRIES_EXHAUSTED":           FailureCategoryInfrastructure,
	}
	for c, cat := range want {
		if c.Category() != cat {
			t.Errorf("%s category = %q, want %q", c, c.Category(), cat)
		}
		if c.IsCorrectable() != (cat == FailureCategoryInput) {
			t.Errorf("%s IsCorrectable mismatch", c)
		}
	}
	if FailureCodeInsufficientFunds == FailureCodeReversalInsufficientFunds {
		t.Fatal("reversal insufficient funds must differ from bet insufficient funds")
	}
	if FailureCode("NOPE").IsValid() || FailureCode("").IsValid() || FailureCode("NOPE").Category() != "" {
		t.Fatal("unknown code accepted")
	}
}

func TestValidationFailureCode(t *testing.T) {
	_, parseScaleErr := Parse("1.001", "BRL")
	_, parseCurErr := Parse("1.00", "XXX")
	cases := []struct {
		name string
		err  error
		want FailureCode
		ok   bool
	}{
		{"nil", nil, "", false},
		{"opening", fmt.Errorf("x: %w", ErrOpeningNotExternal), FailureCodeInvalidKind, true},
		{"kind", ErrInvalidWagerKind, FailureCodeInvalidKind, true},
		{"ref required", ErrReferenceRequired, FailureCodeReferenceRequired, true},
		{"ref not allowed", ErrReferenceNotAllowed, FailureCodeReferenceNotAllowed, true},
		{"currency", parseCurErr, FailureCodeInvalidCurrency, true},
		{"currency mismatch", ErrCurrencyMismatch, FailureCodeCurrencyMismatch, true},
		{"scale", parseScaleErr, FailureCodeInvalidAmount, true},
		{"negative", ErrNegativeAmount, FailureCodeInvalidAmount, true},
		{"generic", fmt.Errorf("%w: roundId is required", ErrInvalidWagerTransaction), FailureCodeInvalidRequest, true},
		{"corrupted", fmt.Errorf("%w: %w", ErrCorruptedWagerTransaction, ErrInvalidAmount), "", false},
		{"unrelated", errors.New("boom"), "", false},
		{"insufficient funds is decided by T06", ErrInsufficientFunds, "", false},
	}
	for _, c := range cases {
		got, ok := ValidationFailureCode(c.err)
		if got != c.want || ok != c.ok {
			t.Errorf("%s: got %q,%v want %q,%v", c.name, got, ok, c.want, c.ok)
		}
	}
}

// --- Construção externa ---------------------------------------------------------

func TestNewExternalWagerTransaction_Valid(t *testing.T) {
	local := testT0.In(time.FixedZone("BRT", -3*3600))
	for _, k := range []WagerKind{WagerKindBet, WagerKindWin, WagerKindLoss, WagerKindRefund, WagerKindRollback} {
		p := externalParams(t, k)
		p.Now = local
		tx, err := NewExternalWagerTransaction(p)
		if err != nil {
			t.Fatalf("%s: %v", k, err)
		}
		if tx.Status() != WagerStatusPending || tx.IsTerminal() || !tx.IsInitialized() {
			t.Fatalf("%s: status %s", k, tx.Status())
		}
		if tx.Origin() != WagerOriginExternal || tx.Kind() != k {
			t.Fatalf("%s: origin/kind %s/%s", k, tx.Origin(), tx.Kind())
		}
		if tx.ID() != p.ID || tx.WalletID() != p.WalletID || tx.PlayerID() != p.PlayerID || tx.Money() != p.Money {
			t.Fatalf("%s: identity not preserved", k)
		}
		if tx.ProviderID() != p.ProviderID || tx.ExternalTransactionID() != p.ExternalTransactionID ||
			tx.IdempotencyKey() != p.IdempotencyKey || tx.RequestHash() != p.RequestHash ||
			tx.RoundID() != p.RoundID || tx.GameID() != p.GameID ||
			tx.ReferenceExternalTransactionID() != p.ReferenceExternalTransactionID ||
			tx.HasReference() != (p.ReferenceExternalTransactionID != "") {
			t.Fatalf("%s: external metadata not preserved", k)
		}
		if tx.FailureCode() != "" || tx.BalanceAfter() != (Money{}) || tx.ReferenceTransactionID() != uuid.Nil ||
			tx.Attempts() != 0 || !tx.NextAttemptAt().IsZero() || !tx.CompletedAt().IsZero() {
			t.Fatalf("%s: unexpected outcome on PENDING", k)
		}
		if tx.CreatedAt() != testT0 || tx.UpdatedAt() != testT0 || tx.CreatedAt().Location() != time.UTC {
			t.Fatalf("%s: timestamps %v/%v not UTC", k, tx.CreatedAt(), tx.UpdatedAt())
		}
	}
}

func TestNewExternalWagerTransaction_WinWithOptionalReference(t *testing.T) {
	p := externalParams(t, WagerKindWin)
	p.ReferenceExternalTransactionID = "bet-1"
	tx, err := NewExternalWagerTransaction(p)
	if err != nil || !tx.HasReference() {
		t.Fatalf("WIN with reference: %v", err)
	}
}

func TestNewExternalWagerTransaction_ZeroPolicy(t *testing.T) {
	brl := func(minor int64) Money { return mustMinor(t, minor, "BRL") }
	type zeroCase struct {
		kind    WagerKind
		amount  Money
		wantErr []error
	}
	cases := []zeroCase{
		{WagerKindLoss, brl(0), nil},
		{WagerKindLoss, brl(1), []error{ErrInvalidAmount}},
		{WagerKindLoss, brl(-1), []error{ErrInvalidAmount, ErrNegativeAmount}},
		{WagerKindLoss, Money{}, []error{ErrInvalidAmount, ErrUninitializedMoney}},
	}
	for _, k := range []WagerKind{WagerKindBet, WagerKindWin, WagerKindRefund, WagerKindRollback} {
		cases = append(cases,
			zeroCase{k, brl(1), nil},
			zeroCase{k, brl(math.MaxInt64), nil},
			zeroCase{k, brl(0), []error{ErrInvalidAmount}},
			zeroCase{k, brl(-2500), []error{ErrInvalidAmount, ErrNegativeAmount}},
			zeroCase{k, Money{}, []error{ErrInvalidAmount, ErrUninitializedMoney}},
		)
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s/%s", c.kind, c.amount), func(t *testing.T) {
			p := externalParams(t, c.kind)
			p.Money = c.amount
			tx, err := NewExternalWagerTransaction(p)
			if c.wantErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if tx != nil {
				t.Fatal("transaction returned with error")
			}
			assertErrs(t, err, append([]error{ErrInvalidWagerTransaction}, c.wantErr...)...)
			if code, ok := ValidationFailureCode(err); !ok || code != FailureCodeInvalidAmount {
				t.Fatalf("code = %q", code)
			}
		})
	}
}

func TestNewExternalWagerTransaction_OpeningRejected(t *testing.T) {
	p := externalParams(t, WagerKindBet)
	p.Kind = WagerKindOpening
	tx, err := NewExternalWagerTransaction(p)
	if tx != nil {
		t.Fatal("OPENING accepted externally")
	}
	assertErrs(t, err, ErrInvalidWagerTransaction, ErrInvalidWagerKind, ErrOpeningNotExternal)
	if code, _ := ValidationFailureCode(err); code != FailureCodeInvalidKind {
		t.Fatalf("code = %q, want INVALID_KIND", code)
	}
	for _, k := range []WagerKind{"", "bet", "CASHOUT"} {
		p.Kind = k
		_, err := NewExternalWagerTransaction(p)
		assertErrs(t, err, ErrInvalidWagerKind)
	}
}

func TestNewExternalWagerTransaction_Invalid(t *testing.T) {
	cases := []struct {
		name   string
		kind   WagerKind
		mutate func(*NewExternalWagerTransactionParams)
		want   []error
		code   FailureCode
	}{
		{"nil id", WagerKindBet, func(p *NewExternalWagerTransactionParams) { p.ID = uuid.Nil }, nil, FailureCodeInvalidRequest},
		{"nil wallet", WagerKindBet, func(p *NewExternalWagerTransactionParams) { p.WalletID = uuid.Nil }, nil, FailureCodeInvalidRequest},
		{"nil player", WagerKindBet, func(p *NewExternalWagerTransactionParams) { p.PlayerID = uuid.Nil }, nil, FailureCodeInvalidRequest},
		{"empty provider", WagerKindBet, func(p *NewExternalWagerTransactionParams) { p.ProviderID = "" }, nil, FailureCodeInvalidRequest},
		{"empty external id", WagerKindBet, func(p *NewExternalWagerTransactionParams) { p.ExternalTransactionID = "" }, nil, FailureCodeInvalidRequest},
		{"empty key", WagerKindBet, func(p *NewExternalWagerTransactionParams) { p.IdempotencyKey = "" }, nil, FailureCodeInvalidRequest},
		{"empty round", WagerKindBet, func(p *NewExternalWagerTransactionParams) { p.RoundID = "" }, nil, FailureCodeInvalidRequest},
		{"empty game", WagerKindBet, func(p *NewExternalWagerTransactionParams) { p.GameID = "" }, nil, FailureCodeInvalidRequest},
		{"long provider", WagerKindBet, func(p *NewExternalWagerTransactionParams) {
			p.ProviderID = strings.Repeat("p", MaxWagerIdentifierLength+1)
		}, nil, FailureCodeInvalidRequest},
		{"whitespace round", WagerKindBet, func(p *NewExternalWagerTransactionParams) { p.RoundID = " round" }, nil, FailureCodeInvalidRequest},
		{"control char game", WagerKindBet, func(p *NewExternalWagerTransactionParams) { p.GameID = "g\n1" }, nil, FailureCodeInvalidRequest},
		{"empty hash", WagerKindBet, func(p *NewExternalWagerTransactionParams) { p.RequestHash = "" }, nil, FailureCodeInvalidRequest},
		{"upper hash", WagerKindBet, func(p *NewExternalWagerTransactionParams) { p.RequestHash = strings.ToUpper(testHash) }, nil, FailureCodeInvalidRequest},
		{"short hash", WagerKindBet, func(p *NewExternalWagerTransactionParams) { p.RequestHash = testHash[:63] }, nil, FailureCodeInvalidRequest},
		{"non-hex hash", WagerKindBet, func(p *NewExternalWagerTransactionParams) { p.RequestHash = "g" + testHash[1:] }, nil, FailureCodeInvalidRequest},
		{"zero now", WagerKindBet, func(p *NewExternalWagerTransactionParams) { p.Now = time.Time{} }, nil, FailureCodeInvalidRequest},
		{"refund without ref", WagerKindRefund, func(p *NewExternalWagerTransactionParams) { p.ReferenceExternalTransactionID = "" },
			[]error{ErrReferenceRequired}, FailureCodeReferenceRequired},
		{"rollback without ref", WagerKindRollback, func(p *NewExternalWagerTransactionParams) { p.ReferenceExternalTransactionID = "" },
			[]error{ErrReferenceRequired}, FailureCodeReferenceRequired},
		{"bet with ref", WagerKindBet, func(p *NewExternalWagerTransactionParams) { p.ReferenceExternalTransactionID = "x" },
			[]error{ErrReferenceNotAllowed}, FailureCodeReferenceNotAllowed},
		{"loss with ref", WagerKindLoss, func(p *NewExternalWagerTransactionParams) { p.ReferenceExternalTransactionID = "x" },
			[]error{ErrReferenceNotAllowed}, FailureCodeReferenceNotAllowed},
		{"self reference", WagerKindRefund, func(p *NewExternalWagerTransactionParams) {
			p.ReferenceExternalTransactionID = p.ExternalTransactionID
		}, []error{ErrReferenceNotAllowed}, FailureCodeReferenceNotAllowed},
		{"bad ref id", WagerKindRollback, func(p *NewExternalWagerTransactionParams) { p.ReferenceExternalTransactionID = "ref " }, nil, FailureCodeInvalidRequest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := externalParams(t, c.kind)
			c.mutate(&p)
			tx, err := NewExternalWagerTransaction(p)
			if tx != nil {
				t.Fatal("transaction returned with error")
			}
			assertErrs(t, err, append([]error{ErrInvalidWagerTransaction}, c.want...)...)
			if errors.Is(err, ErrCorruptedWagerTransaction) {
				t.Fatal("construction error marked as corrupted")
			}
			if code, ok := ValidationFailureCode(err); !ok || code != c.code {
				t.Fatalf("code = %q, want %q", code, c.code)
			}
		})
	}
}

// --- OPENING --------------------------------------------------------------------

func TestNewOpeningWagerTransaction(t *testing.T) {
	amount := mustParse(t, "1000.00", "BRL")
	local := testT0.In(time.FixedZone("BRT", -3*3600))
	tx, err := NewOpeningWagerTransaction(NewOpeningWagerTransactionParams{
		ID: testOpenTxID, WalletID: testWalletID, PlayerID: testPlayerID, Amount: amount, Now: local,
	})
	if err != nil {
		t.Fatalf("NewOpeningWagerTransaction: %v", err)
	}
	if tx.Kind() != WagerKindOpening || tx.Origin() != WagerOriginInternal || tx.Status() != WagerStatusProcessed || !tx.IsTerminal() {
		t.Fatalf("kind/origin/status = %s/%s/%s", tx.Kind(), tx.Origin(), tx.Status())
	}
	if tx.ID() != testOpenTxID || tx.WalletID() != testWalletID || tx.PlayerID() != testPlayerID {
		t.Fatal("identity not preserved")
	}
	if tx.Money() != amount || tx.BalanceAfter() != amount || tx.Money().Currency() != "BRL" {
		t.Fatalf("money %s balanceAfter %s", tx.Money(), tx.BalanceAfter())
	}
	// Metadados externos não se aplicam à origem interna.
	if tx.ProviderID() != "" || tx.ExternalTransactionID() != "" || tx.IdempotencyKey() != "" ||
		tx.RequestHash() != "" || tx.RoundID() != "" || tx.GameID() != "" ||
		tx.ReferenceExternalTransactionID() != "" || tx.HasReference() || tx.ReferenceTransactionID() != uuid.Nil {
		t.Fatal("OPENING carries external metadata")
	}
	if tx.FailureCode() != "" || tx.Attempts() != 0 || !tx.NextAttemptAt().IsZero() {
		t.Fatal("OPENING has failure/scheduling data")
	}
	if tx.CreatedAt() != testT0 || tx.UpdatedAt() != testT0 || tx.CompletedAt() != testT0 || tx.CreatedAt().Location() != time.UTC {
		t.Fatalf("timestamps %v %v %v", tx.CreatedAt(), tx.UpdatedAt(), tx.CompletedAt())
	}
	// Terminal: nenhuma transição é aceita.
	err = tx.Reject(RejectWagerParams{Code: FailureCodeInvalidRequest, Now: testT0.Add(time.Second)})
	assertErrs(t, err, ErrWagerTransactionTerminal, ErrWagerTransitionNotAllowed)
}

func TestNewOpeningWagerTransaction_Invalid(t *testing.T) {
	valid := func() NewOpeningWagerTransactionParams {
		return NewOpeningWagerTransactionParams{
			ID: testOpenTxID, WalletID: testWalletID, PlayerID: testPlayerID,
			Amount: mustParse(t, "10.00", "BRL"), Now: testT0,
		}
	}
	cases := []struct {
		name   string
		mutate func(*NewOpeningWagerTransactionParams)
		want   []error
	}{
		{"nil id", func(p *NewOpeningWagerTransactionParams) { p.ID = uuid.Nil }, nil},
		{"nil wallet", func(p *NewOpeningWagerTransactionParams) { p.WalletID = uuid.Nil }, nil},
		{"nil player", func(p *NewOpeningWagerTransactionParams) { p.PlayerID = uuid.Nil }, nil},
		{"zero amount (no OPENING for zero balance)", func(p *NewOpeningWagerTransactionParams) { p.Amount = mustParse(t, "0.00", "BRL") }, []error{ErrInvalidAmount}},
		{"negative amount", func(p *NewOpeningWagerTransactionParams) { p.Amount = mustParse(t, "-1.00", "BRL") }, []error{ErrInvalidAmount, ErrNegativeAmount}},
		{"uninitialized amount", func(p *NewOpeningWagerTransactionParams) { p.Amount = Money{} }, []error{ErrUninitializedMoney}},
		{"zero now", func(p *NewOpeningWagerTransactionParams) { p.Now = time.Time{} }, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := valid()
			c.mutate(&p)
			tx, err := NewOpeningWagerTransaction(p)
			if tx != nil {
				t.Fatal("transaction returned with error")
			}
			assertErrs(t, err, append([]error{ErrInvalidWagerTransaction}, c.want...)...)
		})
	}
}

func TestOpeningWagerTransaction_MatchesWalletOpening(t *testing.T) {
	amount := mustParse(t, "1000.00", "BRL")
	w, entry, err := NewWallet(NewWalletParams{
		ID: testWalletID, PlayerID: testPlayerID, InitialBalance: amount,
		OpeningTransactionID: testOpenTxID, Now: testT0,
	})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := NewOpeningWagerTransaction(NewOpeningWagerTransactionParams{
		ID: testOpenTxID, WalletID: w.ID(), PlayerID: w.PlayerID(), Amount: amount, Now: testT0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if entry.TransactionID() != tx.ID() || entry.BalanceAfter() != tx.BalanceAfter() || w.Version() != 1 {
		t.Fatal("OPENING transaction and opening ledger entry diverge")
	}
}

// --- Transições -----------------------------------------------------------------

type wagerAction string

const (
	actProcess      wagerAction = "MarkProcessed"
	actPendingRef   wagerAction = "MarkPendingReference"
	actReschedule   wagerAction = "RescheduleReferenceRetry"
	actReject       wagerAction = "Reject"
	actFail         wagerAction = "Fail"
	transitionDelta             = time.Minute
)

var allWagerActions = []wagerAction{actProcess, actPendingRef, actReschedule, actReject, actFail}

// refundIn constrói um REFUND (aceita todas as transições) no estado pedido.
func refundIn(t *testing.T, s WagerStatus) *WagerTransaction {
	t.Helper()
	tx := newExternal(t, WagerKindRefund)
	if s == WagerStatusPending {
		return tx
	}
	if s == WagerStatusPendingReference {
		if err := apply(t, tx, actPendingRef); err != nil {
			t.Fatal(err)
		}
		return tx
	}
	act := map[WagerStatus]wagerAction{WagerStatusProcessed: actProcess, WagerStatusRejected: actReject, WagerStatusFailed: actFail}[s]
	if err := apply(t, tx, act); err != nil {
		t.Fatal(err)
	}
	return tx
}

func apply(t *testing.T, tx *WagerTransaction, a wagerAction) error {
	t.Helper()
	now := testT0
	if tx.IsInitialized() {
		now = tx.UpdatedAt().Add(transitionDelta)
	}
	switch a {
	case actProcess:
		return tx.MarkProcessed(MarkWagerProcessedParams{
			BalanceAfter: mustParse(t, "125.00", "BRL"), ReferenceTransactionID: testRefWagerID, Now: now,
		})
	case actPendingRef:
		return tx.MarkPendingReference(MarkWagerPendingReferenceParams{NextAttemptAt: now.Add(time.Second), Now: now})
	case actReschedule:
		return tx.RescheduleReferenceRetry(MarkWagerPendingReferenceParams{NextAttemptAt: now.Add(2 * time.Second), Now: now})
	case actReject:
		return tx.Reject(RejectWagerParams{Code: FailureCodeReferenceNotFound, Now: now})
	case actFail:
		return tx.Fail(FailWagerParams{Code: FailureCodeProcessingFailed, Now: now})
	}
	t.Fatalf("unknown action %s", a)
	return nil
}

func TestWagerTransaction_TransitionMatrix(t *testing.T) {
	type outcome struct {
		to   WagerStatus
		errs []error
	}
	notAllowed := []error{ErrWagerTransitionNotAllowed}
	terminal := []error{ErrWagerTransactionTerminal, ErrWagerTransitionNotAllowed}
	want := map[WagerStatus]map[wagerAction]outcome{
		WagerStatusPending: {
			actProcess:    {to: WagerStatusProcessed},
			actPendingRef: {to: WagerStatusPendingReference},
			actReschedule: {errs: notAllowed},
			actReject:     {to: WagerStatusRejected},
			actFail:       {to: WagerStatusFailed},
		},
		WagerStatusPendingReference: {
			actProcess:    {to: WagerStatusProcessed},
			actPendingRef: {errs: notAllowed},
			actReschedule: {to: WagerStatusPendingReference},
			actReject:     {to: WagerStatusRejected},
			actFail:       {to: WagerStatusFailed},
		},
	}
	for _, s := range []WagerStatus{WagerStatusProcessed, WagerStatusRejected, WagerStatusFailed} {
		want[s] = map[wagerAction]outcome{}
		for _, a := range allWagerActions {
			want[s][a] = outcome{errs: terminal}
		}
	}
	for _, from := range allWagerStatuses {
		for _, a := range allWagerActions {
			o := want[from][a]
			t.Run(fmt.Sprintf("%s/%s", from, a), func(t *testing.T) {
				tx := refundIn(t, from)
				before := *tx
				err := apply(t, tx, a)
				if o.errs != nil {
					assertErrs(t, err, o.errs...)
					if !reflect.DeepEqual(before, *tx) {
						t.Fatal("rejected transition mutated the transaction")
					}
					return
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if tx.Status() != o.to {
					t.Fatalf("status = %s, want %s", tx.Status(), o.to)
				}
				if !CanTransitionWagerStatus(from, o.to) {
					t.Fatal("method allowed an edge outside the table")
				}
				if tx.UpdatedAt() != before.UpdatedAt().Add(transitionDelta) {
					t.Fatalf("updatedAt not advanced: %v", tx.UpdatedAt())
				}
				if o.to.IsTerminal() {
					if tx.CompletedAt() != tx.UpdatedAt() || !tx.NextAttemptAt().IsZero() {
						t.Fatal("terminal must set completedAt and clear nextAttemptAt")
					}
				} else if !tx.CompletedAt().IsZero() {
					t.Fatal("non-terminal has completedAt")
				}
				// Revalidação via reidratação: todo estado alcançável é persistível.
				if _, err := RehydrateWagerTransaction(rehydrateParamsOf(tx)); err != nil {
					t.Fatalf("reachable state not rehydratable: %v", err)
				}
			})
		}
	}
}

func TestWagerTransaction_TerminalImmutableAfterMultipleAttempts(t *testing.T) {
	for _, s := range []WagerStatus{WagerStatusProcessed, WagerStatusRejected, WagerStatusFailed} {
		tx := refundIn(t, s)
		snapshot := *tx
		for i := 0; i < 3; i++ {
			for _, a := range allWagerActions {
				_ = apply(t, tx, a)
			}
		}
		if !reflect.DeepEqual(snapshot, *tx) {
			t.Fatalf("%s mutated", s)
		}
	}
}

func TestWagerTransaction_Uninitialized(t *testing.T) {
	var nilTx *WagerTransaction
	if nilTx.IsInitialized() || (&WagerTransaction{}).IsInitialized() {
		t.Fatal("zero value reported initialized")
	}
	for _, a := range allWagerActions {
		assertErrs(t, apply(t, nilTx, a), ErrUninitializedWagerTransaction)
		assertErrs(t, apply(t, &WagerTransaction{}, a), ErrUninitializedWagerTransaction)
	}
}

func TestWagerTransaction_MarkProcessed(t *testing.T) {
	now := testT0.Add(time.Minute)
	t.Run("BET without reference", func(t *testing.T) {
		tx := newExternal(t, WagerKindBet)
		bal := mustParse(t, "75.00", "BRL")
		if err := tx.MarkProcessed(MarkWagerProcessedParams{BalanceAfter: bal, Now: now}); err != nil {
			t.Fatal(err)
		}
		if tx.BalanceAfter() != bal || tx.CompletedAt() != now {
			t.Fatal("result not recorded")
		}
	})
	t.Run("LOSS keeps balance and accepts zero amount", func(t *testing.T) {
		tx := newExternal(t, WagerKindLoss)
		if err := tx.MarkProcessed(MarkWagerProcessedParams{BalanceAfter: mustParse(t, "100.00", "BRL"), Now: now}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("zero balance after allowed", func(t *testing.T) {
		tx := newExternal(t, WagerKindBet)
		if err := tx.MarkProcessed(MarkWagerProcessedParams{BalanceAfter: mustParse(t, "0.00", "BRL"), Now: now}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("WIN with reference requires resolved reference", func(t *testing.T) {
		p := externalParams(t, WagerKindWin)
		p.ReferenceExternalTransactionID = "bet-1"
		tx, _ := NewExternalWagerTransaction(p)
		err := tx.MarkProcessed(MarkWagerProcessedParams{BalanceAfter: mustParse(t, "1.00", "BRL"), Now: now})
		assertErrs(t, err, ErrInvalidWagerTransaction)
		if err := tx.MarkProcessed(MarkWagerProcessedParams{
			BalanceAfter: mustParse(t, "1.00", "BRL"), ReferenceTransactionID: testRefWagerID, Now: now,
		}); err != nil || tx.ReferenceTransactionID() != testRefWagerID {
			t.Fatalf("resolved WIN: %v", err)
		}
	})
	invalid := []struct {
		name string
		kind WagerKind
		p    func() MarkWagerProcessedParams
		want []error
	}{
		{"missing balance", WagerKindBet, func() MarkWagerProcessedParams { return MarkWagerProcessedParams{Now: now} },
			[]error{ErrUninitializedMoney}},
		{"negative balance", WagerKindBet, func() MarkWagerProcessedParams {
			return MarkWagerProcessedParams{BalanceAfter: mustParse(t, "-0.01", "BRL"), Now: now}
		}, []error{ErrNegativeAmount}},
		{"currency mismatch", WagerKindBet, func() MarkWagerProcessedParams {
			return MarkWagerProcessedParams{BalanceAfter: mustParse(t, "1.00", "USD"), Now: now}
		}, []error{ErrCurrencyMismatch}},
		{"BET with resolved reference", WagerKindBet, func() MarkWagerProcessedParams {
			return MarkWagerProcessedParams{BalanceAfter: mustParse(t, "1.00", "BRL"), ReferenceTransactionID: testRefWagerID, Now: now}
		}, []error{ErrReferenceNotAllowed}},
		{"REFUND without resolved reference", WagerKindRefund, func() MarkWagerProcessedParams {
			return MarkWagerProcessedParams{BalanceAfter: mustParse(t, "1.00", "BRL"), Now: now}
		}, nil},
		{"self reference", WagerKindRollback, func() MarkWagerProcessedParams {
			return MarkWagerProcessedParams{BalanceAfter: mustParse(t, "1.00", "BRL"), ReferenceTransactionID: testWagerID, Now: now}
		}, []error{ErrReferenceNotAllowed}},
		{"zero now", WagerKindBet, func() MarkWagerProcessedParams {
			return MarkWagerProcessedParams{BalanceAfter: mustParse(t, "1.00", "BRL")}
		}, nil},
	}
	for _, c := range invalid {
		t.Run(c.name, func(t *testing.T) {
			tx := newExternal(t, c.kind)
			before := *tx
			err := tx.MarkProcessed(c.p())
			assertErrs(t, err, append([]error{ErrInvalidWagerTransaction}, c.want...)...)
			if !reflect.DeepEqual(before, *tx) {
				t.Fatal("failed transition mutated the transaction")
			}
		})
	}
}

func TestWagerTransaction_PendingReferenceScheduling(t *testing.T) {
	tx := newExternal(t, WagerKindRollback)
	t1 := testT0.Add(time.Second)
	if err := tx.MarkPendingReference(MarkWagerPendingReferenceParams{NextAttemptAt: t1.Add(time.Second), Now: t1}); err != nil {
		t.Fatal(err)
	}
	if tx.Status() != WagerStatusPendingReference || tx.Attempts() != 1 || tx.NextAttemptAt() != t1.Add(time.Second) {
		t.Fatalf("status %s attempts %d next %v", tx.Status(), tx.Attempts(), tx.NextAttemptAt())
	}
	for i := int32(2); i <= 4; i++ {
		now := tx.NextAttemptAt()
		if err := tx.RescheduleReferenceRetry(MarkWagerPendingReferenceParams{NextAttemptAt: now.Add(time.Duration(i) * time.Second), Now: now}); err != nil {
			t.Fatal(err)
		}
		if tx.Attempts() != i {
			t.Fatalf("attempts = %d, want %d", tx.Attempts(), i)
		}
	}
	// nextAttemptAt no passado ou zero é rejeitado, sem mutação.
	before := *tx
	now := tx.UpdatedAt().Add(time.Minute)
	assertErrs(t, tx.RescheduleReferenceRetry(MarkWagerPendingReferenceParams{NextAttemptAt: now.Add(-time.Second), Now: now}), ErrInvalidWagerTransaction)
	assertErrs(t, tx.RescheduleReferenceRetry(MarkWagerPendingReferenceParams{Now: now}), ErrInvalidWagerTransaction)
	if !reflect.DeepEqual(before, *tx) {
		t.Fatal("failed reschedule mutated the transaction")
	}
	// Expiração (T21): REFERENCE_NOT_FOUND encerra e limpa o agendamento.
	if err := tx.Reject(RejectWagerParams{Code: FailureCodeReferenceNotFound, Now: now}); err != nil {
		t.Fatal(err)
	}
	if tx.FailureCode() != FailureCodeReferenceNotFound || !tx.NextAttemptAt().IsZero() || tx.Attempts() != 4 {
		t.Fatal("expiry did not record outcome / keep attempts for audit")
	}
}

func TestWagerTransaction_PendingReferenceRequiresReference(t *testing.T) {
	for _, k := range []WagerKind{WagerKindBet, WagerKindLoss, WagerKindWin} {
		tx := newExternal(t, k)
		err := tx.MarkPendingReference(MarkWagerPendingReferenceParams{NextAttemptAt: testT0.Add(time.Hour), Now: testT0.Add(time.Minute)})
		assertErrs(t, err, ErrInvalidWagerTransaction)
		if tx.Status() != WagerStatusPending {
			t.Fatalf("%s moved to %s", k, tx.Status())
		}
	}
}

func TestWagerTransaction_AttemptsOverflow(t *testing.T) {
	p := rehydrateParamsOf(refundIn(t, WagerStatusPendingReference))
	p.Attempts = math.MaxInt32
	tx, err := RehydrateWagerTransaction(p)
	if err != nil {
		t.Fatal(err)
	}
	now := tx.UpdatedAt().Add(time.Second)
	assertErrs(t, tx.RescheduleReferenceRetry(MarkWagerPendingReferenceParams{NextAttemptAt: now, Now: now}), ErrInvalidWagerTransaction)
}

func TestWagerTransaction_RejectFailureCodes(t *testing.T) {
	now := testT0.Add(time.Minute)
	cases := []struct {
		kind WagerKind
		code FailureCode
		ok   bool
	}{
		{WagerKindBet, FailureCodeInsufficientFunds, true},
		{WagerKindRollback, FailureCodeInsufficientFunds, false},
		{WagerKindRollback, FailureCodeReversalInsufficientFunds, true},
		{WagerKindBet, FailureCodeReversalInsufficientFunds, false},
		{WagerKindRefund, FailureCodeReversalInsufficientFunds, false},
		{WagerKindRefund, FailureCodeAlreadyReversed, true},
		{WagerKindRollback, FailureCodeAlreadyReversed, true},
		{WagerKindRefund, FailureCodeAmountMismatch, true},
		{WagerKindRollback, FailureCodeReferenceNotReversible, true},
		{WagerKindRefund, FailureCodeReferenceNotProcessed, true},
		{WagerKindRefund, FailureCodeReferenceMismatch, true},
		{WagerKindRefund, FailureCodeReferenceNotFound, true},
		{WagerKindBet, FailureCodeReferenceNotFound, false},
		{WagerKindBet, FailureCodeReferenceMismatch, false},
		{WagerKindBet, FailureCodeCurrencyMismatch, true},
		{WagerKindLoss, FailureCodeCurrencyMismatch, true},
		{WagerKindWin, FailureCodeWalletNotFound, true},
		{WagerKindWin, FailureCodeWalletPlayerMismatch, true},
		{WagerKindBet, FailureCodeProcessingFailed, false},
		{WagerKindBet, FailureCodeRetriesExhausted, false},
		{WagerKindBet, "", false},
		{WagerKindBet, "SOMETHING_ELSE", false},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s/%s", c.kind, c.code), func(t *testing.T) {
			tx := newExternal(t, c.kind)
			before := *tx
			err := tx.Reject(RejectWagerParams{Code: c.code, Now: now})
			if c.ok {
				if err != nil {
					t.Fatalf("unexpected: %v", err)
				}
				if tx.Status() != WagerStatusRejected || tx.FailureCode() != c.code {
					t.Fatal("rejection not recorded")
				}
				return
			}
			assertErrs(t, err, ErrInvalidWagerTransaction, ErrInvalidFailureCode)
			if !reflect.DeepEqual(before, *tx) {
				t.Fatal("failed rejection mutated the transaction")
			}
		})
	}
}

func TestWagerTransaction_RejectOptionalOutcome(t *testing.T) {
	now := testT0.Add(time.Minute)
	tx := newExternal(t, WagerKindBet)
	bal := mustParse(t, "20.00", "BRL")
	if err := tx.Reject(RejectWagerParams{Code: FailureCodeInsufficientFunds, BalanceAfter: bal, Now: now}); err != nil {
		t.Fatal(err)
	}
	if tx.BalanceAfter() != bal {
		t.Fatal("observed balance not recorded")
	}
	rev := newExternal(t, WagerKindRefund)
	if err := rev.Reject(RejectWagerParams{Code: FailureCodeAlreadyReversed, ReferenceTransactionID: testRefWagerID, Now: now}); err != nil {
		t.Fatal(err)
	}
	if rev.ReferenceTransactionID() != testRefWagerID || rev.BalanceAfter() != (Money{}) {
		t.Fatal("resolved reference not recorded")
	}
	other := newExternal(t, WagerKindBet)
	err := other.Reject(RejectWagerParams{Code: FailureCodeInsufficientFunds, BalanceAfter: mustParse(t, "1.00", "USD"), Now: now})
	assertErrs(t, err, ErrCurrencyMismatch)
}

func TestWagerTransaction_FailRequiresInfrastructureCode(t *testing.T) {
	now := testT0.Add(time.Minute)
	for _, code := range AllFailureCodes() {
		tx := newExternal(t, WagerKindRefund)
		err := tx.Fail(FailWagerParams{Code: code, Now: now})
		if code.Category() == FailureCategoryInfrastructure {
			if err != nil || tx.Status() != WagerStatusFailed || tx.FailureCode() != code || tx.BalanceAfter() != (Money{}) {
				t.Fatalf("%s: %v", code, err)
			}
			continue
		}
		assertErrs(t, err, ErrInvalidFailureCode)
		if tx.Status() != WagerStatusPending {
			t.Fatalf("%s: status changed", code)
		}
	}
}

// ValidateFailTransition (T30) é a regra de Fail sem o agregado: para cada
// estado e código, as duas concordam.
func TestValidateFailTransition_AgreesWithFail(t *testing.T) {
	now := testT0.Add(time.Minute)
	for _, s := range allWagerStatuses {
		for _, code := range AllFailureCodes() {
			tx := refundIn(t, s)
			failErr := tx.Fail(FailWagerParams{Code: code, Now: now})
			valErr := ValidateFailTransition(s, code)
			if (failErr == nil) != (valErr == nil) {
				t.Fatalf("%s/%s: Fail err %v, ValidateFailTransition err %v", s, code, failErr, valErr)
			}
			if s.IsTerminal() {
				assertErrs(t, valErr, ErrWagerTransactionTerminal, ErrWagerTransitionNotAllowed)
			} else if code.Category() != FailureCategoryInfrastructure {
				assertErrs(t, valErr, ErrInvalidFailureCode)
			}
		}
	}
	assertErrs(t, ValidateFailTransition("", FailureCodeRetriesExhausted), ErrWagerTransitionNotAllowed)
	assertErrs(t, ValidateFailTransition("pending", FailureCodeRetriesExhausted), ErrWagerTransitionNotAllowed)
	assertErrs(t, ValidateFailTransition(WagerStatusPending, "NOPE"), ErrInvalidFailureCode)
	if err := ValidateFailTransition(WagerStatusPendingReference, FailureCodeRetriesExhausted); err != nil {
		t.Fatal(err)
	}
}

func TestWagerTransaction_UpdatedAtMonotonic(t *testing.T) {
	tx := newExternal(t, WagerKindBet)
	earlier := testT0.Add(-time.Hour) // relógio atrasado de outra instância
	if err := tx.MarkProcessed(MarkWagerProcessedParams{BalanceAfter: mustParse(t, "1.00", "BRL"), Now: earlier}); err != nil {
		t.Fatal(err)
	}
	if tx.UpdatedAt() != testT0 || tx.CompletedAt() != testT0 {
		t.Fatalf("updatedAt %v completedAt %v", tx.UpdatedAt(), tx.CompletedAt())
	}
}

// --- Reidratação ----------------------------------------------------------------

func TestRehydrateWagerTransaction_RoundTrip(t *testing.T) {
	var txs []*WagerTransaction
	for _, s := range allWagerStatuses {
		txs = append(txs, refundIn(t, s))
	}
	for _, k := range []WagerKind{WagerKindBet, WagerKindWin, WagerKindLoss, WagerKindRollback} {
		txs = append(txs, newExternal(t, k))
	}
	opening, err := NewOpeningWagerTransaction(NewOpeningWagerTransactionParams{
		ID: testOpenTxID, WalletID: testWalletID, PlayerID: testPlayerID, Amount: mustParse(t, "5.00", "BRL"), Now: testT0,
	})
	if err != nil {
		t.Fatal(err)
	}
	txs = append(txs, opening)
	for _, tx := range txs {
		p := rehydrateParamsOf(tx)
		// Timestamps vindos do banco podem estar em outro fuso: normaliza para UTC.
		p.CreatedAt = p.CreatedAt.In(time.FixedZone("X", 3600))
		got, err := RehydrateWagerTransaction(p)
		if err != nil {
			t.Fatalf("%s/%s: %v", tx.Kind(), tx.Status(), err)
		}
		if !reflect.DeepEqual(*got, *tx) {
			t.Fatalf("%s/%s: round trip differs\n got %+v\nwant %+v", tx.Kind(), tx.Status(), *got, *tx)
		}
	}
}

func TestRehydrateWagerTransaction_NoSideEffects(t *testing.T) {
	p := rehydrateParamsOf(refundIn(t, WagerStatusPendingReference))
	p.Attempts = 7
	tx, err := RehydrateWagerTransaction(p)
	if err != nil {
		t.Fatal(err)
	}
	if tx.Status() != WagerStatusPendingReference || tx.Attempts() != 7 || tx.UpdatedAt() != p.UpdatedAt || tx.NextAttemptAt() != p.NextAttemptAt {
		t.Fatal("rehydration changed state")
	}
}

func TestRehydrateWagerTransaction_InvalidRejected(t *testing.T) {
	processed := rehydrateParamsOf(refundIn(t, WagerStatusProcessed))
	pending := rehydrateParamsOf(newExternal(t, WagerKindBet))
	pendingRef := rehydrateParamsOf(refundIn(t, WagerStatusPendingReference))
	rejected := rehydrateParamsOf(refundIn(t, WagerStatusRejected))
	failed := rehydrateParamsOf(refundIn(t, WagerStatusFailed))
	opening := func() RehydrateWagerTransactionParams {
		tx, err := NewOpeningWagerTransaction(NewOpeningWagerTransactionParams{
			ID: testOpenTxID, WalletID: testWalletID, PlayerID: testPlayerID, Amount: mustParse(t, "5.00", "BRL"), Now: testT0,
		})
		if err != nil {
			t.Fatal(err)
		}
		return rehydrateParamsOf(tx)
	}()

	type mut = func(*RehydrateWagerTransactionParams)
	cases := []struct {
		name string
		base RehydrateWagerTransactionParams
		m    mut
	}{
		{"nil id", pending, func(p *RehydrateWagerTransactionParams) { p.ID = uuid.Nil }},
		{"unknown kind", pending, func(p *RehydrateWagerTransactionParams) { p.Kind = "CASHOUT" }},
		{"unknown status", pending, func(p *RehydrateWagerTransactionParams) { p.Status = "DONE" }},
		{"missing origin", pending, func(p *RehydrateWagerTransactionParams) { p.Origin = "" }},
		{"external with internal origin", pending, func(p *RehydrateWagerTransactionParams) { p.Origin = WagerOriginInternal }},
		{"opening with external origin", opening, func(p *RehydrateWagerTransactionParams) { p.Origin = WagerOriginExternal }},
		{"nil wallet", pending, func(p *RehydrateWagerTransactionParams) { p.WalletID = uuid.Nil }},
		{"nil player", pending, func(p *RehydrateWagerTransactionParams) { p.PlayerID = uuid.Nil }},
		{"zero BET", pending, func(p *RehydrateWagerTransactionParams) { p.Money = mustParse(t, "0.00", "BRL") }},
		{"uninitialized money", pending, func(p *RehydrateWagerTransactionParams) { p.Money = Money{} }},
		{"empty provider", pending, func(p *RehydrateWagerTransactionParams) { p.ProviderID = "" }},
		{"bad hash", pending, func(p *RehydrateWagerTransactionParams) { p.RequestHash = "abc" }},
		{"BET with reference", pending, func(p *RehydrateWagerTransactionParams) { p.ReferenceExternalTransactionID = "x" }},
		{"REFUND without reference", processed, func(p *RehydrateWagerTransactionParams) { p.ReferenceExternalTransactionID = "" }},
		{"resolved ref without external ref", pending, func(p *RehydrateWagerTransactionParams) { p.ReferenceTransactionID = testRefWagerID }},
		{"pending with failure code", pending, func(p *RehydrateWagerTransactionParams) { p.FailureCode = FailureCodeInsufficientFunds }},
		{"pending with balance", pending, func(p *RehydrateWagerTransactionParams) { p.BalanceAfter = mustParse(t, "1.00", "BRL") }},
		{"pending with completedAt", pending, func(p *RehydrateWagerTransactionParams) { p.CompletedAt = p.UpdatedAt }},
		{"processed without balance", processed, func(p *RehydrateWagerTransactionParams) { p.BalanceAfter = Money{} }},
		{"processed negative balance", processed, func(p *RehydrateWagerTransactionParams) { p.BalanceAfter = mustParse(t, "-1.00", "BRL") }},
		{"processed balance other currency", processed, func(p *RehydrateWagerTransactionParams) { p.BalanceAfter = mustParse(t, "1.00", "USD") }},
		{"processed reversal unresolved", processed, func(p *RehydrateWagerTransactionParams) { p.ReferenceTransactionID = uuid.Nil }},
		{"processed with failure code", processed, func(p *RehydrateWagerTransactionParams) { p.FailureCode = FailureCodeProcessingFailed }},
		{"processed without completedAt", processed, func(p *RehydrateWagerTransactionParams) { p.CompletedAt = time.Time{} }},
		{"processed still scheduled", processed, func(p *RehydrateWagerTransactionParams) { p.NextAttemptAt = p.UpdatedAt }},
		{"completedAt after updatedAt", processed, func(p *RehydrateWagerTransactionParams) { p.CompletedAt = p.UpdatedAt.Add(time.Second) }},
		{"completedAt before createdAt", processed, func(p *RehydrateWagerTransactionParams) { p.CompletedAt = p.CreatedAt.Add(-time.Second) }},
		{"rejected without code", rejected, func(p *RehydrateWagerTransactionParams) { p.FailureCode = "" }},
		{"rejected with infra code", rejected, func(p *RehydrateWagerTransactionParams) { p.FailureCode = FailureCodeRetriesExhausted }},
		{"rejected unknown code", rejected, func(p *RehydrateWagerTransactionParams) { p.FailureCode = "OOPS" }},
		{"rejected REFUND with INSUFFICIENT_FUNDS", rejected, func(p *RehydrateWagerTransactionParams) { p.FailureCode = FailureCodeInsufficientFunds }},
		{"failed with business code", failed, func(p *RehydrateWagerTransactionParams) { p.FailureCode = FailureCodeAlreadyReversed }},
		{"failed with balance", failed, func(p *RehydrateWagerTransactionParams) { p.BalanceAfter = mustParse(t, "1.00", "BRL") }},
		{"pending reference without attempts", pendingRef, func(p *RehydrateWagerTransactionParams) { p.Attempts = 0 }},
		{"pending reference unscheduled", pendingRef, func(p *RehydrateWagerTransactionParams) { p.NextAttemptAt = time.Time{} }},
		{"negative attempts", pending, func(p *RehydrateWagerTransactionParams) { p.Attempts = -1 }},
		{"next attempt before creation", pending, func(p *RehydrateWagerTransactionParams) { p.NextAttemptAt = p.CreatedAt.Add(-time.Second) }},
		{"zero createdAt", pending, func(p *RehydrateWagerTransactionParams) { p.CreatedAt = time.Time{} }},
		{"zero updatedAt", pending, func(p *RehydrateWagerTransactionParams) { p.UpdatedAt = time.Time{} }},
		{"updatedAt before createdAt", pending, func(p *RehydrateWagerTransactionParams) { p.UpdatedAt = p.CreatedAt.Add(-time.Second) }},
		{"opening with provider", opening, func(p *RehydrateWagerTransactionParams) { p.ProviderID = "provider-a" }},
		{"opening with external id", opening, func(p *RehydrateWagerTransactionParams) { p.ExternalTransactionID = "x" }},
		{"opening with key", opening, func(p *RehydrateWagerTransactionParams) { p.IdempotencyKey = "k" }},
		{"opening with hash", opening, func(p *RehydrateWagerTransactionParams) { p.RequestHash = testHash }},
		{"opening with round", opening, func(p *RehydrateWagerTransactionParams) { p.RoundID = "r" }},
		{"opening with game", opening, func(p *RehydrateWagerTransactionParams) { p.GameID = "g" }},
		{"opening with reference", opening, func(p *RehydrateWagerTransactionParams) { p.ReferenceExternalTransactionID = "x" }},
		{"opening with resolved reference", opening, func(p *RehydrateWagerTransactionParams) { p.ReferenceTransactionID = testRefWagerID }},
		{"opening pending", opening, func(p *RehydrateWagerTransactionParams) { p.Status = WagerStatusPending; p.CompletedAt = time.Time{} }},
		{"opening balance differs", opening, func(p *RehydrateWagerTransactionParams) { p.BalanceAfter = mustParse(t, "6.00", "BRL") }},
		{"opening zero amount", opening, func(p *RehydrateWagerTransactionParams) {
			p.Money = mustParse(t, "0.00", "BRL")
			p.BalanceAfter = p.Money
		}},
		{"opening with attempts", opening, func(p *RehydrateWagerTransactionParams) { p.Attempts = 1 }},
		{"REFERENCE_NOT_FOUND with resolved reference", rejected, func(p *RehydrateWagerTransactionParams) {
			p.FailureCode = FailureCodeReferenceNotFound
			p.ReferenceTransactionID = testRefWagerID
		}},
		{"pending with attempts", rehydrateParamsOf(newExternal(t, WagerKindRefund)), func(p *RehydrateWagerTransactionParams) { p.Attempts = 1 }},
		{"pending BET with attempts", pending, func(p *RehydrateWagerTransactionParams) { p.Attempts = 1 }},
		{"processed BET with attempts", rehydrateParamsOf(processedWithoutReference(t, WagerKindBet)), func(p *RehydrateWagerTransactionParams) { p.Attempts = 2 }},
		{"processed WIN without reference with attempts", rehydrateParamsOf(processedWithoutReference(t, WagerKindWin)), func(p *RehydrateWagerTransactionParams) { p.Attempts = 1 }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := c.base
			c.m(&p)
			tx, err := RehydrateWagerTransaction(p)
			if tx != nil {
				t.Fatal("transaction returned with error")
			}
			assertErrs(t, err, ErrCorruptedWagerTransaction, ErrInvalidWagerTransaction)
			if _, ok := ValidationFailureCode(err); ok {
				t.Fatal("corrupted state must not map to an input failure code")
			}
		})
	}
}

// --- Ajustes pós-T05: REFERENCE_NOT_FOUND e attempts --------------------------------

func processedWithoutReference(t *testing.T, k WagerKind) *WagerTransaction {
	t.Helper()
	tx := newExternal(t, k)
	if err := tx.MarkProcessed(MarkWagerProcessedParams{BalanceAfter: mustParse(t, "10.00", "BRL"), Now: testT0.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	return tx
}

func TestWagerTransaction_ReferenceNotFoundForbidsResolvedReference(t *testing.T) {
	now := testT0.Add(time.Minute)
	for _, from := range []WagerStatus{WagerStatusPending, WagerStatusPendingReference} {
		tx := refundIn(t, from)
		before := *tx
		err := tx.Reject(RejectWagerParams{Code: FailureCodeReferenceNotFound, ReferenceTransactionID: testRefWagerID, Now: now})
		assertErrs(t, err, ErrInvalidWagerTransaction, ErrInvalidFailureCode)
		if !reflect.DeepEqual(before, *tx) {
			t.Fatalf("%s: failed rejection mutated the transaction", from)
		}
		// Sem referência resolvida, a expiração é válida.
		if err := tx.Reject(RejectWagerParams{Code: FailureCodeReferenceNotFound, Now: now}); err != nil {
			t.Fatalf("%s: %v", from, err)
		}
	}
	// Os demais códigos de referência podem registrar a referência resolvida.
	for _, code := range []FailureCode{FailureCodeReferenceMismatch, FailureCodeReferenceNotReversible,
		FailureCodeReferenceNotProcessed, FailureCodeAlreadyReversed, FailureCodeAmountMismatch} {
		tx := newExternal(t, WagerKindRefund)
		if err := tx.Reject(RejectWagerParams{Code: code, ReferenceTransactionID: testRefWagerID, Now: now}); err != nil {
			t.Fatalf("%s: %v", code, err)
		}
	}
}

func TestWagerTransaction_AttemptsOnlyCountReferenceLookups(t *testing.T) {
	// Válidos: attempts > 0 com referência, fora de PENDING (inclusive terminal,
	// mantido para auditoria).
	pendingRef := refundIn(t, WagerStatusPendingReference)
	if pendingRef.Attempts() != 1 {
		t.Fatalf("attempts = %d", pendingRef.Attempts())
	}
	now := pendingRef.UpdatedAt().Add(time.Minute)
	if err := pendingRef.MarkProcessed(MarkWagerProcessedParams{BalanceAfter: mustParse(t, "1.00", "BRL"), ReferenceTransactionID: testRefWagerID, Now: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := RehydrateWagerTransaction(rehydrateParamsOf(pendingRef)); err != nil {
		t.Fatalf("processed with attempts must rehydrate: %v", err)
	}
	win := newExternal(t, WagerKindWin)
	p := rehydrateParamsOf(win)
	p.ReferenceExternalTransactionID = "transaction-100"
	p.Status = WagerStatusPendingReference
	p.Attempts = 3
	p.NextAttemptAt = p.CreatedAt.Add(time.Minute)
	if _, err := RehydrateWagerTransaction(p); err != nil {
		t.Fatalf("WIN with reference waiting must rehydrate: %v", err)
	}
	// Transações sem referência nunca chegam a PENDING_REFERENCE, então nunca
	// incrementam attempts por transição.
	for _, k := range []WagerKind{WagerKindBet, WagerKindLoss, WagerKindWin} {
		tx := newExternal(t, k)
		_ = tx.MarkPendingReference(MarkWagerPendingReferenceParams{NextAttemptAt: testT0.Add(time.Hour), Now: testT0.Add(time.Minute)})
		if tx.Attempts() != 0 {
			t.Fatalf("%s: attempts = %d", k, tx.Attempts())
		}
	}
}
