package domain

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
)

// Limites da representação em centavos.
const (
	maxAmount = "92233720368547758.07"  // math.MaxInt64 centavos
	minAmount = "-92233720368547758.08" // math.MinInt64 centavos
)

func mustParse(t *testing.T, amount, currency string) Money {
	t.Helper()
	m, err := Parse(amount, currency)
	if err != nil {
		t.Fatalf("Parse(%q, %q): unexpected error: %v", amount, currency, err)
	}
	return m
}

func mustMinor(t *testing.T, minor int64, currency string) Money {
	t.Helper()
	m, err := FromMinorUnits(minor, currency)
	if err != nil {
		t.Fatalf("FromMinorUnits(%d, %q): %v", minor, currency, err)
	}
	return m
}

func TestParse(t *testing.T) {
	tests := []struct {
		name      string
		amount    string
		currency  string
		wantMinor int64
		wantStr   string
		wantErrs  []error
	}{
		// Válidos e normalização.
		{name: "two decimals", amount: "25.00", currency: "BRL", wantMinor: 2500, wantStr: "25.00"},
		{name: "integer form", amount: "25", currency: "BRL", wantMinor: 2500, wantStr: "25.00"},
		{name: "one decimal", amount: "25.5", currency: "BRL", wantMinor: 2550, wantStr: "25.50"},
		{name: "zero", amount: "0", currency: "BRL", wantMinor: 0, wantStr: "0.00"},
		{name: "zero two decimals", amount: "0.00", currency: "BRL", wantMinor: 0, wantStr: "0.00"},
		{name: "one cent", amount: "0.01", currency: "BRL", wantMinor: 1, wantStr: "0.01"},
		{name: "negative", amount: "-5.25", currency: "BRL", wantMinor: -525, wantStr: "-5.25"},
		{name: "negative one cent", amount: "-0.01", currency: "BRL", wantMinor: -1, wantStr: "-0.01"},
		{name: "other currency", amount: "10.10", currency: "USD", wantMinor: 1010, wantStr: "10.10"},
		{name: "max int64", amount: maxAmount, currency: "BRL", wantMinor: math.MaxInt64, wantStr: maxAmount},
		{name: "min int64", amount: minAmount, currency: "BRL", wantMinor: math.MinInt64, wantStr: minAmount},

		// Malformados.
		{name: "empty", amount: "", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "NaN", amount: "NaN", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "nan lower", amount: "nan", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "Infinity", amount: "Infinity", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "minus Infinity", amount: "-Infinity", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "Inf", amount: "Inf", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "scientific", amount: "1e2", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "scientific upper", amount: "2.5E1", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "scientific negative exp", amount: "25e-2", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "plus sign", amount: "+1.00", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "only minus", amount: "-", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "double minus", amount: "--1", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "leading space", amount: " 1.00", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "trailing space", amount: "1.00 ", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "comma decimal", amount: "1,00", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "thousand separator", amount: "1,000.00", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "underscore", amount: "1_000", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "leading dot", amount: ".50", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "trailing dot", amount: "5.", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "two dots", amount: "1.0.0", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "leading zeros", amount: "01.00", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "hex", amount: "0x10", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "letters", amount: "abc", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "negative zero", amount: "-0", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "negative zero decimals", amount: "-0.00", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},
		{name: "unicode digit", amount: "١", currency: "BRL", wantErrs: []error{ErrInvalidAmount}},

		// Escala excedente: sem arredondamento silencioso.
		{name: "scale 3", amount: "1.001", currency: "BRL", wantErrs: []error{ErrInvalidAmount, ErrAmountScale}},
		{name: "scale 3 trailing zero", amount: "1.000", currency: "BRL", wantErrs: []error{ErrInvalidAmount, ErrAmountScale}},
		{name: "scale 3 negative", amount: "-0.005", currency: "BRL", wantErrs: []error{ErrInvalidAmount, ErrAmountScale}},

		// Overflow no parsing.
		{name: "max plus one cent", amount: "92233720368547758.08", currency: "BRL", wantErrs: []error{ErrInvalidAmount, ErrMoneyOverflow}},
		{name: "min minus one cent", amount: "-92233720368547758.09", currency: "BRL", wantErrs: []error{ErrInvalidAmount, ErrMoneyOverflow}},
		{name: "huge", amount: "100000000000000000000", currency: "BRL", wantErrs: []error{ErrInvalidAmount, ErrMoneyOverflow}},
		{name: "huge integer part only", amount: "92233720368547759", currency: "BRL", wantErrs: []error{ErrInvalidAmount, ErrMoneyOverflow}},

		// Moeda inválida.
		{name: "empty currency", amount: "1.00", currency: "", wantErrs: []error{ErrInvalidCurrency}},
		{name: "lower currency", amount: "1.00", currency: "brl", wantErrs: []error{ErrInvalidCurrency}},
		{name: "two letters", amount: "1.00", currency: "BR", wantErrs: []error{ErrInvalidCurrency}},
		{name: "four letters", amount: "1.00", currency: "BRLL", wantErrs: []error{ErrInvalidCurrency}},
		{name: "numeric currency", amount: "1.00", currency: "986", wantErrs: []error{ErrInvalidCurrency}},
		{name: "unknown code", amount: "1.00", currency: "XYZ", wantErrs: []error{ErrInvalidCurrency}},
		{name: "zero-decimal currency", amount: "1.00", currency: "JPY", wantErrs: []error{ErrInvalidCurrency}},
		{name: "three-decimal currency", amount: "1.00", currency: "KWD", wantErrs: []error{ErrInvalidCurrency}},
		{name: "currency with space", amount: "1.00", currency: " BRL", wantErrs: []error{ErrInvalidCurrency}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.amount, tt.currency)
			if len(tt.wantErrs) > 0 {
				if err == nil {
					t.Fatalf("Parse(%q, %q) = %v, want error", tt.amount, tt.currency, got)
				}
				for _, want := range tt.wantErrs {
					if !errors.Is(err, want) {
						t.Errorf("error %v: errors.Is(%v) = false", err, want)
					}
				}
				if got != (Money{}) {
					t.Errorf("on error got %#v, want zero value", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.MinorUnits() != tt.wantMinor {
				t.Errorf("MinorUnits = %d, want %d", got.MinorUnits(), tt.wantMinor)
			}
			if got.Amount() != tt.wantStr {
				t.Errorf("Amount = %q, want %q", got.Amount(), tt.wantStr)
			}
			if string(got.Currency()) != tt.currency {
				t.Errorf("Currency = %q, want %q", got.Currency(), tt.currency)
			}
			if !got.IsValid() {
				t.Error("IsValid = false")
			}
		})
	}
}

func TestParseNonNegative(t *testing.T) {
	tests := []struct {
		name    string
		amount  string
		wantErr error
	}{
		{name: "positive", amount: "25.00"},
		{name: "zero", amount: "0.00"},
		{name: "negative", amount: "-0.01", wantErr: ErrNegativeAmount},
		{name: "malformed", amount: "NaN", wantErr: ErrInvalidAmount},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseNonNegative(tt.amount, "BRL")
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestZero(t *testing.T) {
	z, err := Zero("BRL")
	if err != nil {
		t.Fatalf("Zero: %v", err)
	}
	if !z.IsZero() || z.IsPositive() || z.IsNegative() {
		t.Errorf("predicates wrong for zero: %v", z)
	}
	if z.String() != "0.00 BRL" {
		t.Errorf("String = %q", z.String())
	}
	if z == (Money{}) {
		t.Error("Zero(BRL) must differ from the uninitialized zero-value")
	}
	if _, err := Zero("brl"); !errors.Is(err, ErrInvalidCurrency) {
		t.Errorf("Zero(brl) err = %v, want ErrInvalidCurrency", err)
	}
	if _, err := Zero(""); !errors.Is(err, ErrInvalidCurrency) {
		t.Errorf("Zero(\"\") err = %v, want ErrInvalidCurrency", err)
	}
}

func TestFromMinorUnits(t *testing.T) {
	m, err := FromMinorUnits(math.MinInt64, "EUR")
	if err != nil {
		t.Fatalf("FromMinorUnits: %v", err)
	}
	if m.Amount() != minAmount || m.Currency() != "EUR" {
		t.Errorf("got %v", m)
	}
	if _, err := FromMinorUnits(1, "XXX"); !errors.Is(err, ErrInvalidCurrency) {
		t.Errorf("err = %v, want ErrInvalidCurrency", err)
	}
}

func TestPredicates(t *testing.T) {
	tests := []struct {
		name                     string
		m                        Money
		valid, zero, pos, negVal bool
	}{
		{name: "positive", m: mustParse(t, "1.00", "BRL"), valid: true, pos: true},
		{name: "zero", m: mustParse(t, "0", "BRL"), valid: true, zero: true},
		{name: "negative", m: mustParse(t, "-1.00", "BRL"), valid: true, negVal: true},
		{name: "uninitialized", m: Money{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.m.IsValid() != tt.valid || tt.m.IsZero() != tt.zero ||
				tt.m.IsPositive() != tt.pos || tt.m.IsNegative() != tt.negVal {
				t.Errorf("valid=%v zero=%v pos=%v neg=%v", tt.m.IsValid(), tt.m.IsZero(), tt.m.IsPositive(), tt.m.IsNegative())
			}
		})
	}
}

func TestAdd(t *testing.T) {
	tests := []struct {
		name    string
		a, b    Money
		want    string
		wantErr error
	}{
		{name: "simple", a: mustParse(t, "10.50", "BRL"), b: mustParse(t, "0.75", "BRL"), want: "11.25"},
		{name: "with negative", a: mustParse(t, "1.00", "BRL"), b: mustParse(t, "-2.50", "BRL"), want: "-1.50"},
		{name: "reach max", a: mustMinor(t, math.MaxInt64-1, "BRL"), b: mustMinor(t, 1, "BRL"), want: maxAmount},
		{name: "reach min", a: mustMinor(t, math.MinInt64+1, "BRL"), b: mustMinor(t, -1, "BRL"), want: minAmount},
		{name: "max plus min", a: mustMinor(t, math.MaxInt64, "BRL"), b: mustMinor(t, math.MinInt64, "BRL"), want: "-0.01"},
		{name: "overflow positive", a: mustMinor(t, math.MaxInt64, "BRL"), b: mustMinor(t, 1, "BRL"), wantErr: ErrMoneyOverflow},
		{name: "overflow negative", a: mustMinor(t, math.MinInt64, "BRL"), b: mustMinor(t, -1, "BRL"), wantErr: ErrMoneyOverflow},
		{name: "overflow max plus max", a: mustMinor(t, math.MaxInt64, "BRL"), b: mustMinor(t, math.MaxInt64, "BRL"), wantErr: ErrMoneyOverflow},
		{name: "currency mismatch", a: mustParse(t, "1.00", "BRL"), b: mustParse(t, "1.00", "USD"), wantErr: ErrCurrencyMismatch},
		{name: "left uninitialized", a: Money{}, b: mustParse(t, "1.00", "BRL"), wantErr: ErrUninitializedMoney},
		{name: "right uninitialized", a: mustParse(t, "1.00", "BRL"), b: Money{}, wantErr: ErrUninitializedMoney},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := tt.a
			got, err := tt.a.Add(tt.b)
			assertResult(t, got, err, tt.want, tt.wantErr)
			if tt.a != before {
				t.Error("Add mutated receiver")
			}
		})
	}
}

func TestSub(t *testing.T) {
	tests := []struct {
		name    string
		a, b    Money
		want    string
		wantErr error
	}{
		{name: "simple", a: mustParse(t, "100.00", "BRL"), b: mustParse(t, "80.00", "BRL"), want: "20.00"},
		{name: "negative result", a: mustParse(t, "20.00", "BRL"), b: mustParse(t, "80.00", "BRL"), want: "-60.00"},
		{name: "minus negative", a: mustParse(t, "1.00", "BRL"), b: mustParse(t, "-1.00", "BRL"), want: "2.00"},
		{name: "reach min", a: mustMinor(t, -1, "BRL"), b: mustMinor(t, math.MaxInt64, "BRL"), want: minAmount},
		{name: "reach max", a: mustMinor(t, -1, "BRL"), b: mustMinor(t, math.MinInt64, "BRL"), want: maxAmount},
		{name: "overflow negative", a: mustMinor(t, math.MinInt64, "BRL"), b: mustMinor(t, 1, "BRL"), wantErr: ErrMoneyOverflow},
		{name: "overflow positive", a: mustMinor(t, math.MaxInt64, "BRL"), b: mustMinor(t, -1, "BRL"), wantErr: ErrMoneyOverflow},
		{name: "overflow zero minus min", a: mustMinor(t, 0, "BRL"), b: mustMinor(t, math.MinInt64, "BRL"), wantErr: ErrMoneyOverflow},
		{name: "overflow min minus max", a: mustMinor(t, math.MinInt64, "BRL"), b: mustMinor(t, math.MaxInt64, "BRL"), wantErr: ErrMoneyOverflow},
		{name: "currency mismatch", a: mustParse(t, "1.00", "BRL"), b: mustParse(t, "1.00", "EUR"), wantErr: ErrCurrencyMismatch},
		{name: "uninitialized", a: Money{}, b: Money{}, wantErr: ErrUninitializedMoney},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := tt.a
			got, err := tt.a.Sub(tt.b)
			assertResult(t, got, err, tt.want, tt.wantErr)
			if tt.a != before {
				t.Error("Sub mutated receiver")
			}
		})
	}
}

func TestNeg(t *testing.T) {
	tests := []struct {
		name    string
		a       Money
		want    string
		wantErr error
	}{
		{name: "positive", a: mustParse(t, "25.00", "BRL"), want: "-25.00"},
		{name: "negative", a: mustParse(t, "-0.01", "BRL"), want: "0.01"},
		{name: "zero", a: mustParse(t, "0", "BRL"), want: "0.00"},
		{name: "max", a: mustMinor(t, math.MaxInt64, "BRL"), want: "-92233720368547758.07"},
		{name: "min overflow", a: mustMinor(t, math.MinInt64, "BRL"), wantErr: ErrMoneyOverflow},
		{name: "uninitialized", a: Money{}, wantErr: ErrUninitializedMoney},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.a.Neg()
			assertResult(t, got, err, tt.want, tt.wantErr)
		})
	}
}

func TestCmp(t *testing.T) {
	tests := []struct {
		name    string
		a, b    Money
		want    int
		wantErr error
	}{
		{name: "less", a: mustParse(t, "1.00", "BRL"), b: mustParse(t, "1.01", "BRL"), want: -1},
		{name: "equal normalized", a: mustParse(t, "1.5", "BRL"), b: mustParse(t, "1.50", "BRL"), want: 0},
		{name: "greater", a: mustParse(t, "0.00", "BRL"), b: mustParse(t, "-0.01", "BRL"), want: 1},
		{name: "extremes", a: mustMinor(t, math.MinInt64, "BRL"), b: mustMinor(t, math.MaxInt64, "BRL"), want: -1},
		{name: "currency mismatch", a: mustParse(t, "1.00", "BRL"), b: mustParse(t, "1.00", "USD"), wantErr: ErrCurrencyMismatch},
		{name: "uninitialized", a: Money{}, b: mustParse(t, "1.00", "BRL"), wantErr: ErrUninitializedMoney},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.a.Cmp(tt.b)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("Cmp = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestString(t *testing.T) {
	tests := []struct {
		m    Money
		want string
	}{
		{m: mustParse(t, "25", "BRL"), want: "25.00 BRL"},
		{m: mustParse(t, "-1.05", "USD"), want: "-1.05 USD"},
		{m: mustMinor(t, 7, "EUR"), want: "0.07 EUR"},
		{m: Money{}, want: "<uninitialized money>"},
	}
	for _, tt := range tests {
		if got := tt.m.String(); got != tt.want {
			t.Errorf("String() = %q, want %q", got, tt.want)
		}
	}
}

func TestJSONMarshal(t *testing.T) {
	tests := []struct {
		name string
		m    Money
		want string
	}{
		{name: "contract example", m: mustParse(t, "25", "BRL"), want: `{"amount":"25.00","currency":"BRL"}`},
		{name: "negative difference", m: mustParse(t, "-0.10", "BRL"), want: `{"amount":"-0.10","currency":"BRL"}`},
		{name: "max", m: mustMinor(t, math.MaxInt64, "BRL"), want: `{"amount":"` + maxAmount + `","currency":"BRL"}`},
		{name: "min", m: mustMinor(t, math.MinInt64, "BRL"), want: `{"amount":"` + minAmount + `","currency":"BRL"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.Marshal(tt.m)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if string(b) != tt.want {
				t.Errorf("Marshal = %s, want %s", b, tt.want)
			}
			var back Money
			if err := json.Unmarshal(b, &back); err != nil {
				t.Fatalf("Unmarshal round-trip: %v", err)
			}
			if back != tt.m {
				t.Errorf("round-trip = %#v, want %#v", back, tt.m)
			}
		})
	}
}

func TestJSONMarshalUninitialized(t *testing.T) {
	_, err := json.Marshal(Money{})
	if !errors.Is(err, ErrUninitializedMoney) {
		t.Fatalf("err = %v, want ErrUninitializedMoney", err)
	}
	// Também quando embutido em outra estrutura.
	_, err = json.Marshal(struct{ Balance Money }{})
	if !errors.Is(err, ErrUninitializedMoney) {
		t.Fatalf("embedded err = %v, want ErrUninitializedMoney", err)
	}
}

func TestJSONUnmarshal(t *testing.T) {
	tests := []struct {
		name      string
		in        string
		wantMinor int64
		wantCur   string
		wantErr   error
	}{
		{name: "contract", in: `{"amount":"25.00","currency":"BRL"}`, wantMinor: 2500, wantCur: "BRL"},
		{name: "field order", in: `{"currency":"USD","amount":"0.5"}`, wantMinor: 50, wantCur: "USD"},
		{name: "amount as number", in: `{"amount":25.00,"currency":"BRL"}`, wantErr: ErrInvalidAmount},
		{name: "amount as integer", in: `{"amount":25,"currency":"BRL"}`, wantErr: ErrInvalidAmount},
		{name: "amount null", in: `{"amount":null,"currency":"BRL"}`, wantErr: ErrInvalidAmount},
		{name: "amount missing", in: `{"currency":"BRL"}`, wantErr: ErrInvalidAmount},
		{name: "amount empty", in: `{"amount":"","currency":"BRL"}`, wantErr: ErrInvalidAmount},
		{name: "amount NaN", in: `{"amount":"NaN","currency":"BRL"}`, wantErr: ErrInvalidAmount},
		{name: "amount scientific", in: `{"amount":"1e3","currency":"BRL"}`, wantErr: ErrInvalidAmount},
		{name: "amount scale", in: `{"amount":"1.234","currency":"BRL"}`, wantErr: ErrAmountScale},
		{name: "amount overflow", in: `{"amount":"92233720368547758.08","currency":"BRL"}`, wantErr: ErrMoneyOverflow},
		{name: "currency missing", in: `{"amount":"1.00"}`, wantErr: ErrInvalidCurrency},
		{name: "currency number", in: `{"amount":"1.00","currency":986}`, wantErr: ErrInvalidCurrency},
		{name: "currency invalid", in: `{"amount":"1.00","currency":"brl"}`, wantErr: ErrInvalidCurrency},
		{name: "unknown field", in: `{"amount":"1.00","currency":"BRL","extra":1}`, wantErr: ErrInvalidAmount},
		{name: "not an object", in: `"25.00"`, wantErr: ErrInvalidAmount},
		{name: "null", in: `null`, wantErr: ErrInvalidAmount},
		{name: "array", in: `[]`, wantErr: ErrInvalidAmount},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var m Money
			err := json.Unmarshal([]byte(tt.in), &m)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				if m != (Money{}) {
					t.Errorf("target modified on error: %#v", m)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if m.MinorUnits() != tt.wantMinor || string(m.Currency()) != tt.wantCur {
				t.Errorf("got %v", m)
			}
		})
	}
}

// jsonBackslash é a barra invertida montada por código (byte 0x5c), para que
// os escapes unicode JSON (barra, "u", 4 dígitos hex) dos casos abaixo cheguem
// literais ao decoder, sem serem decodificados antes (nem pelo compilador nem
// por ferramentas de edição).
const jsonBackslash = "\x5c"

// esc devolve o escape unicode JSON literal (6 bytes) do código hex dado.
func esc(hex string) string { return jsonBackslash + "u" + hex }

// TestJSONUnmarshalEscapes cobre escapes unicode reais em chaves e valores
// (T32): caixa e repetição valem para a chave já decodificada.
func TestJSONUnmarshalEscapes(t *testing.T) {
	tests := []struct {
		name      string
		in        string
		wantMinor int64
		wantCur   string
		wantErr   error
	}{
		{name: "escaped key amount", in: `{"` + esc("0061") + `mount":"1.00","currency":"BRL"}`, wantMinor: 100, wantCur: "BRL"},
		{name: "escaped key currency", in: `{"amount":"1.00","` + esc("0063") + `urrency":"BRL"}`, wantMinor: 100, wantCur: "BRL"},
		{name: "escaped amount value", in: `{"amount":"` + esc("0031") + `.00","currency":"BRL"}`, wantMinor: 100, wantCur: "BRL"},
		{name: "escaped currency value", in: `{"amount":"1.00","currency":"` + esc("0042") + `RL"}`, wantMinor: 100, wantCur: "BRL"},
		{name: "escaped key other case", in: `{"` + esc("0041") + `MOUNT":"1.00","currency":"BRL"}`, wantErr: ErrMalformedMoneyJSON},
		{name: "escaped key Currency title", in: `{"amount":"1.00","` + esc("0043") + `urrency":"BRL"}`, wantErr: ErrMalformedMoneyJSON},
		{name: "duplicate via escape", in: `{"amount":"1.00","currency":"BRL","` + esc("0061") + `mount":"2.00"}`, wantErr: ErrMalformedMoneyJSON},
		{name: "escaped lowercase currency", in: `{"amount":"1.00","currency":"` + esc("0062") + `rl"}`, wantErr: ErrInvalidCurrency},
		{name: "escaped plus sign", in: `{"amount":"` + esc("002b") + `1.00","currency":"BRL"}`, wantErr: ErrInvalidAmount},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Guarda: o caso só vale se o escape estiver literal na entrada.
			if !strings.Contains(tt.in, jsonBackslash+"u") || !json.Valid([]byte(tt.in)) {
				t.Fatalf("input %q lacks a literal unicode escape or is invalid JSON", tt.in)
			}
			m := mustParse(t, "7.00", "USD")
			before := m
			err := json.Unmarshal([]byte(tt.in), &m)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				if m != before {
					t.Errorf("target modified on error: %#v", m)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if m.MinorUnits() != tt.wantMinor || string(m.Currency()) != tt.wantCur {
				t.Errorf("got %#v", m)
			}
		})
	}
}

func TestJSONUnmarshalEmbedded(t *testing.T) {
	var req struct {
		Money Money `json:"money"`
	}
	if err := json.Unmarshal([]byte(`{"money":{"amount":"25.00","currency":"BRL"}}`), &req); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if req.Money.String() != "25.00 BRL" {
		t.Errorf("got %v", req.Money)
	}
}

// TestJSONUnmarshalStrict cobre as regras de objeto estritas (T32, lacuna
// L4): as mesmas do adapter strictjson, sem importá-lo.
func TestJSONUnmarshalStrict(t *testing.T) {
	accepted := []struct {
		name      string
		in        string
		wantMinor int64
		wantCur   string
	}{
		{name: "negative difference", in: `{"amount":"-0.10","currency":"BRL"}`, wantMinor: -10, wantCur: "BRL"},
		{name: "min int64", in: `{"amount":"` + minAmount + `","currency":"BRL"}`, wantMinor: math.MinInt64, wantCur: "BRL"},
		{name: "max int64", in: `{"amount":"` + maxAmount + `","currency":"BRL"}`, wantMinor: math.MaxInt64, wantCur: "BRL"},
		{name: "equivalent forms", in: `{"amount":"25.5","currency":"EUR"}`, wantMinor: 2550, wantCur: "EUR"},
		{name: "JSON whitespace", in: " \n\t{ \"amount\" : \"1.00\" ,\r\n \"currency\" : \"BRL\" } \n", wantMinor: 100, wantCur: "BRL"},
	}
	for _, tt := range accepted {
		t.Run(tt.name, func(t *testing.T) {
			var m Money
			if err := json.Unmarshal([]byte(tt.in), &m); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if m.MinorUnits() != tt.wantMinor || string(m.Currency()) != tt.wantCur {
				t.Errorf("got %#v", m)
			}
		})
	}

	rejected := []struct {
		name    string
		in      string
		wantErr error
	}{
		// Caixa das chaves (encoding/json padrão casaria sem diferenciar).
		{name: "AMOUNT upper", in: `{"AMOUNT":"1.00","currency":"BRL"}`, wantErr: ErrMalformedMoneyJSON},
		{name: "Amount title", in: `{"Amount":"1.00","currency":"BRL"}`, wantErr: ErrMalformedMoneyJSON},
		{name: "Currency title", in: `{"amount":"1.00","Currency":"BRL"}`, wantErr: ErrMalformedMoneyJSON},
		// Chaves duplicadas (encoding/json padrão ficaria com a última).
		{name: "duplicate amount", in: `{"amount":"1.00","amount":"2.00","currency":"BRL"}`, wantErr: ErrMalformedMoneyJSON},
		{name: "duplicate currency", in: `{"amount":"1.00","currency":"BRL","currency":"USD"}`, wantErr: ErrMalformedMoneyJSON},
		{name: "duplicate same value", in: `{"amount":"1.00","currency":"BRL","amount":"1.00"}`, wantErr: ErrMalformedMoneyJSON},
		{name: "duplicate null then value", in: `{"amount":null,"amount":"1.00","currency":"BRL"}`, wantErr: ErrMalformedMoneyJSON},
		// Campos desconhecidos.
		{name: "extra field", in: `{"amount":"1.00","currency":"BRL","x":1}`, wantErr: ErrMalformedMoneyJSON},
		{name: "extra null field", in: `{"amount":"1.00","currency":"BRL","x":null}`, wantErr: ErrMalformedMoneyJSON},
		{name: "minor units field", in: `{"minorUnits":100,"currency":"BRL"}`, wantErr: ErrMalformedMoneyJSON},
		// Ausentes.
		{name: "empty object", in: `{}`, wantErr: ErrInvalidAmount},
		{name: "amount missing", in: `{"currency":"BRL"}`, wantErr: ErrInvalidAmount},
		{name: "currency missing", in: `{"amount":"1.00"}`, wantErr: ErrInvalidCurrency},
		// Tipos errados e null.
		{name: "null", in: `null`, wantErr: ErrMalformedMoneyJSON},
		{name: "string", in: `"25.00 BRL"`, wantErr: ErrMalformedMoneyJSON},
		{name: "number", in: `25`, wantErr: ErrMalformedMoneyJSON},
		{name: "array", in: `["1.00","BRL"]`, wantErr: ErrMalformedMoneyJSON},
		{name: "amount float", in: `{"amount":25.00,"currency":"BRL"}`, wantErr: ErrInvalidAmount},
		{name: "amount bool", in: `{"amount":true,"currency":"BRL"}`, wantErr: ErrInvalidAmount},
		{name: "amount object", in: `{"amount":{},"currency":"BRL"}`, wantErr: ErrInvalidAmount},
		{name: "amount array", in: `{"amount":["1.00"],"currency":"BRL"}`, wantErr: ErrInvalidAmount},
		{name: "currency null", in: `{"amount":"1.00","currency":null}`, wantErr: ErrInvalidCurrency},
		{name: "currency bool", in: `{"amount":"1.00","currency":false}`, wantErr: ErrInvalidCurrency},
		{name: "currency lower", in: `{"amount":"1.00","currency":"brl"}`, wantErr: ErrInvalidCurrency},
		{name: "currency unsupported", in: `{"amount":"1.00","currency":"JPY"}`, wantErr: ErrInvalidCurrency},
		// Valor: gramática estrita de Parse.
		{name: "amount spaces", in: `{"amount":" 1.00","currency":"BRL"}`, wantErr: ErrInvalidAmount},
		{name: "amount plus", in: `{"amount":"+1.00","currency":"BRL"}`, wantErr: ErrInvalidAmount},
		{name: "amount Infinity", in: `{"amount":"Infinity","currency":"BRL"}`, wantErr: ErrInvalidAmount},
		{name: "negative zero", in: `{"amount":"-0.00","currency":"BRL"}`, wantErr: ErrInvalidAmount},
		{name: "negative overflow", in: `{"amount":"-92233720368547758.09","currency":"BRL"}`, wantErr: ErrMoneyOverflow},
		// JSON malformado ou com sobra.
		{name: "empty input", in: ``, wantErr: ErrMalformedMoneyJSON},
		{name: "truncated", in: `{"amount":"1.00","currency":"BRL"`, wantErr: ErrMalformedMoneyJSON},
		{name: "trailing comma", in: `{"amount":"1.00","currency":"BRL",}`, wantErr: ErrMalformedMoneyJSON},
		{name: "trailing object", in: `{"amount":"1.00","currency":"BRL"}{}`, wantErr: ErrMalformedMoneyJSON},
		{name: "trailing garbage", in: `{"amount":"1.00","currency":"BRL"}x`, wantErr: ErrMalformedMoneyJSON},
		{name: "bad value", in: `{"amount":tru,"currency":"BRL"}`, wantErr: ErrMalformedMoneyJSON},
	}
	for _, tt := range rejected {
		t.Run(tt.name, func(t *testing.T) {
			// Chamada direta: json.Unmarshal valida a sintaxe antes de chamar
			// o método, então só a chamada direta exercita a sobra/truncamento.
			m := mustParse(t, "7.00", "USD")
			before := m
			err := m.UnmarshalJSON([]byte(tt.in))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if code, ok := ValidationFailureCode(err); !ok || code == "" {
				t.Errorf("err %v is not classifiable", err)
			}
			if m != before {
				t.Errorf("target modified on error: %#v", m)
			}
		})
	}
}

// TestJSONUnmarshalEmbeddedStrict garante que a regra de caixa vale também
// para o Money embutido: só as chaves de dentro do Money são do método.
func TestJSONUnmarshalEmbeddedStrict(t *testing.T) {
	var req struct {
		Money Money `json:"money"`
	}
	err := json.Unmarshal([]byte(`{"money":{"amount":"1.00","CURRENCY":"BRL"}}`), &req)
	if !errors.Is(err, ErrMalformedMoneyJSON) {
		t.Fatalf("err = %v, want ErrMalformedMoneyJSON", err)
	}
}

// FuzzJSONRoundTrip: todo Money aceito por UnmarshalJSON reserializa na forma
// canônica e volta ao mesmo valor; nada entra em pânico.
func FuzzJSONRoundTrip(f *testing.F) {
	for _, s := range []string{
		`{"amount":"25.00","currency":"BRL"}`,
		`{"currency":"USD","amount":"-0.5"}`,
		`{"AMOUNT":"1","currency":"BRL"}`,
		`{"amount":"1","amount":"2","currency":"BRL"}`,
		`{"amount":1e2,"currency":"BRL"}`,
		`null`,
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var m Money
		if err := m.UnmarshalJSON(data); err != nil {
			if m != (Money{}) {
				t.Fatalf("target modified on error: %#v", m)
			}
			return
		}
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		var back Money
		if err := json.Unmarshal(b, &back); err != nil || back != m {
			t.Fatalf("round-trip %s: %#v, %v", b, back, err)
		}
	})
}

func TestParseCurrency(t *testing.T) {
	for _, code := range []string{"BRL", "USD", "EUR"} {
		c, err := ParseCurrency(code)
		if err != nil || c.String() != code {
			t.Errorf("ParseCurrency(%q) = %q, %v", code, c, err)
		}
	}
	for _, code := range []string{"", "br", "Brl", "BRL1", "JPY", "XXX"} {
		if _, err := ParseCurrency(code); !errors.Is(err, ErrInvalidCurrency) {
			t.Errorf("ParseCurrency(%q) err = %v, want ErrInvalidCurrency", code, err)
		}
	}
}

func assertResult(t *testing.T, got Money, err error, want string, wantErr error) {
	t.Helper()
	if wantErr != nil {
		if !errors.Is(err, wantErr) {
			t.Fatalf("err = %v, want %v", err, wantErr)
		}
		if got != (Money{}) {
			t.Errorf("on error got %#v, want zero value", got)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Amount() != want {
		t.Errorf("Amount = %q, want %q", got.Amount(), want)
	}
}
