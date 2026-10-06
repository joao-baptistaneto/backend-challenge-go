package strictjson_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/strictjson"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

func decode(t *testing.T, body string) (a, b json.RawMessage, err error) {
	t.Helper()
	err = strictjson.DecodeObject([]byte(body), map[string]*json.RawMessage{"a": &a, "b": &b})
	return a, b, err
}

func TestDecodeObject(t *testing.T) {
	a, b, err := decode(t, ` {"a":"x", "b": {"n": 1.5e300}} `)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(a) != `"x"` || string(b) != `{"n": 1.5e300}` {
		t.Fatalf("raw values: %s %s", a, b)
	}
	_, b, err = decode(t, `{"a":"x"}`)
	if err != nil || b != nil {
		t.Fatalf("absent field must stay nil: %v %s", err, b)
	}

	cases := map[string]struct {
		body  string
		want  error
		field string
	}{
		"unknown":          {`{"a":"x","c":1}`, strictjson.ErrUnknownField, "c"},
		"other case":       {`{"A":"x"}`, strictjson.ErrUnknownField, "A"},
		"duplicate":        {`{"a":"x","a":"y"}`, strictjson.ErrDuplicateField, "a"},
		"not object":       {`["a"]`, strictjson.ErrWrongType, ""},
		"null":             {`null`, strictjson.ErrWrongType, ""},
		"trailing":         {`{"a":"x"} {}`, strictjson.ErrInvalidJSON, ""},
		"trailing garbage": {`{"a":"x"}x`, strictjson.ErrInvalidJSON, ""},
		"truncated":        {`{"a":"x"`, strictjson.ErrInvalidJSON, ""},
		"empty":            {``, strictjson.ErrInvalidJSON, ""},
		"bad value":        {`{"a":tru}`, strictjson.ErrInvalidJSON, "a"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := decode(t, tc.body)
			if !errors.Is(err, tc.want) || !errors.Is(err, strictjson.ErrInvalidJSON) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if got := strictjson.Field(err); got != tc.field {
				t.Fatalf("field = %q, want %q", got, tc.field)
			}
		})
	}
}

func TestString(t *testing.T) {
	if s, err := strictjson.String("f", json.RawMessage(`"aé"`)); err != nil || s != "aé" {
		t.Fatalf("string: %q %v", s, err)
	}
	for _, raw := range []string{`1`, `null`, `true`, `{}`, `[]`} {
		if _, err := strictjson.String("f", json.RawMessage(raw)); !errors.Is(err, strictjson.ErrWrongType) {
			t.Fatalf("%s: err = %v", raw, err)
		}
	}
	if _, err := strictjson.String("f", nil); !errors.Is(err, strictjson.ErrMissingField) {
		t.Fatalf("missing: %v", err)
	}
}

func TestMoney(t *testing.T) {
	m, err := strictjson.Money("money", json.RawMessage(`{"currency":"BRL","amount":"25.5"}`))
	if err != nil {
		t.Fatalf("money: %v", err)
	}
	if m.Amount() != "25.50" || m.Currency() != "BRL" {
		t.Fatalf("money = %s", m)
	}

	cases := map[string]struct {
		raw   string
		code  domain.FailureCode
		field string
	}{
		"float amount":      {`{"amount":25.00,"currency":"BRL"}`, domain.FailureCodeInvalidAmount, "money.amount"},
		"integer amount":    {`{"amount":25,"currency":"BRL"}`, domain.FailureCodeInvalidAmount, "money.amount"},
		"null amount":       {`{"amount":null,"currency":"BRL"}`, domain.FailureCodeInvalidAmount, "money.amount"},
		"missing amount":    {`{"currency":"BRL"}`, domain.FailureCodeInvalidAmount, "money.amount"},
		"scientific":        {`{"amount":"1e2","currency":"BRL"}`, domain.FailureCodeInvalidAmount, "money.amount"},
		"scale":             {`{"amount":"1.001","currency":"BRL"}`, domain.FailureCodeInvalidAmount, "money.amount"},
		"negative":          {`{"amount":"-1.00","currency":"BRL"}`, domain.FailureCodeInvalidAmount, "money.amount"},
		"nan":               {`{"amount":"NaN","currency":"BRL"}`, domain.FailureCodeInvalidAmount, "money.amount"},
		"infinity":          {`{"amount":"Infinity","currency":"BRL"}`, domain.FailureCodeInvalidAmount, "money.amount"},
		"empty amount":      {`{"amount":"","currency":"BRL"}`, domain.FailureCodeInvalidAmount, "money.amount"},
		"overflow":          {`{"amount":"92233720368547758.08","currency":"BRL"}`, domain.FailureCodeInvalidAmount, "money.amount"},
		"currency case":     {`{"amount":"1.00","currency":"brl"}`, domain.FailureCodeInvalidCurrency, "money.currency"},
		"currency number":   {`{"amount":"1.00","currency":1}`, domain.FailureCodeInvalidCurrency, "money.currency"},
		"unknown currency":  {`{"amount":"1.00","currency":"XYZ"}`, domain.FailureCodeInvalidCurrency, "money.currency"},
		"duplicate amount":  {`{"amount":"1.00","amount":"2.00","currency":"BRL"}`, domain.FailureCodeInvalidRequest, "money.amount"},
		"amount other case": {`{"Amount":"1.00","currency":"BRL"}`, domain.FailureCodeInvalidRequest, "money.Amount"},
		"extra field":       {`{"amount":"1.00","currency":"BRL","x":1}`, domain.FailureCodeInvalidRequest, "money.x"},
		"not object":        {`"25.00"`, domain.FailureCodeInvalidRequest, "money"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := strictjson.Money("money", json.RawMessage(tc.raw))
			if err == nil {
				t.Fatal("expected error")
			}
			code, ok := domain.ValidationFailureCode(err)
			if !ok {
				code = domain.FailureCodeInvalidRequest
			}
			if code != tc.code {
				t.Fatalf("code = %s, want %s (%v)", code, tc.code, err)
			}
			if got := strictjson.Field(err); got != tc.field {
				t.Fatalf("field = %q, want %q", got, tc.field)
			}
		})
	}
}
