// Package strictjson decodifica corpos JSON de entrada de forma estrita
// (ARCHITECTURE.md, ADR-020). É usado pela API HTTP e pode ser reaproveitado
// pelo consumidor SQS, para que as duas entradas aceitem exatamente o mesmo
// formato.
//
// Decisões:
//
//   - Nenhum valor é decodificado para interface{} nem para tipo numérico:
//     cada campo é lido como json.RawMessage (bytes crus) e convertido pelo
//     chamador. Assim um número JSON nunca passa por float64, nem por engano.
//   - Objetos: chaves com caixa exata (o encoding/json padrão casa chaves sem
//     diferenciar caixa), sem chaves desconhecidas, sem chaves repetidas (o
//     encoding/json padrão fica com a última), um único valor e nada depois
//     dele.
//   - Money: só {"amount":"<decimal>","currency":"<ISO>"}, ambos strings JSON,
//     com as mesmas regras estritas de objeto, e parse por
//     domain.ParseNonNegative (sem NaN, Infinity, notação científica, escala
//     > 2 nem negativos). Os erros de Money embrulham os erros do domínio,
//     então domain.ValidationFailureCode os classifica (INVALID_AMOUNT,
//     INVALID_CURRENCY).
package strictjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// Sentinelas (errors.Is). Todas casam com ErrInvalidJSON.
var (
	// ErrInvalidJSON: corpo que não é um objeto JSON válido conforme as regras.
	ErrInvalidJSON = errors.New("strictjson: invalid JSON")
	// ErrUnknownField: chave não prevista (inclusive outra caixa).
	ErrUnknownField = fmt.Errorf("%w: unknown field", ErrInvalidJSON)
	// ErrDuplicateField: chave repetida.
	ErrDuplicateField = fmt.Errorf("%w: duplicate field", ErrInvalidJSON)
	// ErrMissingField: campo obrigatório ausente.
	ErrMissingField = fmt.Errorf("%w: missing field", ErrInvalidJSON)
	// ErrWrongType: valor com tipo JSON diferente do esperado (inclusive null).
	ErrWrongType = fmt.Errorf("%w: wrong type", ErrInvalidJSON)
)

// FieldError identifica o campo com problema. Field é o caminho do campo
// (ex.: "money.amount"); vazio para erros do objeto como um todo.
type FieldError struct {
	Field string
	Err   error
}

func (e *FieldError) Error() string {
	if e.Field == "" {
		return e.Err.Error()
	}
	return fmt.Sprintf("%s: %v", e.Field, e.Err)
}

// Unwrap devolve a causa.
func (e *FieldError) Unwrap() error { return e.Err }

// Field devolve o campo de um *FieldError na cadeia ("" se não houver).
func Field(err error) string {
	var fe *FieldError
	if errors.As(err, &fe) {
		return fe.Field
	}
	return ""
}

func fieldErr(field string, err error) error { return &FieldError{Field: field, Err: err} }

// DecodeObject lê data como UM objeto JSON e preenche fields com o valor cru
// de cada chave presente. Só as chaves de fields são aceitas (caixa exata),
// cada uma no máximo uma vez. Chaves ausentes deixam o RawMessage como nil.
func DecodeObject(data []byte, fields map[string]*json.RawMessage) error {
	return decodeObject("", data, fields)
}

func decodeObject(prefix string, data []byte, fields map[string]*json.RawMessage) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	tok, err := dec.Token()
	if err != nil {
		return fieldErr(prefix, fmt.Errorf("%w: %v", ErrInvalidJSON, err))
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return fieldErr(prefix, fmt.Errorf("%w: expected an object", ErrWrongType))
	}
	seen := make(map[string]bool, len(fields))
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return fieldErr(prefix, fmt.Errorf("%w: %v", ErrInvalidJSON, err))
		}
		key, ok := tok.(string)
		if !ok {
			return fieldErr(prefix, fmt.Errorf("%w: expected a key", ErrInvalidJSON))
		}
		path := join(prefix, key)
		dst, known := fields[key]
		if !known {
			return fieldErr(path, ErrUnknownField)
		}
		if seen[key] {
			return fieldErr(path, ErrDuplicateField)
		}
		seen[key] = true
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return fieldErr(path, fmt.Errorf("%w: %v", ErrInvalidJSON, err))
		}
		*dst = raw
	}
	if _, err := dec.Token(); err != nil { // '}'
		return fieldErr(prefix, fmt.Errorf("%w: %v", ErrInvalidJSON, err))
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fieldErr(prefix, fmt.Errorf("%w: trailing data after object", ErrInvalidJSON))
	}
	return nil
}

func join(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}

// String converte um valor cru numa string JSON. Ausente (nil), null,
// número, booleano, objeto ou lista são erro.
func String(field string, raw json.RawMessage) (string, error) {
	if raw == nil {
		return "", fieldErr(field, ErrMissingField)
	}
	if len(raw) == 0 || raw[0] != '"' {
		return "", fieldErr(field, fmt.Errorf("%w: expected a string", ErrWrongType))
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", fieldErr(field, fmt.Errorf("%w: %v", ErrInvalidJSON, err))
	}
	return s, nil
}

// Money converte {"amount":"<decimal>","currency":"<ISO>"} num Money não
// negativo. amount numérico (25.00 sem aspas) é ErrWrongType; demais
// problemas de valor embrulham os erros do domínio (ErrInvalidAmount,
// ErrAmountScale, ErrNegativeAmount, ErrInvalidCurrency...).
func Money(field string, raw json.RawMessage) (domain.Money, error) {
	if raw == nil {
		return domain.Money{}, fieldErr(field, ErrMissingField)
	}
	var amountRaw, currencyRaw json.RawMessage
	if err := decodeObject(field, raw, map[string]*json.RawMessage{
		"amount": &amountRaw, "currency": &currencyRaw,
	}); err != nil {
		return domain.Money{}, err
	}
	amount, err := String(join(field, "amount"), amountRaw)
	if err != nil {
		return domain.Money{}, fmt.Errorf("%w: %w", domain.ErrInvalidAmount, err)
	}
	currency, err := String(join(field, "currency"), currencyRaw)
	if err != nil {
		return domain.Money{}, fmt.Errorf("%w: %w", domain.ErrInvalidCurrency, err)
	}
	m, err := domain.ParseNonNegative(amount, currency)
	if err != nil {
		f := join(field, "amount")
		if errors.Is(err, domain.ErrInvalidCurrency) {
			f = join(field, "currency")
		}
		return domain.Money{}, fieldErr(f, err)
	}
	return m, nil
}
