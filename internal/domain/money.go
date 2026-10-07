package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
)

// Decisão arquitetural (ADR-004 em ARCHITECTURE.md): Money é representado por
// int64 em unidades mínimas (centavos, escala fixa 2) + código ISO 4217.
//
//   - Nenhum tipo de ponto flutuante participa do parsing, da aritmética ou da
//     serialização: o parsing é feito dígito a dígito sobre a string decimal e a
//     formatação é feita com divisão/resto inteiros.
//   - Faixa representável: [-92233720368547758.08, 92233720368547758.07]
//     (math.MinInt64 / math.MaxInt64 centavos). Qualquer operação que sairia da
//     faixa retorna ErrMoneyOverflow em vez de dar wrap-around silencioso.
//   - A persistência (T12) mapeia diretamente para BIGINT (MinorUnits) + CHAR(3)
//     (Currency), preservando exatamente valor e moeda.
//   - Money é imutável: campos privados e todas as operações devolvem um novo
//     valor. É comparável com == (int64 + string), o que facilita testes.
//   - O zero-value de Money (moeda vazia) é considerado "não inicializado" e é
//     rejeitado por todas as operações com ErrUninitializedMoney.

// MoneyScale é a quantidade fixa de casas decimais suportadas.
const MoneyScale = 2

// minorUnitsPerMajor = 10^MoneyScale.
const minorUnitsPerMajor = 100

// Erros sentinela de Money. Todos são classificáveis com errors.Is.
// ErrAmountScale e ErrAmountOverflow (no parsing) também satisfazem
// errors.Is(err, ErrInvalidAmount), pois são casos particulares de valor inválido.
var (
	// ErrInvalidAmount indica uma string de valor malformada: vazia, NaN,
	// Infinity, notação científica, sinal "+", espaços, separadores, etc.
	ErrInvalidAmount = errors.New("money: invalid amount")
	// ErrAmountScale indica mais de MoneyScale casas decimais. Não arredondamos
	// silenciosamente: a entrada é rejeitada.
	ErrAmountScale = errors.New("money: amount exceeds scale of 2 decimal places")
	// ErrMoneyOverflow indica que o valor (parsing, soma, subtração ou negação)
	// não cabe em int64 centavos.
	ErrMoneyOverflow = errors.New("money: amount overflows int64 minor units")
	// ErrNegativeAmount indica valor negativo onde só não negativos são aceitos
	// (entradas financeiras externas).
	ErrNegativeAmount = errors.New("money: negative amount not allowed")
	// ErrInvalidCurrency indica código de moeda vazio, fora do formato ISO 4217
	// (três letras maiúsculas) ou não suportado.
	ErrInvalidCurrency = errors.New("money: invalid currency")
	// ErrCurrencyMismatch indica aritmética/comparação entre moedas diferentes.
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	// ErrUninitializedMoney indica uso do zero-value de Money.
	ErrUninitializedMoney = errors.New("money: uninitialized value")
)

// Currency é um código ISO 4217 alfabético validado (ex.: "BRL").
type Currency string

// supportedCurrencies lista os códigos ISO 4217 aceitos.
//
// Decisão: como a escala é fixa em 2 casas, só aceitamos moedas cuja unidade
// mínima ISO 4217 também é 2 (centavos). Moedas com expoente 0 (JPY, CLP, KRW)
// ou 3 (KWD, BHD) ficariam com semântica errada e por isso são rejeitadas.
// Ampliar a lista é seguro desde que o expoente da moeda seja 2.
var supportedCurrencies = map[Currency]struct{}{
	"BRL": {}, "USD": {}, "EUR": {}, "GBP": {}, "CAD": {}, "AUD": {},
	"CHF": {}, "MXN": {}, "ARS": {}, "COP": {}, "PEN": {}, "UYU": {},
}

// ParseCurrency valida um código ISO 4217. Não normaliza caixa: "brl" é
// rejeitado, para que exista uma única forma canônica (relevante para o hash
// de idempotência).
func ParseCurrency(code string) (Currency, error) {
	if len(code) != 3 {
		return "", fmt.Errorf("%w: %q", ErrInvalidCurrency, code)
	}
	for i := 0; i < len(code); i++ {
		if code[i] < 'A' || code[i] > 'Z' {
			return "", fmt.Errorf("%w: %q", ErrInvalidCurrency, code)
		}
	}
	c := Currency(code)
	if _, ok := supportedCurrencies[c]; !ok {
		return "", fmt.Errorf("%w: %q not supported", ErrInvalidCurrency, code)
	}
	return c, nil
}

// String devolve o código da moeda.
func (c Currency) String() string { return string(c) }

// Money é um value object imutável: valor em centavos (int64) + moeda.
type Money struct {
	minor    int64
	currency Currency
}

// Parse converte uma string decimal em Money.
//
// Gramática aceita (estrita, sem arredondamento):
//
//	amount := ["-"] int ["." frac]
//	int    := "0" | [1-9][0-9]*
//	frac   := [0-9]{1,2}
//
// Normalização documentada (anterior ao hash de idempotência, T08): as formas
// "25", "25.5" e "25.50" são equivalentes e produzem 2550 centavos; a forma
// canônica é sempre a de String()/Amount() ("25.50"). Rejeitados: vazio, NaN,
// Infinity, notação científica, "+", espaços, zeros à esquerda ("01.00"),
// ".5", "5.", separadores de milhar, escala > 2 e "-0"/"-0.00" (zero negativo
// não tem forma canônica).
//
// Parse aceita negativos porque Money também representa diferenças internas
// (ex.: reconciliação). Entradas financeiras externas devem usar
// ParseNonNegative.
func Parse(amount, currency string) (Money, error) {
	cur, err := ParseCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	minor, err := parseMinorUnits(amount)
	if err != nil {
		return Money{}, err
	}
	return Money{minor: minor, currency: cur}, nil
}

// ParseNonNegative é Parse + rejeição de valores negativos (ErrNegativeAmount).
// Deve ser usado em todas as entradas financeiras externas (HTTP e SQS).
func ParseNonNegative(amount, currency string) (Money, error) {
	m, err := Parse(amount, currency)
	if err != nil {
		return Money{}, err
	}
	if m.minor < 0 {
		return Money{}, fmt.Errorf("%w: %q", ErrNegativeAmount, amount)
	}
	return m, nil
}

// Zero devolve o valor zero na moeda informada.
func Zero(currency string) (Money, error) {
	cur, err := ParseCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	return Money{minor: 0, currency: cur}, nil
}

// FromMinorUnits reconstrói Money a partir de centavos (ex.: coluna BIGINT).
// Usado na reidratação a partir da persistência.
func FromMinorUnits(minor int64, currency string) (Money, error) {
	cur, err := ParseCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	return Money{minor: minor, currency: cur}, nil
}

// parseMinorUnits faz o parsing dígito a dígito, acumulando em uint64 com
// verificação de overflow. Nenhuma conversão para ponto flutuante é usada.
func parseMinorUnits(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("%w: empty", ErrInvalidAmount)
	}
	orig := s
	neg := false
	if s[0] == '-' {
		neg = true
		s = s[1:]
	}
	intPart, fracPart, hasDot := cutByte(s, '.')
	if intPart == "" || !allDigits(intPart) {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, orig)
	}
	if len(intPart) > 1 && intPart[0] == '0' {
		return 0, fmt.Errorf("%w: leading zeros in %q", ErrInvalidAmount, orig)
	}
	if hasDot {
		if fracPart == "" || !allDigits(fracPart) {
			return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, orig)
		}
		if len(fracPart) > MoneyScale {
			return 0, fmt.Errorf("%w: %w: %q", ErrInvalidAmount, ErrAmountScale, orig)
		}
	}
	// Completa a parte fracionária até MoneyScale dígitos ("5" -> "50").
	for len(fracPart) < MoneyScale {
		fracPart += "0"
	}

	// Limite de magnitude: MaxInt64 para positivos, MaxInt64+1 para negativos
	// (permite representar exatamente math.MinInt64).
	limit := uint64(math.MaxInt64)
	if neg {
		limit++
	}
	var acc uint64
	for _, digits := range [2]string{intPart, fracPart} {
		for i := 0; i < len(digits); i++ {
			d := uint64(digits[i] - '0')
			if acc > (limit-d)/10 {
				return 0, fmt.Errorf("%w: %w: %q", ErrInvalidAmount, ErrMoneyOverflow, orig)
			}
			acc = acc*10 + d
		}
	}
	if neg {
		if acc == 0 {
			return 0, fmt.Errorf("%w: negative zero %q", ErrInvalidAmount, orig)
		}
		// acc <= MaxInt64+1: -(acc) cabe em int64; para acc == MaxInt64+1 a
		// conversão uint64->int64 produz exatamente MinInt64.
		return -int64(acc-1) - 1, nil
	}
	return int64(acc), nil
}

func cutByte(s string, sep byte) (before, after string, found bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			return s[:i], s[i+1:], true
		}
	}
	return s, "", false
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// IsValid indica se o Money foi construído por um construtor (não é o zero-value).
func (m Money) IsValid() bool { return m.currency != "" }

// MinorUnits devolve o valor em centavos (para persistência em BIGINT).
func (m Money) MinorUnits() int64 { return m.minor }

// Currency devolve a moeda.
func (m Money) Currency() Currency { return m.currency }

// IsZero indica valor zero. Retorna false para Money não inicializado.
func (m Money) IsZero() bool { return m.IsValid() && m.minor == 0 }

// IsPositive indica valor > 0. Retorna false para Money não inicializado.
func (m Money) IsPositive() bool { return m.IsValid() && m.minor > 0 }

// IsNegative indica valor < 0. Retorna false para Money não inicializado.
func (m Money) IsNegative() bool { return m.IsValid() && m.minor < 0 }

func (m Money) checkCompatible(o Money) error {
	if !m.IsValid() || !o.IsValid() {
		return ErrUninitializedMoney
	}
	if m.currency != o.currency {
		return fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.currency, o.currency)
	}
	return nil
}

// Add soma dois valores da mesma moeda, com detecção de overflow.
func (m Money) Add(o Money) (Money, error) {
	if err := m.checkCompatible(o); err != nil {
		return Money{}, err
	}
	if (o.minor > 0 && m.minor > math.MaxInt64-o.minor) ||
		(o.minor < 0 && m.minor < math.MinInt64-o.minor) {
		return Money{}, fmt.Errorf("%w: %s + %s", ErrMoneyOverflow, m.Amount(), o.Amount())
	}
	return Money{minor: m.minor + o.minor, currency: m.currency}, nil
}

// Sub subtrai dois valores da mesma moeda, com detecção de overflow.
// O resultado pode ser negativo (diferenças internas); a regra de saldo não
// negativo pertence à Wallet.
func (m Money) Sub(o Money) (Money, error) {
	if err := m.checkCompatible(o); err != nil {
		return Money{}, err
	}
	if (o.minor < 0 && m.minor > math.MaxInt64+o.minor) ||
		(o.minor > 0 && m.minor < math.MinInt64+o.minor) {
		return Money{}, fmt.Errorf("%w: %s - %s", ErrMoneyOverflow, m.Amount(), o.Amount())
	}
	return Money{minor: m.minor - o.minor, currency: m.currency}, nil
}

// Neg devolve o valor com sinal invertido. -MinInt64 não cabe em int64 e
// retorna ErrMoneyOverflow.
func (m Money) Neg() (Money, error) {
	if !m.IsValid() {
		return Money{}, ErrUninitializedMoney
	}
	if m.minor == math.MinInt64 {
		return Money{}, fmt.Errorf("%w: -(%s)", ErrMoneyOverflow, m.Amount())
	}
	return Money{minor: -m.minor, currency: m.currency}, nil
}

// Cmp compara dois valores da mesma moeda: -1 se m < o, 0 se iguais, +1 se m > o.
func (m Money) Cmp(o Money) (int, error) {
	if err := m.checkCompatible(o); err != nil {
		return 0, err
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	default:
		return 0, nil
	}
}

// Amount devolve a forma decimal canônica com exatamente duas casas ("25.00",
// "-0.01"). Formatação apenas com aritmética inteira; usa uint64 para tratar
// MinInt64 sem overflow.
func (m Money) Amount() string {
	var abs uint64
	sign := ""
	if m.minor < 0 {
		sign = "-"
		abs = uint64(-(m.minor + 1)) + 1
	} else {
		abs = uint64(m.minor)
	}
	whole := abs / minorUnitsPerMajor
	frac := abs % minorUnitsPerMajor
	fracStr := strconv.FormatUint(frac, 10)
	if frac < 10 {
		fracStr = "0" + fracStr
	}
	return sign + strconv.FormatUint(whole, 10) + "." + fracStr
}

// String devolve "<amount> <currency>" (ex.: "25.00 BRL"), para logs.
func (m Money) String() string {
	if !m.IsValid() {
		return "<uninitialized money>"
	}
	return m.Amount() + " " + string(m.currency)
}

// moneyJSON é o contrato externo: {"amount":"25.00","currency":"BRL"}.
// amount é sempre string JSON, nunca número.
type moneyJSON struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// MarshalJSON serializa no contrato externo. Money não inicializado é erro.
func (m Money) MarshalJSON() ([]byte, error) {
	if !m.IsValid() {
		return nil, ErrUninitializedMoney
	}
	return json.Marshal(moneyJSON{Amount: m.Amount(), Currency: string(m.currency)})
}

// ErrMalformedMoneyJSON indica um objeto JSON de Money estruturalmente
// inválido: não é objeto (inclusive null), chave desconhecida ou com outra
// caixa, chave repetida, JSON malformado ou dados depois do objeto. Também
// satisfaz errors.Is(err, ErrInvalidAmount), para que a classificação
// (ValidationFailureCode) continue sendo INVALID_AMOUNT.
var ErrMalformedMoneyJSON = fmt.Errorf("%w: malformed money JSON", ErrInvalidAmount)

// UnmarshalJSON aceita apenas {"amount":"<decimal>","currency":"<ISO>"}
// (ARCHITECTURE.md §3, ADR-033).
//
// Decisão: o decoder é estrito e aplica as mesmas regras de objeto que
// internal/adapters/strictjson (sem importá-lo: o domínio não depende de
// adapters):
//
//   - o valor é UM objeto JSON e nada mais (null, lista, string ou número →
//     ErrMalformedMoneyJSON; dados depois do objeto também);
//   - só as chaves "amount" e "currency", com caixa exata (o encoding/json
//     padrão casaria "AMOUNT"), cada uma no máximo uma vez (o padrão ficaria
//     com a última) → ErrMalformedMoneyJSON;
//   - amount e currency são obrigatórios e strings JSON (null, número — que o
//     encoding/json converteria para ponto flutuante —, booleano, objeto ou
//     lista são rejeitados): problema em amount → ErrInvalidAmount, em
//     currency → ErrInvalidCurrency;
//   - os valores passam por Parse (gramática decimal estrita, escala ≤ 2,
//     overflow, moeda suportada em caixa alta). Whitespace JSON entre tokens é
//     aceito; dentro da string do amount, não.
//
// Negativos são aceitos aqui porque o tipo também serializa diferenças
// internas (ex.: "difference" da reconciliação) e Marshal→Unmarshal precisa
// fazer round-trip de qualquer Money válido. A não negatividade é validada
// no ponto de entrada externo (strictjson.Money → ParseNonNegative), que é o
// único caminho usado por HTTP e SQS. Em erro, o receptor não é alterado.
func (m *Money) UnmarshalJSON(data []byte) error {
	amountRaw, currencyRaw, err := decodeMoneyObject(data)
	if err != nil {
		return err
	}
	amount, err := moneyJSONString("amount", amountRaw)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidAmount, err)
	}
	currency, err := moneyJSONString("currency", currencyRaw)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidCurrency, err)
	}
	parsed, err := Parse(amount, currency)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}

// decodeMoneyObject lê data como UM objeto JSON com no máximo as chaves
// "amount" e "currency" (caixa exata, sem repetição) e devolve o valor cru de
// cada uma (nil se ausente). Nenhum valor é decodificado para tipo numérico.
func decodeMoneyObject(data []byte) (amount, currency json.RawMessage, err error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	tok, err := dec.Token()
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrMalformedMoneyJSON, err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, nil, fmt.Errorf("%w: expected an object", ErrMalformedMoneyJSON)
	}
	var seenAmount, seenCurrency bool
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %v", ErrMalformedMoneyJSON, err)
		}
		key, ok := tok.(string)
		if !ok {
			return nil, nil, fmt.Errorf("%w: expected a key", ErrMalformedMoneyJSON)
		}
		var dst *json.RawMessage
		var seen *bool
		switch key {
		case "amount":
			dst, seen = &amount, &seenAmount
		case "currency":
			dst, seen = &currency, &seenCurrency
		default:
			return nil, nil, fmt.Errorf("%w: unknown field %q", ErrMalformedMoneyJSON, key)
		}
		if *seen {
			return nil, nil, fmt.Errorf("%w: duplicate field %q", ErrMalformedMoneyJSON, key)
		}
		*seen = true
		if err := dec.Decode(dst); err != nil {
			return nil, nil, fmt.Errorf("%w: %s: %v", ErrMalformedMoneyJSON, key, err)
		}
	}
	if _, err := dec.Token(); err != nil { // '}'
		return nil, nil, fmt.Errorf("%w: %v", ErrMalformedMoneyJSON, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, nil, fmt.Errorf("%w: trailing data after object", ErrMalformedMoneyJSON)
	}
	return amount, currency, nil
}

// moneyJSONString converte um valor cru numa string JSON. Ausente (nil),
// null, número, booleano, objeto ou lista são erro.
func moneyJSONString(field string, raw json.RawMessage) (string, error) {
	if raw == nil {
		return "", fmt.Errorf("missing field %q", field)
	}
	if len(raw) == 0 || raw[0] != '"' {
		return "", fmt.Errorf("field %q must be a JSON string", field)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", fmt.Errorf("field %q: %w", field, err)
	}
	return s, nil
}
