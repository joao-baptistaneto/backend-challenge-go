package idempotency

import (
	"bytes"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

var (
	testPlayerID = uuid.MustParse("0190a1b2-0000-7000-8000-000000000001")
	testWalletID = uuid.MustParse("0190a1b2-0000-7000-8000-000000000002")
	otherUUID    = uuid.MustParse("0190a1b2-0000-7000-8000-0000000000ff")
)

// Vetor fixo (golden). Se este teste falhar, o algoritmo mudou: hashes já
// persistidos deixariam de casar. Mudança intencional exige incrementar
// HashSchemaVersion e atualizar o ADR.
const (
	goldenCanonical = `{"externalTransactionId":"transaction-123","gameId":"fortune-chimp","kind":"BET",` +
		`"money":{"amount":"25.50","currency":"BRL"},"playerId":"0190a1b2-0000-7000-8000-000000000001",` +
		`"providerId":"provider-a","referenceExternalTransactionId":null,"roundId":"round-987","v":1,` +
		`"walletId":"0190a1b2-0000-7000-8000-000000000002"}`
	goldenHash = "7d102de14b5889228fcf9559c3da8d66cc4df500b66cf48400fe6a37d630d247"
)

func mustMoney(t *testing.T, amount, currency string) domain.Money {
	t.Helper()
	m, err := domain.ParseNonNegative(amount, currency)
	if err != nil {
		t.Fatalf("ParseNonNegative(%q, %q): %v", amount, currency, err)
	}
	return m
}

func baseFields(t *testing.T) RequestFields {
	t.Helper()
	return RequestFields{
		ProviderID:            "provider-a",
		ExternalTransactionID: "transaction-123",
		PlayerID:              testPlayerID,
		WalletID:              testWalletID,
		RoundID:               "round-987",
		GameID:                "fortune-chimp",
		Kind:                  domain.WagerKindBet,
		Money:                 mustMoney(t, "25.5", "BRL"),
	}
}

func mustHash(t *testing.T, f RequestFields) string {
	t.Helper()
	h, err := RequestHash(f)
	if err != nil {
		t.Fatalf("RequestHash: %v", err)
	}
	return h
}

func TestGoldenVector(t *testing.T) {
	f := baseFields(t)
	got, err := CanonicalJSON(f)
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	if string(got) != goldenCanonical {
		t.Fatalf("canonical JSON changed:\n got: %s\nwant: %s", got, goldenCanonical)
	}
	if h := mustHash(t, f); h != goldenHash {
		t.Fatalf("hash changed: got %s want %s", h, goldenHash)
	}
}

func TestGoldenVectorWithReference(t *testing.T) {
	f := baseFields(t)
	f.Kind = domain.WagerKindRefund
	f.ReferenceExternalTransactionID = "transaction-100"
	got, err := CanonicalJSON(f)
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	want := strings.Replace(strings.Replace(goldenCanonical, `"kind":"BET"`, `"kind":"REFUND"`, 1),
		`"referenceExternalTransactionId":null`, `"referenceExternalTransactionId":"transaction-100"`, 1)
	if string(got) != want {
		t.Fatalf("canonical JSON:\n got: %s\nwant: %s", got, want)
	}
}

func TestDeterministic(t *testing.T) {
	f := baseFields(t)
	first := mustHash(t, f)
	for i := 0; i < 100; i++ {
		if h := mustHash(t, baseFields(t)); h != first {
			t.Fatalf("iteration %d: hash %s != %s", i, h, first)
		}
	}
}

func TestDeterministicConcurrent(t *testing.T) {
	f := baseFields(t)
	want := mustHash(t, f)
	done := make(chan string, 16)
	for i := 0; i < cap(done); i++ {
		go func() {
			h, _ := RequestHash(f)
			done <- h
		}()
	}
	for i := 0; i < cap(done); i++ {
		if h := <-done; h != want {
			t.Fatalf("concurrent hash %s != %s", h, want)
		}
	}
}

// A ordem das chaves no JSON recebido é irrelevante: o transporte decodifica
// para struct e o canônico tem ordem fixa.
func TestInputKeyOrderIrrelevant(t *testing.T) {
	bodies := []string{
		`{"providerId":"provider-a","externalTransactionId":"transaction-123","playerId":"0190a1b2-0000-7000-8000-000000000001","walletId":"0190a1b2-0000-7000-8000-000000000002","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.50","currency":"BRL"}}`,
		`{"money":{"currency":"BRL","amount":"25.5"},"kind":"BET","gameId":"fortune-chimp","roundId":"round-987","walletId":"0190a1b2-0000-7000-8000-000000000002","playerId":"0190a1b2-0000-7000-8000-000000000001","externalTransactionId":"transaction-123","providerId":"provider-a"}`,
		`{"walletId":"0190a1b2-0000-7000-8000-000000000002","kind":"BET","providerId":"provider-a","money":{"amount":"25.50","currency":"BRL"},"gameId":"fortune-chimp","externalTransactionId":"transaction-123","roundId":"round-987","playerId":"0190a1b2-0000-7000-8000-000000000001"}`,
	}
	want := mustHash(t, baseFields(t))
	for i, body := range bodies {
		var req httpWagerRequest
		decodeStrict(t, body, &req)
		f := req.toFields(t, "provider-a")
		if h := mustHash(t, f); h != want {
			t.Fatalf("body %d: hash %s != %s", i, h, want)
		}
	}
}

func TestCanonicalKeysSorted(t *testing.T) {
	f := baseFields(t)
	f.ReferenceExternalTransactionID = "x"
	got, err := CanonicalJSON(f)
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(got, &top); err != nil {
		t.Fatalf("canonical is not valid JSON: %v", err)
	}
	checkSorted(t, got)
	if bytes.ContainsAny(got, " \n\t") {
		t.Fatalf("canonical must be compact: %s", got)
	}
	for _, forbidden := range []string{"idempotencyKey", "messageId", "correlationId", "eventId"} {
		if _, ok := top[forbidden]; ok {
			t.Fatalf("canonical contains transport field %q", forbidden)
		}
	}
	if len(top) != 10 {
		t.Fatalf("expected 10 top-level keys, got %d: %s", len(top), got)
	}
}

// checkSorted percorre os tokens e verifica a ordem das chaves em cada objeto.
func checkSorted(t *testing.T, data []byte) {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(data))
	type frame struct {
		keys      []string
		expectKey bool
	}
	var stack []*frame
	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		if d, ok := tok.(json.Delim); ok {
			switch d {
			case '{':
				stack = append(stack, &frame{expectKey: true})
				continue
			case '}':
				top := stack[len(stack)-1]
				if !sort.StringsAreSorted(top.keys) {
					t.Fatalf("keys not sorted: %v", top.keys)
				}
				stack = stack[:len(stack)-1]
				if len(stack) > 0 {
					stack[len(stack)-1].expectKey = true
				}
				continue
			}
		}
		top := stack[len(stack)-1]
		if top.expectKey {
			top.keys = append(top.keys, tok.(string))
			top.expectKey = false
		} else {
			top.expectKey = true
		}
	}
}

func TestSensitiveToEveryField(t *testing.T) {
	base := mustHash(t, baseFields(t))
	mutations := map[string]func(*RequestFields){
		"providerId":            func(f *RequestFields) { f.ProviderID = "provider-b" },
		"externalTransactionId": func(f *RequestFields) { f.ExternalTransactionID = "transaction-124" },
		"playerId":              func(f *RequestFields) { f.PlayerID = otherUUID },
		"walletId":              func(f *RequestFields) { f.WalletID = otherUUID },
		"roundId":               func(f *RequestFields) { f.RoundID = "round-988" },
		"gameId":                func(f *RequestFields) { f.GameID = "other-game" },
		"kind":                  func(f *RequestFields) { f.Kind = domain.WagerKindWin },
		"money.amount":          func(f *RequestFields) { f.Money = mustMoney(t, "25.51", "BRL") },
		"money.currency":        func(f *RequestFields) { f.Money = mustMoney(t, "25.50", "USD") },
		"referenceExternalTransactionId": func(f *RequestFields) {
			f.ReferenceExternalTransactionID = "transaction-100"
		},
		// Sem normalização (ADR-008): caixa e espaços contam.
		"providerId case":  func(f *RequestFields) { f.ProviderID = "Provider-A" },
		"externalId space": func(f *RequestFields) { f.ExternalTransactionID = "transaction-123 " },
	}
	seen := map[string]string{base: "base"}
	for name, mutate := range mutations {
		f := baseFields(t)
		mutate(&f)
		h := mustHash(t, f)
		if prev, dup := seen[h]; dup {
			t.Errorf("%s: hash collides with %s", name, prev)
		}
		seen[h] = name
	}
}

// Swap de valores entre campos ou conteúdo que imita a sintaxe JSON não pode
// colidir: cada valor é escapado e ligado à sua chave.
func TestNoFieldBoundaryAmbiguity(t *testing.T) {
	a := baseFields(t)
	a.RoundID, a.GameID = "x", "y"
	b := baseFields(t)
	b.RoundID, b.GameID = "y", "x"
	if mustHash(t, a) == mustHash(t, b) {
		t.Fatal("swapping roundId/gameId must change the hash")
	}

	c := baseFields(t)
	c.ProviderID = `provider-a","externalTransactionId":"evil`
	got, err := CanonicalJSON(c)
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if decoded["providerId"] != c.ProviderID || decoded["externalTransactionId"] != "transaction-123" {
		t.Fatalf("escaping broken: %s", got)
	}
}

func TestStringEscaping(t *testing.T) {
	f := baseFields(t)
	f.GameID = "a\"b\\c\nd\te\x01f/<>&é😀 "
	got, err := CanonicalJSON(f)
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	want := `"gameId":"a\"b\\c\nd\te\u0001f/<>&é😀` + " " + `"`
	if !strings.Contains(string(got), want) {
		t.Fatalf("escaping: got %s, want fragment %s", got, want)
	}
	var decoded map[string]any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if decoded["gameId"] != f.GameID {
		t.Fatalf("round trip: got %q want %q", decoded["gameId"], f.GameID)
	}
}

func TestInvalidUTF8Rejected(t *testing.T) {
	f := baseFields(t)
	f.RoundID = "round-\xff"
	if _, err := RequestHash(f); !errors.Is(err, ErrInvalidHashInput) {
		t.Fatalf("expected ErrInvalidHashInput, got %v", err)
	}
}

func TestUninitializedMoneyRejected(t *testing.T) {
	f := baseFields(t)
	f.Money = domain.Money{}
	_, err := RequestHash(f)
	if !errors.Is(err, ErrInvalidHashInput) || !errors.Is(err, domain.ErrUninitializedMoney) {
		t.Fatalf("expected ErrInvalidHashInput+ErrUninitializedMoney, got %v", err)
	}
}

func TestMoneyNormalizationEquivalence(t *testing.T) {
	want := ""
	for _, amount := range []string{"25.5", "25.50"} {
		f := baseFields(t)
		f.Money = mustMoney(t, amount, "BRL")
		h := mustHash(t, f)
		if want == "" {
			want = h
		} else if h != want {
			t.Fatalf("%q: hash %s != %s", amount, h, want)
		}
	}
	// "25" e "25.00" também são equivalentes entre si e diferentes de 25.50.
	f25 := baseFields(t)
	f25.Money = mustMoney(t, "25", "BRL")
	f2500 := baseFields(t)
	f2500.Money = mustMoney(t, "25.00", "BRL")
	if mustHash(t, f25) != mustHash(t, f2500) {
		t.Fatal(`"25" and "25.00" must be equivalent`)
	}
	if mustHash(t, f25) == want {
		t.Fatal(`"25" and "25.50" must differ`)
	}
	// Money reidratado de centavos (persistência) é igual ao parseado.
	fMinor := baseFields(t)
	m, err := domain.FromMinorUnits(2550, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	fMinor.Money = m
	if mustHash(t, fMinor) != want {
		t.Fatal("FromMinorUnits(2550) must hash like 25.50")
	}
}

func TestEmptyReferenceIsNull(t *testing.T) {
	got, err := CanonicalJSON(baseFields(t))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), `"referenceExternalTransactionId":null`) {
		t.Fatalf("absent reference must be null: %s", got)
	}
}

// --- Equivalência HTTP x SQS -------------------------------------------------

// httpWagerRequest imita o corpo de POST /wagering/transactions. providerId
// vem da identidade autenticada; Idempotency-Key vem do header.
type httpWagerRequest struct {
	ProviderID                     string          `json:"providerId"`
	ExternalTransactionID          string          `json:"externalTransactionId"`
	PlayerID                       string          `json:"playerId"`
	WalletID                       string          `json:"walletId"`
	RoundID                        string          `json:"roundId"`
	GameID                         string          `json:"gameId"`
	Kind                           string          `json:"kind"`
	Money                          json.RawMessage `json:"money"`
	ReferenceExternalTransactionID *string         `json:"referenceExternalTransactionId"`
}

func (r httpWagerRequest) toFields(t *testing.T, authProvider string) RequestFields {
	t.Helper()
	var m domain.Money
	if err := json.Unmarshal(r.Money, &m); err != nil {
		t.Fatalf("money: %v", err)
	}
	kind, err := domain.ParseExternalWagerKind(r.Kind)
	if err != nil {
		t.Fatalf("kind: %v", err)
	}
	f := RequestFields{
		ProviderID:            authProvider,
		ExternalTransactionID: r.ExternalTransactionID,
		PlayerID:              uuid.MustParse(r.PlayerID),
		WalletID:              uuid.MustParse(r.WalletID),
		RoundID:               r.RoundID,
		GameID:                r.GameID,
		Kind:                  kind,
		Money:                 m,
	}
	if r.ReferenceExternalTransactionID != nil {
		f.ReferenceExternalTransactionID = *r.ReferenceExternalTransactionID
	}
	return f
}

// sqsEnvelope imita a mensagem WagerTransactionRequested: metadados de
// transporte no envelope, negócio em data (com idempotencyKey e money plano).
type sqsEnvelope struct {
	MessageID     string    `json:"messageId"`
	EventType     string    `json:"eventType"`
	CorrelationID string    `json:"correlationId"`
	OccurredAt    time.Time `json:"occurredAt"`
	Data          struct {
		IdempotencyKey        string `json:"idempotencyKey"`
		ProviderID            string `json:"providerId"`
		ExternalTransactionID string `json:"externalTransactionId"`
		PlayerID              string `json:"playerId"`
		WalletID              string `json:"walletId"`
		RoundID               string `json:"roundId"`
		GameID                string `json:"gameId"`
		Kind                  string `json:"kind"`
		Money                 struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"money"`
		ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
	} `json:"data"`
}

func (e sqsEnvelope) toFields(t *testing.T) RequestFields {
	t.Helper()
	d := e.Data
	return RequestFields{
		ProviderID:                     d.ProviderID,
		ExternalTransactionID:          d.ExternalTransactionID,
		PlayerID:                       uuid.MustParse(d.PlayerID),
		WalletID:                       uuid.MustParse(d.WalletID),
		RoundID:                        d.RoundID,
		GameID:                         d.GameID,
		Kind:                           domain.WagerKind(d.Kind),
		Money:                          mustMoney(t, d.Money.Amount, d.Money.Currency),
		ReferenceExternalTransactionID: d.ReferenceExternalTransactionID,
	}
}

func decodeStrict(t *testing.T, body string, v any) {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		t.Fatalf("decode: %v", err)
	}
}

func TestHTTPAndSQSEquivalence(t *testing.T) {
	cases := []struct {
		name     string
		httpBody string
		sqsBody  string
	}{
		{
			name: "bet with different amount forms and transport metadata",
			httpBody: `{"providerId":"provider-a","externalTransactionId":"transaction-123",
				"playerId":"0190a1b2-0000-7000-8000-000000000001","walletId":"0190a1b2-0000-7000-8000-000000000002",
				"roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.5","currency":"BRL"}}`,
			sqsBody: `{"messageId":"msg-1","eventType":"WagerTransactionRequested","correlationId":"corr-9",
				"occurredAt":"2026-10-05T12:00:00Z","data":{"idempotencyKey":"some-other-key",
				"providerId":"provider-a","externalTransactionId":"transaction-123",
				"playerId":"0190a1b2-0000-7000-8000-000000000001","walletId":"0190a1b2-0000-7000-8000-000000000002",
				"roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.50","currency":"BRL"}}}`,
		},
		{
			name: "refund with reference",
			httpBody: `{"providerId":"provider-a","externalTransactionId":"refund-1",
				"playerId":"0190a1b2-0000-7000-8000-000000000001","walletId":"0190a1b2-0000-7000-8000-000000000002",
				"roundId":"round-987","gameId":"fortune-chimp","kind":"REFUND","money":{"amount":"10","currency":"BRL"},
				"referenceExternalTransactionId":"transaction-123"}`,
			sqsBody: `{"messageId":"msg-2","eventType":"WagerTransactionRequested","correlationId":"corr-10",
				"occurredAt":"2026-10-05T13:00:00Z","data":{"idempotencyKey":"provider-a:refund-1",
				"providerId":"provider-a","externalTransactionId":"refund-1",
				"playerId":"0190A1B2-0000-7000-8000-000000000001","walletId":"0190a1b2-0000-7000-8000-000000000002",
				"roundId":"round-987","gameId":"fortune-chimp","kind":"REFUND","money":{"amount":"10.00","currency":"BRL"},
				"referenceExternalTransactionId":"transaction-123"}}`,
		},
		{
			name: "win with explicit null reference vs omitted",
			httpBody: `{"providerId":"provider-a","externalTransactionId":"win-1",
				"playerId":"0190a1b2-0000-7000-8000-000000000001","walletId":"0190a1b2-0000-7000-8000-000000000002",
				"roundId":"round-987","gameId":"fortune-chimp","kind":"WIN","money":{"amount":"0.10","currency":"BRL"},
				"referenceExternalTransactionId":null}`,
			sqsBody: `{"messageId":"msg-3","eventType":"WagerTransactionRequested","correlationId":"corr-11",
				"occurredAt":"2026-10-05T14:00:00Z","data":{"idempotencyKey":"provider-a:win-1",
				"providerId":"provider-a","externalTransactionId":"win-1",
				"playerId":"0190a1b2-0000-7000-8000-000000000001","walletId":"0190a1b2-0000-7000-8000-000000000002",
				"roundId":"round-987","gameId":"fortune-chimp","kind":"WIN","money":{"amount":"0.1","currency":"BRL"}}}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var req httpWagerRequest
			decodeStrict(t, tc.httpBody, &req)
			var env sqsEnvelope
			decodeStrict(t, tc.sqsBody, &env)

			httpHash := mustHash(t, req.toFields(t, "provider-a"))
			sqsHash := mustHash(t, env.toFields(t))
			if httpHash != sqsHash {
				t.Fatalf("HTTP hash %s != SQS hash %s", httpHash, sqsHash)
			}
		})
	}
}

// Mudar apenas metadados de transporte (idempotencyKey, messageId,
// correlationId, occurredAt) não muda o hash.
func TestTransportMetadataIgnored(t *testing.T) {
	body := func(key, msg, corr, at string) string {
		return `{"messageId":"` + msg + `","eventType":"WagerTransactionRequested","correlationId":"` + corr +
			`","occurredAt":"` + at + `","data":{"idempotencyKey":"` + key + `",
			"providerId":"provider-a","externalTransactionId":"transaction-123",
			"playerId":"0190a1b2-0000-7000-8000-000000000001","walletId":"0190a1b2-0000-7000-8000-000000000002",
			"roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.50","currency":"BRL"}}}`
	}
	var a, b sqsEnvelope
	decodeStrict(t, body("k1", "m1", "c1", "2026-01-01T00:00:00Z"), &a)
	decodeStrict(t, body("k2", "m2", "c2", "2026-02-01T00:00:00Z"), &b)
	if mustHash(t, a.toFields(t)) != mustHash(t, b.toFields(t)) {
		t.Fatal("transport metadata must not affect the hash")
	}
	if mustHash(t, a.toFields(t)) != mustHash(t, baseFields(t)) {
		t.Fatal("SQS fields must hash like base fields")
	}
}

// --- Compatibilidade com o domínio --------------------------------------------

func TestHashAcceptedByDomainValidator(t *testing.T) {
	for _, kind := range []domain.WagerKind{
		domain.WagerKindBet, domain.WagerKindWin, domain.WagerKindLoss,
		domain.WagerKindRefund, domain.WagerKindRollback,
	} {
		f := baseFields(t)
		f.Kind = kind
		if kind == domain.WagerKindLoss {
			f.Money = mustMoney(t, "0", "BRL")
		}
		if kind.RequiresReference() {
			f.ReferenceExternalTransactionID = "transaction-100"
		}
		h := mustHash(t, f)
		if len(h) != domain.RequestHashLength || strings.ToLower(h) != h {
			t.Fatalf("%s: hash %q is not %d lowercase hex", kind, h, domain.RequestHashLength)
		}
		_, err := domain.NewExternalWagerTransaction(domain.NewExternalWagerTransactionParams{
			ID:                             uuid.MustParse("0190a1b2-0000-7000-8000-000000000003"),
			ProviderID:                     f.ProviderID,
			ExternalTransactionID:          f.ExternalTransactionID,
			IdempotencyKey:                 "provider-a:transaction-123",
			RequestHash:                    h,
			WalletID:                       f.WalletID,
			PlayerID:                       f.PlayerID,
			RoundID:                        f.RoundID,
			GameID:                         f.GameID,
			Kind:                           f.Kind,
			Money:                          f.Money,
			ReferenceExternalTransactionID: f.ReferenceExternalTransactionID,
			Now:                            time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC),
		})
		if err != nil {
			t.Fatalf("%s: domain rejected hash %s: %v", kind, h, err)
		}
	}
}
