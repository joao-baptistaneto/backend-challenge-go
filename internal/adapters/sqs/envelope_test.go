package sqs

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

const (
	testPlayer = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"
	testWallet = "0192f291-27dd-7d3f-8071-5f8685deef37"
)

// envelopeMap devolve o envelope do README (BET 25.00 BRL) como mapa
// mutável.
func envelopeMap(messageID, ext string) map[string]any {
	return map[string]any{
		"messageId":  messageID,
		"type":       "WagerTransactionRequested",
		"occurredAt": "2026-09-08T12:00:00.000Z",
		"data": map[string]any{
			"providerId":            "provider-a",
			"externalTransactionId": ext,
			"idempotencyKey":        "provider-a:" + ext,
			"playerId":              testPlayer,
			"walletId":              testWallet,
			"roundId":               "round-987",
			"gameId":                "fortune-chimp",
			"kind":                  "BET",
			"money":                 map[string]any{"amount": "25.00", "currency": "BRL"},
		},
	}
}

func marshal(t testing.TB, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func envelopeJSON(t testing.TB, messageID, ext string, mutate func(env, data map[string]any)) string {
	t.Helper()
	env := envelopeMap(messageID, ext)
	if mutate != nil {
		mutate(env, env["data"].(map[string]any))
	}
	return marshal(t, env)
}

func TestDecodeEnvelopeValid(t *testing.T) {
	body := envelopeJSON(t, "msg-123", "transaction-123", func(_, d map[string]any) {
		d["kind"] = "REFUND"
		d["referenceExternalTransactionId"] = "transaction-100"
	})
	env, err := DecodeEnvelope([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if env.MessageID != "msg-123" || env.Type != MessageTypeWagerTransactionRequested ||
		!env.OccurredAt.Equal(time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("envelope inesperado: %+v", env)
	}
	c := env.Command
	want, _ := domain.ParseNonNegative("25.00", "BRL")
	if c.ProviderID != "provider-a" || c.ExternalTransactionID != "transaction-123" ||
		c.IdempotencyKey != "provider-a:transaction-123" || c.PlayerID != uuid.MustParse(testPlayer) ||
		c.WalletID != uuid.MustParse(testWallet) || c.RoundID != "round-987" || c.GameID != "fortune-chimp" ||
		c.Kind != domain.WagerKindRefund || c.Money != want ||
		c.ReferenceExternalTransactionID != "transaction-100" {
		t.Fatalf("comando inesperado: %+v", c)
	}
	if c.Inbox != nil || c.CorrelationID != "" || c.CausationID != "" {
		t.Fatal("metadados de transporte são preenchidos pelo consumidor, não pelo decoder")
	}
}

func TestDecodeEnvelopeInvalid(t *testing.T) {
	const secret = "VALOR-SENSIVEL"
	cases := []struct {
		name      string
		body      string
		wantCode  string
		wantField string
	}{
		{"não é JSON", "not json", CodeInvalidMessage, ""},
		{"lista", "[]", CodeInvalidMessage, ""},
		{"vazio", "", CodeInvalidMessage, ""},
		{"utf8 inválido", "{\"messageId\":\"\xff\"}", CodeInvalidMessage, ""},
		{"grande demais", `{"messageId":"` + strings.Repeat("a", MaxBodyBytes) + `"}`, CodeInvalidMessage, ""},
		{"dados depois do objeto", envelopeJSON(t, "m", "e", nil) + "{}", CodeInvalidMessage, ""},
		{"chave desconhecida (não ecoada)", envelopeJSON(t, "m", "e", func(e, _ map[string]any) { e[secret] = 1 }), CodeInvalidMessage, ""},
		{"chave desconhecida em data", envelopeJSON(t, "m", "e", func(_, d map[string]any) { d[secret] = 1 }), CodeInvalidMessage, ""},
		{"caixa diferente", strings.Replace(envelopeJSON(t, "m", "e", nil), `"messageId"`, `"MessageId"`, 1), CodeInvalidMessage, ""},
		{"chave repetida", strings.Replace(envelopeJSON(t, "m", "e", nil), `"type":`, `"type":"x","type":`, 1), CodeInvalidMessage, "type"},
		{"sem messageId", envelopeJSON(t, "m", "e", func(e, _ map[string]any) { delete(e, "messageId") }), CodeInvalidMessage, "messageId"},
		{"messageId vazio", envelopeJSON(t, "", "e", nil), CodeInvalidMessage, "messageId"},
		{"messageId com espaço na ponta", envelopeJSON(t, " m", "e", nil), CodeInvalidMessage, "messageId"},
		{"messageId longo", envelopeJSON(t, strings.Repeat("m", 256), "e", nil), CodeInvalidMessage, "messageId"},
		{"messageId número", envelopeJSON(t, "m", "e", func(e, _ map[string]any) { e["messageId"] = 1 }), CodeInvalidMessage, "messageId"},
		{"tipo errado", envelopeJSON(t, "m", "e", func(e, _ map[string]any) { e["type"] = "WalletOpened" }), CodeUnsupportedMessageType, "type"},
		{"occurredAt inválido", envelopeJSON(t, "m", "e", func(e, _ map[string]any) { e["occurredAt"] = "ontem" }), CodeInvalidMessage, "occurredAt"},
		{"sem occurredAt", envelopeJSON(t, "m", "e", func(e, _ map[string]any) { delete(e, "occurredAt") }), CodeInvalidMessage, "occurredAt"},
		{"sem data", envelopeJSON(t, "m", "e", func(e, _ map[string]any) { delete(e, "data") }), CodeInvalidMessage, "data"},
		{"data null", envelopeJSON(t, "m", "e", func(e, _ map[string]any) { e["data"] = nil }), CodeInvalidMessage, "data"},
		{"OPENING rejeitado", envelopeJSON(t, "m", "e", func(_, d map[string]any) { d["kind"] = "OPENING" }), string(domain.FailureCodeInvalidKind), "data.kind"},
		{"kind desconhecido", envelopeJSON(t, "m", "e", func(_, d map[string]any) { d["kind"] = "bet" }), string(domain.FailureCodeInvalidKind), "data.kind"},
		{"money numérico", strings.Replace(envelopeJSON(t, "m", "e", nil), `"amount":"25.00"`, `"amount":25.00`, 1), string(domain.FailureCodeInvalidAmount), "data.money.amount"},
		{"money escala", envelopeJSON(t, "m", "e", func(_, d map[string]any) {
			d["money"] = map[string]any{"amount": "25.001", "currency": "BRL"}
		}), string(domain.FailureCodeInvalidAmount), "data.money.amount"},
		{"money notação científica", envelopeJSON(t, "m", "e", func(_, d map[string]any) {
			d["money"] = map[string]any{"amount": "2.5e1", "currency": "BRL"}
		}), string(domain.FailureCodeInvalidAmount), "data.money.amount"},
		{"money negativo", envelopeJSON(t, "m", "e", func(_, d map[string]any) {
			d["money"] = map[string]any{"amount": "-1.00", "currency": "BRL"}
		}), string(domain.FailureCodeInvalidAmount), "data.money.amount"},
		{"moeda inválida", envelopeJSON(t, "m", "e", func(_, d map[string]any) {
			d["money"] = map[string]any{"amount": "1.00", "currency": "XXX"}
		}), string(domain.FailureCodeInvalidCurrency), "data.money.currency"},
		{"sem money", envelopeJSON(t, "m", "e", func(_, d map[string]any) { delete(d, "money") }), CodeInvalidMessage, "data.money"},
		{"playerId não canônico", envelopeJSON(t, "m", "e", func(_, d map[string]any) { d["playerId"] = "{" + testPlayer + "}" }), CodeInvalidMessage, "data.playerId"},
		{"walletId nulo", envelopeJSON(t, "m", "e", func(_, d map[string]any) { d["walletId"] = uuid.Nil.String() }), CodeInvalidMessage, "data.walletId"},
		{"sem idempotencyKey", envelopeJSON(t, "m", "e", func(_, d map[string]any) { delete(d, "idempotencyKey") }), CodeInvalidMessage, "data.idempotencyKey"},
		{"idempotencyKey com controle", envelopeJSON(t, "m", "e", func(_, d map[string]any) { d["idempotencyKey"] = "a\nb" }), CodeInvalidMessage, "data.idempotencyKey"},
		{"providerId número", envelopeJSON(t, "m", "e", func(_, d map[string]any) { d["providerId"] = 7 }), CodeInvalidMessage, "data.providerId"},
		{"referência vazia", envelopeJSON(t, "m", "e", func(_, d map[string]any) { d["referenceExternalTransactionId"] = "" }), CodeInvalidMessage, "data.referenceExternalTransactionId"},
		{"referência null", envelopeJSON(t, "m", "e", func(_, d map[string]any) { d["referenceExternalTransactionId"] = nil }), CodeInvalidMessage, "data.referenceExternalTransactionId"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeEnvelope([]byte(tc.body))
			if !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("esperava ErrInvalidMessage, obteve %v", err)
			}
			var ime *InvalidMessageError
			if !errors.As(err, &ime) {
				t.Fatalf("esperava *InvalidMessageError: %T", err)
			}
			if ime.Code != tc.wantCode || ime.Field != tc.wantField {
				t.Fatalf("code/field = %q/%q, want %q/%q", ime.Code, ime.Field, tc.wantCode, tc.wantField)
			}
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("erro ecoou valor recebido: %v", err)
			}
		})
	}
}
