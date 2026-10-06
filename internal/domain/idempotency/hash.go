// Package idempotency calcula o hash canônico dos campos de negócio de uma
// operação de provedor (T08). A mesma função é usada pela entrada HTTP
// (POST /wagering/transactions) e pelo consumidor SQS (WagerTransactionRequested),
// de modo que a mesma operação produz o mesmo RequestHash nas duas origens.
//
// Decisões arquiteturais (ADR-010 em ARCHITECTURE.md):
//
//   - Entrada tipada com tipos do domínio (domain.Money, domain.WagerKind,
//     uuid.UUID). O hash nunca vê a string de valor recebida, só a forma
//     canônica de Money (Amount() com duas casas + Currency), portanto "25",
//     "25.5" e "25.50" são equivalentes exatamente como o parser de Money define
//     (ADR-004). UUIDs entram na forma canônica minúscula de uuid.UUID.String().
//   - Sem ponto flutuante: o valor monetário entra como string decimal.
//   - Campos excluídos: idempotencyKey e todo metadado de transporte (headers,
//     correlationId, messageId, eventId, occurredAt, receiptHandle, tokens). A
//     chave de idempotência é a identidade da requisição; o hash descreve o
//     conteúdo comparado com ela, logo não pode depender dela.
//   - Identificadores não são normalizados (sem trim nem mudança de caixa),
//     coerente com ADR-008: a mesma string entra no hash e nas chaves únicas.
//   - Campo ausente x vazio: todas as chaves estão sempre presentes no JSON
//     canônico. referenceExternalTransactionId vazio (ausente) é serializado
//     como null; o domínio não distingue "ausente" de "" (ambos significam "sem
//     referência"), então o transporte deve mapear omissão, null e "" para ""
//     antes de chamar esta função. Os demais campos são sempre serializados
//     como string, mesmo vazios (o domínio rejeita vazios depois).
//   - Versão do esquema: a chave "v" (HashSchemaVersion) faz parte do canônico.
//     Mudar campos, normalização ou formato exige incrementar a versão; hashes
//     persistidos com versão anterior deixam de casar (conflito explícito em
//     vez de colisão silenciosa). O teste golden falha se o algoritmo mudar sem
//     intenção.
//   - Formato canônico: objeto JSON compacto (sem espaços), chaves ordenadas
//     por bytes (inclusive no objeto aninhado money), strings escapadas no
//     estilo RFC 8785 (JCS): apenas '"', '\\' e caracteres de controle < 0x20
//     são escapados ('\b','\f','\n','\r','\t' na forma curta, os demais como
//     \u00xx minúsculo); o restante é UTF-8 literal. Strings com UTF-8
//     inválido são rejeitadas, porque encoders comuns as substituiriam por
//     U+FFFD e duas entradas diferentes poderiam colidir.
//   - Digest: SHA-256 do canônico, em 64 caracteres hex minúsculos, que é o
//     formato aceito pelo validador de RequestHash do domínio
//     (domain.RequestHashLength).
package idempotency

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// HashSchemaVersion é a versão do esquema canônico incluída na chave "v".
const HashSchemaVersion = 1

// ErrInvalidHashInput indica entrada que não pode ser canonizada sem
// ambiguidade (Money não inicializado ou string com UTF-8 inválido).
var ErrInvalidHashInput = errors.New("idempotency: invalid hash input")

// RequestFields são os campos de negócio de uma operação de provedor. Não há
// campo para idempotencyKey nem para metadados de transporte, por construção.
type RequestFields struct {
	// ProviderID vem da identidade autenticada.
	ProviderID            string
	ExternalTransactionID string
	PlayerID              uuid.UUID
	WalletID              uuid.UUID
	RoundID               string
	GameID                string
	Kind                  domain.WagerKind
	// Money já parseado (domain.ParseNonNegative): o hash usa a forma canônica.
	Money domain.Money
	// ReferenceExternalTransactionID vazio significa "sem referência" e é
	// serializado como null.
	ReferenceExternalTransactionID string
}

// RequestHash devolve o SHA-256 em hex minúsculo (64 caracteres) do JSON
// canônico de f. É a única função de hash de requisição usada por HTTP e SQS.
func RequestHash(f RequestFields) (string, error) {
	canonical, err := CanonicalJSON(f)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// CanonicalJSON devolve o JSON canônico de f (exposto para diagnóstico e
// testes golden). Formato, com chaves em ordem de bytes:
//
//	{"externalTransactionId":"...","gameId":"...","kind":"BET",
//	 "money":{"amount":"25.50","currency":"BRL"},"playerId":"<uuid>",
//	 "providerId":"...","referenceExternalTransactionId":null|"...",
//	 "roundId":"...","v":1,"walletId":"<uuid>"}
//
// (sem quebras de linha nem espaços na saída real).
func CanonicalJSON(f RequestFields) ([]byte, error) {
	if !f.Money.IsValid() {
		return nil, fmt.Errorf("%w: money: %w", ErrInvalidHashInput, domain.ErrUninitializedMoney)
	}

	var reference *string
	if f.ReferenceExternalTransactionID != "" {
		ref := f.ReferenceExternalTransactionID
		reference = &ref
	}

	// A ordem abaixo é a ordem de bytes das chaves; TestCanonicalKeysSorted
	// garante que ela continue ordenada se alguém adicionar campos.
	fields := []canonicalField{
		{key: "externalTransactionId", str: strPtr(f.ExternalTransactionID)},
		{key: "gameId", str: strPtr(f.GameID)},
		{key: "kind", str: strPtr(string(f.Kind))},
		{key: "money", object: []canonicalField{
			{key: "amount", str: strPtr(f.Money.Amount())},
			{key: "currency", str: strPtr(string(f.Money.Currency()))},
		}},
		{key: "playerId", str: strPtr(f.PlayerID.String())},
		{key: "providerId", str: strPtr(f.ProviderID)},
		{key: "referenceExternalTransactionId", str: reference, nullable: true},
		{key: "roundId", str: strPtr(f.RoundID)},
		{key: "v", integer: intPtr(HashSchemaVersion)},
		{key: "walletId", str: strPtr(f.WalletID.String())},
	}

	var b strings.Builder
	if err := writeObject(&b, fields); err != nil {
		return nil, err
	}
	return []byte(b.String()), nil
}

// canonicalField é um membro do objeto canônico. Exatamente um entre str,
// integer e object é usado; str nil com nullable=true vira null.
type canonicalField struct {
	key      string
	str      *string
	integer  *int
	object   []canonicalField
	nullable bool
}

func strPtr(s string) *string { return &s }
func intPtr(i int) *int       { return &i }

func writeObject(b *strings.Builder, fields []canonicalField) error {
	b.WriteByte('{')
	for i, f := range fields {
		if i > 0 {
			b.WriteByte(',')
		}
		if err := writeString(b, f.key, "key"); err != nil {
			return err
		}
		b.WriteByte(':')
		switch {
		case f.object != nil:
			if err := writeObject(b, f.object); err != nil {
				return err
			}
		case f.integer != nil:
			b.WriteString(strconv.Itoa(*f.integer))
		case f.str != nil:
			if err := writeString(b, *f.str, f.key); err != nil {
				return err
			}
		case f.nullable:
			b.WriteString("null")
		default:
			// Erro de programação: campo sem valor e não anulável.
			return fmt.Errorf("%w: field %q has no value", ErrInvalidHashInput, f.key)
		}
	}
	b.WriteByte('}')
	return nil
}

const hexDigits = "0123456789abcdef"

// writeString escreve s como string JSON no estilo RFC 8785.
func writeString(b *strings.Builder, s, field string) error {
	if !utf8.ValidString(s) {
		return fmt.Errorf("%w: %s is not valid UTF-8", ErrInvalidHashInput, field)
	}
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if c < 0x20 {
				b.WriteString(`\u00`)
				b.WriteByte(hexDigits[c>>4])
				b.WriteByte(hexDigits[c&0xf])
			} else {
				// Bytes >= 0x20 (inclusive multibyte UTF-8 já validado) são literais.
				b.WriteByte(c)
			}
		}
	}
	b.WriteByte('"')
	return nil
}
