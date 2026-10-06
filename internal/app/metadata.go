package app

import (
	"errors"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// validateEventMetadata valida correlationId (obrigatório) e causationId
// (opcional) ANTES de abrir a transação, com as mesmas regras dos
// construtores de eventos (domain.EventMetadata.Validate). Assim o formato é
// rejeitado como entrada mesmo quando o resultado não gera evento (abertura
// com saldo zero), e cada campo tem o seu código (ADR-017).
func validateEventMetadata(op, correlationID, causationID string) (domain.EventMetadata, error) {
	if correlationID == "" {
		return domain.EventMetadata{}, &Error{Kind: KindInvalidInput, Code: CodeInvalidCorrelationID, Op: op,
			Err: errors.New("correlationId is required")}
	}
	if err := (domain.EventMetadata{CorrelationID: correlationID}).Validate(); err != nil {
		return domain.EventMetadata{}, &Error{Kind: KindInvalidInput, Code: CodeInvalidCorrelationID, Op: op, Err: err}
	}
	meta := domain.EventMetadata{CorrelationID: correlationID, CausationID: causationID}
	if causationID != "" {
		if err := meta.Validate(); err != nil {
			return domain.EventMetadata{}, &Error{Kind: KindInvalidInput, Code: CodeInvalidCausationID, Op: op, Err: err}
		}
	}
	return meta, nil
}
