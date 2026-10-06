package app

import (
	"errors"
	"fmt"
)

// Erros tipados da aplicação (ARCHITECTURE.md, ADR-016).
//
// Os casos de uso devolvem *Error para as falhas que o chamador (HTTP, SQS,
// worker) precisa distinguir. A classe (Kind) é casável por errors.Is com as
// sentinelas ErrInvalidInput, ErrConflict, ErrNotFound e ErrUnavailable; o
// Code é um identificador estável para o contrato externo. A causa original
// segue na cadeia (Unwrap). Erros sem classe (bug, falha permanente de
// infraestrutura) saem sem *Error e devem ser tratados como erro interno.

// ErrorKind classifica um erro de aplicação.
type ErrorKind string

const (
	// KindInvalidInput: entrada inválida e corrigível pelo chamador (HTTP 400).
	KindInvalidInput ErrorKind = "INVALID_INPUT"
	// KindConflict: o estado atual impede a operação (HTTP 409).
	KindConflict ErrorKind = "CONFLICT"
	// KindNotFound: recurso inexistente (HTTP 404).
	KindNotFound ErrorKind = "NOT_FOUND"
	// KindUnavailable: falha transitória; pode ser repetida (HTTP 503).
	KindUnavailable ErrorKind = "UNAVAILABLE"
)

// Sentinelas por classe (errors.Is).
var (
	ErrInvalidInput = errors.New("app: invalid input")
	ErrConflict     = errors.New("app: conflict")
	ErrNotFound     = errors.New("app: not found")
	// ErrUnavailable também é usada pelas portas para marcar falhas
	// transitórias da infraestrutura (ver UnitOfWork).
	ErrUnavailable = errors.New("app: temporarily unavailable")
)

// Sentinelas das portas de saída. Os adapters embrulham seus erros com elas
// para que o caso de uso não dependa dos tipos do adapter.
var (
	// ErrWalletAlreadyExists: já existe carteira para (playerId, currency).
	// Também casa com ErrConflict.
	ErrWalletAlreadyExists = fmt.Errorf("%w: wallet already exists for player and currency", ErrConflict)
)

// Códigos estáveis dos erros de aplicação.
const (
	CodeWalletAlreadyExists    = "WALLET_ALREADY_EXISTS"
	CodeInvalidPlayerID        = "INVALID_PLAYER_ID"
	CodeInvalidInitialBalance  = "INVALID_INITIAL_BALANCE"
	CodeInvalidCorrelationID   = "INVALID_CORRELATION_ID"
	CodeTemporarilyUnavailable = "TEMPORARILY_UNAVAILABLE"
)

// Error é o erro tipado devolvido pelos casos de uso.
type Error struct {
	// Kind é a classe do erro.
	Kind ErrorKind
	// Code é o código estável (Code*).
	Code string
	// Op identifica o caso de uso (ex.: "open wallet").
	Op string
	// Err é a causa (pode ser nil).
	Err error
}

func (e *Error) Error() string {
	msg := fmt.Sprintf("app: %s: %s (%s)", e.Op, e.Code, e.Kind)
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

// Unwrap devolve a causa.
func (e *Error) Unwrap() error { return e.Err }

// Is casa o erro com a sentinela da sua classe.
func (e *Error) Is(target error) bool {
	return target != nil && target == e.Kind.sentinel()
}

func (k ErrorKind) sentinel() error {
	switch k {
	case KindInvalidInput:
		return ErrInvalidInput
	case KindConflict:
		return ErrConflict
	case KindNotFound:
		return ErrNotFound
	case KindUnavailable:
		return ErrUnavailable
	default:
		return nil
	}
}

// AsError extrai o *Error da cadeia.
func AsError(err error) (*Error, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}
