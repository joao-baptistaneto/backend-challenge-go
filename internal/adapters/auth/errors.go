package auth

import (
	"errors"
	"fmt"
)

// Sentinelas (errors.Is). A classe decide a resposta HTTP (WriteError):
// ErrUnauthenticated → 401, ErrForbidden → 403, ErrUnavailable → 503.
var (
	// ErrUnauthenticated: sem identidade válida (401).
	ErrUnauthenticated = errors.New("auth: unauthenticated")
	// ErrMissingToken: nenhum token Bearer na requisição.
	ErrMissingToken = fmt.Errorf("%w: missing bearer token", ErrUnauthenticated)
	// ErrInvalidToken: token malformado, com assinatura/alg/iss/aud/nbf/typ
	// inválidos ou claims malformadas.
	ErrInvalidToken = fmt.Errorf("%w: invalid token", ErrUnauthenticated)
	// ErrTokenExpired: exp no passado (além da tolerância).
	ErrTokenExpired = fmt.Errorf("%w: token expired", ErrUnauthenticated)

	// ErrForbidden: identidade válida sem permissão (403).
	ErrForbidden = errors.New("auth: forbidden")
	// ErrMissingRole: falta o papel exigido (ou o providerId da identidade).
	ErrMissingRole = fmt.Errorf("%w: missing role", ErrForbidden)
	// ErrProviderMismatch: o providerId pedido (corpo, path) difere do da
	// identidade.
	ErrProviderMismatch = fmt.Errorf("%w: provider mismatch", ErrForbidden)

	// ErrUnavailable: não foi possível obter as chaves do IdP (JWKS ou
	// discovery fora do ar); transitório (503).
	ErrUnavailable = errors.New("auth: identity provider keys unavailable")
)

// Error carrega a classe (sentinela acima) e uma categoria (reason) para
// logs e testes. Reason é um identificador fixo (ex.: "signature",
// "expired"), nunca contém o token nem valores de claims.
type Error struct {
	Kind   error
	Reason string
	// Err é a causa técnica (só em ErrUnavailable, para log em WARN). Não é
	// exposta ao cliente.
	Err error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%v (%s): %v", e.Kind, e.Reason, e.Err)
	}
	return fmt.Sprintf("%v (%s)", e.Kind, e.Reason)
}

// Unwrap expõe a classe e a causa.
func (e *Error) Unwrap() []error {
	if e.Err != nil {
		return []error{e.Kind, e.Err}
	}
	return []error{e.Kind}
}

func newError(kind error, reason string) *Error { return &Error{Kind: kind, Reason: reason} }

// Reason devolve a categoria de um erro deste pacote ("" se não houver).
func Reason(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Reason
	}
	return ""
}

// Categorias (reason) estáveis.
const (
	ReasonMissing           = "missing"
	ReasonUnsupportedScheme = "unsupported_scheme"
	ReasonMultipleHeaders   = "multiple_authorization_headers"
	ReasonMalformed         = "malformed"
	ReasonTooLarge          = "too_large"
	ReasonSignature         = "signature"
	ReasonUnknownKey        = "unknown_key"
	ReasonClaims            = "claims" // iss/aud ou claims malformadas
	ReasonExpired           = "expired"
	ReasonMissingExpiry     = "missing_exp"
	ReasonNotYetValid       = "not_yet_valid"
	ReasonIssuedInFuture    = "issued_in_future"
	ReasonTokenType         = "token_type"
	ReasonProviderClaim     = "provider_claim"
	ReasonKeysUnavailable   = "keys_unavailable"
	ReasonNoPrincipal       = "no_principal"
	ReasonMissingRole       = "missing_role"
	ReasonMissingProviderID = "missing_provider_id"
	ReasonProviderMismatch  = "provider_mismatch"
)
