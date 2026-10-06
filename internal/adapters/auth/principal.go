package auth

import (
	"context"
	"slices"
	"time"
)

// Principal é a identidade autenticada extraída de um token válido.
type Principal struct {
	// Subject é a claim sub (no client_credentials do Keycloak, o usuário de
	// serviço do client).
	Subject string
	// ClientID é a claim azp (client que obteve o token).
	ClientID string
	// ProviderID vem da claim configurada (Settings.ProviderClaim); vazio se
	// o token não a tiver.
	ProviderID string
	// Roles é a união ordenada e sem repetição de realm_access.roles e
	// resource_access[RolesClient].roles.
	Roles []string
	// ExpiresAt é a claim exp.
	ExpiresAt time.Time
}

// HasRole informa se o principal tem o papel.
func (p Principal) HasRole(role string) bool {
	_, found := slices.BinarySearch(p.Roles, role)
	return role != "" && found
}

type principalKey struct{}

// ContextWithPrincipal devolve ctx com o principal (usado pelo middleware e
// por testes de handlers).
func ContextWithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// PrincipalFromContext devolve o principal autenticado, se houver.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	if ctx == nil {
		return Principal{}, false
	}
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}
