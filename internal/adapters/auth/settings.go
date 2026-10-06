package auth

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
)

// maxClockSkew espelha o limite de OIDC_CLOCK_SKEW em config.
const maxClockSkew = 2 * time.Minute

// Settings são os parâmetros de autenticação e autorização (ADR-019). Vêm de
// config.OIDCConfig (variáveis OIDC_*), já validada no start; Validate repete
// as regras essenciais para quem constrói Settings diretamente.
type Settings struct {
	// Issuer é comparado exatamente com a claim iss.
	Issuer string
	// Audience precisa constar da claim aud.
	Audience string
	// JWKSURL é opcional; vazio → descoberto no documento
	// {Issuer}/.well-known/openid-configuration no primeiro uso.
	JWKSURL string
	// ProviderClaim é a claim (string) com o providerId autorizado.
	ProviderClaim string
	// ProviderRole e InternalRole são os papéis de provedor e de serviço
	// interno, procurados em realm_access.roles e
	// resource_access[RolesClient].roles.
	ProviderRole string
	InternalRole string
	// RolesClient é o client de resource_access considerado (vazio =
	// Audience).
	RolesClient string
	// ClockSkew é a tolerância de relógio para exp/nbf/iat.
	ClockSkew time.Duration
}

// SettingsFromConfig converte a configuração validada do processo.
func SettingsFromConfig(c config.OIDCConfig) Settings {
	return Settings{
		Issuer:        c.IssuerURL,
		Audience:      c.Audience,
		JWKSURL:       c.JWKSURL,
		ProviderClaim: c.ProviderClaim,
		ProviderRole:  c.ProviderRole,
		InternalRole:  c.InternalRole,
		RolesClient:   c.RolesClient,
		ClockSkew:     c.ClockSkew,
	}
}

// Validate devolve todos os problemas de uma vez; o erro casa com
// config.ErrInvalidConfig.
func (s Settings) Validate() error {
	var errs []error
	fail := func(field, msg string) { errs = append(errs, fmt.Errorf("auth %s: %s", field, msg)) }

	if !validHTTPURL(s.Issuer) {
		fail("issuer", "URL http(s) absoluta obrigatória, sem credenciais")
	}
	if s.Audience == "" {
		fail("audience", "obrigatória")
	}
	if s.JWKSURL != "" && !validHTTPURL(s.JWKSURL) {
		fail("jwksURL", "URL http(s) absoluta inválida")
	}
	if !config.IsSimpleName(s.ProviderClaim) || config.IsReservedClaim(s.ProviderClaim) {
		fail("providerClaim", "nome de claim inválido ou reservado")
	}
	if !config.IsSimpleName(s.ProviderRole) {
		fail("providerRole", "nome de papel inválido")
	}
	if !config.IsSimpleName(s.InternalRole) {
		fail("internalRole", "nome de papel inválido")
	}
	if s.ProviderRole != "" && s.ProviderRole == s.InternalRole {
		fail("internalRole", "deve ser diferente de providerRole")
	}
	if s.ClockSkew < 0 || s.ClockSkew > maxClockSkew {
		fail("clockSkew", fmt.Sprintf("deve estar entre 0s e %s", maxClockSkew))
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %w", config.ErrInvalidConfig, errors.Join(errs...))
}

func (s Settings) rolesClient() string {
	if s.RolesClient != "" {
		return s.RolesClient
	}
	return s.Audience
}

func validHTTPURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != "" && u.User == nil
}
