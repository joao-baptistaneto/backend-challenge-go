//go:build integration

package auth

// Integração com um Keycloak real (ADR-019). Obtém um token por
// client_credentials e o valida contra o JWKS real do realm (via discovery).
//
// Variáveis:
//
//	KEYCLOAK_URL            base do Keycloak (ex.: http://localhost:8081); ausente → t.Skip
//	KEYCLOAK_REALM          realm (default junglegaming)
//	KEYCLOAK_CLIENT_ID      client confidencial com service account (obrigatória se KEYCLOAK_URL)
//	KEYCLOAK_CLIENT_SECRET  segredo do client (obrigatória se KEYCLOAK_URL; nunca logado)
//	KEYCLOAK_ISSUER         issuer esperado, se diferente de {KEYCLOAK_URL}/realms/{realm}
//	OIDC_AUDIENCE           audiência exigida (default wallet-api)
//	KEYCLOAK_EXPECT_PROVIDER_ID  providerId esperado no token (opcional)
//	KEYCLOAK_EXPECT_ROLE         papel esperado no token (opcional)
//
// Nesta máquina não há Docker/Keycloak: o teste faz Skip. Ele é executado de
// verdade em T24/T27 com o Keycloak do Docker Compose (realm importado com os
// clients provider-a, provider-b e internal-service).

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestKeycloakClientCredentialsToken(t *testing.T) {
	base := strings.TrimSuffix(os.Getenv("KEYCLOAK_URL"), "/")
	if base == "" {
		t.Skip("KEYCLOAK_URL não definido: sem Keycloak disponível. Este teste roda em T24/T27 " +
			"com o Keycloak do Docker Compose (defina KEYCLOAK_URL, KEYCLOAK_REALM, " +
			"KEYCLOAK_CLIENT_ID e KEYCLOAK_CLIENT_SECRET)")
	}
	realm := envOr("KEYCLOAK_REALM", "junglegaming")
	clientID := os.Getenv("KEYCLOAK_CLIENT_ID")
	secret := os.Getenv("KEYCLOAK_CLIENT_SECRET")
	if clientID == "" || secret == "" {
		t.Fatal("KEYCLOAK_URL definido: KEYCLOAK_CLIENT_ID e KEYCLOAK_CLIENT_SECRET são obrigatórias")
	}
	issuer := envOr("KEYCLOAK_ISSUER", base+"/realms/"+realm)
	audience := envOr("OIDC_AUDIENCE", "wallet-api")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	token := clientCredentialsToken(ctx, t, base+"/realms/"+realm+"/protocol/openid-connect/token", clientID, secret)

	settings := Settings{
		Issuer:        issuer,
		Audience:      audience,
		JWKSURL:       "", // discovery real: {issuer}/.well-known/openid-configuration
		ProviderClaim: "provider_id",
		ProviderRole:  "wager-provider",
		InternalRole:  "wallet-internal",
		ClockSkew:     30 * time.Second,
	}
	authn, err := NewAuthenticator(settings, nil)
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}

	p, err := authn.Authenticate(ctx, token)
	if err != nil {
		t.Fatalf("token real recusado (reason %q): %v — confira o mapper de audiência %q e o issuer %q",
			Reason(err), err, audience, issuer)
	}
	if p.ClientID != clientID {
		t.Fatalf("azp = %q, quer %q", p.ClientID, clientID)
	}
	if want := os.Getenv("KEYCLOAK_EXPECT_PROVIDER_ID"); want != "" && p.ProviderID != want {
		t.Fatalf("providerId = %q, quer %q", p.ProviderID, want)
	}
	if want := os.Getenv("KEYCLOAK_EXPECT_ROLE"); want != "" && !p.HasRole(want) {
		t.Fatalf("papéis %v sem %q", p.Roles, want)
	}

	// Pelo middleware: 200 com token real, 401 sem token.
	h := authn.Middleware()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	for _, tc := range []struct {
		header string
		want   int
	}{{"Bearer " + token, http.StatusNoContent}, {"", http.StatusUnauthorized}} {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/x", nil)
		if tc.header != "" {
			req.Header.Set("Authorization", tc.header)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Fatalf("middleware: status %d, quer %d", rec.Code, tc.want)
		}
	}

	// Assinatura adulterada (troca de um caractere da assinatura) → inválido.
	tampered := token[:len(token)-2] + flip(token[len(token)-2]) + token[len(token)-1:]
	if _, err := authn.Authenticate(ctx, tampered); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("assinatura adulterada: %v", err)
	}

	// Audiência errada → inválido (o token real não é para outra API).
	other := settings
	other.Audience = "outra-api"
	otherAuthn, err := NewAuthenticator(other, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := otherAuthn.Authenticate(ctx, token); !errors.Is(err, ErrInvalidToken) || Reason(err) != ReasonClaims {
		t.Fatalf("audiência errada: %v", err)
	}

	// Issuer errado → inválido.
	wrongIss := settings
	wrongIss.Issuer = issuer + "-outro"
	wrongIss.JWKSURL = issuer + "/protocol/openid-connect/certs"
	wrongAuthn, err := NewAuthenticator(wrongIss, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrongAuthn.Authenticate(ctx, token); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("issuer errado: %v", err)
	}
}

func clientCredentialsToken(ctx context.Context, t *testing.T, tokenURL, clientID, secret string) string {
	t.Helper()
	form := url.Values{"grant_type": {"client_credentials"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(url.QueryEscape(clientID), url.QueryEscape(secret))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Keycloak indisponível em %s: %v", tokenURL, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		// O corpo de erro do Keycloak não contém o segredo.
		t.Fatalf("token endpoint: HTTP %d: %s", resp.StatusCode, body)
	}
	var out struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
	}
	if err := json.Unmarshal(body, &out); err != nil || out.AccessToken == "" {
		t.Fatalf("resposta de token inválida (err=%v)", err)
	}
	if !strings.EqualFold(out.TokenType, "Bearer") {
		t.Fatalf("token_type = %q", out.TokenType)
	}
	return out.AccessToken
}

func envOr(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

func flip(c byte) string {
	if c == 'A' {
		return "B"
	}
	return "A"
}
