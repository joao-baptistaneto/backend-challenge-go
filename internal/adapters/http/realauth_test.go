package httpadapter_test

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/auth"
	httpadapter "github.com/junglegaming/backend-challenge-go/internal/adapters/http"
)

// TestRealAuthenticator monta a API com o auth.Authenticator real (JWT RS256
// validado contra um JWKS servido por httptest) para provar a integração do
// middleware com as rotas: 401 sem token/expirado/assinatura errada, rotas
// públicas e o providerId vindo da claim do token. A integração com o
// Keycloak real fica em auth/keycloak_integration_test.go (ADR-026/ADR-029).
func TestRealAuthenticator(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	other, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	jwks := jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: "k1", Algorithm: "RS256", Use: "sig"}}}
	idp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jwks)
	}))
	t.Cleanup(idp.Close)

	issuer := idp.URL + "/realms/test"
	authn, err := auth.NewAuthenticator(auth.Settings{
		Issuer: issuer, Audience: "wallet-api", JWKSURL: idp.URL + "/certs",
		ProviderClaim: "provider_id", ProviderRole: providerRole, InternalRole: internalRole,
		ClockSkew: 5 * time.Second,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	e := newEnv(t, func(d *httpadapter.Deps) { d.Authenticator = authn })

	sign := func(k *rsa.PrivateKey, claims map[string]any) string {
		t.Helper()
		signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: k},
			(&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", "k1"))
		if err != nil {
			t.Fatal(err)
		}
		payload, _ := json.Marshal(claims)
		obj, err := signer.Sign(payload)
		if err != nil {
			t.Fatal(err)
		}
		s, err := obj.CompactSerialize()
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	now := time.Now()
	claims := func(exp time.Time, provider string, roles ...string) map[string]any {
		c := map[string]any{
			"iss": issuer, "aud": "wallet-api", "sub": "sa", "azp": "client", "typ": "Bearer",
			"iat": now.Unix(), "exp": exp.Unix(),
			"realm_access": map[string]any{"roles": roles},
		}
		if provider != "" {
			c["provider_id"] = provider
		}
		return c
	}
	internalTok := sign(key, claims(now.Add(time.Minute), "", internalRole))
	providerTok := sign(key, claims(now.Add(time.Minute), providerB, "z", providerRole, "a"))
	expired := sign(key, claims(now.Add(-time.Hour), "", internalRole))
	forged := sign(other, claims(now.Add(time.Minute), "", internalRole))

	walletID := uuid.Must(uuid.NewV7())
	get := func(tok, path string) *httptest.ResponseRecorder {
		return e.do(request{method: http.MethodGet, path: path, token: tok})
	}
	path := "/wallets/" + walletID.String()

	// Os tokens do autenticador falso não valem aqui.
	expectError(t, get(tokInternal, path), http.StatusUnauthorized, "UNAUTHENTICATED")
	expectError(t, get("", path), http.StatusUnauthorized, "UNAUTHENTICATED")
	rec := get(expired, path)
	expectError(t, rec, http.StatusUnauthorized, "UNAUTHENTICATED")
	if h := rec.Header().Get("WWW-Authenticate"); h == "" {
		t.Fatal("expired token without WWW-Authenticate")
	}
	expectError(t, get(forged, path), http.StatusUnauthorized, "UNAUTHENTICATED")
	expectError(t, get(providerTok, path), http.StatusForbidden, "FORBIDDEN")
	expectStatus(t, get("", "/health/live"), http.StatusOK)

	// Token interno real: abre e consulta.
	rec = e.do(request{method: http.MethodPost, path: "/wallets", token: internalTok,
		body: `{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"5.00","currency":"BRL"}}`})
	created := expectStatus(t, rec, http.StatusCreated)
	expectStatus(t, get(internalTok, "/wallets/"+created["id"].(string)), http.StatusOK)

	// providerId vem da claim: o provider-b só vê o próprio espaço.
	expectError(t, get(providerTok, "/providers/provider-a/wagering/transactions/x"), http.StatusForbidden, "PROVIDER_MISMATCH")
	expectError(t, get(providerTok, "/providers/provider-b/wagering/transactions/x"), http.StatusNotFound, "TRANSACTION_NOT_FOUND")
}
