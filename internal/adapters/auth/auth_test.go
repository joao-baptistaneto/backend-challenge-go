package auth

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/http/apierror"
	"github.com/junglegaming/backend-challenge-go/internal/platform/logging"
)

// serve monta middleware + handler que registra o principal recebido.
func (f *authFixture) serve(t *testing.T, path, token string, extraHeaders ...string) (*httptest.ResponseRecorder, *Principal) {
	t.Helper()
	var got *Principal
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := PrincipalFromContext(r.Context())
		if ok {
			got = &p
		}
		if ok && logging.IDsFromContext(r.Context()).ProviderID != p.ProviderID {
			t.Errorf("providerId não propagado para os IDs de log")
		}
		w.WriteHeader(http.StatusOK)
	})
	h := f.authn.Middleware("/health/live", "/health/ready")(next)
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for i := 0; i+1 < len(extraHeaders); i += 2 {
		req.Header.Add(extraHeaders[i], extraHeaders[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec, got
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) apierror.Body {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type = %q", ct)
	}
	var b apierror.Body
	dec := json.NewDecoder(rec.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&b); err != nil {
		t.Fatalf("corpo de erro inválido: %v", err)
	}
	return b
}

func assert401(t *testing.T, rec *httptest.ResponseRecorder, wantInvalidToken bool) {
	t.Helper()
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, quer 401; corpo %s", rec.Code, rec.Body.String())
	}
	www := rec.Header().Get("WWW-Authenticate")
	if !strings.HasPrefix(www, `Bearer realm="wallet"`) {
		t.Fatalf("WWW-Authenticate = %q", www)
	}
	if got := strings.Contains(www, `error="invalid_token"`); got != wantInvalidToken {
		t.Fatalf("WWW-Authenticate = %q (invalid_token esperado: %v)", www, wantInvalidToken)
	}
	if b := decodeBody(t, rec); b.Code != CodeUnauthenticated || b.Message == "" {
		t.Fatalf("corpo = %+v", b)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("erro de auth deveria ser no-store")
	}
}

func TestValidTokenPutsPrincipalInContext(t *testing.T) {
	f := newFixture(t, nil)
	tok := f.idp.sign("k1", f.idp.providerClaims(f.clock.Now(), "provider-a"))
	rec, p := f.serve(t, "/wagering/transactions/x", tok)
	if rec.Code != http.StatusOK || p == nil {
		t.Fatalf("status %d, principal %v, corpo %s", rec.Code, p, rec.Body.String())
	}
	want := Principal{
		Subject:    "service-account-provider-a",
		ClientID:   "provider-a",
		ProviderID: "provider-a",
		// realm_access + resource_access[wallet-api]; papéis de outro client
		// ("account": wallet-internal) não contam.
		Roles:     []string{"default-roles-test", "wager-provider"},
		ExpiresAt: f.clock.Now().Add(5 * time.Minute),
	}
	if p.Subject != want.Subject || p.ClientID != want.ClientID || p.ProviderID != want.ProviderID ||
		!slices.Equal(p.Roles, want.Roles) || !p.ExpiresAt.Equal(want.ExpiresAt) {
		t.Fatalf("principal = %+v, quer %+v", *p, want)
	}
	if !p.HasRole("wager-provider") || p.HasRole("wallet-internal") || p.HasRole("") {
		t.Fatal("HasRole inconsistente")
	}

	// Papel do serviço interno via resource_access[audience]; aud como string.
	c := f.idp.internalClaims(f.clock.Now())
	c["aud"] = testAudience
	rec, p = f.serve(t, "/wallets", f.idp.sign("k1", c))
	if rec.Code != http.StatusOK || p == nil || !p.HasRole("wallet-internal") || p.ProviderID != "" {
		t.Fatalf("interno: status %d principal %+v", rec.Code, p)
	}
}

func TestLeewayAcceptsSlightClockDifferences(t *testing.T) {
	f := newFixture(t, nil)
	now := f.clock.Now()
	c := f.idp.providerClaims(now, "provider-a")
	c["exp"] = now.Add(-20 * time.Second).Unix() // expirou há 20s < 30s
	c["nbf"] = now.Add(20 * time.Second).Unix()
	c["iat"] = now.Add(20 * time.Second).Unix()
	if rec, _ := f.serve(t, "/x", f.idp.sign("k1", c)); rec.Code != http.StatusOK {
		t.Fatalf("dentro da tolerância deveria passar: %d %s", rec.Code, rec.Body.String())
	}
}

func TestUnauthenticatedCases(t *testing.T) {
	f := newFixture(t, nil)
	now := f.clock.Now()
	valid := func() map[string]any { return f.idp.providerClaims(now, "provider-a") }
	with := func(k string, v any) string {
		c := valid()
		if v == nil {
			delete(c, k)
		} else {
			c[k] = v
		}
		return f.idp.sign("k1", c)
	}
	other := newRSAKey(t)
	pub, err := x509.MarshalPKIXPublicKey(&f.idp.key("k1").PublicKey)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name        string
		header      []string // Authorization (pode repetir)
		wantInvalid bool     // error="invalid_token"
		wantKind    error
		wantReason  string
	}{
		{"ausente", nil, false, ErrMissingToken, ReasonMissing},
		{"vazio", []string{""}, false, ErrMissingToken, ReasonMissing},
		{"esquema basic", []string{"Basic dXNlcjpwYXNz"}, false, ErrMissingToken, ReasonUnsupportedScheme},
		{"bearer sem token", []string{"Bearer"}, true, ErrInvalidToken, ReasonMalformed},
		{"bearer com espaços", []string{"Bearer a b"}, true, ErrInvalidToken, ReasonMalformed},
		{"dois cabeçalhos", []string{"Bearer " + with("x", 1), "Bearer " + with("x", 1)}, true, ErrInvalidToken, ReasonMultipleHeaders},
		{"malformado", []string{"Bearer abc.def.ghi"}, true, ErrInvalidToken, ReasonMalformed},
		{"não jwt", []string{"Bearer xyz"}, true, ErrInvalidToken, ReasonMalformed},
		{"grande demais", []string{"Bearer " + strings.Repeat("a", maxTokenBytes+1)}, true, ErrInvalidToken, ReasonTooLarge},
		{"assinatura inválida (outra chave, mesmo kid)", []string{"Bearer " + signJWT(t, jose.RS256, other, "k1", valid())}, true, ErrInvalidToken, ReasonSignature},
		{"assinatura adulterada", []string{"Bearer " + tamper(t, f.idp.sign("k1", valid()))}, true, ErrInvalidToken, ReasonSignature},
		{"kid desconhecido", []string{"Bearer " + signJWT(t, jose.RS256, other, "k-intruso", valid())}, true, ErrInvalidToken, ReasonUnknownKey},
		{"expirado", []string{"Bearer " + with("exp", now.Add(-2*time.Minute).Unix())}, true, ErrTokenExpired, ReasonExpired},
		{"sem exp", []string{"Bearer " + with("exp", nil)}, true, ErrInvalidToken, ReasonMissingExpiry},
		{"nbf futuro", []string{"Bearer " + with("nbf", now.Add(2*time.Minute).Unix())}, true, ErrInvalidToken, ReasonNotYetValid},
		{"nbf string", []string{"Bearer " + with("nbf", "amanhã")}, true, ErrInvalidToken, ReasonClaims},
		{"iat futuro", []string{"Bearer " + with("iat", now.Add(2*time.Minute).Unix())}, true, ErrInvalidToken, ReasonIssuedInFuture},
		{"aud errada", []string{"Bearer " + with("aud", "outra-api")}, true, ErrInvalidToken, ReasonClaims},
		{"sem aud", []string{"Bearer " + with("aud", nil)}, true, ErrInvalidToken, ReasonClaims},
		{"iss errado", []string{"Bearer " + with("iss", "http://evil.example/realms/test")}, true, ErrInvalidToken, ReasonClaims},
		{"iss com barra final", []string{"Bearer " + with("iss", f.idp.issuer()+"/")}, true, ErrInvalidToken, ReasonClaims},
		{"alg none", []string{"Bearer " + unsignedJWT(t, "k1", valid())}, true, ErrInvalidToken, ReasonMalformed},
		{"HS256 com a chave pública (confusão de algoritmo)", []string{"Bearer " + signJWT(t, jose.HS256, pub, "k1", valid())}, true, ErrInvalidToken, ReasonMalformed},
		{"typ ID token", []string{"Bearer " + with("typ", "ID")}, true, ErrInvalidToken, ReasonTokenType},
		{"typ refresh", []string{"Bearer " + with("typ", "Refresh")}, true, ErrInvalidToken, ReasonTokenType},
		{"provider_id não string", []string{"Bearer " + with("provider_id", 42)}, true, ErrInvalidToken, ReasonProviderClaim},
		{"provider_id vazio", []string{"Bearer " + with("provider_id", "")}, true, ErrInvalidToken, ReasonProviderClaim},
		{"provider_id com controle", []string{"Bearer " + with("provider_id", "a\nb")}, true, ErrInvalidToken, ReasonProviderClaim},
		{"realm_access malformado", []string{"Bearer " + with("realm_access", "admin")}, true, ErrInvalidToken, ReasonClaims},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var hdrs []string
			for _, h := range tc.header {
				hdrs = append(hdrs, "Authorization", h)
			}
			rec, p := f.serve(t, "/wallets/abc", "", hdrs...)
			assert401(t, rec, tc.wantInvalid)
			if p != nil {
				t.Fatal("handler não deveria ser chamado")
			}
			// Mesma classificação pela API direta.
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			for _, h := range tc.header {
				req.Header.Add("Authorization", h)
			}
			raw, err := bearerToken(req)
			if err == nil {
				_, err = f.authn.Authenticate(context.Background(), raw)
			}
			if !errors.Is(err, tc.wantKind) || Reason(err) != tc.wantReason {
				t.Fatalf("erro = %v (reason %q), quer %v/%s", err, Reason(err), tc.wantKind, tc.wantReason)
			}
			if tc.wantKind == ErrTokenExpired && !strings.Contains(rec.Header().Get("WWW-Authenticate"), "token expired") {
				t.Fatalf("WWW-Authenticate deveria indicar expiração: %q", rec.Header().Get("WWW-Authenticate"))
			}
			// Nada do token nos logs nem no corpo.
			for _, h := range tc.header {
				_, tok, _ := strings.Cut(h, " ")
				if len(tok) > 16 && (strings.Contains(f.logs.String(), tok) || strings.Contains(rec.Body.String(), tok)) {
					t.Fatal("token vazou em log ou resposta")
				}
				if len(tok) > 16 {
					// Nem a assinatura isolada.
					sig := tok[strings.LastIndex(tok, ".")+1:]
					if len(sig) > 16 && strings.Contains(f.logs.String(), sig) {
						t.Fatal("assinatura do token vazou nos logs")
					}
				}
			}
		})
	}
	if !strings.Contains(f.logs.String(), `"reason":"expired"`) {
		t.Fatalf("log deveria registrar a categoria: %s", f.logs.String())
	}
}

func tamper(t *testing.T, tok string) string {
	t.Helper()
	parts := strings.Split(tok, ".")
	var claims map[string]any
	raw, _ := jsonB64Decode(parts[1])
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatal(err)
	}
	claims["provider_id"] = "provider-b"
	b, _ := json.Marshal(claims)
	parts[1] = jsonB64Encode(b)
	return strings.Join(parts, ".")
}

func TestPublicRoutesSkipAuthentication(t *testing.T) {
	f := newFixture(t, nil)
	for _, path := range []string{"/health/live", "/health/ready"} {
		if rec, p := f.serve(t, path, ""); rec.Code != http.StatusOK || p != nil {
			t.Fatalf("%s sem token: %d", path, rec.Code)
		}
		// Cabeçalho inválido é ignorado em rota pública.
		if rec, _ := f.serve(t, path, "lixo"); rec.Code != http.StatusOK {
			t.Fatalf("%s com token inválido: %d", path, rec.Code)
		}
	}
	for _, path := range []string{"/health/live/", "/health", "/health/live/../../wallets", "/metrics"} {
		rec, _ := f.serve(t, path, "")
		assert401(t, rec, false)
	}
	if f.idp.jwksFetches.Load() != 0 {
		t.Fatal("rotas públicas não deveriam buscar o JWKS")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("rota pública sem / deveria causar panic")
		}
	}()
	f.authn.Middleware("health")
}

func TestKeyRotationRefreshesJWKS(t *testing.T) {
	f := newFixture(t, nil, WithJWKSRefreshInterval(10*time.Second))
	now := f.clock.Now
	ok := func(kid string) {
		t.Helper()
		if rec, _ := f.serve(t, "/x", f.idp.sign(kid, f.idp.providerClaims(now(), "provider-a"))); rec.Code != http.StatusOK {
			t.Fatalf("token %s: %d %s", kid, rec.Code, rec.Body.String())
		}
	}
	ok("k1")
	if n := f.idp.jwksFetches.Load(); n != 1 {
		t.Fatalf("buscas = %d, quer 1", n)
	}
	ok("k1") // cache
	if n := f.idp.jwksFetches.Load(); n != 1 {
		t.Fatalf("cache não usado: %d buscas", n)
	}

	// Rotação: o IdP publica k2 e retira k1. Token com kid k2 (desconhecido
	// no cache) força nova busca — passado o intervalo mínimo.
	k1 := f.idp.key("k1")
	f.idp.addKey("k2")
	f.idp.removeKey("k1")
	f.clock.Advance(11 * time.Second)
	ok("k2")
	if n := f.idp.jwksFetches.Load(); n != 2 {
		t.Fatalf("kid desconhecido deveria buscar o JWKS de novo: %d buscas", n)
	}
	// k1 saiu do JWKS renovado: tokens assinados com ela deixam de valer.
	rec, _ := f.serve(t, "/x", signJWT(t, jose.RS256, k1, "k1", f.idp.providerClaims(now(), "provider-a")))
	assert401(t, rec, true)

	// Dentro do intervalo mínimo, kid desconhecido não gera nova busca
	// (proteção contra tokens com kid aleatório): 401 sem tocar o IdP.
	f.idp.addKey("k3")
	tok3 := f.idp.sign("k3", f.idp.providerClaims(now(), "provider-a"))
	for range 5 {
		rec, _ := f.serve(t, "/x", tok3)
		assert401(t, rec, true)
	}
	if n := f.idp.jwksFetches.Load(); n != 2 {
		t.Fatalf("busca não limitada: %d", n)
	}
	f.clock.Advance(11 * time.Second)
	ok("k3")
	if n := f.idp.jwksFetches.Load(); n != 3 {
		t.Fatalf("buscas = %d, quer 3", n)
	}
}

func TestRevokedKeyRejectedAfterMaxAge(t *testing.T) {
	f := newFixture(t, nil)
	f.idp.addKey("k2")
	claims := func() map[string]any { return f.idp.providerClaims(f.clock.Now(), "provider-a") }
	tok1 := f.idp.sign("k1", claims())
	if rec, _ := f.serve(t, "/x", tok1); rec.Code != http.StatusOK {
		t.Fatalf("k1: %d", rec.Code)
	}
	k1 := f.idp.key("k1")
	f.idp.removeKey("k1")
	f.clock.Advance(defaultKeysMaxAge + time.Second)
	tok1 = signJWT(t, jose.RS256, k1, "k1", claims())
	rec, _ := f.serve(t, "/x", tok1)
	assert401(t, rec, true)
	if f.idp.jwksFetches.Load() != 2 {
		t.Fatalf("cache vencido deveria ser renovado: %d", f.idp.jwksFetches.Load())
	}
}

func TestIdPUnavailableReturns503(t *testing.T) {
	f := newFixture(t, nil, WithJWKSRefreshInterval(10*time.Second))
	tok := func() string { return f.idp.sign("k1", f.idp.providerClaims(f.clock.Now(), "provider-a")) }

	f.idp.failing.Store(true)
	rec, _ := f.serve(t, "/x", tok())
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("status %d, Retry-After %q", rec.Code, rec.Header().Get("Retry-After"))
	}
	if b := decodeBody(t, rec); b.Code != CodeTemporarilyUnavailable {
		t.Fatalf("corpo = %+v", b)
	}
	if !strings.Contains(f.logs.String(), "chaves do IdP indisponíveis") {
		t.Fatalf("falha do IdP deveria ser logada em WARN: %s", f.logs.String())
	}
	// Dentro do intervalo mínimo não há nova tentativa, mas segue 503.
	rec, _ = f.serve(t, "/x", tok())
	if rec.Code != http.StatusServiceUnavailable || f.idp.jwksFetches.Load() != 1 {
		t.Fatalf("status %d, buscas %d", rec.Code, f.idp.jwksFetches.Load())
	}

	// IdP volta: após o intervalo, o token é aceito.
	f.idp.failing.Store(false)
	f.clock.Advance(11 * time.Second)
	if rec, _ := f.serve(t, "/x", tok()); rec.Code != http.StatusOK {
		t.Fatalf("após recuperação: %d", rec.Code)
	}

	// Chaves vencidas + IdP fora do ar: as chaves antigas continuam valendo.
	f.idp.failing.Store(true)
	f.clock.Advance(defaultKeysMaxAge + time.Second)
	if rec, _ := f.serve(t, "/x", tok()); rec.Code != http.StatusOK {
		t.Fatalf("chaves antigas deveriam valer com o IdP fora: %d", rec.Code)
	}
	// ... mas um kid desconhecido nessa situação é 503, não 401.
	f.idp.addKey("k9")
	f.clock.Advance(11 * time.Second)
	rec, _ = f.serve(t, "/x", f.idp.sign("k9", f.idp.providerClaims(f.clock.Now(), "provider-a")))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("kid desconhecido com IdP fora: %d", rec.Code)
	}
}

func TestDiscoveryWhenJWKSURLIsEmpty(t *testing.T) {
	f := newFixture(t, func(s *Settings) { s.JWKSURL = "" })
	tok := f.idp.sign("k1", f.idp.providerClaims(f.clock.Now(), "provider-a"))
	for range 3 {
		if rec, _ := f.serve(t, "/x", tok); rec.Code != http.StatusOK {
			t.Fatalf("discovery: %d %s", rec.Code, rec.Body.String())
		}
	}
	if f.idp.discFetches.Load() != 1 || f.idp.jwksFetches.Load() != 1 {
		t.Fatalf("discovery %d, jwks %d", f.idp.discFetches.Load(), f.idp.jwksFetches.Load())
	}

	// Discovery com issuer diferente do configurado é recusado (503: não há
	// chaves confiáveis).
	g := newFixture(t, func(s *Settings) { s.JWKSURL = "" })
	g.idp.mu.Lock()
	g.idp.disc = map[string]any{"issuer": "http://evil.example/realms/test", "jwks_uri": g.idp.jwksURL()}
	g.idp.mu.Unlock()
	rec, _ := g.serve(t, "/x", g.idp.sign("k1", g.idp.providerClaims(g.clock.Now(), "provider-a")))
	if rec.Code != http.StatusServiceUnavailable || g.idp.jwksFetches.Load() != 0 {
		t.Fatalf("issuer divergente no discovery: %d (jwks %d)", rec.Code, g.idp.jwksFetches.Load())
	}
}

func TestJWKSIgnoresUnusableKeys(t *testing.T) {
	f := newFixture(t, nil)
	// Chave simétrica com o mesmo kid, chave de cifragem e chave de tipo
	// desconhecido: nenhuma pode ser usada nem quebrar o parse do JWKS.
	f.idp.mu.Lock()
	f.idp.extra = []map[string]any{
		{"kty": "oct", "kid": "k1", "k": jsonB64Encode([]byte("segredo-simetrico-de-32-bytes!!!")), "alg": "HS256"},
		{"kty": "RSA", "kid": "enc", "use": "enc", "alg": "RSA-OAEP", "n": "AQAB", "e": "AQAB"},
		{"kty": "XYZ", "kid": "weird"},
	}
	f.idp.mu.Unlock()
	if rec, _ := f.serve(t, "/x", f.idp.sign("k1", f.idp.providerClaims(f.clock.Now(), "provider-a"))); rec.Code != http.StatusOK {
		t.Fatalf("token válido: %d", rec.Code)
	}
	hs := signJWT(t, jose.HS256, []byte("segredo-simetrico-de-32-bytes!!!"), "k1", f.idp.providerClaims(f.clock.Now(), "provider-a"))
	rec, _ := f.serve(t, "/x", hs)
	assert401(t, rec, true)
}

func TestConcurrentColdCacheFetchesOnce(t *testing.T) {
	f := newFixture(t, nil)
	tok := f.idp.sign("k1", f.idp.providerClaims(f.clock.Now(), "provider-a"))
	h := f.authn.Middleware()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	var wg sync.WaitGroup
	errs := make(chan int, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/x", nil)
			req.Header.Set("Authorization", "Bearer "+tok)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusNoContent {
				errs <- rec.Code
			}
		}()
	}
	wg.Wait()
	close(errs)
	for code := range errs {
		t.Fatalf("requisição concorrente falhou: %d", code)
	}
	if n := f.idp.jwksFetches.Load(); n != 1 {
		t.Fatalf("buscas concorrentes = %d, quer 1", n)
	}
}

func TestAuthorizer(t *testing.T) {
	f := newFixture(t, nil)
	now := f.clock.Now()
	provider := f.idp.sign("k1", f.idp.providerClaims(now, "provider-a"))
	internal := f.idp.sign("k1", f.idp.internalClaims(now))
	noProviderID := func() string {
		c := f.idp.providerClaims(now, "provider-a")
		delete(c, "provider_id")
		return f.idp.sign("k1", c)
	}()
	noRoles := func() string {
		c := f.idp.providerClaims(now, "provider-a")
		delete(c, "realm_access")
		return f.idp.sign("k1", c)
	}()

	type route struct {
		name string
		h    http.Handler
	}
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	match := func(requested string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id, err := f.authz.MatchProvider(r.Context(), requested)
			if err != nil {
				WriteError(w, r, err)
				return
			}
			_, _ = w.Write([]byte(id))
		})
	}
	wallet := f.authz.RequireInternal(ok)
	tx := f.authz.RequireProvider(ok)
	mw := f.authn.Middleware()

	cases := []struct {
		name     string
		h        http.Handler
		token    string
		wantCode int
		wantBody string // code do corpo de erro
	}{
		{"carteira com serviço interno", wallet, internal, 200, ""},
		{"carteira com provedor", wallet, provider, 403, CodeForbidden},
		{"carteira sem papéis", wallet, noRoles, 403, CodeForbidden},
		{"transação com provedor", tx, provider, 200, ""},
		{"transação com serviço interno", tx, internal, 403, CodeForbidden},
		{"transação com provedor sem providerId", tx, noProviderID, 403, CodeForbidden},
		{"transação sem papel de provedor", tx, noRoles, 403, CodeForbidden},
		{"providerId do corpo igual", match("provider-a"), provider, 200, ""},
		{"providerId do corpo divergente", match("provider-b"), provider, 403, CodeProviderMismatch},
		{"providerId diferindo só na caixa", match("Provider-A"), provider, 403, CodeProviderMismatch},
		{"providerId vazio", match(""), provider, 403, CodeProviderMismatch},
		{"match com serviço interno", match("provider-a"), internal, 403, CodeForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/r", nil)
			req.Header.Set("Authorization", "Bearer "+tc.token)
			rec := httptest.NewRecorder()
			mw(tc.h).ServeHTTP(rec, req)
			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, quer %d (%s)", rec.Code, tc.wantCode, rec.Body.String())
			}
			if tc.wantCode == 200 {
				return
			}
			if b := decodeBody(t, rec); b.Code != tc.wantBody {
				t.Fatalf("code = %s, quer %s", b.Code, tc.wantBody)
			}
			www := rec.Header().Get("WWW-Authenticate")
			if tc.wantBody == CodeForbidden && !strings.Contains(www, `error="insufficient_scope"`) {
				t.Fatalf("WWW-Authenticate = %q", www)
			}
		})
	}
	if rec := httptest.NewRecorder(); true {
		req := httptest.NewRequest(http.MethodGet, "/r", nil)
		req.Header.Set("Authorization", "Bearer "+provider)
		mw(match("provider-a")).ServeHTTP(rec, req)
		if rec.Body.String() != "provider-a" {
			t.Fatalf("MatchProvider deveria devolver o providerId da identidade: %q", rec.Body.String())
		}
	}

	// Sem principal no contexto (middleware ausente por engano): 401.
	if err := f.authz.Internal(context.Background()); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("Internal sem principal: %v", err)
	}
	if _, err := f.authz.Provider(context.Background()); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("Provider sem principal: %v", err)
	}
	rec := httptest.NewRecorder()
	f.authz.RequireInternal(ok).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/r", nil))
	assert401(t, rec, true)

	// Negação registrada só com categoria.
	if !strings.Contains(f.logs.String(), `"reason":"provider_mismatch"`) || strings.Contains(f.logs.String(), provider) {
		t.Fatalf("log de negação inesperado: %s", f.logs.String())
	}
}

func TestWriteErrorMapping(t *testing.T) {
	cases := []struct {
		err  error
		code int
		body string
	}{
		{newError(ErrMissingToken, ReasonMissing), 401, CodeUnauthenticated},
		{newError(ErrTokenExpired, ReasonExpired), 401, CodeUnauthenticated},
		{newError(ErrInvalidToken, ReasonSignature), 401, CodeUnauthenticated},
		{newError(ErrMissingRole, ReasonMissingRole), 403, CodeForbidden},
		{newError(ErrProviderMismatch, ReasonProviderMismatch), 403, CodeProviderMismatch},
		{&Error{Kind: ErrUnavailable, Reason: ReasonKeysUnavailable, Err: errors.New("x")}, 503, CodeTemporarilyUnavailable},
		{errors.New("qualquer"), 500, CodeInternalError},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req = req.WithContext(logging.WithCorrelationID(req.Context(), "corr-1"))
		rec := httptest.NewRecorder()
		WriteError(rec, req, tc.err)
		if rec.Code != tc.code {
			t.Fatalf("%v: status %d, quer %d", tc.err, rec.Code, tc.code)
		}
		b := decodeBody(t, rec)
		if b.Code != tc.body || b.CorrelationID != "corr-1" || b.Message == "" {
			t.Fatalf("%v: corpo %+v", tc.err, b)
		}
	}
	e := &Error{Kind: ErrUnavailable, Reason: ReasonKeysUnavailable, Err: errors.New("dial")}
	if !errors.Is(e, ErrUnavailable) || !strings.Contains(e.Error(), "dial") || Reason(errors.New("x")) != "" {
		t.Fatal("Error/Reason inconsistentes")
	}
}

func TestSettingsValidate(t *testing.T) {
	base := Settings{
		Issuer: "http://kc:8080/realms/r", Audience: "wallet-api", ProviderClaim: "provider_id",
		ProviderRole: "wager-provider", InternalRole: "wallet-internal", ClockSkew: 30 * time.Second,
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("válida: %v", err)
	}
	if base.rolesClient() != "wallet-api" {
		t.Fatal("rolesClient default deveria ser a audience")
	}
	bad := []func(*Settings){
		func(s *Settings) { s.Issuer = "" },
		func(s *Settings) { s.Issuer = "kc/realms/r" },
		func(s *Settings) { s.Issuer = "http://u:p@kc/realms/r" },
		func(s *Settings) { s.Audience = "" },
		func(s *Settings) { s.JWKSURL = "ftp://x/certs" },
		func(s *Settings) { s.ProviderClaim = "" },
		func(s *Settings) { s.ProviderClaim = "aud" },
		func(s *Settings) { s.ProviderRole = "a b" },
		func(s *Settings) { s.InternalRole = "" },
		func(s *Settings) { s.InternalRole = s.ProviderRole },
		func(s *Settings) { s.ClockSkew = -time.Second },
		func(s *Settings) { s.ClockSkew = 10 * time.Minute },
	}
	for i, mutate := range bad {
		s := base
		mutate(&s)
		if err := s.Validate(); err == nil {
			t.Fatalf("caso %d: esperava erro", i)
		}
		if _, err := NewAuthenticator(s, nil); err == nil {
			t.Fatalf("caso %d: NewAuthenticator deveria falhar", i)
		}
	}
}

func jsonB64Encode(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func jsonB64Decode(s string) ([]byte, error) { return base64.RawURLEncoding.DecodeString(s) }
