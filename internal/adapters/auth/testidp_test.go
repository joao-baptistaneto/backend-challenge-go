package auth

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
)

// testIdP é um IdP falso (httptest) que publica um JWKS com chaves RSA
// geradas no teste e o documento de discovery, e assina tokens.
type testIdP struct {
	t   *testing.T
	srv *httptest.Server

	mu      sync.Mutex
	keys    map[string]*rsa.PrivateKey // publicadas
	extra   []map[string]any           // JWKs extras publicados como estão
	disc    map[string]any             // discovery (nil → padrão)
	failing atomic.Bool                // JWKS/discovery respondem 500

	jwksFetches atomic.Int32
	discFetches atomic.Int32
}

const (
	testAudience = "wallet-api"
	testRealm    = "/realms/test"
)

func newTestIdP(t *testing.T) *testIdP {
	t.Helper()
	p := &testIdP{t: t, keys: map[string]*rsa.PrivateKey{}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+testRealm+"/protocol/openid-connect/certs", func(w http.ResponseWriter, _ *http.Request) {
		p.jwksFetches.Add(1)
		if p.failing.Load() {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(p.jwks())
	})
	mux.HandleFunc("GET "+testRealm+"/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		p.discFetches.Add(1)
		if p.failing.Load() {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		p.mu.Lock()
		doc := p.disc
		p.mu.Unlock()
		if doc == nil {
			doc = map[string]any{"issuer": p.issuer(), "jwks_uri": p.jwksURL()}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doc)
	})
	p.srv = httptest.NewServer(mux)
	t.Cleanup(p.srv.Close)
	return p
}

func (p *testIdP) issuer() string  { return p.srv.URL + testRealm }
func (p *testIdP) jwksURL() string { return p.issuer() + "/protocol/openid-connect/certs" }

func (p *testIdP) jwks() map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	var keys []any
	for kid, k := range p.keys {
		raw, err := json.Marshal(jose.JSONWebKey{Key: &k.PublicKey, KeyID: kid, Algorithm: "RS256", Use: "sig"})
		if err != nil {
			p.t.Errorf("marshal jwk: %v", err)
		}
		keys = append(keys, json.RawMessage(raw))
	}
	for _, e := range p.extra {
		keys = append(keys, e)
	}
	return map[string]any{"keys": keys}
}

// addKey gera e publica uma chave RSA com o kid.
func (p *testIdP) addKey(kid string) *rsa.PrivateKey {
	k := newRSAKey(p.t)
	p.mu.Lock()
	p.keys[kid] = k
	p.mu.Unlock()
	return k
}

func (p *testIdP) removeKey(kid string) {
	p.mu.Lock()
	delete(p.keys, kid)
	p.mu.Unlock()
}

func (p *testIdP) key(kid string) *rsa.PrivateKey {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.keys[kid]
}

// newRSAKey gera uma chave RSA 2048 nova.
func newRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa: %v", err)
	}
	return k
}

// claims padrão de um token de provedor válido emitido em now.
func (p *testIdP) providerClaims(now time.Time, providerID string) map[string]any {
	return map[string]any{
		"iss":         p.issuer(),
		"aud":         []string{testAudience, "account"},
		"sub":         "service-account-" + providerID,
		"azp":         providerID,
		"typ":         "Bearer",
		"iat":         now.Unix(),
		"nbf":         now.Unix(),
		"exp":         now.Add(5 * time.Minute).Unix(),
		"provider_id": providerID,
		"realm_access": map[string]any{
			"roles": []string{"wager-provider", "default-roles-test"},
		},
		"resource_access": map[string]any{
			"account": map[string]any{"roles": []string{"manage-account", "wallet-internal"}},
		},
	}
}

func (p *testIdP) internalClaims(now time.Time) map[string]any {
	c := p.providerClaims(now, "internal")
	delete(c, "provider_id")
	c["azp"] = "internal-service"
	c["realm_access"] = map[string]any{"roles": []string{"default-roles-test"}}
	c["resource_access"] = map[string]any{
		testAudience: map[string]any{"roles": []string{"wallet-internal"}},
	}
	return c
}

// sign assina claims com a chave publicada kid (RS256).
func (p *testIdP) sign(kid string, claims map[string]any) string {
	p.t.Helper()
	k := p.key(kid)
	if k == nil {
		p.t.Fatalf("kid %q não publicado", kid)
	}
	return signJWT(p.t, jose.RS256, k, kid, claims)
}

func signJWT(t *testing.T, alg jose.SignatureAlgorithm, key any, kid string, claims map[string]any) string {
	t.Helper()
	opts := (&jose.SignerOptions{}).WithType("JWT")
	if kid != "" {
		opts = opts.WithHeader(jose.HeaderKey("kid"), kid)
	}
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: alg, Key: key}, opts)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := signer.Sign(payload)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	s, err := obj.CompactSerialize()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// unsignedJWT monta um JWT com alg "none" (sem assinatura).
func unsignedJWT(t *testing.T, kid string, claims map[string]any) string {
	t.Helper()
	hdr, _ := json.Marshal(map[string]any{"alg": "none", "typ": "JWT", "kid": kid})
	payload, _ := json.Marshal(claims)
	enc := base64.RawURLEncoding
	return enc.EncodeToString(hdr) + "." + enc.EncodeToString(payload) + "."
}

// fakeClock é um relógio ajustável e seguro para uso concorrente.
type fakeClock struct{ ns atomic.Int64 }

func newFakeClock(t time.Time) *fakeClock {
	c := &fakeClock{}
	c.ns.Store(t.UnixNano())
	return c
}

func (c *fakeClock) Now() time.Time          { return time.Unix(0, c.ns.Load()) }
func (c *fakeClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

// syncBuffer captura logs de várias goroutines.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func testSettings(p *testIdP) Settings {
	return Settings{
		Issuer:        p.issuer(),
		Audience:      testAudience,
		JWKSURL:       p.jwksURL(),
		ProviderClaim: "provider_id",
		ProviderRole:  "wager-provider",
		InternalRole:  "wallet-internal",
		ClockSkew:     30 * time.Second,
	}
}

type authFixture struct {
	idp   *testIdP
	clock *fakeClock
	logs  *syncBuffer
	authn *Authenticator
	authz *Authorizer
}

func newFixture(t *testing.T, mutate func(*Settings), opts ...Option) *authFixture {
	t.Helper()
	idp := newTestIdP(t)
	idp.addKey("k1")
	clock := newFakeClock(time.Now().Truncate(time.Second))
	logs := &syncBuffer{}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	s := testSettings(idp)
	if mutate != nil {
		mutate(&s)
	}
	all := append([]Option{WithClock(clock.Now), WithHTTPClient(idp.srv.Client())}, opts...)
	authn, err := NewAuthenticator(s, logger, all...)
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	return &authFixture{idp: idp, clock: clock, logs: logs, authn: authn, authz: NewAuthorizer(s, logger)}
}
