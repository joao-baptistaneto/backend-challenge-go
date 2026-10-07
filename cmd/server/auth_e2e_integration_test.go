//go:build integration

package main

// Testes E2E de autenticação/autorização contra o Keycloak real (T27,
// ADR-029; README "Autenticação e autorização"). A aplicação completa
// (cmd/server.Options) sobe com a configuração OIDC de produção apontando
// para o realm importado do Docker Compose: OIDC_ISSUER_URL =
// {KEYCLOAK_URL}/realms/{realm}, JWKS por discovery, audiência wallet-api.
// Todos os tokens vêm do Keycloak por client_credentials; nenhum é assinado
// pelo teste. Cobertura:
//
//   - 401: sem Authorization, outro esquema, Bearer vazio/malformado,
//     cabeçalho duplicado, assinatura adulterada, payload adulterado,
//     alg=none, token de outro realm (issuer), audiência errada e token
//     expirado (client e2e-short-lived, vida de 5s no realm);
//   - 403: papel ausente (e2e-short-lived), provedor em rotas de carteira e
//     /metrics, serviço interno em rotas de provedor; 200/201 do serviço
//     interno nas rotas de carteira;
//   - isolamento entre provider-a e provider-b em consultas (por id e por
//     externalId), replays com o corpo/chave do outro, mesma
//     externalTransactionId em provedores diferentes e referência de
//     reversão que só existe no outro provedor;
//   - depois de cada lote de acessos negados, as contagens de wallets,
//     wager_transactions, wallet_ledger_entries, inbox e outbox (e a
//     impressão digital das linhas de carteira/transação) não mudam; no fim,
//     a reconciliação completa (assertConsistent).
//
// Variáveis: KEYCLOAK_URL (ausente → Skip), KEYCLOAK_REALM (junglegaming),
// KEYCLOAK_CLIENT_ID/KEYCLOAK_CLIENT_SECRET (serviço interno; defaults
// internal-service e o segredo de dev do realm), PROVIDER_A_SECRET,
// PROVIDER_B_SECRET, E2E_SHORT_LIVED_SECRET, E2E_OTHER_AUDIENCE_SECRET,
// KEYCLOAK_ADMIN_USER/KEYCLOAK_ADMIN_PASSWORD (token do realm master para o
// caso "outro issuer"; defaults admin/admin do Compose) e TEST_DATABASE_URL.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	httpserver "github.com/junglegaming/backend-challenge-go/internal/adapters/http/server"
	"github.com/junglegaming/backend-challenge-go/internal/adapters/sqs"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
	"github.com/junglegaming/backend-challenge-go/internal/platform/logging"
)

// Tolerância de relógio da aplicação nos testes E2E: curta, para que o teste
// de expiração espere pouco depois do exp real (o default é 30s).
const kcClockSkew = 2 * time.Second

// kcEnv descreve o Keycloak real usado pelos testes.
type kcEnv struct {
	base, realm, issuer string
	internalID          string
	secrets             map[string]string // clientId → secret
	adminUser, adminPW  string
}

func keycloakEnv(t *testing.T) kcEnv {
	t.Helper()
	base := strings.TrimSuffix(os.Getenv("KEYCLOAK_URL"), "/")
	if base == "" {
		t.Skip("KEYCLOAK_URL não definido: Keycloak indisponível " +
			"(docker compose up -d --wait postgres localstack keycloak)")
	}
	realm := envOr("KEYCLOAK_REALM", "junglegaming")
	internalID := envOr("KEYCLOAK_CLIENT_ID", "internal-service")
	return kcEnv{
		base: base, realm: realm, issuer: base + "/realms/" + realm, internalID: internalID,
		secrets: map[string]string{
			internalID:           envOr("KEYCLOAK_CLIENT_SECRET", "internal-service-dev-secret"),
			"provider-a":         envOr("PROVIDER_A_SECRET", "provider-a-dev-secret"),
			"provider-b":         envOr("PROVIDER_B_SECRET", "provider-b-dev-secret"),
			"e2e-short-lived":    envOr("E2E_SHORT_LIVED_SECRET", "e2e-short-lived-dev-secret"),
			"e2e-other-audience": envOr("E2E_OTHER_AUDIENCE_SECRET", "e2e-other-audience-dev-secret"),
		},
		adminUser: envOr("KEYCLOAK_ADMIN_USER", "admin"),
		adminPW:   envOr("KEYCLOAK_ADMIN_PASSWORD", "admin"),
	}
}

// token obtém um token de acesso real por client_credentials.
func (k kcEnv) token(t *testing.T, c *http.Client, clientID string) string {
	t.Helper()
	return clientCredentialsToken(t, c, k.issuer+"/protocol/openid-connect/token", clientID, k.secrets[clientID])
}

// masterToken obtém um token real do realm master (outro issuer, outras
// chaves), pelo admin-cli.
func (k kcEnv) masterToken(t *testing.T, c *http.Client) string {
	t.Helper()
	form := url.Values{"grant_type": {"password"}, "client_id": {"admin-cli"},
		"username": {k.adminUser}, "password": {k.adminPW}}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		k.base+"/realms/master/protocol/openid-connect/token", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("token master: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&out) != nil || out.AccessToken == "" {
		t.Fatalf("token master: status %d", resp.StatusCode)
	}
	return out.AccessToken
}

// kcHarness é a aplicação completa validando tokens do Keycloak real.
type kcHarness struct {
	*ccHarness
	kc      kcEnv
	tokenA  string // provider-a
	tokenB  string // provider-b
	logs    *syncBuffer
	secrets []string
}

func startKeycloakHarness(t *testing.T) *kcHarness {
	t.Helper()
	kc := keycloakEnv(t)
	dsn := migratedDSN(t)
	fake := &fakeSQS{}
	cfg := loadEnv(t, map[string]string{
		config.EnvAppEnv:                    config.EnvTest,
		config.EnvHTTPAddr:                  "127.0.0.1:0",
		config.EnvLogLevel:                  "info",
		config.EnvShutdownTimeout:           "5s",
		config.EnvDatabaseURL:               dsn,
		config.EnvDatabaseMaxConns:          "8",
		config.EnvAWSRegion:                 "us-east-1",
		config.EnvAWSAccessKeyID:            "test",
		config.EnvAWSSecretAccessKey:        "test",
		config.EnvAWSEndpointURL:            "http://127.0.0.1:1", // nunca usado: sqs.API é substituída
		config.EnvSQSInboundQueueURL:        fakeInbound,
		config.EnvSQSInboundDLQURL:          fakeDLQ,
		config.EnvSQSEventsQueueURL:         fakeEvents,
		config.EnvSQSWaitTime:               "1s",
		config.EnvSQSDrainTimeout:           "2s",
		config.EnvOIDCIssuerURL:             kc.issuer, // JWKS por discovery, como em produção
		config.EnvOIDCAudience:              envOr("OIDC_AUDIENCE", "wallet-api"),
		config.EnvOIDCClockSkew:             kcClockSkew.String(),
		config.EnvOutboxPollInterval:        "50ms",
		config.EnvOutboxDrainTimeout:        "2s",
		config.EnvPendingWorkerPollInterval: "50ms",
		config.EnvPendingWorkerDrainTimeout: "2s",
	})

	logs := &syncBuffer{}
	var srv *httpserver.Server
	a := newApp(cfg,
		logging.WithOutput(logs),
		fx.Decorate(func(sqs.API) sqs.API { return fake }),
		fx.Populate(&srv),
	)
	if err := a.Err(); err != nil {
		t.Fatalf("fx.New: %v", err)
	}
	startCtx, cancel := context.WithTimeout(context.Background(), a.StartTimeout())
	defer cancel()
	if err := a.Start(startCtx); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), a.StopTimeout())
		defer cancel()
		if err := a.Stop(ctx); err != nil {
			t.Errorf("stop: %v", err)
		}
	})

	verify, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(verify.Close)
	tr := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(tr.CloseIdleConnections)
	client := &http.Client{Transport: tr, Timeout: 30 * time.Second}

	h := &kcHarness{
		ccHarness: &ccHarness{t: t, base: "http://" + srv.Addr().String(), client: client, verify: verify},
		kc:        kc,
		logs:      logs,
	}
	h.internal = kc.token(t, client, kc.internalID)
	h.tokenA = kc.token(t, client, "provider-a")
	h.tokenB = kc.token(t, client, "provider-b")
	h.provider = h.tokenA // ccHarness.submit envia como provider-a
	for _, s := range kc.secrets {
		h.secrets = append(h.secrets, s)
	}
	h.secrets = append(h.secrets, h.internal, h.tokenA, h.tokenB)
	t.Cleanup(h.assertNoSecretsLogged)
	return h
}

// assertNoSecretsLogged: nem tokens nem segredos de client nos logs.
func (h *kcHarness) assertNoSecretsLogged() {
	out := h.logs.String()
	for _, s := range h.secrets {
		if s != "" && strings.Contains(out, s) {
			h.t.Errorf("credencial apareceu nos logs da aplicação")
			return
		}
	}
}

// rawCall envia a requisição com os cabeçalhos Authorization exatamente
// como dados (nil = sem cabeçalho). Devolve status, cabeçalhos e corpo.
func (h *kcHarness) rawCall(method, path string, authz []string, body string, hdr map[string]string) (int, http.Header, map[string]any) {
	h.t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(h.t.Context(), method, h.base+path, rd)
	if err != nil {
		h.t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	for _, a := range authz {
		req.Header.Add("Authorization", a)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		h.t.Fatal(err)
	}
	var out map[string]any
	if len(raw) > 0 && strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		if err := json.Unmarshal(raw, &out); err != nil {
			h.t.Fatalf("%s %s: JSON inválido %q: %v", method, path, raw, err)
		}
	}
	return resp.StatusCode, resp.Header, out
}

// --- Contagens (nenhum efeito de acessos negados) ----------------------------

type dbSnapshot struct {
	wallets, transactions, ledger, inbox, outbox int64
	// Impressões digitais das linhas: qualquer UPDATE em carteira/transação
	// também muda o snapshot.
	walletsHash, transactionsHash string
}

func (h *kcHarness) snapshot() dbSnapshot {
	h.t.Helper()
	var s dbSnapshot
	err := h.verify.QueryRow(h.t.Context(), `SELECT
 (SELECT count(*) FROM wallets),
 (SELECT count(*) FROM wager_transactions),
 (SELECT count(*) FROM wallet_ledger_entries),
 (SELECT count(*) FROM inbox),
 (SELECT count(*) FROM outbox),
 (SELECT coalesce(md5(string_agg(w::text, '|' ORDER BY w.id)), '') FROM wallets w),
 (SELECT coalesce(md5(string_agg(t::text, '|' ORDER BY t.id)), '') FROM wager_transactions t)`).
		Scan(&s.wallets, &s.transactions, &s.ledger, &s.inbox, &s.outbox, &s.walletsHash, &s.transactionsHash)
	if err != nil {
		h.t.Fatal(err)
	}
	return s
}

// expectUnchanged falha se algo foi criado ou alterado desde before.
func (h *kcHarness) expectUnchanged(what string, before dbSnapshot) {
	h.t.Helper()
	if after := h.snapshot(); after != before {
		h.t.Fatalf("%s: acessos negados alteraram o banco:\nantes  %+v\ndepois %+v", what, before, after)
	}
}

// waitQuiescent espera a outbox ser toda publicada (o publisher só faz UPDATE
// em outbox, que não entra no snapshot, mas assim nada corre em paralelo).
func (h *kcHarness) waitQuiescent() {
	h.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for h.scalar(`SELECT count(*) FROM outbox WHERE published_at IS NULL`) != 0 {
		if time.Now().After(deadline) {
			h.t.Fatal("outbox não foi publicada em 10s")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// --- Montagem de requisições ---------------------------------------------------

type kcRoute struct {
	method, path, body string
	hdr                map[string]string
}

func (r kcRoute) String() string { return r.method + " " + r.path }

// wagerBody monta o corpo de POST /wagering/transactions.
func wagerBody(provider, ext, kind string, w ccWallet, amount, ref string) string {
	d := map[string]any{
		"providerId": provider, "externalTransactionId": ext, "playerId": w.player,
		"walletId": w.id, "roundId": "round-" + ext, "gameId": "fortune-chimp", "kind": kind,
		"money": map[string]string{"amount": amount, "currency": "BRL"},
	}
	if ref != "" {
		d["referenceExternalTransactionId"] = ref
	}
	return mustJSON(d)
}

// protectedRoutes: todas as rotas protegidas, com corpos válidos (um bug que
// processasse antes de autenticar teria efeito visível no banco).
func protectedRoutes(w ccWallet, txID, ext string) []kcRoute {
	newExt := "denied-" + uuid.NewString()
	return []kcRoute{
		{method: http.MethodPost, path: "/wallets",
			body: fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"50.00","currency":"BRL"}}`, uuid.NewString())},
		{method: http.MethodGet, path: "/wallets/" + w.id},
		{method: http.MethodGet, path: "/wallets/" + w.id + "/ledger"},
		{method: http.MethodPost, path: "/wallets/" + w.id + "/reconciliation"},
		{method: http.MethodPost, path: "/wagering/transactions",
			body: wagerBody("provider-a", newExt, "BET", w, "1.00", ""),
			hdr:  map[string]string{"Idempotency-Key": "provider-a:" + newExt}},
		{method: http.MethodGet, path: "/wagering/transactions/" + txID},
		{method: http.MethodGet, path: "/providers/provider-a/wagering/transactions/" + ext},
		{method: http.MethodGet, path: "/metrics"},
	}
}

// --- Manipulação de JWT (sem reassinar: a assinatura continua a do Keycloak) --

func jwtParts(t *testing.T, tok string) [3]string {
	t.Helper()
	p := strings.Split(tok, ".")
	if len(p) != 3 {
		t.Fatalf("token do Keycloak não é um JWS compacto (%d partes)", len(p))
	}
	return [3]string{p[0], p[1], p[2]}
}

func jwtClaims(t *testing.T, tok string) map[string]any {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(jwtParts(t, tok)[1])
	if err != nil {
		t.Fatal(err)
	}
	var c map[string]any
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

func encodeSegment(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// tamperSignature troca um caractere do meio da assinatura.
func tamperSignature(t *testing.T, tok string) string {
	p := jwtParts(t, tok)
	sig := []byte(p[2])
	i := len(sig) / 2
	if sig[i] == 'A' {
		sig[i] = 'B'
	} else {
		sig[i] = 'A'
	}
	return p[0] + "." + p[1] + "." + string(sig)
}

// tamperClaims reescreve o payload (mantendo a assinatura original).
func tamperClaims(t *testing.T, tok string, set map[string]any) string {
	p := jwtParts(t, tok)
	c := jwtClaims(t, tok)
	for k, v := range set {
		c[k] = v
	}
	return p[0] + "." + encodeSegment(t, c) + "." + p[2]
}

// algNone troca o cabeçalho por alg=none e remove a assinatura.
func algNone(t *testing.T, tok string) string {
	p := jwtParts(t, tok)
	return encodeSegment(t, map[string]string{"alg": "none", "typ": "JWT"}) + "." + p[1] + "."
}

func expectAuthError(t *testing.T, what string, code int, hdr http.Header, body map[string]any, wantStatus int, wantCode string) {
	t.Helper()
	if code != wantStatus {
		t.Fatalf("%s: status %d %v, quer %d", what, code, body, wantStatus)
	}
	if body["code"] != wantCode {
		t.Fatalf("%s: code %v, quer %s", what, body["code"], wantCode)
	}
	for _, k := range []string{"transactionId", "referenceTransactionId", "id", "balance"} {
		if _, ok := body[k]; ok {
			t.Fatalf("%s: resposta de erro expõe %q: %v", what, k, body)
		}
	}
	if wantStatus == http.StatusUnauthorized && !strings.HasPrefix(hdr.Get("WWW-Authenticate"), `Bearer realm="wallet"`) {
		t.Fatalf("%s: WWW-Authenticate = %q", what, hdr.Get("WWW-Authenticate"))
	}
}

// --- Testes --------------------------------------------------------------------

// 401 para credenciais ausentes ou inválidas em todas as rotas protegidas,
// com tokens reais do Keycloak adulterados, de outro realm e de audiência
// errada; nenhum efeito no banco.
func TestIntegrationAuthE2EUnauthenticated(t *testing.T) {
	h := startKeycloakHarness(t)
	w := h.openWallet("100.00")
	op := bet(w, "auth-401-"+uuid.NewString(), "10.00")
	res, err := h.submit(op)
	if err != nil || res.code != http.StatusOK || res.status != "PROCESSED" {
		t.Fatalf("aposta de provider-a = %+v %v", res, err)
	}

	// Sanidade: os tokens reais têm o formato esperado do realm.
	if c := jwtClaims(t, h.tokenA); c["iss"] != h.kc.issuer || c["provider_id"] != "provider-a" {
		t.Fatalf("token de provider-a com claims inesperadas: iss=%v provider_id=%v", c["iss"], c["provider_id"])
	}
	other := h.kc.token(t, h.client, "e2e-other-audience")
	if c := jwtClaims(t, other); c["provider_id"] != "provider-a" || strings.Contains(fmt.Sprint(c["aud"]), "wallet-api") {
		t.Fatalf("token de e2e-other-audience deveria ter provider_id e aud sem wallet-api: aud=%v", c["aud"])
	}
	master := h.kc.masterToken(t, h.client)
	if c := jwtClaims(t, master); c["iss"] == h.kc.issuer {
		t.Fatalf("token do realm master com o issuer do realm da aplicação: %v", c["iss"])
	}
	h.secrets = append(h.secrets, other, master)

	creds := []struct {
		name  string
		authz []string
	}{
		{"sem Authorization", nil},
		{"Authorization vazio", []string{""}},
		{"esquema Basic", []string{"Basic " + base64.StdEncoding.EncodeToString([]byte("provider-a:provider-a-dev-secret"))}},
		{"esquema Bearer sem token", []string{"Bearer "}},
		{"token sem o esquema", []string{h.tokenA}},
		{"Bearer não-JWT", []string{"Bearer not-a-jwt"}},
		{"Bearer com espaço no token", []string{"Bearer " + h.tokenA + " extra"}},
		{"dois cabeçalhos Authorization", []string{"Bearer " + h.tokenA, "Bearer " + h.internal}},
		{"assinatura adulterada", []string{"Bearer " + tamperSignature(t, h.tokenA)}},
		{"payload adulterado (provider_id)", []string{"Bearer " + tamperClaims(t, h.tokenB, map[string]any{"provider_id": "provider-a"})}},
		{"payload adulterado (papel interno)", []string{"Bearer " + tamperClaims(t, h.tokenA,
			map[string]any{"realm_access": map[string]any{"roles": []string{"wallet-internal", "wager-provider"}}})}},
		{"payload adulterado (exp estendido)", []string{"Bearer " + tamperClaims(t, h.tokenA,
			map[string]any{"exp": time.Now().Add(24 * time.Hour).Unix()})}},
		{"alg none", []string{"Bearer " + algNone(t, h.tokenA)}},
		{"token de outro issuer (realm master)", []string{"Bearer " + master}},
		{"audiência errada (sem wallet-api)", []string{"Bearer " + other}},
	}

	h.waitQuiescent()
	before := h.snapshot()
	for _, c := range creds {
		for _, r := range protectedRoutes(w, res.txID, op.ext) {
			code, hdr, body := h.rawCall(r.method, r.path, c.authz, r.body, r.hdr)
			expectAuthError(t, c.name+" em "+r.String(), code, hdr, body, http.StatusUnauthorized, "UNAUTHENTICATED")
		}
	}
	h.expectUnchanged("401", before)

	// Os tokens reais intactos continuam valendo depois de tudo isso.
	if code, body, err := h.call(http.MethodGet, "/wagering/transactions/"+res.txID, h.tokenA, "", nil); err != nil || code != http.StatusOK {
		t.Fatalf("GET da própria transação com token válido = %d %v %v", code, body, err)
	}
	if code, body, err := h.call(http.MethodGet, "/wallets/"+w.id, h.internal, "", nil); err != nil || code != http.StatusOK {
		t.Fatalf("GET /wallets/{id} com token interno válido = %d %v %v", code, body, err)
	}
	h.expectWallet(w.id, 9000, 2, 1, 1)
	h.assertConsistent()
}

// Token real expirado: o client e2e-short-lived do realm emite tokens de 5s
// (sem papéis da aplicação). Antes de expirar, o token é autêntico (403 por
// falta de papel); depois do exp (+ tolerância), 401 "token expired".
func TestIntegrationAuthE2EExpiredToken(t *testing.T) {
	h := startKeycloakHarness(t)
	w := h.openWallet("100.00")
	op := bet(w, "auth-exp-"+uuid.NewString(), "10.00")
	res, err := h.submit(op)
	if err != nil || res.code != http.StatusOK {
		t.Fatalf("aposta de provider-a = %+v %v", res, err)
	}

	short := h.kc.token(t, h.client, "e2e-short-lived")
	h.secrets = append(h.secrets, short)
	c := jwtClaims(t, short)
	exp, _ := c["exp"].(float64)
	iat, _ := c["iat"].(float64)
	if exp == 0 || iat == 0 || exp-iat > 10 {
		t.Fatalf("e2e-short-lived: exp-iat = %v s; o realm importado deveria emitir tokens de 5s "+
			"(recrie o Keycloak: docker compose up -d --wait --force-recreate keycloak)", exp-iat)
	}
	expiresAt := time.Unix(int64(exp), 0)

	h.waitQuiescent()
	before := h.snapshot()

	// Ainda válido: autenticado, mas sem papel → 403 FORBIDDEN em tudo.
	for _, r := range protectedRoutes(w, res.txID, op.ext) {
		code, hdr, body := h.rawCall(r.method, r.path, []string{"Bearer " + short}, r.body, r.hdr)
		if time.Now().After(expiresAt) {
			t.Fatalf("o token expirou antes do fim da verificação de 403 (lento demais)")
		}
		expectAuthError(t, "papel ausente em "+r.String(), code, hdr, body, http.StatusForbidden, "FORBIDDEN")
		if !strings.Contains(hdr.Get("WWW-Authenticate"), `error="insufficient_scope"`) {
			t.Fatalf("papel ausente em %s: WWW-Authenticate = %q", r, hdr.Get("WWW-Authenticate"))
		}
	}
	h.expectUnchanged("403 (papel ausente)", before)

	// Espera o exp real passar além da tolerância de relógio da aplicação.
	time.Sleep(time.Until(expiresAt.Add(kcClockSkew + time.Second)))
	for _, r := range protectedRoutes(w, res.txID, op.ext) {
		code, hdr, body := h.rawCall(r.method, r.path, []string{"Bearer " + short}, r.body, r.hdr)
		expectAuthError(t, "token expirado em "+r.String(), code, hdr, body, http.StatusUnauthorized, "UNAUTHENTICATED")
		if !strings.Contains(hdr.Get("WWW-Authenticate"), `error_description="token expired"`) {
			t.Fatalf("token expirado em %s: WWW-Authenticate = %q", r, hdr.Get("WWW-Authenticate"))
		}
	}
	h.expectUnchanged("401 (expirado)", before)

	// Um token novo do mesmo client volta a ser aceito (403 de novo).
	fresh := h.kc.token(t, h.client, "e2e-short-lived")
	h.secrets = append(h.secrets, fresh)
	if code, hdr, body := h.rawCall(http.MethodGet, "/wallets/"+w.id, []string{"Bearer " + fresh}, "", nil); code != http.StatusForbidden {
		t.Fatalf("token novo de e2e-short-lived = %d %v %v", code, hdr, body)
	}
	h.assertConsistent()
}

// Papéis: provedores não acessam rotas de carteira nem /metrics; o serviço
// interno acessa as rotas de carteira mas não as de provedor.
func TestIntegrationAuthE2ERoles(t *testing.T) {
	h := startKeycloakHarness(t)

	// Serviço interno: acesso completo às rotas de carteira (e /metrics).
	player := uuid.NewString()
	code, body, err := h.call(http.MethodPost, "/wallets", h.internal,
		fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"100.00","currency":"BRL"}}`, player), nil)
	if err != nil || code != http.StatusCreated {
		t.Fatalf("POST /wallets (internal-service) = %d %v %v", code, body, err)
	}
	w := ccWallet{id: body["id"].(string), player: player}
	op := bet(w, "auth-roles-"+uuid.NewString(), "10.00")
	res, err := h.submit(op)
	if err != nil || res.code != http.StatusOK {
		t.Fatalf("aposta de provider-a = %+v %v", res, err)
	}
	for _, r := range []kcRoute{
		{method: http.MethodGet, path: "/wallets/" + w.id},
		{method: http.MethodGet, path: "/wallets/" + w.id + "/ledger"},
		{method: http.MethodPost, path: "/wallets/" + w.id + "/reconciliation"},
	} {
		code, _, body := h.rawCall(r.method, r.path, []string{"Bearer " + h.internal}, r.body, r.hdr)
		if code != http.StatusOK {
			t.Fatalf("internal-service em %s = %d %v", r, code, body)
		}
	}
	if code, _, _ := h.rawCall(http.MethodGet, "/metrics", []string{"Bearer " + h.internal}, "", nil); code != http.StatusOK {
		t.Fatalf("internal-service em /metrics = %d", code)
	}

	h.waitQuiescent()
	before := h.snapshot()
	routes := protectedRoutes(w, res.txID, op.ext)
	walletRoutes := []kcRoute{routes[0], routes[1], routes[2], routes[3], routes[7]}
	providerRoutes := []kcRoute{routes[4], routes[5], routes[6]}

	for _, p := range []struct{ name, token string }{{"provider-a", h.tokenA}, {"provider-b", h.tokenB}} {
		for _, r := range walletRoutes {
			code, hdr, body := h.rawCall(r.method, r.path, []string{"Bearer " + p.token}, r.body, r.hdr)
			expectAuthError(t, p.name+" em "+r.String(), code, hdr, body, http.StatusForbidden, "FORBIDDEN")
		}
	}
	for _, r := range providerRoutes {
		code, hdr, body := h.rawCall(r.method, r.path, []string{"Bearer " + h.internal}, r.body, r.hdr)
		expectAuthError(t, "internal-service em "+r.String(), code, hdr, body, http.StatusForbidden, "FORBIDDEN")
	}
	// Provedor tentando abrir carteira para um jogador novo.
	code, hdr, body := h.rawCall(http.MethodPost, "/wallets", []string{"Bearer " + h.tokenA},
		fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"1.00","currency":"BRL"}}`, uuid.NewString()), nil)
	expectAuthError(t, "provider-a abrindo carteira", code, hdr, body, http.StatusForbidden, "FORBIDDEN")
	h.expectUnchanged("403 (papéis)", before)

	h.expectWallet(w.id, 9000, 2, 1, 1)
	h.assertConsistent()
}

// Isolamento entre provedores em consultas, replays e referências.
func TestIntegrationAuthE2EProviderIsolation(t *testing.T) {
	h := startKeycloakHarness(t)
	wa := h.openWallet("100.00")
	wb := h.openWallet("100.00")
	ext := "shared-ext-" + uuid.NewString()
	keyA := "provider-a:" + ext
	bodyA := wagerBody("provider-a", ext, "BET", wa, "10.00", "")

	code, body, err := h.call(http.MethodPost, "/wagering/transactions", h.tokenA, bodyA, map[string]string{"Idempotency-Key": keyA})
	if err != nil || code != http.StatusOK || body["status"] != "PROCESSED" {
		t.Fatalf("aposta de provider-a = %d %v %v", code, body, err)
	}
	txA, _ := body["transactionId"].(string)

	h.waitQuiescent()
	before := h.snapshot()

	// 1) Consulta por id da transação de A, por B: 404 idêntico ao de um id
	// inexistente (não revela existência).
	code, hdr, notFound := h.rawCall(http.MethodGet, "/wagering/transactions/"+uuid.NewString(), []string{"Bearer " + h.tokenB}, "", nil)
	expectAuthError(t, "provider-b consultando id inexistente", code, hdr, notFound, http.StatusNotFound, "TRANSACTION_NOT_FOUND")
	code, hdr, body = h.rawCall(http.MethodGet, "/wagering/transactions/"+txA, []string{"Bearer " + h.tokenB}, "", nil)
	expectAuthError(t, "provider-b consultando a transação de provider-a", code, hdr, body, http.StatusNotFound, "TRANSACTION_NOT_FOUND")
	if body["message"] != notFound["message"] {
		t.Fatalf("404 distinguível de inexistente: %v vs %v", body, notFound)
	}

	// 2) Consulta por externalId no path de A, por B: 403 PROVIDER_MISMATCH.
	code, hdr, body = h.rawCall(http.MethodGet, "/providers/provider-a/wagering/transactions/"+ext, []string{"Bearer " + h.tokenB}, "", nil)
	expectAuthError(t, "provider-b no path de provider-a", code, hdr, body, http.StatusForbidden, "PROVIDER_MISMATCH")
	// ... e no próprio path, a mesma externalId não acha a de A.
	code, hdr, body = h.rawCall(http.MethodGet, "/providers/provider-b/wagering/transactions/"+ext, []string{"Bearer " + h.tokenB}, "", nil)
	expectAuthError(t, "provider-b com a externalId de provider-a", code, hdr, body, http.StatusNotFound, "TRANSACTION_NOT_FOUND")

	// 3) Replay do corpo e da chave exatos de A, por B: 403, sem devolver nada
	// da transação de A.
	code, hdr, body = h.rawCall(http.MethodPost, "/wagering/transactions", []string{"Bearer " + h.tokenB}, bodyA,
		map[string]string{"Idempotency-Key": keyA})
	expectAuthError(t, "replay de provider-b com o corpo de provider-a", code, hdr, body, http.StatusForbidden, "PROVIDER_MISMATCH")
	// O mesmo com corpo divergente (outro valor): continua 403, não 409.
	code, hdr, body = h.rawCall(http.MethodPost, "/wagering/transactions", []string{"Bearer " + h.tokenB},
		wagerBody("provider-a", ext, "BET", wa, "99.00", ""), map[string]string{"Idempotency-Key": keyA})
	expectAuthError(t, "replay divergente de provider-b", code, hdr, body, http.StatusForbidden, "PROVIDER_MISMATCH")
	h.expectUnchanged("isolamento (negados)", before)

	// 4) A mesma externalId e até a mesma chave literal, agora como
	// provider-b: é outra operação (idempotência por provedor). Cria a
	// transação de B, sem tocar na de A.
	fpA := h.scalar(`SELECT hashtext(t::text) FROM wager_transactions t WHERE id = $1`, txA)
	code, body, err = h.call(http.MethodPost, "/wagering/transactions", h.tokenB,
		wagerBody("provider-b", ext, "BET", wb, "10.00", ""), map[string]string{"Idempotency-Key": keyA})
	if err != nil || code != http.StatusOK || body["status"] != "PROCESSED" {
		t.Fatalf("aposta de provider-b com a mesma externalId = %d %v %v", code, body, err)
	}
	txB, _ := body["transactionId"].(string)
	if txB == "" || txB == txA || body["idempotentReplay"] == true {
		t.Fatalf("provider-b recebeu a transação de provider-a (ou replay): txA=%s resposta=%v", txA, body)
	}
	if got := h.scalar(`SELECT hashtext(t::text) FROM wager_transactions t WHERE id = $1`, txA); got != fpA {
		t.Fatal("a transação de provider-a mudou com a operação de provider-b")
	}
	// Cada um vê só a sua pela mesma externalId.
	for _, c := range []struct{ provider, token, want string }{{"provider-a", h.tokenA, txA}, {"provider-b", h.tokenB, txB}} {
		code, body, err := h.call(http.MethodGet, "/providers/"+c.provider+"/wagering/transactions/"+ext, c.token, "", nil)
		if err != nil || code != http.StatusOK || body["transactionId"] != c.want || body["providerId"] != c.provider {
			t.Fatalf("%s consultando %s = %d %v %v (quer %s)", c.provider, ext, code, body, err, c.want)
		}
	}
	// Replays legítimos devolvem cada um a sua.
	code, body, err = h.call(http.MethodPost, "/wagering/transactions", h.tokenA, bodyA, map[string]string{"Idempotency-Key": keyA})
	if err != nil || code != http.StatusOK || body["transactionId"] != txA || body["idempotentReplay"] != true {
		t.Fatalf("replay de provider-a = %d %v %v", code, body, err)
	}
	code, body, err = h.call(http.MethodPost, "/wagering/transactions", h.tokenB,
		wagerBody("provider-b", ext, "BET", wb, "10.00", ""), map[string]string{"Idempotency-Key": keyA})
	if err != nil || code != http.StatusOK || body["transactionId"] != txB || body["idempotentReplay"] != true {
		t.Fatalf("replay de provider-b = %d %v %v", code, body, err)
	}

	// 5) Reversão de B referenciando uma externalId que só A tem: nunca
	// resolve para a transação de A (fica aguardando a referência de B).
	onlyA := "only-a-" + uuid.NewString()
	code, body, err = h.call(http.MethodPost, "/wagering/transactions", h.tokenA,
		wagerBody("provider-a", onlyA, "BET", wa, "5.00", ""), map[string]string{"Idempotency-Key": "provider-a:" + onlyA})
	if err != nil || code != http.StatusOK {
		t.Fatalf("segunda aposta de provider-a = %d %v %v", code, body, err)
	}
	refund := "refund-b-" + uuid.NewString()
	code, body, err = h.call(http.MethodPost, "/wagering/transactions", h.tokenB,
		wagerBody("provider-b", refund, "REFUND", wa, "5.00", onlyA), map[string]string{"Idempotency-Key": "provider-b:" + refund})
	if err != nil {
		t.Fatal(err)
	}
	if body["status"] == "PROCESSED" || body["referenceTransactionId"] != nil {
		t.Fatalf("reversão de provider-b resolveu a referência de provider-a: %d %v", code, body)
	}
	if code != http.StatusAccepted || body["status"] != "PENDING_REFERENCE" {
		t.Fatalf("reversão de provider-b sem referência própria = %d %v, quer 202 PENDING_REFERENCE", code, body)
	}
	if n := h.scalar(`SELECT count(*) FROM wager_transactions WHERE provider_id = 'provider-b'
 AND external_transaction_id = $1 AND reference_transaction_id IS NOT NULL`, refund); n != 0 {
		t.Fatal("reversão de provider-b gravou referência à transação de provider-a")
	}

	// Carteira de A: só as duas apostas de A (100 − 10 − 5); a de B: só a de B.
	h.expectWallet(wa.id, 8500, 3, 1, 2)
	h.expectWallet(wb.id, 9000, 2, 1, 1)
	h.assertConsistent()
}
