package httpadapter_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/auth"
	httpadapter "github.com/junglegaming/backend-challenge-go/internal/adapters/http"
	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

// Tokens do autenticador falso (cada um mapeia para um Principal).
const (
	tokProviderA     = "tok-provider-a"
	tokProviderB     = "tok-provider-b"
	tokInternal      = "tok-internal"
	tokNoRole        = "tok-no-role"
	tokProviderNoID  = "tok-provider-no-id"
	providerRole     = "wager-provider"
	internalRole     = "wallet-internal"
	providerA        = "provider-a"
	providerB        = "provider-b"
	jsonContentType  = "application/json"
	correlationValue = "corr-test-1"
)

// newPrincipal monta o Principal como o Authenticator faria: papéis
// ordenados e sem repetição (Principal.HasRole usa busca binária).
func newPrincipal(client, providerID string, roles ...string) auth.Principal {
	r := slices.Clone(roles)
	slices.Sort(r)
	return auth.Principal{Subject: "sa-" + client, ClientID: client, ProviderID: providerID, Roles: slices.Compact(r)}
}

var principals = map[string]auth.Principal{
	// Papéis fora de ordem de propósito: newPrincipal ordena.
	tokProviderA:    newPrincipal(providerA, providerA, "z-other", providerRole, "default-roles"),
	tokProviderB:    newPrincipal(providerB, providerB, providerRole),
	tokInternal:     newPrincipal("internal-service", "", internalRole, "default-roles"),
	tokNoRole:       newPrincipal("nobody", "", "default-roles"),
	tokProviderNoID: newPrincipal("provider-x", "", providerRole),
}

// fakeAuthenticator reproduz o contrato de auth.Authenticator.Middleware
// (rotas públicas por path exato, 401 via auth.WriteError) sem JWT. A
// validação real de tokens é coberta em realauth_test.go e no pacote auth.
type fakeAuthenticator struct{}

func (fakeAuthenticator) Middleware(public ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if slices.Contains(public, r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}
			tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || tok == "" {
				auth.WriteError(w, r, auth.ErrMissingToken)
				return
			}
			p, known := principals[tok]
			if !known {
				auth.WriteError(w, r, auth.ErrInvalidToken)
				return
			}
			next.ServeHTTP(w, r.WithContext(auth.ContextWithPrincipal(r.Context(), p)))
		})
	}
}

func testAuthorizer() *auth.Authorizer {
	return auth.NewAuthorizer(auth.Settings{ProviderRole: providerRole, InternalRole: internalRole}, nil)
}

// env é a API completa sobre os casos de uso reais e a memUoW.
type env struct {
	t       *testing.T
	uow     *memUoW
	handler http.Handler
	metrics *metrics.Metrics
	reg     *prometheus.Registry
	logs    *bytes.Buffer
}

type envOption func(*httpadapter.Deps)

func newEnv(t *testing.T, opts ...envOption) *env {
	t.Helper()
	u := newMemUoW()
	reg, err := metrics.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	m, err := metrics.New(reg)
	if err != nil {
		t.Fatal(err)
	}
	ow, err := app.NewOpenWallet(u)
	if err != nil {
		t.Fatal(err)
	}
	pw, err := app.NewProcessWagerTransaction(u)
	if err != nil {
		t.Fatal(err)
	}
	rw, err := app.NewReconcileWallet(u, app.WithReconciliationRecorder(m))
	if err != nil {
		t.Fatal(err)
	}
	q, err := app.NewQueries(u)
	if err != nil {
		t.Fatal(err)
	}
	logs := &bytes.Buffer{}
	d := httpadapter.Deps{
		OpenWallet: ow, Process: pw, Reconcile: rw, Queries: q,
		Authenticator: fakeAuthenticator{}, Authorizer: testAuthorizer(),
		Metrics: m, MetricsHandler: metrics.Handler(reg),
		Logger: slog.New(slog.NewJSONHandler(&lockedWriter{w: logs}, nil)),
	}
	for _, o := range opts {
		o(&d)
	}
	h, err := httpadapter.NewHandler(d)
	if err != nil {
		t.Fatal(err)
	}
	return &env{t: t, uow: u, handler: h, metrics: m, reg: reg, logs: logs}
}

// lockedWriter serializa escritas (o log de acesso roda por requisição).
type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// request descreve uma chamada.
type request struct {
	method, path, token, body string
	headers                   map[string]string
	noContentType             bool
}

func (e *env) do(req request) *httptest.ResponseRecorder {
	e.t.Helper()
	var body io.Reader
	if req.body != "" {
		body = strings.NewReader(req.body)
	}
	r := httptest.NewRequest(req.method, req.path, body)
	if req.body != "" && !req.noContentType {
		r.Header.Set("Content-Type", jsonContentType)
	}
	if req.token != "" {
		r.Header.Set("Authorization", "Bearer "+req.token)
	}
	for k, v := range req.headers {
		r.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	e.handler.ServeHTTP(rec, r)
	return rec
}

// jsonBody decodifica o corpo com UseNumber (nenhum número vira float64).
func jsonBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(rec.Body.Bytes()))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("body is not a JSON object (%d): %q: %v", rec.Code, rec.Body.String(), err)
	}
	return m
}

func expectStatus(t *testing.T, rec *httptest.ResponseRecorder, want int) map[string]any {
	t.Helper()
	if rec.Code != want {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, want, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type = %q", ct)
	}
	return jsonBody(t, rec)
}

// expectError confere status e código do corpo de erro padrão.
func expectError(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) map[string]any {
	t.Helper()
	b := expectStatus(t, rec, status)
	if b["code"] != code {
		t.Fatalf("code = %v, want %s; body = %s", b["code"], code, rec.Body.String())
	}
	if msg, _ := b["message"].(string); msg == "" {
		t.Fatalf("error without message: %s", rec.Body.String())
	}
	return b
}

func moneyOf(t *testing.T, v any) string {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("money is not an object: %#v", v)
	}
	a, okA := m["amount"].(string)
	c, okC := m["currency"].(string)
	if !okA || !okC || len(m) != 2 {
		t.Fatalf("money must be {amount:string, currency:string}: %#v", m)
	}
	return a + " " + c
}

// openWallet abre uma carteira pela API e devolve (walletId, playerId).
func (e *env) openWallet(amount string) (uuid.UUID, uuid.UUID) {
	e.t.Helper()
	player := uuid.Must(uuid.NewV7())
	rec := e.do(request{method: http.MethodPost, path: "/wallets", token: tokInternal,
		body: fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":%q,"currency":"BRL"}}`, player, amount)})
	b := expectStatus(e.t, rec, http.StatusCreated)
	id, err := uuid.Parse(b["id"].(string))
	if err != nil {
		e.t.Fatal(err)
	}
	return id, player
}

// wager monta o corpo de POST /wagering/transactions.
type wager struct {
	provider, external, kind, amount, currency, reference string
	player, wallet                                        uuid.UUID
}

func (w wager) json() string {
	cur := w.currency
	if cur == "" {
		cur = "BRL"
	}
	ref := ""
	if w.reference != "" {
		ref = fmt.Sprintf(`,"referenceExternalTransactionId":%q`, w.reference)
	}
	return fmt.Sprintf(`{"providerId":%q,"externalTransactionId":%q,"playerId":%q,"walletId":%q,`+
		`"roundId":"round-1","gameId":"fortune-chimp","kind":%q,"money":{"amount":%q,"currency":%q}%s}`,
		w.provider, w.external, w.player, w.wallet, w.kind, w.amount, cur, ref)
}

func (e *env) submit(token, key string, w wager) *httptest.ResponseRecorder {
	e.t.Helper()
	h := map[string]string{}
	if key != "" {
		h["Idempotency-Key"] = key
	}
	return e.do(request{method: http.MethodPost, path: "/wagering/transactions", token: token, body: w.json(), headers: h})
}

// expectNoEffects confere que nada mudou no estado e que nenhuma unidade de
// trabalho foi aberta.
func (e *env) expectNoEffects(before snapshot, txBefore int) {
	e.t.Helper()
	after := e.uow.snapshot()
	if after.wallets != before.wallets || after.wagers != before.wagers ||
		after.ledger != before.ledger || after.events != before.events {
		e.t.Fatalf("effects detected: before=%+v after=%+v", before, after)
	}
	for id, b := range before.balances {
		if after.balances[id] != b || after.versions[id] != before.versions[id] {
			e.t.Fatalf("wallet %s changed: %s v%d -> %s v%d", id, b, before.versions[id], after.balances[id], after.versions[id])
		}
	}
	if got := e.uow.transactions(); got != txBefore {
		e.t.Fatalf("use case reached the database: %d -> %d units of work", txBefore, got)
	}
}

func httpRequest(t *testing.T, req request) *http.Request {
	t.Helper()
	r := httptest.NewRequest(req.method, req.path, strings.NewReader(req.body))
	if req.body != "" {
		r.Header.Set("Content-Type", jsonContentType)
	}
	if req.token != "" {
		r.Header.Set("Authorization", "Bearer "+req.token)
	}
	return r
}

func serve(e *env, r *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	e.handler.ServeHTTP(rec, r)
	return rec
}
