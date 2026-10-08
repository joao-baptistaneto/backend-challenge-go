package httpadapter_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/auth"
	httpadapter "github.com/junglegaming/backend-challenge-go/internal/adapters/http"
	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/platform/logging"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

// providerIDAuthenticator reproduz, sobre o fakeAuthenticator, o que o
// auth.Authenticator real faz com o contexto: põe o providerId da identidade
// nos IDs de log (logging.WithProviderID).
type providerIDAuthenticator struct{ fakeAuthenticator }

func (a providerIDAuthenticator) Middleware(public ...string) func(http.Handler) http.Handler {
	inner := a.fakeAuthenticator.Middleware(public...)
	return func(next http.Handler) http.Handler {
		return inner(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if p, ok := auth.PrincipalFromContext(r.Context()); ok && p.ProviderID != "" {
				r = r.WithContext(logging.WithProviderID(r.Context(), p.ProviderID))
			}
			next.ServeHTTP(w, r)
		}))
	}
}

// TestLogsHaveNoDuplicateKeys percorre os caminhos reais (middleware HTTP que
// põe correlationId/providerId no contexto + casos de uso que também os logam
// explicitamente) com o logger de produção e exige que toda linha seja um
// objeto JSON sem chaves repetidas em nenhum nível.
func TestLogsHaveNoDuplicateKeys(t *testing.T) {
	logs := &bytes.Buffer{}
	logger := logging.New(&lockedWriter{w: logs}, logging.Options{})

	u := newMemUoW()
	reg, err := metrics.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	m, err := metrics.New(reg)
	if err != nil {
		t.Fatal(err)
	}
	ow, err := app.NewOpenWallet(u, app.WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	pw, err := app.NewProcessWagerTransaction(u, app.WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	rw, err := app.NewReconcileWallet(u, app.WithLogger(logger), app.WithReconciliationRecorder(m))
	if err != nil {
		t.Fatal(err)
	}
	q, err := app.NewQueries(u, app.WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	h, err := httpadapter.NewHandler(httpadapter.Deps{
		OpenWallet: ow, Process: pw, Reconcile: rw, Queries: q,
		Authenticator: providerIDAuthenticator{}, Authorizer: testAuthorizer(),
		Metrics: m, MetricsHandler: metrics.Handler(reg), Logger: logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, uow: u, handler: h, metrics: m, reg: reg, logs: logs}

	walletID, player := e.openWallet("100.00")
	// Abertura recusada (carteira já existe).
	expectError(t, e.do(request{method: http.MethodPost, path: "/wallets", token: tokInternal,
		body: fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"0","currency":"BRL"}}`, player)}),
		http.StatusConflict, "WALLET_ALREADY_EXISTS")
	bet := wager{provider: providerA, external: "b1", kind: "BET", amount: "30.00", player: player, wallet: walletID}
	expectStatus(t, e.submit(tokProviderA, "k1", bet), http.StatusOK)
	expectStatus(t, e.submit(tokProviderA, "k1", bet), http.StatusOK) // replay
	expectStatus(t, e.submit(tokProviderA, "k2", wager{provider: providerA, external: "b2", kind: "BET",
		amount: "500.00", player: player, wallet: walletID}), http.StatusUnprocessableEntity)
	expectStatus(t, e.submit(tokProviderA, "k3", wager{provider: providerA, external: "w1", kind: "WIN",
		amount: "5.00", reference: "bet-later", player: player, wallet: walletID}), http.StatusAccepted)
	expectError(t, e.submit(tokProviderB, "k4", bet), http.StatusForbidden, "PROVIDER_MISMATCH")
	expectStatus(t, e.do(request{method: http.MethodPost, path: "/wallets/" + walletID.String() + "/reconciliation",
		token: tokInternal}), http.StatusOK)
	expectError(t, e.do(request{method: http.MethodGet, path: "/wallets/" + uuid.Must(uuid.NewV7()).String(),
		token: tokInternal}), http.StatusNotFound, "WALLET_NOT_FOUND")

	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	var withBoth int
	for _, line := range lines {
		keys, err := strictTopLevelKeys(line)
		if err != nil {
			t.Errorf("%v: %s", err, line)
			continue
		}
		if keys["correlationId"] && keys["providerId"] {
			withBoth++
		}
	}
	// Sanidade: os caminhos que antes duplicavam (caso de uso logando
	// correlationId/providerId explicitamente sob contexto HTTP) foram
	// exercitados.
	if withBoth < 3 {
		t.Fatalf("esperava ao menos 3 linhas com correlationId e providerId, obteve %d:\n%s", withBoth, logs.String())
	}
	for _, want := range []string{"carteira aberta", "abertura recusada", "operação processada"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log %q não emitido:\n%s", want, logs.String())
		}
	}
}

// strictTopLevelKeys valida line como um único objeto JSON sem chaves
// duplicadas em nenhum objeto (encoding/json aceita duplicatas em silêncio,
// por isso a varredura por tokens) e devolve as chaves da raiz.
func strictTopLevelKeys(line string) (map[string]bool, error) {
	dec := json.NewDecoder(strings.NewReader(line))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, errors.New("linha não é um objeto JSON")
	}
	keys, err := scanObject(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("conteúdo após o objeto")
	}
	return keys, nil
}

// scanObject consome um objeto já aberto e falha em chave repetida.
func scanObject(dec *json.Decoder) (map[string]bool, error) {
	keys := map[string]bool{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		k := tok.(string)
		if keys[k] {
			return nil, fmt.Errorf("chave duplicada %q", k)
		}
		keys[k] = true
		if err := scanValue(dec); err != nil {
			return nil, err
		}
	}
	_, err := dec.Token() // '}'
	return keys, err
}

func scanValue(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	d, ok := tok.(json.Delim)
	if !ok {
		return nil
	}
	switch d {
	case '{':
		_, err = scanObject(dec)
		return err
	case '[':
		for dec.More() {
			if err := scanValue(dec); err != nil {
				return err
			}
		}
		_, err = dec.Token() // ']'
		return err
	}
	return fmt.Errorf("delimitador inesperado %v", d)
}
