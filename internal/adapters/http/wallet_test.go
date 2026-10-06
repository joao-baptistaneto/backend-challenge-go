package httpadapter_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// Contrato das rotas de carteira (serviço interno).

func TestOpenWallet_Created(t *testing.T) {
	e := newEnv(t)
	player := uuid.Must(uuid.NewV7())
	rec := e.do(request{method: http.MethodPost, path: "/wallets", token: tokInternal,
		body: fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"1000.00","currency":"BRL"}}`, player)})
	b := expectStatus(t, rec, http.StatusCreated)

	id := b["id"].(string)
	if b["playerId"] != player.String() || moneyOf(t, b["balance"]) != "1000.00 BRL" || b["version"].(fmt.Stringer).String() != "1" {
		t.Fatalf("body = %s", rec.Body.String())
	}
	if loc := rec.Header().Get("Location"); loc != "/wallets/"+id {
		t.Fatalf("Location = %q", loc)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("responses must not be cached")
	}
	// OPENING + crédito + 2 eventos no mesmo commit (caso de uso real).
	s := e.uow.snapshot()
	if s.wallets != 1 || s.wagers != 1 || s.ledger != 1 || s.events != 2 {
		t.Fatalf("effects = %+v", s)
	}

	// Mesmo jogador e moeda: 409.
	rec = e.do(request{method: http.MethodPost, path: "/wallets", token: tokInternal,
		body: fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"0.00","currency":"BRL"}}`, player)})
	expectError(t, rec, http.StatusConflict, "WALLET_ALREADY_EXISTS")

	// Saldo zero: só a carteira.
	_, _ = e.openWallet("0")
	s2 := e.uow.snapshot()
	if s2.wallets != 2 || s2.ledger != 1 || s2.events != 2 {
		t.Fatalf("zero balance created financial effects: %+v", s2)
	}
}

func TestOpenWallet_StrictBody(t *testing.T) {
	player := uuid.Must(uuid.NewV7()).String()
	valid := func(amount string) string {
		return fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":%s,"currency":"BRL"}}`, player, amount)
	}
	cases := []struct {
		name, body string
		status     int
		code       string
	}{
		{"float amount", valid(`1000.00`), 400, "INVALID_INITIAL_BALANCE"},
		{"integer amount", valid(`1000`), 400, "INVALID_INITIAL_BALANCE"},
		{"scientific", valid(`"1e3"`), 400, "INVALID_INITIAL_BALANCE"},
		{"scale", valid(`"10.001"`), 400, "INVALID_INITIAL_BALANCE"},
		{"negative", valid(`"-1.00"`), 400, "INVALID_INITIAL_BALANCE"},
		{"NaN", valid(`"NaN"`), 400, "INVALID_INITIAL_BALANCE"},
		{"overflow", valid(`"99999999999999999999.00"`), 400, "INVALID_INITIAL_BALANCE"},
		{"bad currency", fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"1.00","currency":"XXX"}}`, player), 400, "INVALID_INITIAL_BALANCE"},
		{"missing balance", fmt.Sprintf(`{"playerId":%q}`, player), 400, "INVALID_REQUEST"},
		{"bad player", `{"playerId":"abc","initialBalance":{"amount":"1.00","currency":"BRL"}}`, 400, "INVALID_PLAYER_ID"},
		{"non canonical player", fmt.Sprintf(`{"playerId":"urn:uuid:%s","initialBalance":{"amount":"1.00","currency":"BRL"}}`, player), 400, "INVALID_PLAYER_ID"},
		{"nil player", `{"playerId":"00000000-0000-0000-0000-000000000000","initialBalance":{"amount":"1.00","currency":"BRL"}}`, 400, "INVALID_PLAYER_ID"},
		{"numeric player", `{"playerId":1,"initialBalance":{"amount":"1.00","currency":"BRL"}}`, 400, "INVALID_PLAYER_ID"},
		{"unknown field", strings.Replace(valid(`"1.00"`), "{", `{"x":1,`, 1), 400, "INVALID_REQUEST"},
		{"field other case", strings.Replace(valid(`"1.00"`), "playerId", "PlayerId", 1), 400, "INVALID_REQUEST"},
		{"duplicate field", strings.Replace(valid(`"1.00"`), "{", fmt.Sprintf(`{"playerId":%q,`, player), 1), 400, "INVALID_REQUEST"},
		{"trailing data", valid(`"1.00"`) + `{}`, 400, "INVALID_REQUEST"},
		{"array", `[]`, 400, "INVALID_REQUEST"},
		{"invalid utf8", "{\"playerId\":\"\xff\"}", 400, "INVALID_REQUEST"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			before, tx := e.uow.snapshot(), e.uow.transactions()
			rec := e.do(request{method: http.MethodPost, path: "/wallets", token: tokInternal, body: tc.body})
			expectError(t, rec, tc.status, tc.code)
			e.expectNoEffects(before, tx)
		})
	}
}

func TestOpenWallet_TransportErrors(t *testing.T) {
	e := newEnv(t)
	body := fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"1.00","currency":"BRL"}}`, uuid.Must(uuid.NewV7()))

	rec := e.do(request{method: http.MethodPost, path: "/wallets", token: tokInternal, body: body, noContentType: true})
	expectError(t, rec, http.StatusUnsupportedMediaType, "UNSUPPORTED_MEDIA_TYPE")

	rec = e.do(request{method: http.MethodPost, path: "/wallets", token: tokInternal, body: body,
		headers: map[string]string{"Content-Type": "text/plain"}})
	expectError(t, rec, http.StatusUnsupportedMediaType, "UNSUPPORTED_MEDIA_TYPE")

	rec = e.do(request{method: http.MethodPost, path: "/wallets", token: tokInternal, body: body,
		headers: map[string]string{"Content-Type": "application/json; charset=UTF-8"}})
	expectStatus(t, rec, http.StatusCreated)

	big := `{"playerId":"` + strings.Repeat("a", 70<<10) + `"}`
	rec = e.do(request{method: http.MethodPost, path: "/wallets", token: tokInternal, body: big})
	expectError(t, rec, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE")
}

func TestWalletRoutes_RequireInternalService(t *testing.T) {
	e := newEnv(t)
	walletID, player := e.openWallet("100.00")
	openBody := fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"5.00","currency":"USD"}}`, player)

	routes := []request{
		{method: http.MethodPost, path: "/wallets", body: openBody},
		{method: http.MethodGet, path: "/wallets/" + walletID.String()},
		{method: http.MethodGet, path: "/wallets/" + walletID.String() + "/ledger"},
		{method: http.MethodPost, path: "/wallets/" + walletID.String() + "/reconciliation"},
	}
	for _, rt := range routes {
		t.Run(rt.method+" "+rt.path, func(t *testing.T) {
			before, tx := e.uow.snapshot(), e.uow.transactions()

			r := rt
			rec := e.do(r)
			expectError(t, rec, http.StatusUnauthorized, "UNAUTHENTICATED")
			if !strings.HasPrefix(rec.Header().Get("WWW-Authenticate"), "Bearer") {
				t.Fatal("401 without WWW-Authenticate")
			}

			r.token = "forged"
			expectError(t, e.do(r), http.StatusUnauthorized, "UNAUTHENTICATED")

			for _, tok := range []string{tokProviderA, tokNoRole, tokProviderNoID} {
				r.token = tok
				rec := e.do(r)
				b := expectError(t, rec, http.StatusForbidden, "FORBIDDEN")
				// Nenhum dado da carteira no corpo negado.
				if strings.Contains(rec.Body.String(), walletID.String()) || b["balance"] != nil {
					t.Fatalf("forbidden response leaks data: %s", rec.Body.String())
				}
			}
			e.expectNoEffects(before, tx)
		})
	}
}

func TestGetWalletAndLedger(t *testing.T) {
	e := newEnv(t)
	walletID, player := e.openWallet("100.00")
	for i, amount := range []string{"10.00", "20.00"} {
		rec := e.submit(tokProviderA, fmt.Sprintf("k-%d", i), wager{provider: providerA, external: fmt.Sprintf("bet-%d", i),
			kind: "BET", amount: amount, player: player, wallet: walletID})
		expectStatus(t, rec, http.StatusOK)
	}

	b := expectStatus(t, e.do(request{method: http.MethodGet, path: "/wallets/" + walletID.String(), token: tokInternal}), http.StatusOK)
	if moneyOf(t, b["balance"]) != "70.00 BRL" || b["version"].(fmt.Stringer).String() != "3" || b["createdAt"] == nil {
		t.Fatalf("wallet = %v", b)
	}

	// Página 1 (limit=2), página 2 pelo cursor, ordem estável.
	path := "/wallets/" + walletID.String() + "/ledger"
	p1 := expectStatus(t, e.do(request{method: http.MethodGet, path: path + "?limit=2", token: tokInternal}), http.StatusOK)
	entries := p1["entries"].([]any)
	if len(entries) != 2 || p1["nextCursor"] == nil || p1["walletId"] != walletID.String() {
		t.Fatalf("page 1 = %v", p1)
	}
	first := entries[0].(map[string]any)
	if first["direction"] != "CREDIT" || moneyOf(t, first["amount"]) != "100.00 BRL" ||
		moneyOf(t, first["balanceBefore"]) != "0.00 BRL" || moneyOf(t, first["balanceAfter"]) != "100.00 BRL" {
		t.Fatalf("opening entry = %v", first)
	}
	p2 := expectStatus(t, e.do(request{method: http.MethodGet,
		path: path + "?limit=2&cursor=" + p1["nextCursor"].(string), token: tokInternal}), http.StatusOK)
	entries2 := p2["entries"].([]any)
	if len(entries2) != 1 || p2["nextCursor"] != nil {
		t.Fatalf("page 2 = %v", p2)
	}
	last := entries2[0].(map[string]any)
	if last["direction"] != "DEBIT" || moneyOf(t, last["balanceAfter"]) != "70.00 BRL" {
		t.Fatalf("last entry = %v", last)
	}

	// Página padrão: 3 lançamentos e lista vazia serializada como [].
	all := expectStatus(t, e.do(request{method: http.MethodGet, path: path, token: tokInternal}), http.StatusOK)
	if len(all["entries"].([]any)) != 3 {
		t.Fatalf("default page = %v", all)
	}
	empty, _ := e.openWallet("0.00")
	rec := e.do(request{method: http.MethodGet, path: "/wallets/" + empty.String() + "/ledger", token: tokInternal})
	if !strings.Contains(rec.Body.String(), `"entries":[]`) {
		t.Fatalf("empty ledger = %s", rec.Body.String())
	}

	// Erros.
	unknown := uuid.Must(uuid.NewV7()).String()
	cases := []struct {
		path, code string
		status     int
	}{
		{"/wallets/" + unknown, "WALLET_NOT_FOUND", 404},
		{"/wallets/" + unknown + "/ledger", "WALLET_NOT_FOUND", 404},
		{"/wallets/not-a-uuid", "INVALID_WALLET_ID", 400},
		{"/wallets/not-a-uuid/ledger", "INVALID_WALLET_ID", 400},
		{path + "?limit=0", "INVALID_PAGE_LIMIT", 400},
		{path + "?limit=201", "INVALID_PAGE_LIMIT", 400},
		{path + "?limit=-1", "INVALID_PAGE_LIMIT", 400},
		{path + "?limit=1.5", "INVALID_PAGE_LIMIT", 400},
		{path + "?limit=1&limit=2", "INVALID_PAGE_LIMIT", 400},
		{path + "?cursor=garbage", "INVALID_CURSOR", 400},
		{path + "?cursor=a&cursor=b", "INVALID_CURSOR", 400},
	}
	for _, tc := range cases {
		expectError(t, e.do(request{method: http.MethodGet, path: tc.path, token: tokInternal}), tc.status, tc.code)
	}
}

func TestReconciliation(t *testing.T) {
	e := newEnv(t)
	walletID, player := e.openWallet("1000.00")
	expectStatus(t, e.submit(tokProviderA, "k1", wager{provider: providerA, external: "t1", kind: "BET",
		amount: "25.00", player: player, wallet: walletID}), http.StatusOK)

	before := e.uow.snapshot()
	rec := e.do(request{method: http.MethodPost, path: "/wallets/" + walletID.String() + "/reconciliation", token: tokInternal})
	b := expectStatus(t, rec, http.StatusOK)
	if b["walletId"] != walletID.String() || moneyOf(t, b["storedBalance"]) != "975.00 BRL" ||
		moneyOf(t, b["calculatedBalance"]) != "975.00 BRL" || moneyOf(t, b["difference"]) != "0.00 BRL" ||
		b["consistent"] != true || b["checkedEntries"].(fmt.Stringer).String() != "2" {
		t.Fatalf("report = %s", rec.Body.String())
	}
	after := e.uow.snapshot()
	if after.ledger != before.ledger || after.events != before.events || after.balances[walletID] != before.balances[walletID] {
		t.Fatal("reconciliation changed data")
	}

	expectError(t, e.do(request{method: http.MethodPost, path: "/wallets/" + uuid.Must(uuid.NewV7()).String() + "/reconciliation",
		token: tokInternal}), http.StatusNotFound, "WALLET_NOT_FOUND")
}
