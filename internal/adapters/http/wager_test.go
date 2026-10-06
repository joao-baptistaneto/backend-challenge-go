package httpadapter_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// Contrato de POST /wagering/transactions e das consultas de transação.

func TestSubmit_ProcessedAndReplay(t *testing.T) {
	e := newEnv(t)
	walletID, player := e.openWallet("1000.00")
	bet := wager{provider: providerA, external: "transaction-123", kind: "BET", amount: "25.00", player: player, wallet: walletID}

	rec := e.submit(tokProviderA, "provider-a:transaction-123", bet)
	b := expectStatus(t, rec, http.StatusOK)
	txID := b["transactionId"].(string)
	if b["status"] != "PROCESSED" || moneyOf(t, b["balance"]) != "975.00 BRL" || b["idempotentReplay"] != false {
		t.Fatalf("first = %s", rec.Body.String())
	}
	if _, ok := b["failureCode"]; ok {
		t.Fatalf("processed with failureCode: %s", rec.Body.String())
	}
	if rec.Header().Get("X-Correlation-ID") == "" {
		t.Fatal("missing correlation id")
	}

	// Outra movimentação depois: o replay devolve o saldo ORIGINAL.
	expectStatus(t, e.submit(tokProviderA, "k-other", wager{provider: providerA, external: "other", kind: "BET",
		amount: "100.00", player: player, wallet: walletID}), http.StatusOK)

	afterSecond := e.uow.snapshot()
	rec = e.submit(tokProviderA, "provider-a:transaction-123", bet)
	b = expectStatus(t, rec, http.StatusOK)
	if b["transactionId"] != txID || b["status"] != "PROCESSED" || moneyOf(t, b["balance"]) != "975.00 BRL" ||
		b["idempotentReplay"] != true {
		t.Fatalf("replay = %s", rec.Body.String())
	}
	// Formas equivalentes do valor ("25" == "25.00") são o mesmo conteúdo.
	equivalent := bet
	equivalent.amount = "25"
	b = expectStatus(t, e.submit(tokProviderA, "provider-a:transaction-123", equivalent), http.StatusOK)
	if b["idempotentReplay"] != true || b["transactionId"] != txID {
		t.Fatalf("equivalent replay = %v", b)
	}
	after := e.uow.snapshot()
	if after.ledger != afterSecond.ledger || after.events != afterSecond.events || after.wagers != afterSecond.wagers ||
		after.balances[walletID] != "875.00 BRL" {
		t.Fatalf("replay produced effects: %+v -> %+v", afterSecond, after)
	}

	// Mesma chave, outro conteúdo: 409.
	changed := bet
	changed.amount = "26.00"
	expectError(t, e.submit(tokProviderA, "provider-a:transaction-123", changed), http.StatusConflict, "IDEMPOTENCY_KEY_CONFLICT")
	// Mesma operação (provider, externalId) sob outra chave: 409.
	expectError(t, e.submit(tokProviderA, "another-key", bet), http.StatusConflict, "EXTERNAL_TRANSACTION_CONFLICT")
	if s := e.uow.snapshot(); s.ledger != after.ledger || s.wagers != after.wagers || s.events != after.events {
		t.Fatalf("conflicts produced effects: %+v", s)
	}
}

func TestSubmit_RejectedIs422AndReplayable(t *testing.T) {
	e := newEnv(t)
	walletID, player := e.openWallet("100.00")
	expectStatus(t, e.submit(tokProviderA, "k1", wager{provider: providerA, external: "b1", kind: "BET",
		amount: "80.00", player: player, wallet: walletID}), http.StatusOK)

	second := wager{provider: providerA, external: "b2", kind: "BET", amount: "80.00", player: player, wallet: walletID}
	rec := e.submit(tokProviderA, "k2", second)
	b := expectStatus(t, rec, http.StatusUnprocessableEntity)
	if b["status"] != "REJECTED" || b["failureCode"] != "INSUFFICIENT_FUNDS" || b["failureCategory"] != "BUSINESS" ||
		b["idempotentReplay"] != false || b["transactionId"] == nil {
		t.Fatalf("rejected = %s", rec.Body.String())
	}
	rec = e.submit(tokProviderA, "k2", second)
	b = expectStatus(t, rec, http.StatusUnprocessableEntity)
	if b["idempotentReplay"] != true || b["failureCode"] != "INSUFFICIENT_FUNDS" {
		t.Fatalf("rejected replay = %s", rec.Body.String())
	}
	s := e.uow.snapshot()
	if s.balances[walletID] != "20.00 BRL" || s.ledger != 2 {
		t.Fatalf("state = %+v", s)
	}

	// Carteira inexistente: rejeição sem expor saldo.
	rec = e.submit(tokProviderA, "k3", wager{provider: providerA, external: "b3", kind: "BET", amount: "1.00",
		player: player, wallet: uuid.Must(uuid.NewV7())})
	b = expectStatus(t, rec, http.StatusUnprocessableEntity)
	if b["failureCode"] != "WALLET_NOT_FOUND" || b["failureCategory"] != "INPUT" || b["balance"] != nil {
		t.Fatalf("wallet not found = %s", rec.Body.String())
	}
}

func TestSubmit_PendingReferenceIs202(t *testing.T) {
	e := newEnv(t)
	walletID, player := e.openWallet("100.00")
	refund := wager{provider: providerA, external: "r1", kind: "REFUND", amount: "10.00", reference: "bet-later",
		player: player, wallet: walletID}
	rec := e.submit(tokProviderA, "kr1", refund)
	b := expectStatus(t, rec, http.StatusAccepted)
	id := b["transactionId"].(string)
	if b["status"] != "PENDING_REFERENCE" || b["balance"] != nil || b["idempotentReplay"] != false {
		t.Fatalf("pending = %s", rec.Body.String())
	}
	if loc := rec.Header().Get("Location"); loc != "/wagering/transactions/"+id {
		t.Fatalf("Location = %q", loc)
	}
	b = expectStatus(t, e.submit(tokProviderA, "kr1", refund), http.StatusAccepted)
	if b["idempotentReplay"] != true {
		t.Fatalf("pending replay = %v", b)
	}
	// A consulta acompanha a pendência.
	v := expectStatus(t, e.do(request{method: http.MethodGet, path: "/wagering/transactions/" + id, token: tokProviderA}), http.StatusOK)
	if v["status"] != "PENDING_REFERENCE" || v["referenceExternalTransactionId"] != "bet-later" || v["nextAttemptAt"] == nil {
		t.Fatalf("view = %v", v)
	}
}

func TestSubmit_InvalidInput(t *testing.T) {
	player, wallet := uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7())
	base := wager{provider: providerA, external: "t1", kind: "BET", amount: "1.00", player: player, wallet: wallet}
	with := func(f func(*wager)) string { w := base; f(&w); return w.json() }

	cases := []struct {
		name, body, key string
		status          int
		code            string
	}{
		{name: "float money", body: strings.Replace(base.json(), `"amount":"1.00"`, `"amount":1.00`, 1), status: 400, code: "INVALID_AMOUNT"},
		{name: "scientific", body: with(func(w *wager) { w.amount = "1e2" }), status: 400, code: "INVALID_AMOUNT"},
		{name: "scale", body: with(func(w *wager) { w.amount = "1.001" }), status: 400, code: "INVALID_AMOUNT"},
		{name: "negative", body: with(func(w *wager) { w.amount = "-1.00" }), status: 400, code: "INVALID_AMOUNT"},
		{name: "infinity", body: with(func(w *wager) { w.amount = "Infinity" }), status: 400, code: "INVALID_AMOUNT"},
		{name: "zero bet", body: with(func(w *wager) { w.amount = "0.00" }), status: 400, code: "INVALID_AMOUNT"},
		{name: "loss not zero", body: with(func(w *wager) { w.kind = "LOSS" }), status: 400, code: "INVALID_AMOUNT"},
		{name: "currency", body: with(func(w *wager) { w.currency = "brl" }), status: 400, code: "INVALID_CURRENCY"},
		{name: "opening", body: with(func(w *wager) { w.kind = "OPENING" }), status: 400, code: "INVALID_KIND"},
		{name: "lowercase kind", body: with(func(w *wager) { w.kind = "bet" }), status: 400, code: "INVALID_KIND"},
		{name: "refund without reference", body: with(func(w *wager) { w.kind = "REFUND" }), status: 400, code: "REFERENCE_REQUIRED"},
		{name: "bet with reference", body: with(func(w *wager) { w.reference = "x" }), status: 400, code: "REFERENCE_NOT_ALLOWED"},
		{name: "empty reference", body: strings.Replace(base.json(), `}}`, `},"referenceExternalTransactionId":""}`, 1), status: 400, code: "INVALID_REQUEST"},
		{name: "null reference", body: strings.Replace(base.json(), `}}`, `},"referenceExternalTransactionId":null}`, 1), status: 400, code: "INVALID_REQUEST"},
		{name: "bad wallet id", body: strings.Replace(base.json(), wallet.String(), "w-1", 1), status: 400, code: "INVALID_REQUEST"},
		{name: "unknown field", body: strings.Replace(base.json(), `{`, `{"extra":true,`, 1), status: 400, code: "INVALID_REQUEST"},
		{name: "duplicate kind", body: strings.Replace(base.json(), `"kind":"BET"`, `"kind":"WIN","kind":"BET"`, 1), status: 400, code: "INVALID_REQUEST"},
		{name: "missing provider", body: strings.Replace(base.json(), `"providerId":"provider-a",`, ``, 1), status: 400, code: "INVALID_REQUEST"},
		{name: "missing round", body: strings.Replace(base.json(), `"roundId":"round-1",`, ``, 1), status: 400, code: "INVALID_REQUEST"},
		{name: "numeric external id", body: strings.Replace(base.json(), `"t1"`, `1`, 1), status: 400, code: "INVALID_REQUEST"},
		{name: "empty external id", body: with(func(w *wager) { w.external = "" }), status: 400, code: "INVALID_REQUEST"},
		{name: "missing key", body: base.json(), key: "-", status: 400, code: "IDEMPOTENCY_KEY_REQUIRED"},
		{name: "key with spaces", body: base.json(), key: " k ", status: 400, code: "INVALID_IDEMPOTENCY_KEY"},
		{name: "key too long", body: base.json(), key: strings.Repeat("k", 256), status: 400, code: "INVALID_IDEMPOTENCY_KEY"},
		{name: "provider mismatch", body: with(func(w *wager) { w.provider = providerB }), status: 403, code: "PROVIDER_MISMATCH"},
		{name: "provider case mismatch", body: with(func(w *wager) { w.provider = "Provider-A" }), status: 403, code: "PROVIDER_MISMATCH"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			before, tx := e.uow.snapshot(), e.uow.transactions()
			h := map[string]string{"Idempotency-Key": "key-1"}
			switch tc.key {
			case "-":
				delete(h, "Idempotency-Key")
			case "":
			default:
				h["Idempotency-Key"] = tc.key
			}
			rec := e.do(request{method: http.MethodPost, path: "/wagering/transactions", token: tokProviderA, body: tc.body, headers: h})
			expectError(t, rec, tc.status, tc.code)
			// Entradas inválidas são recusadas antes de abrir qualquer transação.
			e.expectNoEffects(before, tx)
		})
	}
}

func TestSubmit_MultipleIdempotencyKeys(t *testing.T) {
	e := newEnv(t)
	r := request{method: http.MethodPost, path: "/wagering/transactions", token: tokProviderA,
		body: wager{provider: providerA, external: "t", kind: "BET", amount: "1.00",
			player: uuid.Must(uuid.NewV7()), wallet: uuid.Must(uuid.NewV7())}.json()}
	req := e.do(r) // sem chave
	expectError(t, req, http.StatusBadRequest, "IDEMPOTENCY_KEY_REQUIRED")

	hr := httpRequest(t, r)
	hr.Header.Add("Idempotency-Key", "a")
	hr.Header.Add("Idempotency-Key", "b")
	expectError(t, serve(e, hr), http.StatusBadRequest, "INVALID_IDEMPOTENCY_KEY")
}

// TestCrossProviderAccess: um provedor não lê, não faz replay e não
// movimenta a operação de outro; nenhuma resposta revela a existência dela.
func TestCrossProviderAccess(t *testing.T) {
	e := newEnv(t)
	walletID, player := e.openWallet("100.00")
	bet := wager{provider: providerA, external: "a-tx-1", kind: "BET", amount: "10.00", player: player, wallet: walletID}
	b := expectStatus(t, e.submit(tokProviderA, "provider-a:a-tx-1", bet), http.StatusOK)
	txID := b["transactionId"].(string)
	openingID := e.uow.state.wagers[0].ID().String()

	// O dono enxerga.
	v := expectStatus(t, e.do(request{method: http.MethodGet, path: "/wagering/transactions/" + txID, token: tokProviderA}), http.StatusOK)
	if v["providerId"] != providerA || v["externalTransactionId"] != "a-tx-1" || moneyOf(t, v["money"]) != "10.00 BRL" ||
		moneyOf(t, v["balance"]) != "90.00 BRL" || v["kind"] != "BET" {
		t.Fatalf("owner view = %v", v)
	}
	if _, ok := v["idempotencyKey"]; ok {
		t.Fatal("view exposes the idempotency key")
	}
	expectStatus(t, e.do(request{method: http.MethodGet, path: "/providers/provider-a/wagering/transactions/a-tx-1",
		token: tokProviderA}), http.StatusOK)

	before, tx := e.uow.snapshot(), e.uow.transactions()

	// Por id: 404 idêntico ao de um id inexistente (corpo igual, exceto o
	// correlationId).
	other := e.do(request{method: http.MethodGet, path: "/wagering/transactions/" + txID, token: tokProviderB,
		headers: map[string]string{"X-Correlation-ID": "same"}})
	missing := e.do(request{method: http.MethodGet, path: "/wagering/transactions/" + uuid.Must(uuid.NewV7()).String(),
		token: tokProviderB, headers: map[string]string{"X-Correlation-ID": "same"}})
	expectError(t, other, http.StatusNotFound, "TRANSACTION_NOT_FOUND")
	if other.Body.String() != missing.Body.String() || other.Code != missing.Code {
		t.Fatalf("existence leak: %q vs %q", other.Body.String(), missing.Body.String())
	}
	// Transação interna (OPENING) também não é visível a provedores.
	expectError(t, e.do(request{method: http.MethodGet, path: "/wagering/transactions/" + openingID, token: tokProviderA}),
		http.StatusNotFound, "TRANSACTION_NOT_FOUND")

	// Pelo path de outro provedor: 403 decidido só pela identidade (o
	// mesmo para externalId existente ou não).
	r1 := e.do(request{method: http.MethodGet, path: "/providers/provider-a/wagering/transactions/a-tx-1", token: tokProviderB})
	r2 := e.do(request{method: http.MethodGet, path: "/providers/provider-a/wagering/transactions/nope", token: tokProviderB})
	expectError(t, r1, http.StatusForbidden, "PROVIDER_MISMATCH")
	expectError(t, r2, http.StatusForbidden, "PROVIDER_MISMATCH")
	// Pelo próprio path, o externalId alheio é simplesmente inexistente.
	expectError(t, e.do(request{method: http.MethodGet, path: "/providers/provider-b/wagering/transactions/a-tx-1",
		token: tokProviderB}), http.StatusNotFound, "TRANSACTION_NOT_FOUND")

	// Replay com o corpo do provider-a e a chave dele: 403, sem resultado.
	rec := e.submit(tokProviderB, "provider-a:a-tx-1", bet)
	expectError(t, rec, http.StatusForbidden, "PROVIDER_MISMATCH")
	if strings.Contains(rec.Body.String(), txID) || strings.Contains(rec.Body.String(), "90.00") {
		t.Fatalf("replay leak: %s", rec.Body.String())
	}
	// Serviço interno e identidade sem papel também não acessam transações.
	for _, tok := range []string{tokInternal, tokNoRole, tokProviderNoID} {
		expectError(t, e.submit(tok, "provider-a:a-tx-1", bet), http.StatusForbidden, "FORBIDDEN")
		expectError(t, e.do(request{method: http.MethodGet, path: "/wagering/transactions/" + txID, token: tok}),
			http.StatusForbidden, "FORBIDDEN")
		expectError(t, e.do(request{method: http.MethodGet, path: "/providers/provider-a/wagering/transactions/a-tx-1", token: tok}),
			http.StatusForbidden, "FORBIDDEN")
	}
	// Até aqui, nenhum efeito e nenhuma transação de escrita.
	after := e.uow.snapshot()
	if after.ledger != before.ledger || after.events != before.events || after.wagers != before.wagers ||
		after.balances[walletID] != before.balances[walletID] {
		t.Fatalf("cross access produced effects: %+v -> %+v", before, after)
	}
	// Só as 4 consultas que passaram da autorização (2 por id do provider-b,
	// OPENING pelo provider-a e o próprio path do provider-b) abriram unidade
	// de trabalho, todas somente leitura.
	if got := e.uow.transactions() - tx; got != 4 {
		t.Fatalf("units of work = %d, want 4 read-only queries", got)
	}

	// O provider-b pode usar o mesmo externalId/chave no próprio espaço: é
	// outra operação (sem replay da operação do provider-a).
	betB := bet
	betB.provider = providerB
	b = expectStatus(t, e.submit(tokProviderB, "provider-a:a-tx-1", betB), http.StatusOK)
	if b["transactionId"] == txID || b["idempotentReplay"] != false || moneyOf(t, b["balance"]) != "80.00 BRL" {
		t.Fatalf("provider-b operation = %v", b)
	}
}
