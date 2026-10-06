package httpadapter_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	httpadapter "github.com/junglegaming/backend-challenge-go/internal/adapters/http"
	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

func withReadiness(checks ...httpadapter.ReadinessCheck) envOption {
	return func(d *httpadapter.Deps) { d.Readiness = checks }
}

func TestHealth(t *testing.T) {
	var dbErr error
	e := newEnv(t, withReadiness(
		httpadapter.ReadinessCheck{Name: "postgres", Check: func(context.Context) error { return dbErr }},
		httpadapter.ReadinessCheck{Name: "sqs", Check: func(context.Context) error { return nil }},
	))

	// Públicos: sem token (e um token inválido é ignorado).
	b := expectStatus(t, e.do(request{method: http.MethodGet, path: "/health/live"}), http.StatusOK)
	if b["status"] != "UP" {
		t.Fatalf("live = %v", b)
	}
	expectStatus(t, e.do(request{method: http.MethodGet, path: "/health/live", token: "garbage"}), http.StatusOK)
	b = expectStatus(t, e.do(request{method: http.MethodGet, path: "/health/ready"}), http.StatusOK)
	checks := b["checks"].(map[string]any)
	if b["status"] != "UP" || checks["postgres"] != "UP" || checks["sqs"] != "UP" {
		t.Fatalf("ready = %v", b)
	}

	dbErr = errors.New("dial tcp 10.0.0.1:5432: password=secret refused")
	rec := e.do(request{method: http.MethodGet, path: "/health/ready"})
	b = expectStatus(t, rec, http.StatusServiceUnavailable)
	if b["status"] != "DOWN" || b["checks"].(map[string]any)["postgres"] != "DOWN" || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("ready down = %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "secret") || strings.Contains(rec.Body.String(), "10.0.0.1") {
		t.Fatalf("readiness leaks error details: %s", rec.Body.String())
	}

	// Só o path exato é público.
	expectError(t, e.do(request{method: http.MethodGet, path: "/health/live/"}), http.StatusUnauthorized, "UNAUTHENTICATED")
	expectError(t, e.do(request{method: http.MethodGet, path: "/health"}), http.StatusUnauthorized, "UNAUTHENTICATED")

	// Panic numa verificação vira DOWN, não derruba o processo.
	e2 := newEnv(t, withReadiness(httpadapter.ReadinessCheck{Name: "boom", Check: func(context.Context) error { panic("x") }}))
	expectStatus(t, e2.do(request{method: http.MethodGet, path: "/health/ready"}), http.StatusServiceUnavailable)
}

func TestMetricsEndpoint(t *testing.T) {
	e := newEnv(t)
	walletID, player := e.openWallet("10.00")
	expectStatus(t, e.submit(tokProviderA, "k", wager{provider: providerA, external: "x", kind: "BET", amount: "1.00",
		player: player, wallet: walletID}), http.StatusOK)
	expectStatus(t, e.submit(tokProviderA, "k", wager{provider: providerA, external: "x", kind: "BET", amount: "1.00",
		player: player, wallet: walletID}), http.StatusOK)

	// Padrão: só o serviço interno.
	expectError(t, e.do(request{method: http.MethodGet, path: "/metrics"}), http.StatusUnauthorized, "UNAUTHENTICATED")
	expectError(t, e.do(request{method: http.MethodGet, path: "/metrics", token: tokProviderA}), http.StatusForbidden, "FORBIDDEN")
	rec := e.do(request{method: http.MethodGet, path: "/metrics", token: tokInternal})
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics = %d", rec.Code)
	}
	text := rec.Body.String()
	for _, want := range []string{
		`wallet_http_request_duration_seconds_count{code="201",method="POST",route="/wallets"}`,
		`route="/wagering/transactions"`,
		`wallet_wager_transactions_total{source="http",status="PROCESSED",type="BET"} 1`,
		`wallet_duplicates_total{kind="idempotent_replay",source="http"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q:\n%s", want, text)
		}
	}
	// Ids nunca viram rótulo.
	if strings.Contains(text, walletID.String()) {
		t.Fatal("wallet id used as metric label")
	}

	pub := newEnv(t, func(d *httpadapter.Deps) { d.MetricsAccess = httpadapter.MetricsPublic })
	if rec := pub.do(request{method: http.MethodGet, path: "/metrics"}); rec.Code != http.StatusOK {
		t.Fatalf("public metrics = %d", rec.Code)
	}
}

func TestCorrelationID(t *testing.T) {
	e := newEnv(t)
	rec := e.do(request{method: http.MethodGet, path: "/health/live", headers: map[string]string{"X-Correlation-ID": correlationValue}})
	if rec.Header().Get("X-Correlation-ID") != correlationValue {
		t.Fatalf("correlation not echoed: %q", rec.Header().Get("X-Correlation-ID"))
	}
	rec = e.do(request{method: http.MethodGet, path: "/health/live"})
	if _, err := uuid.Parse(rec.Header().Get("X-Correlation-ID")); err != nil {
		t.Fatalf("generated correlation id = %q", rec.Header().Get("X-Correlation-ID"))
	}
	// Erros (inclusive 401) trazem o correlationId no corpo.
	rec = e.do(request{method: http.MethodGet, path: "/wallets/x", headers: map[string]string{"X-Correlation-ID": correlationValue}})
	b := expectError(t, rec, http.StatusUnauthorized, "UNAUTHENTICATED")
	if b["correlationId"] != correlationValue {
		t.Fatalf("error body = %v", b)
	}
	// Valor inválido: 400 sem ecoar o valor.
	for _, bad := range []string{" padded", strings.Repeat("c", 256), "a\x01b"} {
		rec = e.do(request{method: http.MethodGet, path: "/health/live", headers: map[string]string{"X-Correlation-ID": bad}})
		expectError(t, rec, http.StatusBadRequest, "INVALID_CORRELATION_ID")
		if strings.Contains(rec.Body.String(), strings.TrimSpace(bad)) {
			t.Fatal("invalid correlation id echoed")
		}
	}

	// O correlationId chega ao caso de uso (eventos da outbox).
	walletID, _ := e.openWallet("1.00")
	_ = walletID
	e.uow.mu.Lock()
	defer e.uow.mu.Unlock()
	for _, ev := range e.uow.state.events {
		if ev.CorrelationID() == "" {
			t.Fatal("event without correlation id")
		}
	}
}

func TestUnknownRoute(t *testing.T) {
	e := newEnv(t)
	expectError(t, e.do(request{method: http.MethodGet, path: "/nope"}), http.StatusUnauthorized, "UNAUTHENTICATED")
	expectError(t, e.do(request{method: http.MethodGet, path: "/nope", token: tokInternal}), http.StatusNotFound, "ROUTE_NOT_FOUND")
	rec := e.do(request{method: http.MethodDelete, path: "/wallets", token: tokInternal})
	expectError(t, rec, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED")
	if rec.Header().Get("Allow") != "POST" {
		t.Fatalf("Allow = %q", rec.Header().Get("Allow"))
	}
	rec = e.do(request{method: http.MethodPut, path: "/wallets/" + uuid.Must(uuid.NewV7()).String(), token: tokInternal})
	expectError(t, rec, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED")
	if rec.Header().Get("Allow") != "GET" {
		t.Fatalf("Allow = %q", rec.Header().Get("Allow"))
	}
}

// --- casos de uso falsos (mapeamento de erros) -------------------------------

type stubProcessor struct {
	res app.WagerResult
	err error
}

func (s stubProcessor) Execute(context.Context, app.ProcessWagerCommand) (app.WagerResult, error) {
	return s.res, s.err
}

type stubOpener struct{ err error }

func (s stubOpener) Execute(context.Context, app.OpenWalletCommand) (app.OpenWalletResult, error) {
	if s.err != nil {
		return app.OpenWalletResult{}, s.err
	}
	panic("stub opener: boom")
}

func TestErrorMapping(t *testing.T) {
	player, wallet := uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7())
	body := wager{provider: providerA, external: "t", kind: "BET", amount: "1.00", player: player, wallet: wallet}

	cases := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"unavailable", &app.Error{Kind: app.KindUnavailable, Code: app.CodeTemporarilyUnavailable, Err: errors.New("db down")}, 503, "TEMPORARILY_UNAVAILABLE"},
		{"deadline", context.DeadlineExceeded, 503, "TEMPORARILY_UNAVAILABLE"},
		{"conflict", &app.Error{Kind: app.KindConflict, Code: app.CodeIdempotencyKeyConflict}, 409, "IDEMPOTENCY_KEY_CONFLICT"},
		{"invalid", &app.Error{Kind: app.KindInvalidInput, Code: app.CodeInvalidCorrelationID}, 400, "INVALID_CORRELATION_ID"},
		{"not found", &app.Error{Kind: app.KindNotFound, Code: app.CodeWalletNotFound}, 404, "WALLET_NOT_FOUND"},
		{"internal", errors.New("pq: secret internal detail"), 500, "INTERNAL_ERROR"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, func(d *httpadapter.Deps) { d.Process = stubProcessor{err: tc.err} })
			rec := e.submit(tokProviderA, "k", body)
			expectError(t, rec, tc.status, tc.code)
			if tc.status == 503 && rec.Header().Get("Retry-After") == "" {
				t.Fatal("503 without Retry-After")
			}
			if strings.Contains(rec.Body.String(), "secret") || strings.Contains(rec.Body.String(), "db down") {
				t.Fatalf("internal detail leaked: %s", rec.Body.String())
			}
		})
	}

	// FAILED (falha permanente registrada) também é resultado definitivo: 422.
	e := newEnv(t, func(d *httpadapter.Deps) {
		d.Process = stubProcessor{res: app.WagerResult{TransactionID: uuid.Must(uuid.NewV7()), Status: domain.WagerStatusFailed,
			FailureCode: domain.FailureCodeRetriesExhausted}}
	})
	b := expectStatus(t, e.submit(tokProviderA, "k", body), http.StatusUnprocessableEntity)
	if b["status"] != "FAILED" || b["failureCategory"] != "INFRASTRUCTURE" {
		t.Fatalf("failed = %v", b)
	}
}

func TestPanicIs500(t *testing.T) {
	e := newEnv(t, func(d *httpadapter.Deps) { d.OpenWallet = stubOpener{} })
	rec := e.do(request{method: http.MethodPost, path: "/wallets", token: tokInternal,
		body:    `{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1.00","currency":"BRL"}}`,
		headers: map[string]string{"X-Correlation-ID": correlationValue}})
	b := expectError(t, rec, http.StatusInternalServerError, "INTERNAL_ERROR")
	if b["correlationId"] != correlationValue || strings.Contains(rec.Body.String(), "boom") {
		t.Fatalf("panic body = %s", rec.Body.String())
	}
}

func TestNewHandler_MissingDependencies(t *testing.T) {
	if _, err := httpadapter.NewHandler(httpadapter.Deps{}); err == nil {
		t.Fatal("expected error for missing dependencies")
	}
}
