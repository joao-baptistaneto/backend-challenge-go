package httpadapter_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/auth"
	httpadapter "github.com/junglegaming/backend-challenge-go/internal/adapters/http"
	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

func moduleOptions() fx.Option {
	settings := auth.Settings{
		Issuer: "http://idp.invalid/realms/test", Audience: "wallet-api", ProviderClaim: "provider_id",
		ProviderRole: providerRole, InternalRole: internalRole,
	}
	return fx.Options(
		fx.NopLogger,
		app.Module,
		metrics.Module,
		httpadapter.Module,
		fx.Provide(
			func() app.UnitOfWork { return newMemUoW() },
			func() (*auth.Authenticator, error) { return auth.NewAuthenticator(settings, nil) },
			func() *auth.Authorizer { return auth.NewAuthorizer(settings, nil) },
			fx.Annotate(
				func() httpadapter.ReadinessCheck {
					return httpadapter.ReadinessCheck{Name: "postgres", Check: func(context.Context) error { return nil }}
				},
				fx.ResultTags(`group:"`+httpadapter.ReadinessGroup+`"`),
			),
		),
	)
}

func TestModule_Validate(t *testing.T) {
	if err := fx.ValidateApp(moduleOptions()); err != nil {
		t.Fatal(err)
	}
}

func TestModule_ProvidesNamedHandler(t *testing.T) {
	var out http.Handler
	fxApp := fxtest.New(t, moduleOptions(), fx.Invoke(func(p struct {
		fx.In
		Handler http.Handler `name:"apiHandler"`
	}) {
		out = p.Handler
	}))
	fxApp.RequireStart()
	defer fxApp.RequireStop()

	rec := httptest.NewRecorder()
	out.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("ready = %d %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	out.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/wallets", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("protected route without token = %d", rec.Code)
	}
}
