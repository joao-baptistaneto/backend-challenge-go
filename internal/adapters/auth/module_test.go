package auth

import (
	"errors"
	"testing"
	"time"

	"go.uber.org/fx"

	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
	"github.com/junglegaming/backend-challenge-go/internal/platform/logging"
)

func TestModuleValidateApp(t *testing.T) {
	err := fx.ValidateApp(config.Module, logging.Module, Module,
		fx.Invoke(func(*Authenticator, *Authorizer, Settings) {}))
	if err != nil {
		t.Fatalf("ValidateApp: %v", err)
	}
	// Logger é opcional.
	err = fx.ValidateApp(fx.Supply(config.Config{}), Module, fx.Invoke(func(*Authenticator) {}))
	if err != nil {
		t.Fatalf("ValidateApp sem logger: %v", err)
	}
}

func validOIDC() config.OIDCConfig {
	return config.OIDCConfig{
		IssuerURL: "http://127.0.0.1:1/realms/r", Audience: "wallet-api",
		ProviderClaim: "provider_id", ProviderRole: "wager-provider", InternalRole: "wallet-internal",
		RolesClient: "wallet-api", ClockSkew: 30 * time.Second,
	}
}

func TestModuleStartsWithoutContactingIdP(t *testing.T) {
	var a *Authenticator
	var z *Authorizer
	app := fx.New(fx.NopLogger, fx.Supply(config.Config{OIDC: validOIDC()}), Module, fx.Populate(&a, &z))
	if err := app.Err(); err != nil {
		t.Fatalf("app.Err: %v", err)
	}
	if a == nil || z == nil || a.Settings().Audience != "wallet-api" {
		t.Fatal("componentes não providos")
	}
}

func TestModuleInvalidConfigFailsStart(t *testing.T) {
	bad := validOIDC()
	bad.ProviderRole = bad.InternalRole
	app := fx.New(fx.NopLogger, fx.Supply(config.Config{OIDC: bad}), Module, fx.Invoke(func(*Authenticator) {}))
	if err := app.Err(); !errors.Is(err, config.ErrInvalidConfig) {
		t.Fatalf("config inválida deveria impedir o start: %v", err)
	}
}

func TestModuleInvalidEnvFailsStart(t *testing.T) {
	env := map[string]string{
		config.EnvDatabaseURL:        "postgres://u@localhost:5432/db",
		config.EnvAWSRegion:          "us-east-1",
		config.EnvSQSInboundQueueURL: "http://localhost:4566/000/in.fifo",
		config.EnvSQSEventsQueueURL:  "http://localhost:4566/000/ev.fifo",
		config.EnvOIDCIssuerURL:      "http://localhost:8081/realms/junglegaming",
		config.EnvOIDCAudience:       "wallet-api",
	}
	for k, v := range env {
		t.Setenv(k, v)
	}
	for _, k := range []string{
		config.EnvOIDCJWKSURL, config.EnvOIDCProviderClaim, config.EnvOIDCProviderRole,
		config.EnvOIDCInternalRole, config.EnvOIDCRolesClient, config.EnvOIDCClockSkew,
	} {
		t.Setenv(k, "")
	}

	start := func() error {
		return fx.New(fx.NopLogger, config.Module, logging.Module, Module,
			fx.Invoke(func(*Authenticator) {})).Err()
	}
	if err := start(); err != nil {
		t.Fatalf("env válido: %v", err)
	}
	t.Setenv(config.EnvOIDCClockSkew, "10m")
	if err := start(); !errors.Is(err, config.ErrInvalidConfig) {
		t.Fatalf("OIDC_CLOCK_SKEW inválido deveria impedir o start: %v", err)
	}
}
