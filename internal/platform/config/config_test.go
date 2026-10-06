package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.uber.org/fx"
)

const (
	dbPassword  = "s3nh4-sup3r-secreta"
	awsSecret   = "aws-secret-key-value"
	validDBURL  = "postgres://wallet:" + dbPassword + "@localhost:5432/wallet?sslmode=disable"
	inboundURL  = "http://localhost:4566/000000000000/wager-transactions.fifo"
	dlqURL      = "http://localhost:4566/000000000000/wager-transactions-dlq.fifo"
	eventsURL   = "http://localhost:4566/000000000000/wallet-events.fifo"
	issuerURL   = "http://localhost:8081/realms/junglegaming"
	oidcAudienc = "wallet-api"
	jwksURL     = "http://localhost:8081/realms/junglegaming/protocol/openid-connect/certs"
)

// validEnv espelha .env.example (com senhas trocadas por marcadores
// detectáveis nos testes de vazamento).
func validEnv() map[string]string {
	return map[string]string{
		EnvAppEnv:                      "local",
		EnvHTTPAddr:                    ":8080",
		EnvLogLevel:                    "info",
		EnvShutdownTimeout:             "15s",
		EnvDatabaseURL:                 validDBURL,
		EnvDatabaseMaxConns:            "20",
		EnvDatabaseConnectTimeout:      "30s",
		EnvAWSRegion:                   "us-east-1",
		EnvAWSAccessKeyID:              "test",
		EnvAWSSecretAccessKey:          awsSecret,
		EnvAWSEndpointURL:              "http://localhost:4566",
		EnvSQSInboundQueueURL:          inboundURL,
		EnvSQSInboundDLQURL:            dlqURL,
		EnvSQSEventsQueueURL:           eventsURL,
		EnvOIDCIssuerURL:               issuerURL,
		EnvOIDCAudience:                oidcAudienc,
		EnvOIDCJWKSURL:                 jwksURL,
		EnvOIDCProviderClaim:           "provider_id",
		EnvOIDCProviderRole:            "wager-provider",
		EnvOIDCInternalRole:            "wallet-internal",
		EnvOIDCClockSkew:               "30s",
		EnvOutboxPollInterval:          "1s",
		EnvOutboxBatchSize:             "50",
		EnvOutboxLease:                 "30s",
		EnvPendingReferenceMaxAttempts: "10",
		EnvPendingReferenceTTL:         "10m",
	}
}

func lookupFrom(m map[string]string) LookupFunc {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestLoadValidEnvExample(t *testing.T) {
	c, err := Load(lookupFrom(validEnv()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := Config{
		App: AppConfig{Env: "local", HTTPAddr: ":8080", LogLevel: slog.LevelInfo, ShutdownTimeout: 15 * time.Second},
		Database: DatabaseConfig{
			URL: Secret(validDBURL), MaxConns: 20, ConnectTimeout: 30 * time.Second,
		},
		AWS: AWSConfig{
			Region: "us-east-1", AccessKeyID: "test", SecretAccessKey: Secret(awsSecret),
			EndpointURL: "http://localhost:4566",
		},
		SQS: SQSConfig{InboundQueueURL: inboundURL, InboundDLQURL: dlqURL, EventsQueueURL: eventsURL},
		OIDC: OIDCConfig{
			IssuerURL: issuerURL, Audience: oidcAudienc, JWKSURL: jwksURL,
			ProviderClaim: "provider_id", ProviderRole: "wager-provider", InternalRole: "wallet-internal",
			RolesClient: oidcAudienc, ClockSkew: 30 * time.Second,
		},
		Workers: WorkersConfig{
			OutboxPollInterval: time.Second, OutboxBatchSize: 50, OutboxLease: 30 * time.Second,
			PendingReferenceMaxAttempts: 10, PendingReferenceTTL: 10 * time.Minute,
		},
	}
	if c != want {
		t.Fatalf("config inesperada:\n got %#v\nwant %#v", c, want)
	}
	if !c.AWS.HasStaticCredentials() {
		t.Fatal("esperava credenciais estáticas")
	}
	if c.Database.URL.Reveal() != validDBURL {
		t.Fatal("Reveal deve devolver o valor real")
	}
}

func TestLoadDefaults(t *testing.T) {
	env := validEnv()
	for _, k := range []string{
		EnvAppEnv, EnvHTTPAddr, EnvLogLevel, EnvShutdownTimeout, EnvDatabaseMaxConns,
		EnvDatabaseConnectTimeout, EnvAWSAccessKeyID, EnvAWSSecretAccessKey, EnvAWSEndpointURL,
		EnvSQSInboundDLQURL, EnvOutboxPollInterval, EnvOutboxBatchSize, EnvOutboxLease,
		EnvPendingReferenceMaxAttempts, EnvPendingReferenceTTL, EnvOIDCJWKSURL, EnvOIDCProviderClaim,
		EnvOIDCProviderRole, EnvOIDCInternalRole, EnvOIDCRolesClient, EnvOIDCClockSkew,
	} {
		delete(env, k)
	}
	env[EnvLogLevel] = "   " // só espaços == ausente
	c, err := Load(lookupFrom(env))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.App.Env != EnvLocal || c.App.HTTPAddr != ":8080" || c.App.LogLevel != slog.LevelInfo ||
		c.App.ShutdownTimeout != 15*time.Second || c.Database.MaxConns != 20 ||
		c.Database.ConnectTimeout != 30*time.Second || c.Workers.OutboxPollInterval != time.Second ||
		c.Workers.OutboxBatchSize != 50 || c.Workers.OutboxLease != 30*time.Second ||
		c.Workers.PendingReferenceMaxAttempts != 10 || c.Workers.PendingReferenceTTL != 10*time.Minute {
		t.Fatalf("defaults inesperados: %#v", c)
	}
	if c.AWS.HasStaticCredentials() || c.AWS.EndpointURL != "" || c.SQS.InboundDLQURL != "" {
		t.Fatalf("opcionais deveriam estar vazios: %#v", c)
	}
	wantOIDC := OIDCConfig{
		IssuerURL: issuerURL, Audience: oidcAudienc, ProviderClaim: DefaultOIDCProviderClaim,
		ProviderRole: DefaultOIDCProviderRole, InternalRole: DefaultOIDCInternalRole,
		RolesClient: oidcAudienc, ClockSkew: DefaultOIDCClockSkew,
	}
	if c.OIDC != wantOIDC {
		t.Fatalf("defaults OIDC inesperados: %#v", c.OIDC)
	}
}

func TestLoadLogLevels(t *testing.T) {
	cases := map[string]slog.Level{
		"debug": slog.LevelDebug, "INFO": slog.LevelInfo, "warn": slog.LevelWarn,
		"warning": slog.LevelWarn, "Error": slog.LevelError,
	}
	for in, want := range cases {
		env := validEnv()
		env[EnvLogLevel] = in
		c, err := Load(lookupFrom(env))
		if err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if c.App.LogLevel != want {
			t.Fatalf("%s: got %v want %v", in, c.App.LogLevel, want)
		}
	}
}

func TestLoadInvalid(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(map[string]string)
		wantKey string
	}{
		{"app env", func(e map[string]string) { e[EnvAppEnv] = "prod" }, EnvAppEnv},
		{"http addr sem porta", func(e map[string]string) { e[EnvHTTPAddr] = "8080" }, EnvHTTPAddr},
		{"http addr porta inválida", func(e map[string]string) { e[EnvHTTPAddr] = ":99999" }, EnvHTTPAddr},
		{"http addr porta não numérica", func(e map[string]string) { e[EnvHTTPAddr] = ":http" }, EnvHTTPAddr},
		{"log level", func(e map[string]string) { e[EnvLogLevel] = "verbose" }, EnvLogLevel},
		{"shutdown malformado", func(e map[string]string) { e[EnvShutdownTimeout] = "15" }, EnvShutdownTimeout},
		{"shutdown zero", func(e map[string]string) { e[EnvShutdownTimeout] = "0s" }, EnvShutdownTimeout},
		{"shutdown negativo", func(e map[string]string) { e[EnvShutdownTimeout] = "-1s" }, EnvShutdownTimeout},
		{"db url ausente", func(e map[string]string) { delete(e, EnvDatabaseURL) }, EnvDatabaseURL},
		{"db url esquema", func(e map[string]string) { e[EnvDatabaseURL] = "mysql://u:p@h/db" }, EnvDatabaseURL},
		{"db url sem host", func(e map[string]string) { e[EnvDatabaseURL] = "postgres:///db" }, EnvDatabaseURL},
		{"db max conns zero", func(e map[string]string) { e[EnvDatabaseMaxConns] = "0" }, EnvDatabaseMaxConns},
		{"db max conns alto", func(e map[string]string) { e[EnvDatabaseMaxConns] = "100000" }, EnvDatabaseMaxConns},
		{"db max conns texto", func(e map[string]string) { e[EnvDatabaseMaxConns] = "vinte" }, EnvDatabaseMaxConns},
		{"db connect timeout", func(e map[string]string) { e[EnvDatabaseConnectTimeout] = "x" }, EnvDatabaseConnectTimeout},
		{"aws region ausente", func(e map[string]string) { delete(e, EnvAWSRegion) }, EnvAWSRegion},
		{"aws só key id", func(e map[string]string) { delete(e, EnvAWSSecretAccessKey) }, EnvAWSAccessKeyID},
		{"aws só secret", func(e map[string]string) { delete(e, EnvAWSAccessKeyID) }, EnvAWSSecretAccessKey},
		{"aws endpoint", func(e map[string]string) { e[EnvAWSEndpointURL] = "localhost:4566" }, EnvAWSEndpointURL},
		{"inbound ausente", func(e map[string]string) { delete(e, EnvSQSInboundQueueURL) }, EnvSQSInboundQueueURL},
		{"inbound não fifo", func(e map[string]string) {
			e[EnvSQSInboundQueueURL] = "http://localhost:4566/000000000000/wager-transactions"
		}, EnvSQSInboundQueueURL},
		{"inbound só .fifo", func(e map[string]string) {
			e[EnvSQSInboundQueueURL] = "http://localhost:4566/000000000000/.fifo"
		}, EnvSQSInboundQueueURL},
		{"dlq não fifo", func(e map[string]string) {
			e[EnvSQSInboundDLQURL] = "http://localhost:4566/000000000000/dlq"
		}, EnvSQSInboundDLQURL},
		{"dlq igual inbound", func(e map[string]string) { e[EnvSQSInboundDLQURL] = inboundURL }, EnvSQSInboundDLQURL},
		{"events ausente", func(e map[string]string) { delete(e, EnvSQSEventsQueueURL) }, EnvSQSEventsQueueURL},
		{"events igual inbound", func(e map[string]string) { e[EnvSQSEventsQueueURL] = inboundURL }, EnvSQSEventsQueueURL},
		{"issuer ausente", func(e map[string]string) { delete(e, EnvOIDCIssuerURL) }, EnvOIDCIssuerURL},
		{"issuer esquema", func(e map[string]string) { e[EnvOIDCIssuerURL] = "ftp://idp/realm" }, EnvOIDCIssuerURL},
		{"issuer com credenciais", func(e map[string]string) {
			e[EnvOIDCIssuerURL] = "http://admin:pw@localhost:8081/realms/x"
		}, EnvOIDCIssuerURL},
		{"audience vazia", func(e map[string]string) { e[EnvOIDCAudience] = "" }, EnvOIDCAudience},
		{"jwks url esquema", func(e map[string]string) { e[EnvOIDCJWKSURL] = "file:///certs" }, EnvOIDCJWKSURL},
		{"jwks url com credenciais", func(e map[string]string) {
			e[EnvOIDCJWKSURL] = "http://u:p@localhost:8081/certs"
		}, EnvOIDCJWKSURL},
		{"provider claim inválida", func(e map[string]string) { e[EnvOIDCProviderClaim] = "provider id" }, EnvOIDCProviderClaim},
		{"provider claim reservada", func(e map[string]string) { e[EnvOIDCProviderClaim] = "aud" }, EnvOIDCProviderClaim},
		{"provider claim longa", func(e map[string]string) { e[EnvOIDCProviderClaim] = strings.Repeat("a", 129) }, EnvOIDCProviderClaim},
		{"provider role inválido", func(e map[string]string) { e[EnvOIDCProviderRole] = "role/x" }, EnvOIDCProviderRole},
		{"internal role inválido", func(e map[string]string) { e[EnvOIDCInternalRole] = "a b" }, EnvOIDCInternalRole},
		{"roles iguais", func(e map[string]string) { e[EnvOIDCInternalRole] = "wager-provider" }, EnvOIDCInternalRole},
		{"clock skew negativo", func(e map[string]string) { e[EnvOIDCClockSkew] = "-1s" }, EnvOIDCClockSkew},
		{"clock skew alto", func(e map[string]string) { e[EnvOIDCClockSkew] = "10m" }, EnvOIDCClockSkew},
		{"clock skew inválido", func(e map[string]string) { e[EnvOIDCClockSkew] = "30" }, EnvOIDCClockSkew},
		{"poll interval", func(e map[string]string) { e[EnvOutboxPollInterval] = "0" }, EnvOutboxPollInterval},
		{"batch zero", func(e map[string]string) { e[EnvOutboxBatchSize] = "0" }, EnvOutboxBatchSize},
		{"batch alto", func(e map[string]string) { e[EnvOutboxBatchSize] = "5000" }, EnvOutboxBatchSize},
		{"lease <= poll", func(e map[string]string) { e[EnvOutboxLease] = "1s" }, EnvOutboxLease},
		{"max attempts", func(e map[string]string) { e[EnvPendingReferenceMaxAttempts] = "-1" }, EnvPendingReferenceMaxAttempts},
		{"ttl", func(e map[string]string) { e[EnvPendingReferenceTTL] = "1d" }, EnvPendingReferenceTTL},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := validEnv()
			tc.mutate(env)
			c, err := Load(lookupFrom(env))
			if err == nil {
				t.Fatalf("esperava erro, obteve config %#v", c)
			}
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("erro deveria satisfazer ErrInvalidConfig: %v", err)
			}
			if !strings.Contains(err.Error(), tc.wantKey) {
				t.Fatalf("erro deveria citar %s: %v", tc.wantKey, err)
			}
			if c != (Config{}) {
				t.Fatalf("config deveria ser zero-value em erro: %#v", c)
			}
			assertNoSecrets(t, err.Error())
		})
	}
}

func TestLoadAggregatesAllErrors(t *testing.T) {
	_, err := Load(lookupFrom(map[string]string{EnvLogLevel: "loud"}))
	if err == nil {
		t.Fatal("esperava erro")
	}
	for _, k := range []string{
		EnvLogLevel, EnvDatabaseURL, EnvAWSRegion, EnvSQSInboundQueueURL,
		EnvSQSEventsQueueURL, EnvOIDCIssuerURL, EnvOIDCAudience,
	} {
		if !strings.Contains(err.Error(), k) {
			t.Errorf("erro agregado deveria citar %s: %v", k, err)
		}
	}
}

func TestLoadErrorNeverLeaksDatabaseURL(t *testing.T) {
	for _, bad := range []string{
		"mysql://wallet:" + dbPassword + "@h/db",
		"postgres://wallet:" + dbPassword + "@%zz/db", // url.Parse falha e ecoaria a URL
		"postgres://wallet:" + dbPassword + "@/db",
	} {
		env := validEnv()
		env[EnvDatabaseURL] = bad
		_, err := Load(lookupFrom(env))
		if err == nil {
			t.Fatalf("esperava erro para URL inválida")
		}
		assertNoSecrets(t, err.Error())
	}
}

// Regressão (avaliação T09): httpURL/fifoQueueURL ecoavam a URL inválida
// inteira, inclusive query (token pré-assinado) e fragment.
func TestLoadErrorEchoesOnlySafeURLParts(t *testing.T) {
	const tok = "TOKEN-QUE-NAO-PODE-VAZAR"
	cases := []struct {
		name, key, value, wantEcho string
	}{
		{"esquema inválido com query", EnvSQSInboundQueueURL,
			"ftp://q/000/a.fifo?X-Amz-Security-Token=" + tok + "#f=" + tok, `"ftp://q/000/a.fifo"`},
		{"não fifo com query", EnvSQSEventsQueueURL,
			"http://localhost:4566/000/events?X-Amz-Security-Token=" + tok, `"http://localhost:4566/000/events"`},
		{"dlq não fifo com fragment", EnvSQSInboundDLQURL,
			"http://localhost:4566/000/dlq#access_token=" + tok, `"http://localhost:4566/000/dlq"`},
		{"issuer esquema com userinfo", EnvOIDCIssuerURL,
			"ftp://admin:" + tok + "@idp/realm?x=" + tok, `"ftp://idp/realm"`},
		{"endpoint sem host", EnvAWSEndpointURL, "http:///p?token=" + tok, `"http:///p"`},
		{"opaco", EnvAWSEndpointURL, "user:" + tok + "@host", `"user:(omitido)"`},
		{"malformada", EnvOIDCIssuerURL, "http://h/%zz?token=" + tok, "(valor omitido: URL malformada)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := validEnv()
			env[tc.key] = tc.value
			_, err := Load(lookupFrom(env))
			if err == nil {
				t.Fatal("esperava erro")
			}
			msg := err.Error()
			if strings.Contains(msg, tok) {
				t.Fatalf("erro ecoou credencial: %s", msg)
			}
			if !strings.Contains(msg, tc.key) || !strings.Contains(msg, tc.wantEcho) {
				t.Fatalf("erro deveria citar %s e %s: %s", tc.key, tc.wantEcho, msg)
			}
		})
	}
}

func TestConfigFormattingRedactsSecrets(t *testing.T) {
	c, err := Load(lookupFrom(validEnv()))
	if err != nil {
		t.Fatal(err)
	}
	js, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var logBuf bytes.Buffer
	slog.New(slog.NewJSONHandler(&logBuf, nil)).Info("cfg", "config", c, "dsn", c.Database.URL)

	outputs := []string{
		fmt.Sprintf("%v", c), fmt.Sprintf("%+v", c), fmt.Sprintf("%#v", c),
		fmt.Sprintf("%s", c.Database.URL), fmt.Sprintf("%q", c.AWS.SecretAccessKey),
		fmt.Sprint(c.Database.URL), c.Database.URL.String(), string(js), logBuf.String(),
	}
	for _, out := range outputs {
		assertNoSecrets(t, out)
		if !strings.Contains(out, redacted) {
			t.Errorf("saída deveria conter %s: %s", redacted, out)
		}
	}
	if Secret("").String() != "" {
		t.Fatal("segredo vazio deve formatar vazio (indica ausência sem vazar nada)")
	}

	// Regressão (avaliação T09): verbos não-string ("%d" → "%!d(config.Secret=
	// valor)") vazavam o valor bruto, inclusive dentro de Config e *Config.
	// O segredo também não pode aparecer codificado em hex/base. %p e %w (fora
	// de Errorf) são resolvidos pelo fmt sem consultar métodos e continuam
	// fora do alcance de Secret. O go vet (printf) acusa só o %w; o %p direto
	// em Secret passa, porque o vet aceita qualquer verbo para um Formatter.
	hexLeaks := []string{
		fmt.Sprintf("%x", dbPassword), fmt.Sprintf("%X", dbPassword),
		fmt.Sprintf("%x", awsSecret), fmt.Sprintf("%X", awsSecret),
	}
	verbs := []string{
		"%v", "%+v", "%#v", "%s", "%q", "%#q", "%x", "%X", "% x", "%d", "%5d", "%-30s",
		"%T", "%t", "%b", "%o", "%e", "%f", "%g", "%c", "%U", "%10.3v", "%+q",
	}
	for _, verb := range verbs {
		for name, arg := range map[string]any{
			"Secret": c.Database.URL, "AWSSecret": c.AWS.SecretAccessKey,
			"Config": c, "*Config": &c, "[]Secret": []Secret{c.Database.URL},
			"map": map[string]Secret{"k": c.AWS.SecretAccessKey},
		} {
			out := fmt.Sprintf(verb, arg)
			assertNoSecrets(t, out)
			for _, h := range hexLeaks {
				if strings.Contains(out, h) {
					t.Fatalf("%s com %s vazou o segredo em hex: %s", name, verb, out)
				}
			}
		}
	}
	if got := fmt.Sprintf("%d|%q|%#v|%-12s|", Secret("x"), Secret("x"), Secret("x"), Secret("x")); got != `[REDACTED]|"[REDACTED]"|[REDACTED]|[REDACTED]  |` {
		t.Fatalf("formatação inesperada: %q", got)
	}
	if got := fmt.Sprintf("%T", Secret("x")); got != "config.Secret" {
		t.Fatalf("%%T = %q", got)
	}
}

func assertNoSecrets(t *testing.T, s string) {
	t.Helper()
	for _, secret := range []string{dbPassword, awsSecret} {
		if strings.Contains(s, secret) {
			t.Fatalf("saída vazou segredo %q: %s", secret, s)
		}
	}
}

func setEnv(t *testing.T, env map[string]string) {
	t.Helper()
	// Limpa todas as chaves conhecidas para não herdar o ambiente da máquina.
	for k := range validEnv() {
		t.Setenv(k, "")
	}
	for k, v := range env {
		t.Setenv(k, v)
	}
}

func TestModuleValidateApp(t *testing.T) {
	if err := fx.ValidateApp(Module, fx.Invoke(func(Config) {})); err != nil {
		t.Fatalf("ValidateApp: %v", err)
	}
}

func TestModuleLoadsFromEnv(t *testing.T) {
	setEnv(t, validEnv())
	var got Config
	app := fx.New(Module, fx.NopLogger, fx.Populate(&got))
	if err := app.Err(); err != nil {
		t.Fatalf("app.Err: %v", err)
	}
	if got.SQS.EventsQueueURL != eventsURL {
		t.Fatalf("config não carregada do ambiente: %#v", got)
	}
}

func TestModuleFailsOnInvalidConfig(t *testing.T) {
	env := validEnv()
	env[EnvOutboxBatchSize] = "-5"
	setEnv(t, env)
	app := fx.New(Module, fx.NopLogger, fx.Invoke(func(Config) {}))
	err := app.Err()
	if err == nil {
		t.Fatal("app deveria falhar com config inválida")
	}
	if !errors.Is(err, ErrInvalidConfig) || !strings.Contains(err.Error(), EnvOutboxBatchSize) {
		t.Fatalf("erro pouco claro: %v", err)
	}
	assertNoSecrets(t, err.Error())
}

func TestLoadOIDCOptionals(t *testing.T) {
	env := validEnv()
	env[EnvOIDCRolesClient] = "wallet-roles"
	env[EnvOIDCClockSkew] = "0s"
	c, err := Load(lookupFrom(env))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.OIDC.RolesClient != "wallet-roles" || c.OIDC.ClockSkew != 0 {
		t.Fatalf("OIDC inesperado: %#v", c.OIDC)
	}
}
