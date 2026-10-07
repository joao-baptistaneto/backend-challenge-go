//go:build integration

package main

// Integração da composição completa (T23, ADR-025): a aplicação real
// (cmd/server.Options) sobe contra um PostgreSQL de verdade, com as
// dependências externas indisponíveis nesta máquina substituídas de forma
// controlada:
//
//   - SQS: fx.Decorate troca sqs.API por uma fila em memória (consumidor e
//     publisher da outbox a usam); o *sqs.Client real continua no grafo
//     (construído sem rede) para que a ordem de parada dele seja verificada;
//   - Keycloak: um IdP de teste (httptest) publica o JWKS e assina tokens;
//     a configuração aponta OIDC_ISSUER_URL/OIDC_JWKS_URL para ele.
//
// Banco: TEST_DATABASE_URL (precisa de CREATE/DROP DATABASE) ou, na ausência,
// PostgreSQL via testcontainers; um banco novo por teste, migrations
// aplicadas. Goroutines vazadas falham o teste (goleak).
//
// TestIntegrationRealDependencies usa SQS (LocalStack) e Keycloak reais e só
// roda com SQS_ENDPOINT e KEYCLOAK_URL definidos (T24).

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/go-jose/go-jose/v4"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"go.uber.org/goleak"

	httpserver "github.com/junglegaming/backend-challenge-go/internal/adapters/http/server"
	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres/migrate"
	"github.com/junglegaming/backend-challenge-go/internal/adapters/sqs"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
	"github.com/junglegaming/backend-challenge-go/internal/platform/logging"
	"github.com/junglegaming/backend-challenge-go/migrations"
)

const postgresImage = "postgres:17-alpine"

var adminDSN string

func TestMain(m *testing.M) { os.Exit(runIntegration(m)) }

func runWithLeakCheck(m *testing.M) int {
	ignore := goleak.IgnoreCurrent()
	code := m.Run()
	if code != 0 {
		return code
	}
	if err := goleak.Find(ignore); err != nil {
		fmt.Fprintf(os.Stderr, "goleak: goroutines vazadas depois dos testes: %v\n", err)
		return 1
	}
	return 0
}

func runIntegration(m *testing.M) int {
	if dsn := os.Getenv("TEST_DATABASE_URL"); dsn != "" {
		adminDSN = dsn
		return runWithLeakCheck(m)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ctr, err := tcpostgres.Run(ctx, postgresImage,
		tcpostgres.WithDatabase("pg_admin"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container (or set TEST_DATABASE_URL): %v\n", err)
		return 1
	}
	defer func() {
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}()
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err)
		return 1
	}
	adminDSN = dsn
	return runWithLeakCheck(m)
}

func randomHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// migratedDSN cria um banco exclusivo com todas as migrations aplicadas e
// devolve seu DSN. O banco é removido no cleanup.
func migratedDSN(t *testing.T) string {
	t.Helper()
	ctx := t.Context()
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	defer admin.Close(context.Background())

	name := "server_" + randomHex(t, 8)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		a, err := pgx.Connect(ctx, adminDSN)
		if err != nil {
			t.Errorf("cleanup connect: %v", err)
			return
		}
		defer a.Close(ctx)
		if _, err := a.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Errorf("drop database %s: %v", name, err)
		}
	})

	u, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatalf("TEST_DATABASE_URL must be a URL: %v", err)
	}
	u.Path = "/" + name
	dsn := u.String()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect %s: %v", name, err)
	}
	defer conn.Close(context.Background())
	mig, err := migrate.New(migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mig.Up(ctx, conn); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	return dsn
}

// --- SQS em memória --------------------------------------------------------

const (
	fakeAccount   = "000000000000"
	fakeInbound   = "http://sqs.fake/" + fakeAccount + "/wager-transactions.fifo"
	fakeDLQ       = "http://sqs.fake/" + fakeAccount + "/wager-transactions-dlq.fifo"
	fakeEvents    = "http://sqs.fake/" + fakeAccount + "/wager-events.fifo"
	fakeRedrive   = `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:` + fakeAccount + `:wager-transactions-dlq.fifo","maxReceiveCount":5}`
	fakeRecvPause = 50 * time.Millisecond
)

// fakeSQS implementa sqs.API: a fila de entrada fica sempre vazia (o
// ReceiveMessage espera um pouco, respeitando o ctx, como o long polling) e
// SendMessage registra os eventos publicados pela outbox.
type fakeSQS struct {
	mu   sync.Mutex
	sent []*awssqs.SendMessageInput
}

func (f *fakeSQS) ReceiveMessage(ctx context.Context, _ *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	t := time.NewTimer(fakeRecvPause)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.C:
		return &awssqs.ReceiveMessageOutput{}, nil
	}
}

func (f *fakeSQS) DeleteMessage(context.Context, *awssqs.DeleteMessageInput, ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	return &awssqs.DeleteMessageOutput{}, nil
}

func (f *fakeSQS) ChangeMessageVisibility(context.Context, *awssqs.ChangeMessageVisibilityInput, ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	return &awssqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeSQS) GetQueueAttributes(_ context.Context, in *awssqs.GetQueueAttributesInput, _ ...func(*awssqs.Options)) (*awssqs.GetQueueAttributesOutput, error) {
	if aws.ToString(in.QueueUrl) != fakeInbound {
		return nil, &types.QueueDoesNotExist{Message: aws.String("fila desconhecida")}
	}
	return &awssqs.GetQueueAttributesOutput{Attributes: map[string]string{
		string(types.QueueAttributeNameRedrivePolicy): fakeRedrive,
		string(types.QueueAttributeNameQueueArn):      "arn:aws:sqs:us-east-1:" + fakeAccount + ":wager-transactions.fifo",
	}}, nil
}

func (f *fakeSQS) GetQueueUrl(_ context.Context, in *awssqs.GetQueueUrlInput, _ ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error) {
	if aws.ToString(in.QueueName) != "wager-transactions-dlq.fifo" {
		return nil, &types.QueueDoesNotExist{Message: aws.String("fila desconhecida")}
	}
	return &awssqs.GetQueueUrlOutput{QueueUrl: aws.String(fakeDLQ)}, nil
}

func (f *fakeSQS) SendMessage(_ context.Context, in *awssqs.SendMessageInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, in)
	return &awssqs.SendMessageOutput{MessageId: aws.String(uuid.NewString())}, nil
}

func (f *fakeSQS) sentTo(queueURL string) []*awssqs.SendMessageInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*awssqs.SendMessageInput
	for _, in := range f.sent {
		if aws.ToString(in.QueueUrl) == queueURL {
			out = append(out, in)
		}
	}
	return out
}

var _ sqs.API = (*fakeSQS)(nil)

// --- IdP de teste ------------------------------------------------------------

const (
	idpRealm    = "/realms/test"
	idpAudience = "wallet-api"
	idpKID      = "k1"
)

type testIdP struct {
	srv *httptest.Server
	key *rsa.PrivateKey
}

func newTestIdP(t *testing.T) *testIdP {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	p := &testIdP{key: key}
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+idpRealm+"/protocol/openid-connect/certs", func(w http.ResponseWriter, _ *http.Request) {
		jwk := jose.JSONWebKey{Key: &key.PublicKey, KeyID: idpKID, Algorithm: "RS256", Use: "sig"}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{jwk}})
	})
	p.srv = httptest.NewServer(mux)
	return p
}

func (p *testIdP) issuer() string  { return p.srv.URL + idpRealm }
func (p *testIdP) jwksURL() string { return p.issuer() + "/protocol/openid-connect/certs" }

// internalToken assina um token de acesso do serviço interno.
func (p *testIdP) internalToken(t *testing.T) string {
	t.Helper()
	now := time.Now()
	claims := map[string]any{
		"iss": p.issuer(), "aud": []string{idpAudience}, "sub": "service-account-internal",
		"azp": "internal-service", "typ": "Bearer",
		"iat": now.Unix(), "nbf": now.Unix(), "exp": now.Add(5 * time.Minute).Unix(),
		"realm_access": map[string]any{"roles": []string{"wallet-internal"}},
	}
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: p.key},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader(jose.HeaderKey("kid"), idpKID))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := signer.Sign(payload)
	if err != nil {
		t.Fatal(err)
	}
	s, err := obj.CompactSerialize()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// --- Eventos do Fx -----------------------------------------------------------

// stopRecorder é o logger de eventos do Fx do teste: registra a ordem dos
// OnStop (pela função que registrou o hook) e os erros de start/stop.
type stopRecorder struct {
	mu          sync.Mutex
	stops       []string
	errs        []error
	rolledBack  bool
	rollbackErr error
	logger      fxevent.Logger
}

func (r *stopRecorder) LogEvent(e fxevent.Event) {
	r.mu.Lock()
	switch ev := e.(type) {
	case *fxevent.OnStopExecuted:
		r.stops = append(r.stops, ev.CallerName)
		if ev.Err != nil {
			r.errs = append(r.errs, fmt.Errorf("OnStop %s: %w", ev.CallerName, ev.Err))
		}
	case *fxevent.OnStartExecuted:
		if ev.Err != nil {
			r.errs = append(r.errs, fmt.Errorf("OnStart %s: %w", ev.CallerName, ev.Err))
		}
	case *fxevent.RolledBack:
		r.rolledBack, r.rollbackErr = true, ev.Err
	}
	r.mu.Unlock()
	if r.logger != nil {
		r.logger.LogEvent(e)
	}
}

func (r *stopRecorder) snapshot() ([]string, []error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.stops...), append([]error(nil), r.errs...)
}

// rollback informa se o Fx concluiu um rollback de start e com que erro.
func (r *stopRecorder) rollback() (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rolledBack, r.rollbackErr
}

// indexOf devolve a posição do primeiro hook cujo caller termina em suffix.
func indexOf(t *testing.T, stops []string, suffix string) int {
	t.Helper()
	for i, s := range stops {
		if strings.HasSuffix(s, suffix) {
			return i
		}
	}
	t.Fatalf("hook de %s não executado no stop: %v", suffix, stops)
	return -1
}

// --- Cliente HTTP ------------------------------------------------------------

type httpClient struct {
	t    *testing.T
	base string
	tr   *http.Transport
	c    *http.Client
}

func newHTTPClient(t *testing.T, base string) *httpClient {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	return &httpClient{t: t, base: base, tr: tr, c: &http.Client{Transport: tr, Timeout: 10 * time.Second}}
}

func (h *httpClient) do(method, path, token, body string) (int, map[string]any) {
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
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := h.c.Do(req)
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
	return resp.StatusCode, out
}

// syncBuffer captura os logs da aplicação (várias goroutines).
type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
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

// --- Testes ------------------------------------------------------------------

func fakeEnv(dsn string, idp *testIdP) map[string]string {
	return map[string]string{
		config.EnvAppEnv:                    config.EnvTest,
		config.EnvHTTPAddr:                  "127.0.0.1:0",
		config.EnvLogLevel:                  "debug",
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
		config.EnvOIDCIssuerURL:             idp.issuer(),
		config.EnvOIDCJWKSURL:               idp.jwksURL(),
		config.EnvOIDCAudience:              idpAudience,
		config.EnvOutboxPollInterval:        "50ms",
		config.EnvOutboxDrainTimeout:        "2s",
		config.EnvPendingWorkerPollInterval: "50ms",
		config.EnvPendingWorkerDrainTimeout: "2s",
	}
}

func loadEnv(t *testing.T, env map[string]string) config.Config {
	t.Helper()
	cfg, err := config.Load(func(k string) (string, bool) { v, ok := env[k]; return v, ok })
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	return cfg
}

// TestIntegrationFullAppStartStop sobe a aplicação completa, exercita
// health, rota autenticada, /metrics e a publicação da outbox, para e
// confere: ordem de parada (servidor e workers antes de pool e clients),
// pool fechado e nenhuma goroutine vazada.
func TestIntegrationFullAppStartStop(t *testing.T) {
	leaks := goleak.IgnoreCurrent()
	dsn := migratedDSN(t)
	idp := newTestIdP(t)
	fake := &fakeSQS{}
	logs := &syncBuffer{}
	rec := &stopRecorder{}
	cfg := loadEnv(t, fakeEnv(dsn, idp))

	var (
		srv *httpserver.Server
		db  *postgres.DB
	)
	a := newApp(cfg,
		logging.WithOutput(logs),
		fx.WithLogger(func() fxevent.Logger { return rec }),
		fx.Decorate(func(sqs.API) sqs.API { return fake }),
		fx.Populate(&srv, &db),
	)
	if err := a.Err(); err != nil {
		t.Fatalf("fx.New: %v", err)
	}
	startCtx, cancel := context.WithTimeout(context.Background(), a.StartTimeout())
	defer cancel()
	if err := a.Start(startCtx); err != nil {
		t.Fatalf("start: %v", err)
	}
	started := true
	defer func() {
		if started {
			_ = a.Stop(context.Background())
		}
	}()

	h := newHTTPClient(t, "http://"+srv.Addr().String())
	token := idp.internalToken(t)

	if code, _ := h.do(http.MethodGet, "/health/live", "", ""); code != http.StatusOK {
		t.Fatalf("/health/live = %d", code)
	}
	code, body := h.do(http.MethodGet, "/health/ready", "", "")
	if code != http.StatusOK {
		t.Fatalf("/health/ready = %d %v", code, body)
	}
	for _, name := range []string{"postgres", "sqs", "http-server"} {
		if !strings.Contains(fmt.Sprint(body), name) {
			t.Fatalf("/health/ready sem a verificação %q: %v", name, body)
		}
	}

	// Rota autenticada: sem token → 401; com token do serviço interno → 201.
	player := uuid.Must(uuid.NewV7())
	openBody := fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"100.00","currency":"BRL"}}`, player)
	if code, _ := h.do(http.MethodPost, "/wallets", "", openBody); code != http.StatusUnauthorized {
		t.Fatalf("POST /wallets sem token = %d, quer 401", code)
	}
	code, body = h.do(http.MethodPost, "/wallets", token, openBody)
	if code != http.StatusCreated {
		t.Fatalf("POST /wallets = %d %v", code, body)
	}
	walletID, _ := body["id"].(string)
	if code, body = h.do(http.MethodGet, "/wallets/"+walletID, token, ""); code != http.StatusOK {
		t.Fatalf("GET /wallets/{id} = %d %v", code, body)
	}

	// /metrics: interno por padrão (METRICS_ACCESS=internal).
	if code, _ := h.do(http.MethodGet, "/metrics", "", ""); code != http.StatusUnauthorized {
		t.Fatalf("/metrics sem token = %d, quer 401", code)
	}
	if code, _ := h.do(http.MethodGet, "/metrics", token, ""); code != http.StatusOK {
		t.Fatalf("/metrics com token interno = %d", code)
	}

	// O publisher da outbox (sobre o mesmo pool e a mesma sqs.API) publica
	// o evento da abertura com saldo.
	deadline := time.Now().Add(10 * time.Second)
	for len(fake.sentTo(fakeEvents)) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("nenhum evento publicado na fila de eventos")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Stop completo dentro do StopTimeout calculado da config.
	stopCtx, cancelStop := context.WithTimeout(context.Background(), a.StopTimeout())
	defer cancelStop()
	if err := a.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	started = false

	stops, errs := rec.snapshot()
	if len(errs) > 0 {
		t.Fatalf("erros de lifecycle: %v", errs)
	}
	t.Logf("ordem dos OnStop: %v", stops)
	server := indexOf(t, stops, "http/server.NewFx")
	consumer := indexOf(t, stops, "sqs.NewConsumerFx")
	publisher := indexOf(t, stops, "outboxpublisher.NewPublisherFx")
	worker := indexOf(t, stops, "pendingworker.NewWorkerFx")
	authTransport := indexOf(t, stops, "auth.newAuthenticatorFx")
	sqsClient := indexOf(t, stops, "sqs.NewClientFx")
	pool := indexOf(t, stops, "postgres.newDBWithLifecycle")
	if server != 0 {
		t.Fatalf("o servidor HTTP deve ser o primeiro a parar: %v", stops)
	}
	if pool != len(stops)-1 {
		t.Fatalf("o pool pgx deve ser o último a fechar: %v", stops)
	}
	for name, i := range map[string]int{"consumidor": consumer, "publisher": publisher, "worker": worker} {
		if i > sqsClient && name != "worker" {
			t.Fatalf("%s parou depois do cliente SQS: %v", name, stops)
		}
		if i > pool {
			t.Fatalf("%s parou depois do pool: %v", name, stops)
		}
	}
	if authTransport < server {
		t.Fatalf("transporte do JWKS fechado antes do servidor: %v", stops)
	}
	// O worker de pendências para antes do publisher da outbox: eventos
	// gravados na drenagem do worker ainda saem por esta instância.
	if worker > publisher {
		t.Fatalf("o worker de pendências deve parar antes do publisher da outbox: %v", stops)
	}

	// Pool fechado: nenhuma conexão pode mais ser obtida.
	pingCtx, cancelPing := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelPing()
	if err := db.Ping(pingCtx); err == nil {
		t.Fatal("pool ainda aceita conexões depois do stop")
	}

	// Segredos não vazam nos logs da aplicação.
	if u, _ := url.Parse(dsn); u != nil {
		if pw, ok := u.User.Password(); ok && pw != "" && strings.Contains(logs.String(), pw) {
			t.Fatal("senha do banco apareceu nos logs")
		}
	}
	if strings.Contains(logs.String(), token) {
		t.Fatal("token apareceu nos logs")
	}

	h.tr.CloseIdleConnections()
	idp.srv.Close()
	goleak.VerifyNone(t, leaks)
}

// TestIntegrationStartFailureRollsBack: falha de start de um
// componente (consumidor: fila inexistente) desfaz os já iniciados — o pool
// fecha — sem vazar
// goroutines.
func TestIntegrationStartFailureRollsBack(t *testing.T) {
	leaks := goleak.IgnoreCurrent()
	dsn := migratedDSN(t)
	idp := newTestIdP(t)
	env := fakeEnv(dsn, idp)
	env[config.EnvSQSInboundQueueURL] = "http://sqs.fake/" + fakeAccount + "/missing.fifo"
	cfg := loadEnv(t, env)

	var db *postgres.DB
	a := newApp(cfg,
		logging.WithOutput(io.Discard),
		fx.Decorate(func(sqs.API) sqs.API { return &fakeSQS{} }),
		fx.Populate(&db),
	)
	if err := a.Err(); err != nil {
		t.Fatalf("fx.New: %v", err)
	}
	if code := runApp(a); code != 1 {
		t.Fatalf("exit code = %d, quer 1", code)
	}
	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := db.Ping(pingCtx); err == nil {
		t.Fatal("pool deveria ter sido fechado no rollback do start")
	}
	idp.srv.Close()
	goleak.VerifyNone(t, leaks)
}

// unavailableSQS simula um SQS fora do ar: as chamadas do start do consumidor
// falham com erro transitório (o consumidor repete com backoff até o prazo).
type unavailableSQS struct{ fakeSQS }

var errSQSUnavailable = errors.New("dial tcp 127.0.0.1:4566: connect: connection refused")

func (*unavailableSQS) GetQueueAttributes(context.Context, *awssqs.GetQueueAttributesInput, ...func(*awssqs.Options)) (*awssqs.GetQueueAttributesOutput, error) {
	return nil, errSQSUnavailable
}

func (*unavailableSQS) GetQueueUrl(context.Context, *awssqs.GetQueueUrlInput, ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error) {
	return nil, errSQSUnavailable
}

// TestIntegrationStartTimeoutRollsBack: com o SQS indisponível, o
// consumidor repete as chamadas do start até estourar o orçamento. O start
// tem de falhar ainda com tempo para o rollback: o Fx executa os OnStop dos
// hooks já iniciados (pool, worker, publisher, clients), o pool fecha e
// nenhuma goroutine vaza.
func TestIntegrationStartTimeoutRollsBack(t *testing.T) {
	leaks := goleak.IgnoreCurrent()
	dsn := migratedDSN(t)
	idp := newTestIdP(t)
	cfg := loadEnv(t, fakeEnv(dsn, idp))
	const startBudget = 2 * time.Second

	rec := &stopRecorder{}
	var db *postgres.DB
	a := newApp(cfg,
		logging.WithOutput(io.Discard),
		fx.WithLogger(func() fxevent.Logger { return rec }),
		fx.Decorate(func(sqs.API) sqs.API { return &unavailableSQS{} }),
		// Orçamento curto para o teste; o prazo do Fx continua sendo
		// orçamento + reserva de rollback, como em StartTimeout.
		fx.StartTimeout(startBudget+StopTimeout(cfg)),
		fx.Populate(&db),
	)
	if err := a.Err(); err != nil {
		t.Fatalf("fx.New: %v", err)
	}
	begin := time.Now()
	if code := runApp(a); code != 1 {
		t.Fatalf("exit code = %d, quer 1", code)
	}
	elapsed := time.Since(begin)

	stops, _ := rec.snapshot()
	rolledBack, rollbackErr := rec.rollback()
	if !rolledBack {
		t.Fatalf("o Fx não executou o rollback (OnStop executados: %v)", stops)
	}
	if rollbackErr != nil {
		t.Fatalf("rollback com erro: %v", rollbackErr)
	}
	t.Logf("start falhou em %s; OnStop do rollback: %v", elapsed, stops)
	pool := indexOf(t, stops, "postgres.newDBWithLifecycle")
	indexOf(t, stops, "pendingworker.NewWorkerFx")
	indexOf(t, stops, "outboxpublisher.NewPublisherFx")
	indexOf(t, stops, "sqs.NewClientFx")
	if pool != len(stops)-1 {
		t.Fatalf("o pool pgx deve ser o último a fechar: %v", stops)
	}
	if elapsed > startBudget+10*time.Second {
		t.Fatalf("start levou %s; orçamento era %s", elapsed, startBudget)
	}

	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := db.Ping(pingCtx); err == nil {
		t.Fatal("pool deveria ter sido fechado no rollback do start")
	}
	idp.srv.Close()
	goleak.VerifyNone(t, leaks)
}

// TestIntegrationRealDependencies sobe a aplicação contra SQS (LocalStack) e
// Keycloak reais, sem substituições. Variáveis (T24):
//
//	SQS_ENDPOINT            ex.: http://localhost:4566; ausente → Skip
//	KEYCLOAK_URL            ex.: http://localhost:8081; ausente → Skip
//	KEYCLOAK_REALM          default junglegaming
//	KEYCLOAK_CLIENT_ID      client com o papel wallet-internal (default internal-service)
//	KEYCLOAK_CLIENT_SECRET  obrigatória com KEYCLOAK_URL (nunca logada)
//	SQS_ACCOUNT_ID          default 000000000000
func TestIntegrationRealDependencies(t *testing.T) {
	endpoint := strings.TrimSuffix(os.Getenv("SQS_ENDPOINT"), "/")
	kc := strings.TrimSuffix(os.Getenv("KEYCLOAK_URL"), "/")
	if endpoint == "" || kc == "" {
		t.Skip("SQS_ENDPOINT/KEYCLOAK_URL ausentes: LocalStack e Keycloak indisponíveis (roda na T24 com o Docker Compose)")
	}
	secret := os.Getenv("KEYCLOAK_CLIENT_SECRET")
	if secret == "" {
		t.Fatal("KEYCLOAK_URL definido: KEYCLOAK_CLIENT_SECRET é obrigatória")
	}
	realm := envOr("KEYCLOAK_REALM", "junglegaming")
	clientID := envOr("KEYCLOAK_CLIENT_ID", "internal-service")
	account := envOr("SQS_ACCOUNT_ID", fakeAccount)
	issuer := kc + "/realms/" + realm

	leaks := goleak.IgnoreCurrent()
	dsn := migratedDSN(t)
	cfg := loadEnv(t, map[string]string{
		config.EnvAppEnv:                    config.EnvTest,
		config.EnvHTTPAddr:                  "127.0.0.1:0",
		config.EnvShutdownTimeout:           "5s",
		config.EnvDatabaseURL:               dsn,
		config.EnvAWSRegion:                 envOr("AWS_REGION", "us-east-1"),
		config.EnvAWSAccessKeyID:            envOr("AWS_ACCESS_KEY_ID", "test"),
		config.EnvAWSSecretAccessKey:        envOr("AWS_SECRET_ACCESS_KEY", "test"),
		config.EnvAWSEndpointURL:            endpoint,
		config.EnvSQSInboundQueueURL:        endpoint + "/" + account + "/wager-transactions.fifo",
		config.EnvSQSInboundDLQURL:          endpoint + "/" + account + "/wager-transactions-dlq.fifo",
		config.EnvSQSEventsQueueURL:         endpoint + "/" + account + "/wager-events.fifo",
		config.EnvSQSWaitTime:               "1s",
		config.EnvSQSDrainTimeout:           "2s",
		config.EnvOIDCIssuerURL:             issuer,
		config.EnvOIDCAudience:              envOr("OIDC_AUDIENCE", "wallet-api"),
		config.EnvOutboxDrainTimeout:        "2s",
		config.EnvPendingWorkerDrainTimeout: "2s",
	})

	var (
		srv *httpserver.Server
		db  *postgres.DB
	)
	logs := &syncBuffer{}
	a := newApp(cfg, logging.WithOutput(logs), fx.Populate(&srv, &db))
	if err := a.Err(); err != nil {
		t.Fatalf("fx.New: %v", err)
	}
	startCtx, cancel := context.WithTimeout(context.Background(), a.StartTimeout())
	defer cancel()
	if err := a.Start(startCtx); err != nil {
		t.Fatalf("start: %v", err)
	}
	// Um t.Fatalf daqui em diante não pode deixar a aplicação rodando.
	started := true
	defer func() {
		if started {
			stopCtx, cancelStop := context.WithTimeout(context.Background(), a.StopTimeout())
			defer cancelStop()
			_ = a.Stop(stopCtx)
		}
	}()
	h := newHTTPClient(t, "http://"+srv.Addr().String())
	defer h.tr.CloseIdleConnections()
	token := clientCredentialsToken(t, h.c, issuer+"/protocol/openid-connect/token", clientID, secret)

	if code, body := h.do(http.MethodGet, "/health/ready", "", ""); code != http.StatusOK {
		t.Fatalf("/health/ready = %d %v", code, body)
	}
	player := uuid.Must(uuid.NewV7())
	code, body := h.do(http.MethodPost, "/wallets", token,
		fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"10.00","currency":"BRL"}}`, player))
	if code != http.StatusCreated {
		t.Fatalf("POST /wallets = %d %v", code, body)
	}

	stopCtx, cancelStop := context.WithTimeout(context.Background(), a.StopTimeout())
	defer cancelStop()
	if err := a.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	started = false

	pingCtx, cancelPing := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelPing()
	if err := db.Ping(pingCtx); err == nil {
		t.Fatal("pool ainda aceita conexões depois do stop")
	}

	// Segredos não vazam nos logs da aplicação.
	out := logs.String()
	if strings.Contains(out, secret) {
		t.Fatal("client secret do Keycloak apareceu nos logs")
	}
	if strings.Contains(out, token) {
		t.Fatal("token apareceu nos logs")
	}
	if u, _ := url.Parse(dsn); u != nil {
		if pw, ok := u.User.Password(); ok && pw != "" && strings.Contains(out, pw) {
			t.Fatal("senha do banco apareceu nos logs")
		}
	}

	h.tr.CloseIdleConnections()
	goleak.VerifyNone(t, leaks)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func clientCredentialsToken(t *testing.T, c *http.Client, tokenURL, clientID, secret string) string {
	t.Helper()
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {clientID}, "client_secret": {secret}}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&out) != nil || out.AccessToken == "" {
		t.Fatalf("token: status %d", resp.StatusCode)
	}
	return out.AccessToken
}
