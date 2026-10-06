package httpserver

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/goleak"

	httpadapter "github.com/junglegaming/backend-challenge-go/internal/adapters/http"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// testConfig: porta efêmera em loopback e prazos curtos.
func testConfig() config.Config {
	var c config.Config
	c.App.HTTPAddr = "127.0.0.1:0"
	c.App.ShutdownTimeout = 5 * time.Second
	c.HTTP = config.HTTPConfig{
		ReadHeaderTimeout: time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: 5 * time.Second, IdleTimeout: 10 * time.Second,
	}
	return c
}

// testHandler imita a API: /ok, /ready (executa o grupo de readiness, como
// /health/ready), /slow (bloqueia até release) e /hang (só termina quando o
// contexto da requisição é cancelado).
type testHandler struct {
	checks    []httpadapter.ReadinessCheck
	started   chan struct{}
	release   chan struct{}
	cancelled chan struct{}
}

func newTestHandler() *testHandler {
	return &testHandler{
		started: make(chan struct{}, 1), release: make(chan struct{}),
		cancelled: make(chan struct{}, 1),
	}
}

func (h *testHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/ok":
		_, _ = io.WriteString(w, "ok")
	case "/ready":
		for _, c := range h.checks {
			if err := c.Check(r.Context()); err != nil {
				http.Error(w, c.Name+" DOWN", http.StatusServiceUnavailable)
				return
			}
		}
		_, _ = io.WriteString(w, "UP")
	case "/slow":
		h.started <- struct{}{}
		<-h.release
		_, _ = io.WriteString(w, "drained")
	case "/hang":
		h.started <- struct{}{}
		<-r.Context().Done()
		h.cancelled <- struct{}{}
	default:
		http.NotFound(w, r)
	}
}

// newApp monta a aplicação com o Module real e um handler de teste nomeado
// "apiHandler" que consome o grupo de readiness.
func newApp(t *testing.T, cfg config.Config, h *testHandler) (*fxtest.App, *Server, *Drain) {
	t.Helper()
	var (
		srv   *Server
		drain *Drain
	)
	app := fxtest.New(t,
		fx.Supply(cfg),
		fx.Provide(fx.Annotate(
			func(checks []httpadapter.ReadinessCheck) http.Handler {
				h.checks = checks
				return h
			},
			fx.ParamTags(`group:"`+httpadapter.ReadinessGroup+`"`),
			fx.ResultTags(`name:"`+httpadapter.HandlerName+`"`),
		)),
		Module,
		fx.Populate(&srv, &drain),
	)
	return app, srv, drain
}

func newClient() *http.Client {
	return &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
}

func get(t *testing.T, c *http.Client, srv *Server, path string) (int, string, error) {
	t.Helper()
	resp, err := c.Get("http://" + srv.Addr().String() + path)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), err
}

func mustGet(t *testing.T, c *http.Client, srv *Server, path string, wantStatus int) string {
	t.Helper()
	st, body, err := get(t, c, srv, path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	if st != wantStatus {
		t.Fatalf("GET %s: status %d, quer %d (corpo %q)", path, st, wantStatus, body)
	}
	return body
}

// waitRefused espera até uma conexão nova ao endereço ser recusada.
func waitRefused(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			return
		}
		_ = c.Close()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("o listener %s continuou aceitando conexões", addr)
}

func TestValidateApp(t *testing.T) {
	err := fx.ValidateApp(
		fx.Supply(testConfig()),
		fx.Provide(fx.Annotate(func() http.Handler { return http.NotFoundHandler() },
			fx.ResultTags(`name:"`+httpadapter.HandlerName+`"`))),
		Module,
	)
	if err != nil {
		t.Fatalf("ValidateApp: %v", err)
	}
}

func TestStartStopNoLeak(t *testing.T) {
	defer goleak.VerifyNone(t)

	h := newTestHandler()
	app, srv, drain := newApp(t, testConfig(), h)
	if srv.Addr() != nil {
		t.Fatal("Addr deve ser nil antes do start")
	}
	app.RequireStart()
	addr := srv.Addr().String()

	c := newClient()
	if body := mustGet(t, c, srv, "/ok", http.StatusOK); body != "ok" {
		t.Fatalf("corpo %q", body)
	}
	if body := mustGet(t, c, srv, "/ready", http.StatusOK); body != "UP" {
		t.Fatalf("readiness %q", body)
	}
	if drain.Draining() {
		t.Fatal("draining antes do stop")
	}

	app.RequireStop()
	if !drain.Draining() {
		t.Fatal("stop deve ligar o draining")
	}
	waitRefused(t, addr)
	if _, _, err := get(t, c, srv, "/ok"); err == nil {
		t.Fatal("requisição após o stop deveria falhar")
	}
}

func TestServerTimeoutsFromConfig(t *testing.T) {
	cfg := testConfig()
	cfg.HTTP = config.HTTPConfig{
		ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second,
		WriteTimeout: 4 * time.Second, IdleTimeout: 5 * time.Second, DrainDelay: time.Second,
	}
	cfg.App.ShutdownTimeout = 7 * time.Second
	got := ConfigFrom(cfg)
	want := Config{
		Addr: "127.0.0.1:0", ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second,
		WriteTimeout: 4 * time.Second, IdleTimeout: 5 * time.Second, DrainDelay: time.Second,
		ShutdownTimeout: 7 * time.Second,
	}
	if got != want {
		t.Fatalf("ConfigFrom:\n got %#v\nwant %#v", got, want)
	}
	s := New(got, http.NotFoundHandler(), nil, nil)
	defer s.baseCancel()
	if s.srv.ReadHeaderTimeout != 2*time.Second || s.srv.ReadTimeout != 3*time.Second ||
		s.srv.WriteTimeout != 4*time.Second || s.srv.IdleTimeout != 5*time.Second {
		t.Fatalf("timeouts do http.Server não aplicados: %+v", s.srv)
	}
}

func TestReadHeaderTimeoutClosesSlowClient(t *testing.T) {
	defer goleak.VerifyNone(t)

	cfg := testConfig()
	cfg.HTTP.ReadHeaderTimeout = 200 * time.Millisecond
	app, srv, _ := newApp(t, cfg, newTestHandler())
	app.RequireStart()
	defer app.RequireStop()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Cabeçalhos incompletos: o servidor deve encerrar a conexão.
	if _, err := io.WriteString(conn, "GET /ok HTTP/1.1\r\nHost: x\r\n"); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	start := time.Now()
	_, err = io.ReadAll(conn)
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatal("o servidor não encerrou a conexão com cabeçalhos incompletos")
	}
	if el := time.Since(start); el > 3*time.Second {
		t.Fatalf("encerramento demorou %s", el)
	}
}

func TestStartFailsWhenPortInUse(t *testing.T) {
	defer goleak.VerifyNone(t)

	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()

	cfg := testConfig()
	cfg.App.HTTPAddr = busy.Addr().String()
	app, srv, _ := newApp(t, cfg, newTestHandler())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = app.Start(ctx)
	if err == nil {
		_ = app.Stop(ctx)
		t.Fatal("start deveria falhar com a porta ocupada")
	}
	if !strings.Contains(err.Error(), "listen HTTP") {
		t.Fatalf("erro inesperado: %v", err)
	}
	if srv.Addr() != nil {
		t.Fatal("nenhum listener deveria ficar aberto")
	}
}

// Requisição em voo é concluída durante o stop; o listener fecha antes
// (novas conexões recusadas) e o stop só termina depois da resposta.
func TestStopDrainsInFlightAndRefusesNew(t *testing.T) {
	defer goleak.VerifyNone(t)

	h := newTestHandler()
	app, srv, drain := newApp(t, testConfig(), h)
	app.RequireStart()
	addr := srv.Addr().String()

	// Cliente com keep-alive: a conexão ociosa também precisa ser fechada.
	keepAlive := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{}}
	defer keepAlive.CloseIdleConnections()
	mustGet(t, keepAlive, srv, "/ok", http.StatusOK)

	type result struct {
		status int
		body   string
		err    error
	}
	inflight := make(chan result, 1)
	go func() {
		st, body, err := get(t, newClient(), srv, "/slow")
		inflight <- result{st, body, err}
	}()
	<-h.started

	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		stopped <- app.Stop(ctx)
	}()

	waitRefused(t, addr)
	if !drain.Draining() {
		t.Fatal("readiness deveria estar DOWN durante a parada")
	}
	select {
	case err := <-stopped:
		t.Fatalf("stop terminou com requisição em voo: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	// Conexão keep-alive que estava ociosa também não serve mais.
	if _, _, err := get(t, keepAlive, srv, "/ok"); err == nil {
		t.Fatal("requisição nova após o início do stop deveria falhar")
	}

	close(h.release)
	r := <-inflight
	if r.err != nil || r.status != http.StatusOK || r.body != "drained" {
		t.Fatalf("requisição em voo não foi drenada: %+v", r)
	}
	if err := <-stopped; err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// Com DrainDelay, /health/ready responde DOWN (com o servidor ainda
// aceitando conexões) antes de o listener fechar.
func TestReadinessDownBeforeShutdown(t *testing.T) {
	defer goleak.VerifyNone(t)

	cfg := testConfig()
	cfg.HTTP.DrainDelay = 2 * time.Second
	h := newTestHandler()
	app, srv, _ := newApp(t, cfg, h)
	app.RequireStart()
	addr := srv.Addr().String()

	c := newClient()
	mustGet(t, c, srv, "/ready", http.StatusOK)

	stopped := make(chan error, 1)
	start := time.Now()
	go func() { stopped <- app.Stop(context.Background()) }()

	deadline := time.Now().Add(cfg.HTTP.DrainDelay)
	for {
		st, body, err := get(t, c, srv, "/ready")
		if err != nil {
			t.Fatalf("listener fechou antes do DrainDelay: %v", err)
		}
		if st == http.StatusServiceUnavailable {
			if body != ReadinessCheckName+" DOWN\n" {
				t.Fatalf("corpo %q", body)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("readiness não passou a DOWN")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Ainda dentro da espera: requisições comuns continuam atendidas.
	mustGet(t, c, srv, "/ok", http.StatusOK)

	if err := <-stopped; err != nil {
		t.Fatalf("stop: %v", err)
	}
	if el := time.Since(start); el < cfg.HTTP.DrainDelay {
		t.Fatalf("stop terminou em %s, antes do DrainDelay", el)
	}
	waitRefused(t, addr)
}

// Prazo estourado: Close forçado, contexto da requisição cancelado e erro
// ErrForcedClose; nada fica pendurado.
func TestStopForcesCloseAfterDeadline(t *testing.T) {
	defer goleak.VerifyNone(t)

	cfg := testConfig()
	cfg.App.ShutdownTimeout = 300 * time.Millisecond
	h := newTestHandler()
	app, srv, _ := newApp(t, cfg, h)
	app.RequireStart()

	clientErr := make(chan error, 1)
	go func() {
		_, _, err := get(t, newClient(), srv, "/hang")
		clientErr <- err
	}()
	<-h.started

	start := time.Now()
	err := app.Stop(context.Background())
	if !errors.Is(err, ErrForcedClose) {
		t.Fatalf("stop: quer ErrForcedClose, obteve %v", err)
	}
	if el := time.Since(start); el > 3*time.Second {
		t.Fatalf("stop forçado demorou %s", el)
	}
	select {
	case <-h.cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("contexto da requisição não foi cancelado no fechamento forçado")
	}
	if err := <-clientErr; err == nil {
		t.Fatal("cliente deveria ver a conexão encerrada")
	}
}

// O prazo do ctx do Stop também é respeitado quando é menor que
// ShutdownTimeout. Chamado direto no Server: com o ctx do OnStop vencido, o
// Fx devolve ctx.Err() por conta própria (por isso a composição usa
// fx.StopTimeout maior que SHUTDOWN_TIMEOUT, ADR-021).
func TestStopHonorsContextDeadline(t *testing.T) {
	defer goleak.VerifyNone(t)

	h := newTestHandler()
	app, srv, _ := newApp(t, testConfig(), h) // ShutdownTimeout 5s
	app.RequireStart()

	clientErr := make(chan error, 1)
	go func() {
		_, _, err := get(t, newClient(), srv, "/hang")
		clientErr <- err
	}()
	<-h.started

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := srv.Stop(ctx); !errors.Is(err, ErrForcedClose) {
		t.Fatalf("stop: quer ErrForcedClose, obteve %v", err)
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("stop ignorou o prazo do ctx: %s", el)
	}
	<-h.cancelled
	<-clientErr
	// Stop repetido (o do OnStop) é inofensivo.
	app.RequireStop()
}

// Falha inesperada do Serve (listener morto) derruba a aplicação.
func TestServeFailureShutsDownApp(t *testing.T) {
	defer goleak.VerifyNone(t)

	app, srv, _ := newApp(t, testConfig(), newTestHandler())
	app.RequireStart()
	done := app.Wait()

	srv.mu.Lock()
	_ = srv.ln.Close()
	srv.mu.Unlock()

	select {
	case sig := <-done:
		if sig.ExitCode != 1 {
			t.Fatalf("exit code %d, quer 1", sig.ExitCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a aplicação não recebeu o pedido de shutdown")
	}
	app.RequireStop()
}

func TestStopWithoutStartIsNoop(t *testing.T) {
	s := New(Config{Addr: "127.0.0.1:0"}, http.NotFoundHandler(), nil, nil)
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("stop sem start: %v", err)
	}
	s.baseCancel()
}

func TestDrainReadinessCheck(t *testing.T) {
	d := NewDrain()
	c := d.ReadinessCheck()
	if c.Name != ReadinessCheckName || c.Check(context.Background()) != nil {
		t.Fatal("check deveria estar UP")
	}
	d.Begin()
	if !errors.Is(c.Check(context.Background()), ErrDraining) {
		t.Fatal("check deveria falhar após Begin")
	}
}
