//go:build integration && multiinstance

package main

// Harness dos testes multi-instância e de recuperação de falhas (T26,
// ARCHITECTURE.md ADR-028).
//
// Cada instância é um PROCESSO do SO: o binário cmd/server é compilado uma
// vez por execução (go build -tags failpoint, com -race quando o teste roda
// com -race) e iniciado várias vezes, com portas HTTP distintas, contra o
// MESMO banco (exclusivo do teste) e as MESMAS filas FIFO do LocalStack
// (exclusivas do teste). Cada processo tem seu pool pgx, seu cliente SQS, seu
// consumidor, seu publisher da outbox e seu worker de pendências: nenhuma
// memória é compartilhada. "Crash" é encerramento abrupto do processo
// (os.Process.Kill = TerminateProcess no Windows, SIGKILL no Linux), seja
// pelo teste, seja pelo próprio processo num ponto de falha nomeado
// (internal/platform/failpoint, só ativo com a tag failpoint).
//
// Indisponibilidade de PostgreSQL/SQS: um proxy TCP do teste fica entre as
// instâncias e a dependência; "reset" derruba as conexões e recusa novas,
// "pause" congela o tráfego (como docker pause). As conexões de verificação
// do teste vão direto à dependência.
//
// Requer: TEST_DATABASE_URL (ou testcontainers), SQS_ENDPOINT (LocalStack) e
// a tag multiinstance (make test-multiinstance). O IdP é o de teste do
// pacote (httptest): os processos filhos buscam o JWKS nele por HTTP.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/outboxpublisher"
	"github.com/junglegaming/backend-challenge-go/internal/adapters/sqs"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
	"github.com/junglegaming/backend-challenge-go/internal/platform/failpoint"
)

// --- Binário -------------------------------------------------------------------

var serverBinary struct {
	once sync.Once
	path string
	err  error
}

// buildServer compila cmd/server com a tag failpoint (e -race se o teste
// também usa) uma vez por execução do pacote.
func buildServer(t *testing.T) string {
	t.Helper()
	serverBinary.once.Do(func() {
		dir, err := os.MkdirTemp("", "wallet-multiinstance-")
		if err != nil {
			serverBinary.err = err
			return
		}
		afterAll = append(afterAll, func() { _ = os.RemoveAll(dir) })
		path := filepath.Join(dir, "server")
		if runtime.GOOS == "windows" {
			path += ".exe"
		}
		args := []string{"build", "-tags", "failpoint", "-o", path}
		if raceEnabled {
			args = append(args, "-race")
		}
		cmd := exec.Command("go", append(args, ".")...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			serverBinary.err = fmt.Errorf("go %s: %w\n%s", strings.Join(args, " "), err, out)
			return
		}
		serverBinary.path = path
	})
	if serverBinary.err != nil {
		t.Fatal(serverBinary.err)
	}
	return serverBinary.path
}

// --- Instâncias ------------------------------------------------------------------

// instance é um processo cmd/server.
type instance struct {
	name string
	base string
	cmd  *exec.Cmd
	out  *syncBuffer
	done chan struct{}
}

func (in *instance) exited() bool {
	select {
	case <-in.done:
		return true
	default:
		return false
	}
}

// hit informa se o ponto de falha disparou neste processo.
func (in *instance) hit(point string) bool {
	return strings.Contains(in.out.String(), failpoint.Marker+" "+point)
}

// kill encerra o processo abruptamente (sem OnStop) e espera o fim.
func (in *instance) kill(t *testing.T) {
	t.Helper()
	if in.exited() {
		return
	}
	if err := in.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("kill %s: %v", in.name, err)
	}
	select {
	case <-in.done:
	case <-time.After(30 * time.Second):
		t.Fatalf("%s não terminou depois do kill", in.name)
	}
}

// waitCrash espera o processo morrer sozinho no ponto de falha.
func (in *instance) waitCrash(t *testing.T, point string, timeout time.Duration) {
	t.Helper()
	select {
	case <-in.done:
	case <-time.After(timeout):
		t.Fatalf("%s não caiu no ponto %s em %s", in.name, point, timeout)
	}
	if !in.hit(point) {
		t.Fatalf("%s terminou sem passar pelo ponto %s:\n%s", in.name, point, tail(in.out.String(), 40))
	}
}

// logLines devolve as linhas do log que contêm todos os trechos.
func (in *instance) logLines(parts ...string) []string {
	var out []string
	for _, l := range strings.Split(in.out.String(), "\n") {
		ok := true
		for _, p := range parts {
			if !strings.Contains(l, p) {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, l)
		}
	}
	return out
}

func tail(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// cluster é o conjunto de processos de um teste, com o banco e as filas
// exclusivos do teste.
type cluster struct {
	*ccHarness
	bin       string
	env       map[string]string
	events    string // fila de eventos (URL direta no LocalStack)
	instances []*instance
}

// miOptions ajusta o ambiente comum antes do start.
type miOptions struct {
	// pgProxy/sqsProxy: as instâncias falam com a dependência pelo proxy.
	pgProxy, sqsProxy *tcpProxy
	env               map[string]string
}

// newCluster prepara banco, filas, IdP e o ambiente comum dos processos.
func newCluster(t *testing.T, o miOptions) *cluster {
	t.Helper()
	endpoint := strings.TrimSuffix(os.Getenv("SQS_ENDPOINT"), "/")
	if endpoint == "" {
		t.Fatal("SQS_ENDPOINT ausente: os testes multi-instância exigem LocalStack (docker compose up -d --wait localstack)")
	}
	bin := buildServer(t)
	dsn := migratedDSN(t)
	idp := newTestIdP(t)
	t.Cleanup(idp.srv.Close) // depois dos processos (LIFO)

	awsCfg := config.AWSConfig{
		Region: envOr("AWS_REGION", "us-east-1"), AccessKeyID: envOr("AWS_ACCESS_KEY_ID", "test"),
		SecretAccessKey: config.Secret(envOr("AWS_SECRET_ACCESS_KEY", "test")), EndpointURL: endpoint,
	}
	client, err := sqs.NewClient(t.Context(), awsCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	q := createMIQueues(t, client.SQS())

	appDSN, appEndpoint := dsn, endpoint
	inbound, dlq, events := q.inbound, q.dlq, q.events
	if o.pgProxy != nil {
		appDSN = o.pgProxy.rewriteURL(t, dsn)
	}
	if o.sqsProxy != nil {
		appEndpoint = "http://" + o.sqsProxy.addr()
		inbound = o.sqsProxy.rewriteURL(t, inbound)
		dlq = o.sqsProxy.rewriteURL(t, dlq)
		events = o.sqsProxy.rewriteURL(t, events)
	}
	env := map[string]string{
		config.EnvAppEnv:                         config.EnvTest,
		config.EnvLogLevel:                       "info",
		config.EnvShutdownTimeout:                "5s",
		config.EnvMetricsAccess:                  config.MetricsAccessPublic,
		config.EnvHTTPWriteTimeout:               "60s",
		config.EnvDatabaseURL:                    appDSN,
		config.EnvDatabaseMaxConns:               "8",
		config.EnvDatabaseConnectTimeout:         "5s",
		config.EnvAWSRegion:                      awsCfg.Region,
		config.EnvAWSAccessKeyID:                 awsCfg.AccessKeyID,
		config.EnvAWSSecretAccessKey:             string(awsCfg.SecretAccessKey),
		config.EnvAWSEndpointURL:                 appEndpoint,
		config.EnvSQSInboundQueueURL:             inbound,
		config.EnvSQSInboundDLQURL:               dlq,
		config.EnvSQSEventsQueueURL:              events,
		config.EnvSQSConsumerConcurrency:         "4",
		config.EnvSQSWaitTime:                    "1s",
		config.EnvSQSVisibilityTimeout:           "4s",
		config.EnvSQSProcessTimeout:              "3s",
		config.EnvSQSRetryBaseDelay:              "1s",
		config.EnvSQSRetryMaxDelay:               "4s",
		config.EnvSQSMaxAttempts:                 "8", // < maxReceiveCount da fila (miMaxReceiveCount)
		config.EnvSQSDrainTimeout:                "2s",
		config.EnvOIDCIssuerURL:                  idp.issuer(),
		config.EnvOIDCJWKSURL:                    idp.jwksURL(),
		config.EnvOIDCAudience:                   idpAudience,
		config.EnvOutboxPollInterval:             "100ms",
		config.EnvOutboxBatchSize:                "10",
		config.EnvOutboxLease:                    "3s",
		config.EnvOutboxPublishTimeout:           "1s",
		config.EnvOutboxRetryBaseDelay:           "200ms",
		config.EnvOutboxRetryMaxDelay:            "1s",
		config.EnvOutboxDrainTimeout:             "2s",
		config.EnvPendingWorkerPollInterval:      "100ms",
		config.EnvPendingWorkerLease:             "3s",
		config.EnvPendingWorkerProcessTimeout:    "2s",
		config.EnvPendingWorkerDrainTimeout:      "2s",
		config.EnvPendingReferenceRetryBaseDelay: "200ms",
		config.EnvPendingReferenceRetryMaxDelay:  "1s",
		config.EnvPendingReferenceMaxAttempts:    "1000",
		config.EnvPendingReferenceTTL:            "60s",
		config.EnvPendingOrphanGrace:             "2s",
	}
	maps.Copy(env, o.env)

	verify, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(verify.Close)
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = 128
	t.Cleanup(tr.CloseIdleConnections)

	c := &cluster{
		ccHarness: &ccHarness{
			t: t, client: &http.Client{Transport: tr, Timeout: 60 * time.Second}, verify: verify,
			internal: idp.internalToken(t), provider: idp.providerToken(t, ccProvider),
			sqsAPI: client.SQS(), inbound: q.inbound, dlq: q.dlq,
		},
		bin: bin, env: env, events: q.events,
	}
	t.Cleanup(c.shutdown)
	return c
}

// appEnvPrefixes: variáveis do ambiente do teste que não passam aos filhos
// (a configuração deles é só a do cluster).
var appEnvPrefixes = []string{
	"APP_", "HTTP_", "LOG_", "SHUTDOWN_", "METRICS_", "DATABASE_", "AWS_", "SQS_", "OIDC_",
	"OUTBOX_", "PENDING_", failpoint.EnvVar, "GORACE",
}

func childEnv(over map[string]string) []string {
	var env []string
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		if slices.ContainsFunc(appEnvPrefixes, func(p string) bool { return strings.HasPrefix(strings.ToUpper(k), p) }) {
			continue
		}
		env = append(env, kv)
	}
	// Uma corrida detectada derruba o filho na hora (o teste percebe).
	env = append(env, "GORACE=halt_on_error=1")
	for k, v := range over {
		env = append(env, k+"="+v)
	}
	return env
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// launch inicia um processo sem esperar o readiness. failpoints: ex.
// failpoint.SQSConsumerAfterCommit+"=crash".
func (c *cluster) launch(name string, extra map[string]string, failpoints ...string) *instance {
	c.t.Helper()
	env := maps.Clone(c.env)
	addr := freePort(c.t)
	env[config.EnvHTTPAddr] = addr
	maps.Copy(env, extra)
	if len(failpoints) > 0 {
		env[failpoint.EnvVar] = strings.Join(failpoints, ",")
	}
	in := &instance{name: name, base: "http://" + addr, out: &syncBuffer{}, done: make(chan struct{})}
	in.cmd = exec.Command(c.bin)
	in.cmd.Env = childEnv(env)
	in.cmd.Stdout, in.cmd.Stderr = in.out, in.out
	if err := in.cmd.Start(); err != nil {
		c.t.Fatalf("start %s: %v", name, err)
	}
	go func() {
		_ = in.cmd.Wait()
		close(in.done)
	}()
	c.instances = append(c.instances, in)
	return in
}

// waitReady espera GET /health/ready = 200 (postgres, sqs, http-server).
func (c *cluster) waitReady(ins ...*instance) {
	c.t.Helper()
	for _, in := range ins {
		deadline := time.Now().Add(90 * time.Second)
		for {
			if in.exited() {
				c.t.Fatalf("%s terminou antes de ficar pronto:\n%s", in.name, tail(in.out.String(), 40))
			}
			if code, _, err := c.ready(in); err == nil && code == http.StatusOK {
				break
			}
			if time.Now().After(deadline) {
				c.t.Fatalf("%s não ficou pronto:\n%s", in.name, tail(in.out.String(), 40))
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (c *cluster) ready(in *instance) (int, map[string]any, error) {
	return c.on(in).call(http.MethodGet, "/health/ready", c.internal, "", nil)
}

// start inicia os processos e espera todos ficarem prontos.
func (c *cluster) start(names ...string) []*instance {
	c.t.Helper()
	var ins []*instance
	for _, n := range names {
		ins = append(ins, c.launch(n, nil))
	}
	c.waitReady(ins...)
	return ins
}

// on devolve o harness HTTP apontado para a instância.
func (c *cluster) on(in *instance) *ccHarness {
	h := *c.ccHarness
	h.base = in.base
	return &h
}

// alive devolve uma instância viva (para as verificações pela API).
func (c *cluster) alive() *instance {
	for _, in := range slices.Backward(c.instances) {
		if !in.exited() {
			return in
		}
	}
	c.t.Fatal("nenhuma instância viva")
	return nil
}

// shutdown mata o que sobrou, espera os processos e procura corridas
// relatadas pelos filhos (-race). Em falha, mostra o fim dos logs.
func (c *cluster) shutdown() {
	for _, in := range c.instances {
		if !in.exited() {
			_ = in.cmd.Process.Kill()
		}
		<-in.done
	}
	for _, in := range c.instances {
		out := in.out.String()
		if strings.Contains(out, "WARNING: DATA RACE") {
			c.t.Errorf("corrida de dados no processo %s:\n%s", in.name, out)
		}
		if c.t.Failed() {
			c.t.Logf("--- log de %s (fim, sem eventos do Fx) ---\n%s", in.name, tail(withoutFx(out), 60))
		}
	}
}

// --- Espera e verificações ------------------------------------------------------

// eventually repete cond até ok ou o prazo.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok, detail := cond()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s não aconteceu em %s: %s", what, timeout, detail)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// submitRetry é o provedor at-least-once: reenvia a mesma operação (mesma
// chave e corpo) até um resultado de negócio (200, 202 ou 422). 503 e erros
// de transporte são repetidos; o resto falha.
func (h *ccHarness) submitRetry(o ccWager, timeout time.Duration) (ccResult, int, error) {
	deadline := time.Now().Add(timeout)
	for n := 1; ; n++ {
		r, err := h.submit(o)
		switch {
		case err == nil && (r.code == http.StatusOK || r.code == http.StatusAccepted || r.code == http.StatusUnprocessableEntity):
			return r, n, nil
		case err == nil, r.code != 0 && r.code != http.StatusServiceUnavailable:
			return r, n, fmt.Errorf("%s: resposta inesperada %+v %v", o.ext, r, err)
		}
		if time.Now().After(deadline) {
			return r, n, fmt.Errorf("%s: sem resultado em %s (%d tentativas): %+v %v", o.ext, timeout, n, r, err)
		}
		time.Sleep(150 * time.Millisecond)
	}
}

// txState devolve status e failureCode da operação externa.
func (h *ccHarness) txState(ext string) (status, fc string) {
	h.t.Helper()
	var f *string
	if err := h.verify.QueryRow(h.t.Context(), `SELECT status, failure_code FROM wager_transactions
 WHERE provider_id = $1 AND external_transaction_id = $2`, ccProvider, ext).Scan(&status, &f); err != nil {
		h.t.Fatalf("%s: %v", ext, err)
	}
	if f != nil {
		fc = *f
	}
	return status, fc
}

// assertEventsDelivered: toda a outbox foi publicada (sem perda) e a fila de
// eventos contém exatamente os eventIds da outbox, com o corpo igual ao
// snapshot gravado; uma eventual cópia repetida (at-least-once) tem o MESMO
// eventId e corpo. A primeira entrega de cada agregado segue a ordem de seq
// (MessageGroupId = aggregateId, ADR-024). Devolve o número de cópias
// repetidas observadas.
func (c *cluster) assertEventsDelivered(timeout time.Duration) int {
	t := c.t
	t.Helper()
	eventually(t, timeout, "publicação de toda a outbox", func() (bool, string) {
		n := c.scalar(`SELECT count(*) FROM outbox WHERE published_at IS NULL`)
		return n == 0, fmt.Sprintf("%d eventos não publicados", n)
	})
	type ev struct {
		seq       int64
		aggregate string
		payload   string
	}
	want := map[string]ev{}
	rows, err := c.verify.Query(t.Context(), `SELECT event_id::text, seq, aggregate_id, payload::text FROM outbox`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		var e ev
		if err := rows.Scan(&id, &e.seq, &e.aggregate, &e.payload); err != nil {
			t.Fatal(err)
		}
		want[id] = e
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	seen := map[string]int{}
	lastSeq := map[string]int64{}
	dups := 0
	deadline := time.Now().Add(timeout)
	for drained := false; !drained; {
		out, err := c.sqsAPI.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{
			QueueUrl: aws.String(c.events), MaxNumberOfMessages: 10, WaitTimeSeconds: 1,
			MessageAttributeNames: []string{"All"},
		})
		if err != nil {
			t.Fatal(err)
		}
		var del []types.DeleteMessageBatchRequestEntry
		for _, m := range out.Messages {
			id := ""
			if a, ok := m.MessageAttributes[outboxpublisher.AttrEventID]; ok {
				id = aws.ToString(a.StringValue)
			}
			e, ok := want[id]
			if !ok {
				t.Fatalf("evento publicado fora da outbox: eventId %q", id)
			}
			if body := aws.ToString(m.Body); body != e.payload {
				t.Fatalf("evento %s publicado com corpo diferente do snapshot:\n%s\n%s", id, body, e.payload)
			}
			if seen[id]++; seen[id] > 1 {
				dups++
			} else {
				if e.seq < lastSeq[e.aggregate] {
					t.Fatalf("agregado %s: evento seq %d entregue depois de seq %d", e.aggregate, e.seq, lastSeq[e.aggregate])
				}
				lastSeq[e.aggregate] = e.seq
			}
			del = append(del, types.DeleteMessageBatchRequestEntry{
				Id: aws.String(fmt.Sprint(len(del))), ReceiptHandle: m.ReceiptHandle,
			})
		}
		if len(del) > 0 {
			res, err := c.sqsAPI.DeleteMessageBatch(t.Context(), &awssqs.DeleteMessageBatchInput{
				QueueUrl: aws.String(c.events), Entries: del,
			})
			if err != nil || len(res.Failed) != 0 {
				t.Fatalf("delete da fila de eventos: %v %v", err, res)
			}
		}
		switch {
		case len(seen) == len(want) && len(out.Messages) == 0:
			drained = true // tudo visto e mais um receive vazio (cópias tardias)
		case time.Now().After(deadline):
			t.Fatalf("fila de eventos: %d de %d eventIds entregues", len(seen), len(want))
		}
	}
	return dups
}

// outboxPublishedCount devolve publicados e total da outbox.
func (c *cluster) outboxState() (published, total int64) {
	return c.scalar(`SELECT count(*) FROM outbox WHERE published_at IS NOT NULL`), c.scalar(`SELECT count(*) FROM outbox`)
}

// queueDepth devolve visíveis e em voo na fila de entrada.
func (c *cluster) queueDepth() (visible, inFlight string) {
	out, err := c.sqsAPI.GetQueueAttributes(c.t.Context(), &awssqs.GetQueueAttributesInput{
		QueueUrl: aws.String(c.inbound),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		c.t.Fatal(err)
	}
	return out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)],
		out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)]
}

// --- Proxy TCP de falhas ---------------------------------------------------------

// tcpProxy encaminha conexões para target e simula indisponibilidade:
//   - reset: derruba as conexões abertas e recusa (fecha na hora) as novas;
//   - pause: congela o tráfego das abertas e segura as novas sem conectar
//     (como docker pause: nada é recusado, nada responde).
type tcpProxy struct {
	t      *testing.T
	target string
	ln     net.Listener

	mu     sync.Mutex
	cond   *sync.Cond
	mode   string // "", "reset", "pause"
	closed bool
	conns  map[net.Conn]struct{}
	wg     sync.WaitGroup
}

func newTCPProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &tcpProxy{t: t, target: target, ln: ln, conns: map[net.Conn]struct{}{}}
	p.cond = sync.NewCond(&p.mu)
	p.wg.Add(1)
	go p.accept()
	t.Cleanup(p.close)
	return p
}

func (p *tcpProxy) addr() string { return p.ln.Addr().String() }

// rewriteURL troca o host:porta de raw pelo do proxy.
func (p *tcpProxy) rewriteURL(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	u.Host = p.addr()
	return u.String()
}

func (p *tcpProxy) accept() {
	defer p.wg.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.wg.Add(1)
		go p.serve(c)
	}
}

// track registra a conexão; false se o proxy fechou.
func (p *tcpProxy) track(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.conns[c] = struct{}{}
	return true
}

func (p *tcpProxy) untrack(c net.Conn) {
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
	_ = c.Close()
}

// gate espera enquanto pausado; false se deve derrubar (reset ou fechado).
func (p *tcpProxy) gate() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.mode == "pause" && !p.closed {
		p.cond.Wait()
	}
	return p.mode != "reset" && !p.closed
}

func (p *tcpProxy) serve(client net.Conn) {
	defer p.wg.Done()
	if !p.track(client) {
		_ = client.Close()
		return
	}
	defer p.untrack(client)
	if !p.gate() {
		return
	}
	upstream, err := net.DialTimeout("tcp", p.target, 5*time.Second)
	if err != nil {
		return
	}
	if !p.track(upstream) {
		_ = upstream.Close()
		return
	}
	defer p.untrack(upstream)
	var inner sync.WaitGroup
	inner.Add(2)
	pipe := func(dst, src net.Conn) {
		defer inner.Done()
		defer func() { _ = dst.Close(); _ = src.Close() }()
		buf := make([]byte, 32*1024)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				if !p.gate() {
					return
				}
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}
	go pipe(upstream, client)
	go pipe(client, upstream)
	inner.Wait()
}

// outage deixa a dependência indisponível no modo dado ("reset" ou "pause").
func (p *tcpProxy) outage(mode string) {
	p.mu.Lock()
	p.mode = mode
	var drop []net.Conn
	if mode == "reset" {
		for c := range p.conns {
			drop = append(drop, c)
		}
	}
	p.mu.Unlock()
	for _, c := range drop {
		_ = c.Close()
	}
}

// restore volta ao normal (o tráfego pausado segue).
func (p *tcpProxy) restore() {
	p.mu.Lock()
	p.mode = ""
	p.cond.Broadcast()
	p.mu.Unlock()
}

func (p *tcpProxy) close() {
	p.mu.Lock()
	p.closed = true
	p.cond.Broadcast()
	conns := slices.Collect(maps.Keys(p.conns))
	p.mu.Unlock()
	_ = p.ln.Close()
	for _, c := range conns {
		_ = c.Close()
	}
	p.wg.Wait()
}

var _ io.Writer = (*syncBuffer)(nil)

// miMaxReceiveCount é o maxReceiveCount do redrive das filas dos testes
// multi-instância: maior que o padrão do Compose (5) para que uma mensagem
// atravesse as duas janelas de indisponibilidade dos testes de falha dentro
// do orçamento de tentativas (SQS_MAX_ATTEMPTS = 8, backoff 1s..2s ≈ 13s)
// sem ir à DLQ.
const miMaxReceiveCount = 10

// createMIQueues cria filas FIFO exclusivas no LocalStack (entrada com
// redrive para a DLQ e fila de eventos sem deduplicação por conteúdo).
func createMIQueues(t *testing.T, api *awssqs.Client) ccQueues {
	t.Helper()
	ctx := t.Context()
	name := "mi-" + randomHex(t, 6)
	create := func(qname string, attrs map[string]string) string {
		attrs["FifoQueue"] = "true"
		out, err := api.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String(qname), Attributes: attrs})
		if err != nil {
			t.Fatalf("create queue %s: %v", qname, err)
		}
		u := aws.ToString(out.QueueUrl)
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, _ = api.DeleteQueue(ctx, &awssqs.DeleteQueueInput{QueueUrl: aws.String(u)})
		})
		return u
	}
	dlq := create(name+"-dlq.fifo", map[string]string{})
	attrs, err := api.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl: aws.String(dlq), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("DLQ arn: %v", err)
	}
	redrive := fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":"%d"}`,
		attrs.Attributes[string(types.QueueAttributeNameQueueArn)], miMaxReceiveCount)
	inbound := create(name+".fifo", map[string]string{"VisibilityTimeout": "30", "RedrivePolicy": redrive})
	events := create(name+"-events.fifo", map[string]string{"ContentBasedDeduplication": "false"})
	return ccQueues{inbound: inbound, dlq: dlq, events: events}
}

// withoutFx remove do log os eventos do ciclo de vida do Fx (ruído).
func withoutFx(s string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if !strings.Contains(l, `"component":"fx"`) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}
