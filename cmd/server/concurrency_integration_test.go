//go:build integration

package main

// Testes de concorrência obrigatórios (T25, README §8 e "Testes de
// concorrência e recuperação"), pela aplicação completa (cmd/server.Options):
// HTTP real (servidor + middleware OIDC com o IdP de teste deste pacote),
// casos de uso e repositórios PostgreSQL reais, consumidor SQS real.
//
//   - 50 envios paralelos da mesma aposta → um débito;
//   - 80.00 x 2 sobre 100.00 → 1 PROCESSED, 1 REJECTED INSUFFICIENT_FUNDS,
//     saldo 20.00, um débito; reenvios sem efeito;
//   - carteiras distintas em paralelo, inclusive com uma carteira travada
//     (prova de que não há lock global);
//   - a mesma operação via HTTP e SQS ao mesmo tempo (LocalStack real:
//     SQS_ENDPOINT; sem ele esses testes são pulados);
//   - ao final de cada cenário: saldo armazenado == soma do ledger (SQL e
//     POST /wallets/{id}/reconciliation), sem débito duplicado e outbox
//     coerente (um evento terminal por transação, um WalletBalanceChanged por
//     lançamento).
//
// Os cenários sem SQS de entrada usam a sqs.API em memória do pacote (fila de
// entrada vazia), porque a entrada exercitada ali é só o HTTP.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	httpserver "github.com/junglegaming/backend-challenge-go/internal/adapters/http/server"
	"github.com/junglegaming/backend-challenge-go/internal/adapters/sqs"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
	"github.com/junglegaming/backend-challenge-go/internal/platform/logging"
)

const (
	ccProvider     = "provider-a"
	ccConsumerName = config.DefaultSQSConsumerName
)

// ccHarness é uma instância da aplicação completa sobre um banco exclusivo.
type ccHarness struct {
	t        *testing.T
	base     string
	client   *http.Client
	verify   *pgxpool.Pool // conexões próprias do teste (verificação)
	internal string        // token do serviço interno
	provider string        // token do provider-a
	// SQS real (nil sem LocalStack).
	sqsAPI  *awssqs.Client
	inbound string
	dlq     string
}

type ccQueues struct{ inbound, dlq, events string }

// createRealQueues cria filas FIFO exclusivas no LocalStack: entrada com
// redrive para a DLQ e a fila de eventos.
func createRealQueues(t *testing.T, api *awssqs.Client) ccQueues {
	t.Helper()
	ctx := t.Context()
	name := "cc-" + randomHex(t, 6)
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
	redrive := fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":"5"}`,
		attrs.Attributes[string(types.QueueAttributeNameQueueArn)])
	inbound := create(name+".fifo", map[string]string{"VisibilityTimeout": "30", "RedrivePolicy": redrive})
	events := create(name+"-events.fifo", map[string]string{"ContentBasedDeduplication": "false"})
	return ccQueues{inbound: inbound, dlq: dlq, events: events}
}

// startHarness sobe a aplicação. realSQS: consumidor e publisher contra o
// LocalStack (pula sem SQS_ENDPOINT); senão a sqs.API em memória.
func startHarness(t *testing.T, realSQS bool) *ccHarness {
	t.Helper()
	endpoint := strings.TrimSuffix(os.Getenv("SQS_ENDPOINT"), "/")
	if realSQS && endpoint == "" {
		t.Skip("SQS_ENDPOINT ausente: LocalStack indisponível (docker compose up -d --wait localstack)")
	}
	dsn := migratedDSN(t)
	idp := newTestIdP(t)
	t.Cleanup(idp.srv.Close) // depois do stop da aplicação (LIFO)
	env := fakeEnv(dsn, idp)
	env[config.EnvLogLevel] = "warn"
	env[config.EnvDatabaseMaxConns] = "16"
	env[config.EnvHTTPWriteTimeout] = "60s"
	h := &ccHarness{t: t}

	var opts []fx.Option
	if realSQS {
		awsCfg := config.AWSConfig{
			Region: envOr("AWS_REGION", "us-east-1"), AccessKeyID: envOr("AWS_ACCESS_KEY_ID", "test"),
			SecretAccessKey: config.Secret(envOr("AWS_SECRET_ACCESS_KEY", "test")), EndpointURL: endpoint,
		}
		c, err := sqs.NewClient(t.Context(), awsCfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close(context.Background()) })
		q := createRealQueues(t, c.SQS())
		h.sqsAPI, h.inbound, h.dlq = c.SQS(), q.inbound, q.dlq
		env[config.EnvAWSRegion] = awsCfg.Region
		env[config.EnvAWSAccessKeyID] = awsCfg.AccessKeyID
		env[config.EnvAWSSecretAccessKey] = string(awsCfg.SecretAccessKey)
		env[config.EnvAWSEndpointURL] = endpoint
		env[config.EnvSQSInboundQueueURL] = q.inbound
		env[config.EnvSQSInboundDLQURL] = q.dlq
		env[config.EnvSQSEventsQueueURL] = q.events
		env[config.EnvSQSConsumerConcurrency] = "8"
	} else {
		fake := &fakeSQS{}
		opts = append(opts, fx.Decorate(func(sqs.API) sqs.API { return fake }))
	}
	cfg := loadEnv(t, env)

	var srv *httpserver.Server
	opts = append(opts, logging.WithOutput(io.Discard), fx.Populate(&srv))
	a := newApp(cfg, opts...)
	if err := a.Err(); err != nil {
		t.Fatalf("fx.New: %v", err)
	}
	startCtx, cancel := context.WithTimeout(context.Background(), a.StartTimeout())
	defer cancel()
	if err := a.Start(startCtx); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), a.StopTimeout())
		defer cancel()
		if err := a.Stop(ctx); err != nil {
			t.Errorf("stop: %v", err)
		}
	})

	verify, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(verify.Close)
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = 128
	t.Cleanup(tr.CloseIdleConnections)

	h.base = "http://" + srv.Addr().String()
	h.client = &http.Client{Transport: tr, Timeout: 60 * time.Second}
	h.verify = verify
	h.internal = idp.internalToken(t)
	h.provider = idp.providerToken(t, ccProvider)
	return h
}

// call faz uma requisição; seguro em goroutines (não chama t.Fatal).
func (h *ccHarness) call(method, path, token, body string, hdr map[string]string) (int, map[string]any, error) {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, h.base+path, rd)
	if err != nil {
		return 0, nil, err
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	var out map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			return resp.StatusCode, nil, fmt.Errorf("JSON inválido %q: %w", raw, err)
		}
	}
	return resp.StatusCode, out, nil
}

type ccWallet struct{ id, player string }

func (h *ccHarness) openWallet(amount string) ccWallet {
	h.t.Helper()
	player := uuid.Must(uuid.NewV7()).String()
	code, body, err := h.call(http.MethodPost, "/wallets", h.internal,
		fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":%q,"currency":"BRL"}}`, player, amount), nil)
	if err != nil || code != http.StatusCreated {
		h.t.Fatalf("POST /wallets = %d %v %v", code, body, err)
	}
	return ccWallet{id: body["id"].(string), player: player}
}

// ccWager é uma operação de provedor (mesmos campos no HTTP e no SQS).
type ccWager struct {
	w           ccWallet
	ext, kind   string
	amount, ref string
	roundID     string
}

func bet(w ccWallet, ext, amount string) ccWager {
	return ccWager{w: w, ext: ext, kind: "BET", amount: amount, roundID: "round-" + ext}
}

func (o ccWager) key() string {
	return ccProvider + ":" + o.ext
}

func (o ccWager) data(withKey bool) map[string]any {
	d := map[string]any{
		"providerId": ccProvider, "externalTransactionId": o.ext, "playerId": o.w.player,
		"walletId": o.w.id, "roundId": o.roundID, "gameId": "fortune-chimp", "kind": o.kind,
		"money": map[string]string{"amount": o.amount, "currency": "BRL"},
	}
	if o.ref != "" {
		d["referenceExternalTransactionId"] = o.ref
	}
	if withKey {
		d["idempotencyKey"] = o.key()
	}
	return d
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// ccResult é a resposta de POST /wagering/transactions.
type ccResult struct {
	code                      int
	txID, status, balance, fc string
	replay                    bool
}

func (h *ccHarness) submit(o ccWager) (ccResult, error) {
	code, body, err := h.call(http.MethodPost, "/wagering/transactions", h.provider,
		mustJSON(o.data(false)), map[string]string{"Idempotency-Key": o.key()})
	if err != nil {
		return ccResult{}, err
	}
	r := ccResult{code: code}
	r.txID, _ = body["transactionId"].(string)
	r.status, _ = body["status"].(string)
	r.fc, _ = body["failureCode"].(string)
	r.replay, _ = body["idempotentReplay"].(bool)
	if b, ok := body["balance"].(map[string]any); ok {
		r.balance, _ = b["amount"].(string)
	}
	if r.txID == "" {
		return r, fmt.Errorf("resposta sem transactionId: %d %v", code, body)
	}
	return r, nil
}

// submitParallel envia as operações ao mesmo tempo (barreira de largada).
func (h *ccHarness) submitParallel(ops []ccWager) []ccResult {
	h.t.Helper()
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		start = make(chan struct{})
		errs  []error
		out   = make([]ccResult, len(ops))
	)
	for i, o := range ops {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r, err := h.submit(o)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", o.ext, err))
			}
			out[i] = r
		}()
	}
	close(start)
	wg.Wait()
	if len(errs) != 0 {
		h.t.Fatalf("envios falharam: %v", errs)
	}
	return out
}

// sendSQS publica a operação na fila de entrada real como
// WagerTransactionRequested. Cada envio tem deduplicação própria (o SQS não
// deduplica): a deduplicação exercitada é a da aplicação (inbox + chave).
func (h *ccHarness) sendSQS(o ccWager, messageID, group string) error {
	body := mustJSON(map[string]any{
		"messageId": messageID, "type": sqs.MessageTypeWagerTransactionRequested,
		"occurredAt": time.Now().UTC().Format(time.RFC3339Nano), "data": o.data(true),
	})
	_, err := h.sqsAPI.SendMessage(context.Background(), &awssqs.SendMessageInput{
		QueueUrl: aws.String(h.inbound), MessageBody: aws.String(body),
		MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(uuid.NewString()),
	})
	return err
}

// --- Verificações no banco ---------------------------------------------------

func (h *ccHarness) scalar(q string, args ...any) int64 {
	h.t.Helper()
	var v int64
	if err := h.verify.QueryRow(h.t.Context(), q, args...).Scan(&v); err != nil {
		h.t.Fatalf("%s: %v", q, err)
	}
	return v
}

// walletState devolve saldo (centavos), versão, créditos e débitos.
func (h *ccHarness) walletState(id string) (balance, version, credits, debits int64) {
	h.t.Helper()
	err := h.verify.QueryRow(h.t.Context(), `SELECT w.balance, w.version,
 (SELECT count(*) FROM wallet_ledger_entries l WHERE l.wallet_id = w.id AND l.direction = 'CREDIT'),
 (SELECT count(*) FROM wallet_ledger_entries l WHERE l.wallet_id = w.id AND l.direction = 'DEBIT')
 FROM wallets w WHERE w.id = $1`, id).Scan(&balance, &version, &credits, &debits)
	if err != nil {
		h.t.Fatal(err)
	}
	return
}

func (h *ccHarness) expectWallet(id string, balance, version, credits, debits int64) {
	h.t.Helper()
	b, v, c, d := h.walletState(id)
	if b != balance || v != version || c != credits || d != debits {
		h.t.Fatalf("carteira %s: saldo %d v%d créditos %d débitos %d; quer %d v%d %d %d",
			id, b, v, c, d, balance, version, credits, debits)
	}
}

// assertConsistent é a reconciliação final do banco inteiro (cada teste tem
// o seu): saldo armazenado == créditos − débitos do ledger e cadeia
// before/after íntegra em toda carteira; nenhum lançamento duplicado por
// transação; outbox coerente; e POST /wallets/{id}/reconciliation consistente.
func (h *ccHarness) assertConsistent() {
	h.t.Helper()
	checks := []struct{ what, q string }{
		{"saldo armazenado != soma do ledger", `SELECT count(*) FROM wallets w WHERE w.balance <>
 COALESCE((SELECT sum(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END)
           FROM wallet_ledger_entries WHERE wallet_id = w.id), 0)`},
		{"cadeia do ledger quebrada", `SELECT count(*) FROM (
 SELECT balance_before, lag(balance_after) OVER (PARTITION BY wallet_id ORDER BY seq) AS prev
 FROM wallet_ledger_entries) x WHERE prev IS NOT NULL AND prev <> balance_before`},
		{"saldo negativo", `SELECT count(*) FROM wallets WHERE balance < 0`},
		{"transação com mais de um lançamento", `SELECT count(*) FROM (SELECT transaction_id
 FROM wallet_ledger_entries GROUP BY transaction_id HAVING count(*) > 1) x`},
		{"lançamento de transação não PROCESSED", `SELECT count(*) FROM wallet_ledger_entries l
 JOIN wager_transactions t ON t.id = l.transaction_id WHERE t.status <> 'PROCESSED'`},
		{"operação externa duplicada", `SELECT count(*) FROM (SELECT 1 FROM wager_transactions WHERE provider_id IS NOT NULL
 GROUP BY provider_id, external_transaction_id HAVING count(*) > 1) x`},
		{"WalletBalanceChanged != um por lançamento", `SELECT
 (SELECT count(*) FROM wallet_ledger_entries l WHERE (SELECT count(*) FROM outbox o
   WHERE o.event_type = 'WalletBalanceChanged'
     AND o.payload::jsonb -> 'data' ->> 'transactionId' = l.transaction_id::text) <> 1)
 + abs((SELECT count(*) FROM outbox WHERE event_type = 'WalletBalanceChanged')
       - (SELECT count(*) FROM wallet_ledger_entries))`},
		{"PROCESSED sem exatamente um WagerTransactionProcessed", `SELECT count(*) FROM wager_transactions t
 WHERE t.status = 'PROCESSED' AND (
   (SELECT count(*) FROM outbox o WHERE o.aggregate_id = t.id::text AND o.event_type = 'WagerTransactionProcessed') <> 1
   OR EXISTS (SELECT 1 FROM outbox o WHERE o.aggregate_id = t.id::text AND o.event_type = 'WagerTransactionRejected'))`},
		{"REJECTED sem exatamente um WagerTransactionRejected", `SELECT count(*) FROM wager_transactions t
 WHERE t.status = 'REJECTED' AND (
   (SELECT count(*) FROM outbox o WHERE o.aggregate_id = t.id::text AND o.event_type = 'WagerTransactionRejected') <> 1
   OR EXISTS (SELECT 1 FROM outbox o WHERE o.aggregate_id = t.id::text AND o.event_type = 'WagerTransactionProcessed'))`},
		{"eventos terminais sem transação", `SELECT count(*) FROM outbox o
 WHERE o.event_type IN ('WagerTransactionProcessed', 'WagerTransactionRejected')
   AND NOT EXISTS (SELECT 1 FROM wager_transactions t WHERE t.id::text = o.aggregate_id)`},
	}
	for _, c := range checks {
		if n := h.scalar(c.q); n != 0 {
			h.t.Fatalf("reconciliação: %s (%d)", c.what, n)
		}
	}
	rows, err := h.verify.Query(h.t.Context(), `SELECT id::text FROM wallets`)
	if err != nil {
		h.t.Fatal(err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			h.t.Fatal(err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		code, body, err := h.call(http.MethodPost, "/wallets/"+id+"/reconciliation", h.internal, "", nil)
		if err != nil || code != http.StatusOK || body["consistent"] != true {
			h.t.Fatalf("reconciliação %s = %d %v %v", id, code, body, err)
		}
		if d, _ := body["difference"].(map[string]any); d["amount"] != "0.00" {
			h.t.Fatalf("reconciliação %s: diferença %v", id, body["difference"])
		}
	}
}

func (h *ccHarness) outboxCount() int64 { return h.scalar(`SELECT count(*) FROM outbox`) }

// --- Testes (HTTP) -----------------------------------------------------------

// 50 envios paralelos da mesma aposta (mesma chave e corpo) pelo HTTP: um
// único débito; todas as respostas são a mesma transação, uma só "fresca", e
// todas devolvem o saldo observado no processamento original.
func TestIntegrationConcurrencySameBet50HTTP(t *testing.T) {
	h := startHarness(t, false)
	w := h.openWallet("100.00")
	op := bet(w, "same-bet-50", "25.00")

	ops := make([]ccWager, 50)
	for i := range ops {
		ops[i] = op
	}
	results := h.submitParallel(ops)
	fresh := 0
	for _, r := range results {
		if r.code != http.StatusOK || r.status != "PROCESSED" || r.txID != results[0].txID || r.balance != "75.00" {
			t.Fatalf("resposta %+v (primeira %+v)", r, results[0])
		}
		if !r.replay {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("%d respostas não-replay, quer 1", fresh)
	}
	h.expectWallet(w.id, 7500, 2, 1, 1)
	if n := h.scalar(`SELECT count(*) FROM wager_transactions WHERE kind = 'BET'`); n != 1 {
		t.Fatalf("%d BETs gravadas", n)
	}

	// Reenvios posteriores: replay, sem efeito (nem eventos novos).
	before := h.outboxCount()
	for _, r := range h.submitParallel(ops[:10]) {
		if !r.replay || r.txID != results[0].txID || r.balance != "75.00" {
			t.Fatalf("reenvio %+v", r)
		}
	}
	if after := h.outboxCount(); after != before {
		t.Fatalf("reenvios gravaram %d eventos", after-before)
	}
	h.expectWallet(w.id, 7500, 2, 1, 1)
	h.assertConsistent()
}

// Cenário obrigatório: duas apostas distintas de 80.00 ao mesmo tempo sobre
// 100.00 (em várias carteiras, para variar as intercalações): uma PROCESSED,
// uma REJECTED INSUFFICIENT_FUNDS, saldo 20.00, um débito. Reenvios das duas,
// também em paralelo, não alteram o resultado.
func TestIntegrationConcurrencyTwo80BetsOn100HTTP(t *testing.T) {
	h := startHarness(t, false)
	const rounds = 5
	for i := range rounds {
		w := h.openWallet("100.00")
		ops := []ccWager{bet(w, fmt.Sprintf("b80-%d-a", i), "80.00"), bet(w, fmt.Sprintf("b80-%d-b", i), "80.00")}
		first := h.submitParallel(ops)
		var processed, rejected int
		for _, r := range first {
			switch {
			case r.code == http.StatusOK && r.status == "PROCESSED" && r.balance == "20.00" && !r.replay:
				processed++
			case r.code == http.StatusUnprocessableEntity && r.status == "REJECTED" &&
				r.fc == "INSUFFICIENT_FUNDS" && !r.replay:
				rejected++
			default:
				t.Fatalf("rodada %d: resposta %+v", i, r)
			}
		}
		if processed != 1 || rejected != 1 {
			t.Fatalf("rodada %d: processed %d rejected %d", i, processed, rejected)
		}
		h.expectWallet(w.id, 2000, 2, 1, 1)

		// Reenvios (5 de cada, em paralelo): replays idênticos ao original.
		before := h.outboxCount()
		var resend []ccWager
		for range 5 {
			resend = append(resend, ops...)
		}
		for j, r := range h.submitParallel(resend) {
			orig := first[j%2]
			if !r.replay || r.code != orig.code || r.txID != orig.txID || r.status != orig.status || r.fc != orig.fc {
				t.Fatalf("rodada %d: reenvio %+v, original %+v", i, r, orig)
			}
		}
		if after := h.outboxCount(); after != before {
			t.Fatalf("rodada %d: reenvios gravaram %d eventos", i, after-before)
		}
		h.expectWallet(w.id, 2000, 2, 1, 1)
	}
	h.assertConsistent()
}

// Carteiras distintas avançam em paralelo: com a linha de uma carteira
// travada por outra transação, as apostas das demais carteiras concluem; a
// da carteira travada só termina depois do unlock. Em seguida, várias
// carteiras recebem a disputa 80/80 ao mesmo tempo, cada uma com o seu
// resultado correto.
func TestIntegrationConcurrencyDistinctWalletsParallel(t *testing.T) {
	h := startHarness(t, false)
	const n = 8
	wallets := make([]ccWallet, n)
	for i := range wallets {
		wallets[i] = h.openWallet("100.00")
	}

	// Trava a carteira 0 (como faria um escritor concorrente demorado).
	lockTx, err := h.verify.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(context.Background()) }()
	if _, err := lockTx.Exec(t.Context(), `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, wallets[0].id); err != nil {
		t.Fatal(err)
	}

	type outcome struct {
		i   int
		r   ccResult
		err error
	}
	done := make(chan outcome, n)
	for i, w := range wallets {
		go func() {
			r, err := h.submit(bet(w, fmt.Sprintf("par-%d", i), "10.00"))
			done <- outcome{i, r, err}
		}()
	}
	got := map[int]ccResult{}
	timeout := time.After(30 * time.Second)
	for len(got) < n-1 {
		select {
		case o := <-done:
			if o.err != nil {
				t.Fatal(o.err)
			}
			if o.i == 0 {
				t.Fatalf("a aposta da carteira travada concluiu antes do unlock: %+v", o.r)
			}
			got[o.i] = o.r
		case <-timeout:
			t.Fatalf("carteiras livres bloqueadas pela travada: %d/%d concluídas", len(got), n-1)
		}
	}
	// A da carteira travada continua esperando o lock da linha.
	select {
	case o := <-done:
		t.Fatalf("a aposta da carteira travada concluiu antes do unlock: %+v", o)
	case <-time.After(300 * time.Millisecond):
	}
	if err := lockTx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case o := <-done:
		if o.err != nil {
			t.Fatal(o.err)
		}
		got[o.i] = o.r
	case <-time.After(30 * time.Second):
		t.Fatal("a aposta da carteira travada não concluiu depois do unlock")
	}
	for i, r := range got {
		if r.code != http.StatusOK || r.status != "PROCESSED" || r.balance != "90.00" {
			t.Fatalf("carteira %d: %+v", i, r)
		}
		h.expectWallet(wallets[i].id, 9000, 2, 1, 1)
	}

	// Disputa 80/80 simultânea em todas as carteiras (2n envios juntos).
	var ops []ccWager
	for i, w := range wallets {
		ops = append(ops, bet(w, fmt.Sprintf("par80-%d-a", i), "80.00"), bet(w, fmt.Sprintf("par80-%d-b", i), "80.00"))
	}
	results := h.submitParallel(ops)
	for i := range wallets {
		a, b := results[2*i], results[2*i+1]
		if a.status == b.status {
			t.Fatalf("carteira %d: %+v / %+v", i, a, b)
		}
		for _, r := range []ccResult{a, b} {
			ok := (r.status == "PROCESSED" && r.balance == "10.00") ||
				(r.status == "REJECTED" && r.fc == "INSUFFICIENT_FUNDS")
			if !ok {
				t.Fatalf("carteira %d: %+v", i, r)
			}
		}
		h.expectWallet(wallets[i].id, 1000, 3, 1, 2)
	}
	h.assertConsistent()
}

// --- Testes (HTTP x SQS, LocalStack) -----------------------------------------

// waitSQS espera a fila de entrada esvaziar (nada visível nem em voo) e a
// inbox registrar as mensagens esperadas como concluídas.
func (h *ccHarness) waitSQS(inboxCompleted int) {
	h.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		done := h.scalar(`SELECT count(*) FROM inbox WHERE consumer_name = $1 AND completed_at IS NOT NULL`, ccConsumerName)
		out, err := h.sqsAPI.GetQueueAttributes(h.t.Context(), &awssqs.GetQueueAttributesInput{
			QueueUrl: aws.String(h.inbound),
			AttributeNames: []types.QueueAttributeName{
				types.QueueAttributeNameApproximateNumberOfMessages,
				types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			},
		})
		if err != nil {
			h.t.Fatal(err)
		}
		empty := out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)] == "0" &&
			out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)] == "0"
		if empty && done == int64(inboxCompleted) {
			break
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("SQS não drenou: inbox concluída %d/%d, atributos %v", done, inboxCompleted, out.Attributes)
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Nada foi para a DLQ (nenhum conflito de hash entre HTTP e SQS).
	out, err := h.sqsAPI.ReceiveMessage(h.t.Context(), &awssqs.ReceiveMessageInput{
		QueueUrl: aws.String(h.dlq), MaxNumberOfMessages: 10, WaitTimeSeconds: 0,
	})
	if err != nil {
		h.t.Fatal(err)
	}
	if len(out.Messages) != 0 {
		h.t.Fatalf("%d mensagens na DLQ: %s", len(out.Messages), aws.ToString(out.Messages[0].Body))
	}
}

// A mesma operação entra ao mesmo tempo por HTTP (25 envios) e por SQS (25
// entregas: 20 messageIds distintos e 5 reentregas do mesmo messageId, cada
// uma com MessageDeduplicationId próprio e grupo próprio, para o SQS não
// deduplicar nem serializar). Resultado: uma única transação e um único
// débito; toda entrada da inbox aponta para essa transação; a fila e a DLQ
// ficam vazias.
func TestIntegrationConcurrencySameBetHTTPAndSQS(t *testing.T) {
	h := startHarness(t, true)
	w := h.openWallet("100.00")
	op := bet(w, "cross-bet", "10.00")

	const httpN, sqsDistinct, sqsRepeat = 25, 20, 5
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		start = make(chan struct{})
		errs  []error
		https []ccResult
	)
	fail := func(err error) {
		mu.Lock()
		errs = append(errs, err)
		mu.Unlock()
	}
	for i := range sqsDistinct + sqsRepeat {
		msgID := fmt.Sprintf("cross-msg-%d", i)
		if i >= sqsDistinct {
			msgID = "cross-msg-repeat"
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := h.sendSQS(op, msgID, uuid.NewString()); err != nil {
				fail(err)
			}
		}()
	}
	for range httpN {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r, err := h.submit(op)
			if err != nil {
				fail(err)
				return
			}
			mu.Lock()
			https = append(https, r)
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	if len(errs) != 0 {
		t.Fatalf("envios falharam: %v", errs)
	}
	h.waitSQS(sqsDistinct + 1)

	txID := https[0].txID
	for _, r := range https {
		if r.code != http.StatusOK || r.status != "PROCESSED" || r.txID != txID || r.balance != "90.00" {
			t.Fatalf("resposta HTTP %+v (primeira %+v)", r, https[0])
		}
	}
	if n := h.scalar(`SELECT count(*) FROM inbox WHERE consumer_name = $1 AND transaction_id = $2`, ccConsumerName, txID); n != sqsDistinct+1 {
		t.Fatalf("inbox: %d mensagens apontam para a transação, quer %d", n, sqsDistinct+1)
	}
	if n := h.scalar(`SELECT count(*) FROM wager_transactions WHERE kind = 'BET'`); n != 1 {
		t.Fatalf("%d BETs gravadas", n)
	}
	h.expectWallet(w.id, 9000, 2, 1, 1)
	h.assertConsistent()
}

// Disputa 80/80 com uma aposta por transporte (HTTP e SQS) em várias
// carteiras ao mesmo tempo: em cada carteira uma PROCESSED e uma REJECTED
// INSUFFICIENT_FUNDS, saldo 20.00 e um débito. Depois cada operação é
// reenviada pelo outro transporte: replays, sem efeito.
func TestIntegrationConcurrencyTwo80BetsHTTPAndSQS(t *testing.T) {
	h := startHarness(t, true)
	const n = 4
	wallets := make([]ccWallet, n)
	for i := range wallets {
		wallets[i] = h.openWallet("100.00")
	}
	viaHTTP := make([]ccWager, n)
	viaSQS := make([]ccWager, n)
	for i, w := range wallets {
		viaHTTP[i] = bet(w, fmt.Sprintf("x80-%d-http", i), "80.00")
		viaSQS[i] = bet(w, fmt.Sprintf("x80-%d-sqs", i), "80.00")
	}

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		start = make(chan struct{})
		errs  []error
		httpR = make([]ccResult, n)
	)
	for i := range n {
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			// Contrato do produtor (ADR-022): MessageGroupId = walletId.
			if err := h.sendSQS(viaSQS[i], "x80-msg-"+viaSQS[i].ext, wallets[i].id); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			r, err := h.submit(viaHTTP[i])
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
			}
			httpR[i] = r
		}()
	}
	close(start)
	wg.Wait()
	if len(errs) != 0 {
		t.Fatalf("envios falharam: %v", errs)
	}
	h.waitSQS(n)

	status := func(ext string) (string, string) {
		var st string
		var fc *string
		if err := h.verify.QueryRow(t.Context(), `SELECT status, failure_code FROM wager_transactions
 WHERE provider_id = $1 AND external_transaction_id = $2`, ccProvider, ext).Scan(&st, &fc); err != nil {
			t.Fatalf("%s: %v", ext, err)
		}
		if fc == nil {
			return st, ""
		}
		return st, *fc
	}
	type final struct{ status, fc string }
	finals := map[string]final{}
	for i, w := range wallets {
		hs, hfc := status(viaHTTP[i].ext)
		ss, sfc := status(viaSQS[i].ext)
		if httpR[i].status != hs || httpR[i].fc != hfc {
			t.Fatalf("carteira %d: resposta HTTP %+v, banco %s %s", i, httpR[i], hs, hfc)
		}
		got := []final{{hs, hfc}, {ss, sfc}}
		processed, rejected := 0, 0
		for _, f := range got {
			switch {
			case f.status == "PROCESSED" && f.fc == "":
				processed++
			case f.status == "REJECTED" && f.fc == "INSUFFICIENT_FUNDS":
				rejected++
			}
		}
		if processed != 1 || rejected != 1 {
			t.Fatalf("carteira %d: HTTP %s %s, SQS %s %s", i, hs, hfc, ss, sfc)
		}
		finals[viaHTTP[i].ext], finals[viaSQS[i].ext] = got[0], got[1]
		h.expectWallet(w.id, 2000, 2, 1, 1)
	}

	// Reenvio cruzado: a operação do HTTP pelo SQS e a do SQS pelo HTTP.
	before := h.outboxCount()
	for i := range n {
		if err := h.sendSQS(viaHTTP[i], "x80-resend-"+viaHTTP[i].ext, wallets[i].id); err != nil {
			t.Fatal(err)
		}
	}
	for i, r := range h.submitParallel(viaSQS) {
		f := finals[viaSQS[i].ext]
		if !r.replay || r.status != f.status || r.fc != f.fc {
			t.Fatalf("reenvio HTTP de %s: %+v, original %+v", viaSQS[i].ext, r, f)
		}
	}
	h.waitSQS(2 * n)
	for i := range n {
		if st, fc := status(viaHTTP[i].ext); st != finals[viaHTTP[i].ext].status || fc != finals[viaHTTP[i].ext].fc {
			t.Fatalf("reenvio SQS alterou %s: %s %s", viaHTTP[i].ext, st, fc)
		}
	}
	if after := h.outboxCount(); after != before {
		t.Fatalf("reenvios gravaram %d eventos", after-before)
	}
	for _, w := range wallets {
		h.expectWallet(w.id, 2000, 2, 1, 1)
	}
	h.assertConsistent()
}
