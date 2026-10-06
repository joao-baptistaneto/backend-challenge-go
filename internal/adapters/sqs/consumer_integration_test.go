//go:build integration

package sqs

// Integração do consumidor (T20) com LocalStack (SQS real) e PostgreSQL real.
//
// Dependências:
//   - SQS: SQS_ENDPOINT (ou AWS_ENDPOINT_URL), ex.: http://localhost:4566.
//     AWS_REGION/AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY (padrão us-east-1,
//     test/test). Ausente → os testes de integração deste pacote são pulados.
//   - PostgreSQL: TEST_DATABASE_URL (precisa de CREATE/DROP DATABASE) ou,
//     na ausência, container real via testcontainers (mesmo padrão de
//     internal/app e internal/adapters/postgres).
//
// Cada teste cria filas FIFO exclusivas (wager-transactions + DLQ com
// redrive) e um banco exclusivo com todas as migrations.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/goleak"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres/migrate"
	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
	"github.com/junglegaming/backend-challenge-go/migrations"
)

const postgresImage = "postgres:17-alpine"

var (
	adminDSN    string
	sqsEndpoint string
	skipReason  string
)

func TestMain(m *testing.M) { os.Exit(runIntegration(m)) }

// runWithLeakCheck roda os testes (unitários e de integração, com a tag) e
// falha se sobrar alguma goroutine criada por eles: consumidor, cliente SQS,
// pool do PostgreSQL. As goroutines que já existiam (ex.: as do
// testcontainers, iniciado antes) são ignoradas.
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
	sqsEndpoint = os.Getenv("SQS_ENDPOINT")
	if sqsEndpoint == "" {
		sqsEndpoint = os.Getenv("AWS_ENDPOINT_URL")
	}
	if sqsEndpoint == "" {
		skipReason = "SQS_ENDPOINT/AWS_ENDPOINT_URL ausente: LocalStack indisponível"
		return runWithLeakCheck(m)
	}
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

func requireInfra(t *testing.T) {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func randomHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// --- SQS (LocalStack) -------------------------------------------------------

func awsConfig() config.AWSConfig {
	return config.AWSConfig{
		Region:          envOr("AWS_REGION", "us-east-1"),
		AccessKeyID:     envOr("AWS_ACCESS_KEY_ID", "test"),
		SecretAccessKey: config.Secret(envOr("AWS_SECRET_ACCESS_KEY", "test")),
		EndpointURL:     sqsEndpoint,
	}
}

func integrationClient(t *testing.T) *Client {
	t.Helper()
	c, err := NewClient(t.Context(), awsConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

type queues struct{ main, dlq string }

// createQueues cria a fila FIFO de entrada e a DLQ FIFO com redrive
// (maxReceiveCount), como wager-transactions.fifo/wager-transactions-dlq.fifo.
func createQueues(t *testing.T, c *Client, maxReceive int) queues {
	t.Helper()
	ctx := t.Context()
	api := c.SQS()
	name := "it-" + randomHex(t, 6)
	dlq, err := api.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName:  aws.String(name + "-dlq.fifo"),
		Attributes: map[string]string{"FifoQueue": "true"},
	})
	if err != nil {
		t.Fatalf("create DLQ: %v", err)
	}
	attrs, err := api.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl: dlq.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("DLQ arn: %v", err)
	}
	redrive := fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":"%d"}`,
		attrs.Attributes[string(types.QueueAttributeNameQueueArn)], maxReceive)
	main, err := api.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String(name + ".fifo"),
		Attributes: map[string]string{
			"FifoQueue": "true", "VisibilityTimeout": "30", "RedrivePolicy": redrive,
		},
	})
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = api.DeleteQueue(ctx, &awssqs.DeleteQueueInput{QueueUrl: main.QueueUrl})
		_, _ = api.DeleteQueue(ctx, &awssqs.DeleteQueueInput{QueueUrl: dlq.QueueUrl})
	})
	return queues{main: aws.ToString(main.QueueUrl), dlq: aws.ToString(dlq.QueueUrl)}
}

// sendMessage publica com MessageGroupId = group e deduplicação única (cada
// envio é uma entrega distinta, mesmo com corpo igual).
func sendMessage(t *testing.T, c *Client, queueURL, group, body string) {
	t.Helper()
	_, err := c.SQS().SendMessage(t.Context(), &awssqs.SendMessageInput{
		QueueUrl: aws.String(queueURL), MessageBody: aws.String(body),
		MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(uuid.NewString()),
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
}

// queueDepth devolve (visíveis, em voo).
func queueDepth(t *testing.T, c *Client, queueURL string) (int, int) {
	t.Helper()
	out, err := c.SQS().GetQueueAttributes(t.Context(), &awssqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		t.Fatalf("queue attributes: %v", err)
	}
	visible, _ := strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)])
	inflight, _ := strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)])
	return visible, inflight
}

func queueEmpty(t *testing.T, c *Client, queueURL string) bool {
	v, f := queueDepth(t, c, queueURL)
	return v == 0 && f == 0
}

func integrationConfig(q queues) ConsumerConfig {
	return ConsumerConfig{
		QueueURL: q.main, DLQURL: q.dlq, Name: "it-consumer", Concurrency: 4,
		WaitTime: time.Second, VisibilityTimeout: 30 * time.Second, ProcessTimeout: 10 * time.Second,
		RetryBaseDelay: time.Second, RetryMaxDelay: 2 * time.Second, DrainTimeout: 5 * time.Second, MaxAttempts: 2,
	}
}

func startIntegrationConsumer(t *testing.T, cfg ConsumerConfig, api API, proc WagerProcessor) *Consumer {
	t.Helper()
	c, err := NewConsumer(cfg, api, proc)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = c.Stop(ctx)
	})
	return c
}

func waitLong(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout esperando: %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// --- PostgreSQL -------------------------------------------------------------

func migratedDSN(t *testing.T) string {
	t.Helper()
	ctx := t.Context()
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	defer admin.Close(context.Background())
	name := "sqstest_" + randomHex(t, 8)
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

func dbConfig(dsn string) config.DatabaseConfig {
	return config.DatabaseConfig{URL: config.Secret(dsn), MaxConns: 16, ConnectTimeout: 5 * time.Second}
}

type itEnv struct {
	db      *postgres.DB
	uow     app.UnitOfWork
	process *app.ProcessWagerTransaction
	client  *Client
	q       queues
}

func newITEnv(t *testing.T, maxReceive int) itEnv {
	t.Helper()
	requireInfra(t)
	db, err := postgres.NewDB(dbConfig(migratedDSN(t)), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := db.Start(ctx); err != nil {
		t.Fatalf("db start: %v", err)
	}
	t.Cleanup(func() { _ = db.Stop(context.Background()) })
	uow := postgres.NewUnitOfWork(postgres.NewTxManager(db, nil, nil), postgres.NewWalletRepository(),
		postgres.NewLedgerRepository(), postgres.NewWagerTransactionRepository(), postgres.NewOutboxRepository(),
		postgres.NewInboxRepository())
	process, err := app.NewProcessWagerTransaction(uow)
	if err != nil {
		t.Fatal(err)
	}
	client := integrationClient(t)
	return itEnv{db: db, uow: uow, process: process, client: client, q: createQueues(t, client, maxReceive)}
}

func (e itEnv) pool() *pgxpool.Pool { return e.db.Pool() }

// openWallet abre uma carteira com saldo inicial e devolve (walletId, playerId).
func (e itEnv) openWallet(t *testing.T, amount string) (uuid.UUID, uuid.UUID) {
	t.Helper()
	uc, err := app.NewOpenWallet(e.uow)
	if err != nil {
		t.Fatal(err)
	}
	m, err := domain.ParseNonNegative(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	player := uuid.New()
	res, err := uc.Execute(t.Context(), app.OpenWalletCommand{PlayerID: player, InitialBalance: m, CorrelationID: "open-" + player.String()})
	if err != nil {
		t.Fatal(err)
	}
	return res.Wallet.ID, player
}

// bet monta o envelope de uma operação na carteira.
func bet(t *testing.T, messageID, ext string, wallet, player uuid.UUID, kind, amount string) string {
	t.Helper()
	return envelopeJSON(t, messageID, ext, func(_, d map[string]any) {
		d["walletId"] = wallet.String()
		d["playerId"] = player.String()
		d["kind"] = kind
		d["money"] = map[string]any{"amount": amount, "currency": "BRL"}
	})
}

type walletState struct {
	balance, version int64
	ledger           int
}

func (e itEnv) wallet(t *testing.T, id uuid.UUID) walletState {
	t.Helper()
	var s walletState
	err := e.pool().QueryRow(t.Context(), `SELECT w.balance, w.version,
 (SELECT count(*) FROM wallet_ledger_entries l WHERE l.wallet_id = w.id)
 FROM wallets w WHERE w.id = $1`, id).Scan(&s.balance, &s.version, &s.ledger)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (e itEnv) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := e.pool().QueryRow(t.Context(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (e itEnv) completedInbox(t *testing.T) int {
	return e.count(t, `SELECT count(*) FROM inbox WHERE consumer_name = 'it-consumer' AND completed_at IS NOT NULL`)
}

// --- Testes -----------------------------------------------------------------

func TestIntegrationConsumerProcessesMessage(t *testing.T) {
	e := newITEnv(t, 3)
	wallet, player := e.openWallet(t, "100.00")
	sendMessage(t, e.client, e.q.main, wallet.String(), bet(t, "msg-1", "tx-1", wallet, player, "BET", "25.00"))

	startIntegrationConsumer(t, integrationConfig(e.q), e.client.SQS(), e.process)
	waitLong(t, "inbox concluída", func() bool { return e.completedInbox(t) == 1 })
	waitLong(t, "mensagem apagada", func() bool { return queueEmpty(t, e.client, e.q.main) })

	if s := e.wallet(t, wallet); s.balance != 7500 || s.version != 2 || s.ledger != 2 {
		t.Fatalf("carteira inesperada: %+v", s)
	}
	var status, msgTx string
	err := e.pool().QueryRow(t.Context(), `SELECT w.status, i.transaction_id::text FROM inbox i
 JOIN wager_transactions w ON w.id = i.transaction_id WHERE i.message_id = 'msg-1'`).Scan(&status, &msgTx)
	if err != nil {
		t.Fatal(err)
	}
	if status != "PROCESSED" {
		t.Fatalf("status = %s", status)
	}
	// causationId/correlationId dos eventos = messageId do envelope.
	if n := e.count(t, `SELECT count(*) FROM outbox WHERE correlation_id = 'msg-1' AND payload->>'causationId' = 'msg-1'`); n != 2 {
		t.Fatalf("eventos com correlation/causation = messageId: %d", n)
	}
	if v, _ := queueDepth(t, e.client, e.q.dlq); v != 0 {
		t.Fatalf("DLQ deveria estar vazia: %d", v)
	}
}

func TestIntegrationConsumerRedeliveryHasNoEffect(t *testing.T) {
	e := newITEnv(t, 3)
	wallet, player := e.openWallet(t, "100.00")
	body := bet(t, "msg-1", "tx-1", wallet, player, "BET", "25.00")
	group := wallet.String()
	sendMessage(t, e.client, e.q.main, group, body)
	sendMessage(t, e.client, e.q.main, group, body) // reentrega da mesma mensagem
	// Outra mensagem para a mesma operação (mesma chave): replay idempotente.
	sendMessage(t, e.client, e.q.main, group, bet(t, "msg-2", "tx-1", wallet, player, "BET", "25.00"))
	// Mesmo messageId com outro conteúdo: MESSAGE_CONFLICT → DLQ, sem efeito.
	sendMessage(t, e.client, e.q.main, group, bet(t, "msg-1", "tx-1", wallet, player, "BET", "26.00"))

	startIntegrationConsumer(t, integrationConfig(e.q), e.client.SQS(), e.process)
	waitLong(t, "fila vazia", func() bool { return queueEmpty(t, e.client, e.q.main) })
	waitLong(t, "conflito na DLQ", func() bool { v, _ := queueDepth(t, e.client, e.q.dlq); return v == 1 })

	if s := e.wallet(t, wallet); s.balance != 7500 || s.ledger != 2 || s.version != 2 {
		t.Fatalf("reentrega teve efeito: %+v", s)
	}
	if n := e.count(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND kind = 'BET'`, wallet); n != 1 {
		t.Fatalf("transações = %d", n)
	}
	if n := e.completedInbox(t); n != 2 {
		t.Fatalf("inbox concluídas = %d (msg-1 e msg-2)", n)
	}
	if n := e.count(t, `SELECT count(DISTINCT transaction_id) FROM inbox`); n != 1 {
		t.Fatalf("as duas mensagens devem apontar para a mesma transação: %d", n)
	}
}

func TestIntegrationConsumerBusinessRejectionIsDeleted(t *testing.T) {
	e := newITEnv(t, 3)
	wallet, player := e.openWallet(t, "100.00")
	sendMessage(t, e.client, e.q.main, wallet.String(), bet(t, "msg-1", "tx-1", wallet, player, "BET", "500.00"))

	startIntegrationConsumer(t, integrationConfig(e.q), e.client.SQS(), e.process)
	waitLong(t, "inbox concluída", func() bool { return e.completedInbox(t) == 1 })
	waitLong(t, "mensagem apagada", func() bool { return queueEmpty(t, e.client, e.q.main) })

	var status, failure string
	err := e.pool().QueryRow(t.Context(), `SELECT status, failure_code FROM wager_transactions
 WHERE provider_id = 'provider-a' AND external_transaction_id = 'tx-1'`).Scan(&status, &failure)
	if err != nil {
		t.Fatal(err)
	}
	if status != "REJECTED" || failure != "INSUFFICIENT_FUNDS" {
		t.Fatalf("rejeição = %s/%s", status, failure)
	}
	if n := e.count(t, `SELECT count(*) FROM outbox WHERE event_type = 'WagerTransactionRejected'`); n != 1 {
		t.Fatalf("evento de rejeição = %d", n)
	}
	if s := e.wallet(t, wallet); s.balance != 10000 || s.ledger != 1 {
		t.Fatalf("rejeição alterou a carteira: %+v", s)
	}
	if v, _ := queueDepth(t, e.client, e.q.dlq); v != 0 {
		t.Fatalf("rejeição de negócio não vai à DLQ: %d", v)
	}
}

// receiveDLQ lê n mensagens da DLQ (com atributos), indexadas pelo corpo.
func receiveDLQ(t *testing.T, c *Client, queueURL string, n int) map[string]types.Message {
	t.Helper()
	out := map[string]types.Message{}
	deadline := time.Now().Add(30 * time.Second)
	for len(out) < n {
		if time.Now().After(deadline) {
			t.Fatalf("DLQ: esperava %d mensagens, obteve %d", n, len(out))
		}
		res, err := c.SQS().ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{
			QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 1, VisibilityTimeout: 60,
			MessageAttributeNames:       []string{"All"},
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameMessageGroupId},
		})
		if err != nil {
			t.Fatalf("receive DLQ: %v", err)
		}
		for _, m := range res.Messages {
			out[aws.ToString(m.Body)] = m
		}
	}
	return out
}

func dlqAttr(m types.Message, name string) string {
	return aws.ToString(m.MessageAttributes[name].StringValue)
}

func TestIntegrationConsumerInvalidMessagesGoToDLQ(t *testing.T) {
	e := newITEnv(t, 3)
	wallet, player := e.openWallet(t, "100.00")
	cases := []struct{ body, reason, code string }{
		{"not json", "invalid_message", CodeInvalidMessage},
		{bet(t, "msg-opening", "tx-o", wallet, player, "OPENING", "10.00"), "invalid_message", "INVALID_KIND"},
		{bet(t, "msg-zero", "tx-z", wallet, player, "BET", "0.00"), "permanent_error", "INVALID_AMOUNT"},
		{envelopeJSON(t, "msg-type", "tx-t", func(env, _ map[string]any) { env["type"] = "WalletOpened" }), "invalid_message", CodeUnsupportedMessageType},
	}
	for i, tc := range cases {
		sendMessage(t, e.client, e.q.main, fmt.Sprintf("g-%d", i), tc.body)
	}

	startIntegrationConsumer(t, integrationConfig(e.q), e.client.SQS(), e.process)
	waitLong(t, "todas na DLQ", func() bool { v, _ := queueDepth(t, e.client, e.q.dlq); return v == len(cases) })
	waitLong(t, "fila vazia", func() bool { return queueEmpty(t, e.client, e.q.main) })

	// Envio explícito pelo consumidor (não redrive): corpo e grupo preservados,
	// motivo e código nos atributos, já na primeira entrega.
	got := receiveDLQ(t, e.client, e.q.dlq, len(cases))
	for i, tc := range cases {
		m, ok := got[tc.body]
		if !ok {
			t.Fatalf("caso %d ausente da DLQ", i)
		}
		if g := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]; g != fmt.Sprintf("g-%d", i) {
			t.Fatalf("caso %d: MessageGroupId = %q", i, g)
		}
		if dlqAttr(m, DLQAttrReason) != tc.reason || dlqAttr(m, DLQAttrCode) != tc.code ||
			dlqAttr(m, DLQAttrReceiveCount) != "1" || dlqAttr(m, DLQAttrConsumer) != "it-consumer" {
			t.Fatalf("caso %d: atributos inesperados: %+v", i, m.MessageAttributes)
		}
	}

	if n := e.count(t, `SELECT count(*) FROM wager_transactions WHERE origin = 'EXTERNAL'`); n != 0 {
		t.Fatalf("mensagem inválida gerou transação: %d", n)
	}
	if n := e.count(t, `SELECT count(*) FROM inbox`); n != 0 {
		t.Fatalf("mensagem inválida gravou inbox: %d", n)
	}
	if s := e.wallet(t, wallet); s.balance != 10000 || s.ledger != 1 {
		t.Fatalf("carteira alterada: %+v", s)
	}
}

// Uma mensagem venenosa no mesmo MessageGroupId (a carteira) não leva as
// válidas seguintes para a DLQ: ela vai sozinha (envio explícito) e as
// seguintes são processadas em ordem. maxReceiveCount=2 é o cenário em que o
// redrive nativo moveria as seguintes junto.
func TestIntegrationConsumerValidBehindPoisonInSameGroup(t *testing.T) {
	e := newITEnv(t, 2)
	wallet, player := e.openWallet(t, "100.00")
	group := wallet.String()
	poisons := []string{
		"not json",
		bet(t, "msg-opening", "tx-o", wallet, player, "OPENING", "10.00"),
		bet(t, "msg-zero", "tx-z", wallet, player, "BET", "0.00"),
	}
	sendMessage(t, e.client, e.q.main, group, poisons[0])
	sendMessage(t, e.client, e.q.main, group, bet(t, "msg-1", "tx-1", wallet, player, "BET", "10.00"))
	sendMessage(t, e.client, e.q.main, group, poisons[1])
	sendMessage(t, e.client, e.q.main, group, poisons[2])
	sendMessage(t, e.client, e.q.main, group, bet(t, "msg-2", "tx-2", wallet, player, "BET", "20.00"))

	cfg := integrationConfig(e.q)
	cfg.MaxAttempts = 1
	startIntegrationConsumer(t, cfg, e.client.SQS(), e.process)
	waitLong(t, "inbox concluídas", func() bool { return e.completedInbox(t) == 2 })
	waitLong(t, "fila vazia", func() bool { return queueEmpty(t, e.client, e.q.main) })
	waitLong(t, "venenosas na DLQ", func() bool { v, _ := queueDepth(t, e.client, e.q.dlq); return v == len(poisons) })

	if s := e.wallet(t, wallet); s.balance != 7000 || s.ledger != 3 {
		t.Fatalf("as válidas atrás das venenosas devem ser processadas: %+v", s)
	}
	got := receiveDLQ(t, e.client, e.q.dlq, len(poisons))
	for i, p := range poisons {
		m, ok := got[p]
		if !ok {
			t.Fatalf("venenosa %d ausente da DLQ", i)
		}
		if m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)] != group || dlqAttr(m, DLQAttrReason) == "" {
			t.Fatalf("venenosa %d: grupo/motivo inesperados: %+v %+v", i, m.Attributes, m.MessageAttributes)
		}
	}
	// A ordem do grupo foi preservada: tx-1 antes de tx-2.
	var first string
	if err := e.pool().QueryRow(t.Context(), `SELECT external_transaction_id FROM wager_transactions
 WHERE wallet_id = $1 AND origin = 'EXTERNAL' ORDER BY created_at, id LIMIT 1`, wallet).Scan(&first); err != nil {
		t.Fatal(err)
	}
	if first != "tx-1" {
		t.Fatalf("primeira processada = %s", first)
	}
}

// blockingUoW segura a primeira transação até release (ou até o contexto
// ser cancelado, se release for nil).
type blockingUoW struct {
	inner   app.UnitOfWork
	entered chan struct{}
	release chan struct{}
	first   chan struct{}
}

func newBlockingUoW(inner app.UnitOfWork, release chan struct{}) *blockingUoW {
	first := make(chan struct{}, 1)
	first <- struct{}{}
	return &blockingUoW{inner: inner, entered: make(chan struct{}), release: release, first: first}
}

func (b *blockingUoW) WithinTx(ctx context.Context, opts app.TxOptions, fn app.TxFunc) error {
	select {
	case <-b.first:
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	default:
	}
	return b.inner.WithinTx(ctx, opts, fn)
}

func TestIntegrationConsumerGracefulShutdownDrains(t *testing.T) {
	e := newITEnv(t, 3)
	wallet, player := e.openWallet(t, "100.00")
	release := make(chan struct{})
	slow := newBlockingUoW(e.uow, release)
	proc, err := app.NewProcessWagerTransaction(slow)
	if err != nil {
		t.Fatal(err)
	}
	sendMessage(t, e.client, e.q.main, wallet.String(), bet(t, "msg-1", "tx-1", wallet, player, "BET", "25.00"))
	c := startIntegrationConsumer(t, integrationConfig(e.q), e.client.SQS(), proc)
	<-slow.entered

	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		stopped <- c.Stop(ctx)
	}()
	select {
	case err := <-stopped:
		t.Fatalf("Stop não esperou a mensagem em processamento: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	if err := <-stopped; err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if n := e.completedInbox(t); n != 1 {
		t.Fatalf("mensagem em voo deveria concluir no shutdown (inbox %d)", n)
	}
	if !queueEmpty(t, e.client, e.q.main) {
		t.Fatal("mensagem concluída deveria ser apagada antes do fim do Stop")
	}
	if s := e.wallet(t, wallet); s.balance != 7500 {
		t.Fatalf("saldo = %d", s.balance)
	}
}

func TestIntegrationConsumerShutdownReleasesVisibility(t *testing.T) {
	e := newITEnv(t, 3)
	wallet, player := e.openWallet(t, "100.00")
	slow := newBlockingUoW(e.uow, nil) // só termina com o cancelamento
	proc, err := app.NewProcessWagerTransaction(slow)
	if err != nil {
		t.Fatal(err)
	}
	sendMessage(t, e.client, e.q.main, wallet.String(), bet(t, "msg-1", "tx-1", wallet, player, "BET", "25.00"))
	cfg := integrationConfig(e.q)
	cfg.DrainTimeout = 300 * time.Millisecond
	c := startIntegrationConsumer(t, cfg, e.client.SQS(), proc)
	<-slow.entered

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.Stop(ctx); !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("Stop = %v, want ErrDrainTimeout", err)
	}
	if n := e.count(t, `SELECT count(*) FROM wager_transactions WHERE origin = 'EXTERNAL'`); n != 0 {
		t.Fatalf("transação cancelada não pode ter efeito: %d", n)
	}
	// Visibilidade 0: volta a ficar disponível já, não depois dos 30s.
	waitLong(t, "mensagem liberada", func() bool { v, f := queueDepth(t, e.client, e.q.main); return v == 1 && f == 0 })

	// Outra instância assume e processa uma única vez.
	startIntegrationConsumer(t, integrationConfig(e.q), e.client.SQS(), e.process)
	waitLong(t, "reprocessada", func() bool { return e.completedInbox(t) == 1 })
	waitLong(t, "apagada", func() bool { return queueEmpty(t, e.client, e.q.main) })
	if s := e.wallet(t, wallet); s.balance != 7500 || s.ledger != 2 {
		t.Fatalf("carteira = %+v", s)
	}
}

// Composição Fx real (config → postgres → app → sqs) contra LocalStack:
// processa uma mensagem, para, e nenhuma goroutine do pool, do consumidor ou
// do cliente SQS sobra (goleak).
func TestIntegrationConsumerFxLifecycleNoLeak(t *testing.T) {
	e := newITEnv(t, 3)
	wallet, player := e.openWallet(t, "100.00")
	dsn := e.pool().Config().ConnString()
	sendMessage(t, e.client, e.q.main, wallet.String(), bet(t, "msg-1", "tx-1", wallet, player, "BET", "25.00"))

	var cfg config.Config
	cfg.Database = dbConfig(dsn)
	cfg.AWS = awsConfig()
	cfg.SQS = config.SQSConfig{
		InboundQueueURL: e.q.main, InboundDLQURL: e.q.dlq,
		Consumer: config.SQSConsumerConfig{
			Name: "it-consumer", Concurrency: 2, WaitTime: time.Second, VisibilityTimeout: 30 * time.Second,
			ProcessTimeout: 10 * time.Second, RetryBaseDelay: time.Second, RetryMaxDelay: 2 * time.Second,
			DrainTimeout: 5 * time.Second, MaxAttempts: 2,
		},
	}

	ignore := goleak.IgnoreCurrent()
	fxApp := fxtest.New(t, fx.Supply(cfg), postgres.Module, app.Module, Module)
	fxApp.RequireStart()
	waitLong(t, "inbox concluída", func() bool { return e.completedInbox(t) == 1 })
	waitLong(t, "apagada", func() bool { return queueEmpty(t, e.client, e.q.main) })
	fxApp.RequireStop()
	// O cliente do próprio teste (criado antes do snapshot) pode ter aberto
	// conexões novas durante a espera; fechá-las isola o que é do app.
	_ = e.client.Close(context.Background())
	goleak.VerifyNone(t, ignore)

	if s := e.wallet(t, wallet); s.balance != 7500 {
		t.Fatalf("saldo = %d", s.balance)
	}
}
