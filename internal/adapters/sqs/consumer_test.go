package sqs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
	"github.com/junglegaming/backend-challenge-go/internal/platform/startbudget"
)

// testConfig: na fila falsa 1 "segundo" vale f.unit (10ms por padrão), então
// backoff e visibilidade são rápidos; os prazos reais do consumidor
// (ProcessTimeout, DrainTimeout) são de tempo real.
func testConfig(f *fakeSQS) ConsumerConfig {
	return ConsumerConfig{
		QueueURL: f.queueURL, DLQURL: f.dlqURL, Name: "test-consumer", Concurrency: 4,
		WaitTime: time.Second, VisibilityTimeout: 30 * time.Second, ProcessTimeout: 5 * time.Second,
		RetryBaseDelay: time.Second, RetryMaxDelay: 4 * time.Second, DrainTimeout: 2 * time.Second,
		MaxAttempts:         2,
		ReceiveErrorBackoff: 5 * time.Millisecond, CallTimeout: time.Second, DLQDepthInterval: 10 * time.Millisecond,
	}
}

// fakeProcessor registra as chamadas; fn decide o resultado (nil = PROCESSED).
type fakeProcessor struct {
	mu    sync.Mutex
	calls []app.ProcessWagerCommand
	fn    func(ctx context.Context, cmd app.ProcessWagerCommand, call int) (app.WagerResult, error)
}

func (p *fakeProcessor) Execute(ctx context.Context, cmd app.ProcessWagerCommand) (app.WagerResult, error) {
	p.mu.Lock()
	p.calls = append(p.calls, cmd)
	n := len(p.calls)
	fn := p.fn
	p.mu.Unlock()
	if fn == nil {
		return result(cmd, domain.WagerStatusProcessed, false), nil
	}
	return fn(ctx, cmd, n)
}

func (p *fakeProcessor) snapshot() []app.ProcessWagerCommand {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.calls)
}

func result(cmd app.ProcessWagerCommand, status domain.WagerStatus, replay bool) app.WagerResult {
	r := app.WagerResult{
		TransactionID: uuid.New(), WalletID: cmd.WalletID, Kind: cmd.Kind, Status: status, IdempotentReplay: replay,
	}
	if status == domain.WagerStatusRejected {
		r.FailureCode = domain.FailureCodeInsufficientFunds
	}
	return r
}

func unavailable() error {
	return &app.Error{Kind: app.KindUnavailable, Code: app.CodeTemporarilyUnavailable, Op: "process wager"}
}

func newTestMetrics(t *testing.T) *metrics.Metrics {
	t.Helper()
	m, err := metrics.New(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// startConsumer inicia o consumidor; o Stop fica no cleanup (idempotente).
func startConsumer(t *testing.T, f *fakeSQS, p WagerProcessor, cfg ConsumerConfig, opts ...Option) *Consumer {
	t.Helper()
	c, err := NewConsumer(cfg, f, p, opts...)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = c.Stop(ctx)
	})
	return c
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout esperando: %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func stop(t *testing.T, c *Consumer) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.Stop(ctx)
}

func extIDs(cmds []app.ProcessWagerCommand) []string {
	out := make([]string, len(cmds))
	for i, c := range cmds {
		out[i] = c.ExternalTransactionID
	}
	return out
}

func TestConsumerProcessesAndDeletesAfterCommit(t *testing.T) {
	f := newFakeSQS(3)
	id := f.send("wallet-1", envelopeJSON(t, "msg-1", "tx-1", nil))
	m := newTestMetrics(t)
	p := &fakeProcessor{}
	c := startConsumer(t, f, p, testConfig(f), WithMetrics(m))
	waitFor(t, "delete", func() bool { return len(f.state().deleted) == 1 })
	if err := stop(t, c); err != nil {
		t.Fatal(err)
	}

	s := f.state()
	if s.deleted[0] != id || len(s.pending) != 0 || len(s.dlq) != 0 || len(s.visChanges) != 0 {
		t.Fatalf("estado inesperado: %+v", s)
	}
	calls := p.snapshot()
	if len(calls) != 1 {
		t.Fatalf("chamadas: %d", len(calls))
	}
	cmd := calls[0]
	if cmd.ProviderID != "provider-a" || cmd.ExternalTransactionID != "tx-1" || cmd.IdempotencyKey != "provider-a:tx-1" ||
		cmd.Kind != domain.WagerKindBet || cmd.WalletID != uuid.MustParse(testWallet) ||
		cmd.CorrelationID != "msg-1" || cmd.CausationID != "msg-1" ||
		cmd.Inbox == nil || *cmd.Inbox != (app.InboxMessageRef{ConsumerName: "test-consumer", MessageID: "msg-1"}) {
		t.Fatalf("comando inesperado: %+v", cmd)
	}
	if got := testutil.ToFloat64(m.TransactionsTotal.WithLabelValues("sqs", "BET", "PROCESSED")); got != 1 {
		t.Fatalf("métrica de resultado = %v", got)
	}
}

func TestConsumerDeletesEveryConfirmedOutcome(t *testing.T) {
	cases := []struct {
		name   string
		status domain.WagerStatus
		replay bool
	}{
		{"rejeição de negócio", domain.WagerStatusRejected, false},
		{"referência pendente", domain.WagerStatusPendingReference, false},
		{"replay", domain.WagerStatusProcessed, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeSQS(3)
			f.send("g", envelopeJSON(t, "msg-1", "tx-1", nil))
			p := &fakeProcessor{fn: func(_ context.Context, cmd app.ProcessWagerCommand, _ int) (app.WagerResult, error) {
				return result(cmd, tc.status, tc.replay), nil
			}}
			c := startConsumer(t, f, p, testConfig(f))
			waitFor(t, "delete", func() bool { return len(f.state().deleted) == 1 })
			_ = stop(t, c)
			if s := f.state(); len(s.dlq) != 0 || len(s.visChanges) != 0 || len(s.pending) != 0 {
				t.Fatalf("estado inesperado: %+v", s)
			}
		})
	}
}

// requireExplicitDLQ confere que id está na DLQ por envio explícito do
// consumidor (não pelo redrive nativo), com corpo e grupo preservados e o
// motivo nos atributos.
func requireExplicitDLQ(t *testing.T, f *fakeSQS, id, group, body, reason, code string) dlqEntry {
	t.Helper()
	for _, e := range f.dlqEntries() {
		if e.id != id {
			continue
		}
		if !e.explicit {
			t.Fatalf("%s foi para a DLQ pelo redrive nativo, não pelo envio explícito", id)
		}
		if e.group != group || e.body != body {
			t.Fatalf("%s: grupo/corpo não preservados: grupo=%q corpo=%q", id, e.group, e.body)
		}
		if e.attrs[DLQAttrReason] != reason || e.attrs[DLQAttrCode] != code ||
			e.attrs[DLQAttrSourceMessageID] != id || e.attrs[DLQAttrSourceQueue] != "in.fifo" ||
			e.attrs[DLQAttrConsumer] != "test-consumer" {
			t.Fatalf("%s: atributos inesperados: %+v (want reason=%s code=%s)", id, e.attrs, reason, code)
		}
		return e
	}
	t.Fatalf("%s não está na DLQ: %+v", id, f.dlqEntries())
	return dlqEntry{}
}

func TestConsumerInvalidMessagesGoToDLQWithoutProcessing(t *testing.T) {
	f := newFakeSQS(3)
	// Chave desconhecida não é ecoada: nem no log, nem no atributo dlqField.
	cases := []struct{ body, code, field string }{
		{"not json", CodeInvalidMessage, ""},
		{envelopeJSON(t, "msg-opening", "tx-o", func(_, d map[string]any) { d["kind"] = "OPENING" }), string(domain.FailureCodeInvalidKind), "data.kind"},
		{envelopeJSON(t, "msg-type", "tx-t", func(e, _ map[string]any) { e["type"] = "Other" }), CodeUnsupportedMessageType, "type"},
		{envelopeJSON(t, "msg-unknown", "tx-u", func(_, d map[string]any) { d["extra"] = true }), CodeInvalidMessage, ""},
	}
	ids := make([]string, len(cases))
	for i, tc := range cases {
		ids[i] = f.send(fmt.Sprintf("g%d", i), tc.body)
	}
	m := newTestMetrics(t)
	m.AllowQueues("dlq.fifo")
	p := &fakeProcessor{}
	c := startConsumer(t, f, p, testConfig(f), WithMetrics(m))
	waitFor(t, "DLQ", func() bool { return len(f.state().dlq) == len(cases) && f.drained() })
	waitFor(t, "métrica de profundidade da DLQ", func() bool {
		return testutil.ToFloat64(m.DLQDepth.WithLabelValues("dlq.fifo")) == float64(len(cases))
	})
	_ = stop(t, c)

	s := f.state()
	if len(p.snapshot()) != 0 {
		t.Fatal("mensagem inválida não pode chegar ao caso de uso")
	}
	// Enviada à DLQ na primeira entrega e só então apagada da origem: nenhum
	// recebimento extra, nenhuma visibilidade alterada.
	if len(s.pending) != 0 || len(s.visChanges) != 0 || len(s.deleted) != len(cases) {
		t.Fatalf("estado inesperado: %+v", s)
	}
	for i, tc := range cases {
		e := requireExplicitDLQ(t, f, ids[i], fmt.Sprintf("g%d", i), tc.body, metrics.DLQReasonInvalidMessage, tc.code)
		if e.attrs[DLQAttrField] != tc.field || e.attrs[DLQAttrReceiveCount] != "1" || e.attrs[DLQAttrAttempts] != "1" {
			t.Fatalf("%s: atributos inesperados: %+v", ids[i], e.attrs)
		}
	}
	if got := testutil.ToFloat64(m.DLQMessagesTotal.WithLabelValues(metrics.DLQReasonInvalidMessage)); got != float64(len(cases)) {
		t.Fatalf("métrica DLQ = %v", got)
	}
}

func TestConsumerPermanentErrorsGoToDLQ(t *testing.T) {
	cases := []struct {
		name string
		err  *app.Error
	}{
		{"conflito de inbox", &app.Error{Kind: app.KindConflict, Code: app.CodeMessageConflict, Op: "x"}},
		{"conflito de chave", &app.Error{Kind: app.KindConflict, Code: app.CodeIdempotencyKeyConflict, Op: "x"}},
		{"entrada inválida", &app.Error{Kind: app.KindInvalidInput, Code: string(domain.FailureCodeInvalidAmount), Op: "x"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeSQS(3)
			body := envelopeJSON(t, "msg-1", "tx-1", nil)
			id := f.send("g", body)
			p := &fakeProcessor{fn: func(context.Context, app.ProcessWagerCommand, int) (app.WagerResult, error) {
				return app.WagerResult{}, tc.err
			}}
			m := newTestMetrics(t)
			c := startConsumer(t, f, p, testConfig(f), WithMetrics(m))
			waitFor(t, "DLQ", func() bool { return len(f.state().dlq) == 1 && f.drained() })
			_ = stop(t, c)
			if n := len(p.snapshot()); n != 1 {
				t.Fatalf("erro permanente não é repetido: %d tentativas", n)
			}
			e := requireExplicitDLQ(t, f, id, "g", body, metrics.DLQReasonPermanentError, tc.err.Code)
			if e.attrs[DLQAttrMessageID] != "msg-1" {
				t.Fatalf("messageId do envelope ausente: %+v", e.attrs)
			}
			if s := f.state(); !slices.Equal(s.deleted, []string{id}) || len(s.visChanges) != 0 {
				t.Fatalf("estado inesperado: %+v", s)
			}
			if got := testutil.ToFloat64(m.DLQMessagesTotal.WithLabelValues(metrics.DLQReasonPermanentError)); got != 1 {
				t.Fatalf("métrica DLQ = %v", got)
			}
		})
	}
}

func TestConsumerTransientFailureBacksOffThenSucceeds(t *testing.T) {
	f := newFakeSQS(5)
	f.send("g", envelopeJSON(t, "msg-1", "tx-1", nil))
	p := &fakeProcessor{fn: func(_ context.Context, cmd app.ProcessWagerCommand, call int) (app.WagerResult, error) {
		if call <= 3 {
			return app.WagerResult{}, unavailable()
		}
		return result(cmd, domain.WagerStatusProcessed, false), nil
	}}
	m := newTestMetrics(t)
	cfg := testConfig(f)
	cfg.MaxAttempts = 4
	c := startConsumer(t, f, p, cfg, WithMetrics(m))
	waitFor(t, "delete", func() bool { return len(f.state().deleted) == 1 })
	_ = stop(t, c)

	s := f.state()
	var secs []int32
	for _, v := range s.visChanges {
		secs = append(secs, v.seconds)
	}
	// base 1s, teto 4s: tentativas 1, 2, 3 → 1s, 2s, 4s.
	if !slices.Equal(secs, []int32{1, 2, 4}) {
		t.Fatalf("backoff = %v, want [1 2 4]", secs)
	}
	if got := testutil.ToFloat64(m.RetriesTotal.WithLabelValues(metrics.OperationSQSConsume)); got != 3 {
		t.Fatalf("métrica de retry = %v", got)
	}
	if len(s.dlq) != 0 {
		t.Fatalf("DLQ deveria estar vazia: %+v", s.dlq)
	}
}

func TestConsumerTransientFailuresExhaustToDLQ(t *testing.T) {
	cases := []struct {
		name string
		code string
		err  func(ctx context.Context) error
	}{
		{"indisponível", app.CodeTemporarilyUnavailable, func(context.Context) error { return unavailable() }},
		{"erro sem classe", codeUnclassifiedError, func(context.Context) error { return errors.New("boom") }},
		{"prazo de processamento", codeProcessTimeout, func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeSQS(5)
			body := envelopeJSON(t, "msg-1", "tx-1", nil)
			id := f.send("g", body)
			p := &fakeProcessor{fn: func(ctx context.Context, _ app.ProcessWagerCommand, _ int) (app.WagerResult, error) {
				return app.WagerResult{}, tc.err(ctx)
			}}
			cfg := testConfig(f)
			cfg.ProcessTimeout = 20 * time.Millisecond
			cfg.MaxAttempts = 3
			m := newTestMetrics(t)
			c := startConsumer(t, f, p, cfg, WithMetrics(m))
			waitFor(t, "DLQ", func() bool { return len(f.state().dlq) == 1 && f.drained() })
			_ = stop(t, c)
			if n := len(p.snapshot()); n != 3 {
				t.Fatalf("tentativas = %d, want 3 (MaxAttempts)", n)
			}
			e := requireExplicitDLQ(t, f, id, "g", body, metrics.DLQReasonMaxReceives, tc.code)
			if e.attrs[DLQAttrAttempts] != "3" || e.attrs[DLQAttrReceiveCount] != "3" {
				t.Fatalf("atributos inesperados: %+v", e.attrs)
			}
			s := f.state()
			// Backoff nas tentativas 1 e 2; a 3ª esgota e vai à DLQ (antes do
			// maxReceiveCount=5 do redrive nativo).
			if !slices.Equal(s.deleted, []string{id}) || !slices.Equal(s.visChanges, []visChange{{id, 1}, {id, 2}}) {
				t.Fatalf("estado inesperado: %+v", s)
			}
			if got := testutil.ToFloat64(m.DLQMessagesTotal.WithLabelValues(metrics.DLQReasonMaxReceives)); got != 1 {
				t.Fatalf("métrica DLQ = %v", got)
			}
		})
	}
}

// Achado da avaliação: com o redrive nativo, a seguinte de uma mensagem
// venenosa no mesmo MessageGroupId subia o ApproximateReceiveCount junto com
// ela e ia para a DLQ sem ser processada. Agora a venenosa vai à DLQ por
// envio explícito na primeira entrega e o grupo segue.
func TestConsumerValidMessagesBehindPoisonAreProcessed(t *testing.T) {
	poisonErr := func(e *app.Error) func(app.ProcessWagerCommand) error {
		return func(cmd app.ProcessWagerCommand) error {
			if cmd.ExternalTransactionID == "tx-poison" {
				return e
			}
			return nil
		}
	}
	cases := []struct {
		name   string
		body   func(t *testing.T) string
		fail   func(app.ProcessWagerCommand) error
		reason string
		code   string
		calls  []string
	}{
		{
			name: "JSON inválido", body: func(*testing.T) string { return "not json" },
			reason: metrics.DLQReasonInvalidMessage, code: CodeInvalidMessage, calls: []string{"tx-v1", "tx-v2"},
		},
		{
			name: "OPENING",
			body: func(t *testing.T) string {
				return envelopeJSON(t, "msg-poison", "tx-poison", func(_, d map[string]any) { d["kind"] = "OPENING" })
			},
			reason: metrics.DLQReasonInvalidMessage, code: string(domain.FailureCodeInvalidKind), calls: []string{"tx-v1", "tx-v2"},
		},
		{
			name:   "permanente de negócio",
			body:   func(t *testing.T) string { return envelopeJSON(t, "msg-poison", "tx-poison", nil) },
			fail:   poisonErr(&app.Error{Kind: app.KindInvalidInput, Code: string(domain.FailureCodeInvalidAmount), Op: "x"}),
			reason: metrics.DLQReasonPermanentError, code: string(domain.FailureCodeInvalidAmount),
			calls: []string{"tx-poison", "tx-v1", "tx-v2"},
		},
		{
			name:   "conflito",
			body:   func(t *testing.T) string { return envelopeJSON(t, "msg-poison", "tx-poison", nil) },
			fail:   poisonErr(&app.Error{Kind: app.KindConflict, Code: app.CodeMessageConflict, Op: "x"}),
			reason: metrics.DLQReasonPermanentError, code: app.CodeMessageConflict,
			calls: []string{"tx-poison", "tx-v1", "tx-v2"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// maxReceiveCount=2 e MaxAttempts=1: o cenário exato do achado.
			f := newFakeSQS(2)
			poisonBody := tc.body(t)
			poison := f.send("G", poisonBody)
			v1 := f.send("G", envelopeJSON(t, "msg-v1", "tx-v1", nil))
			v2 := f.send("G", envelopeJSON(t, "msg-v2", "tx-v2", nil))
			p := &fakeProcessor{fn: func(_ context.Context, cmd app.ProcessWagerCommand, _ int) (app.WagerResult, error) {
				if tc.fail != nil {
					if err := tc.fail(cmd); err != nil {
						return app.WagerResult{}, err
					}
				}
				return result(cmd, domain.WagerStatusProcessed, false), nil
			}}
			cfg := testConfig(f)
			cfg.MaxAttempts = 1
			c := startConsumer(t, f, p, cfg)
			waitFor(t, "fila vazia", func() bool { return f.drained() })
			_ = stop(t, c)

			s := f.state()
			if !slices.Equal(s.dlq, []string{poison}) {
				t.Fatalf("só a venenosa vai à DLQ: dlq=%v", s.dlq)
			}
			requireExplicitDLQ(t, f, poison, "G", poisonBody, tc.reason, tc.code)
			// Ordem do grupo preservada; as válidas processadas uma vez cada.
			if !slices.Equal(s.deleted, []string{poison, v1, v2}) {
				t.Fatalf("deleted = %v, want [%s %s %s]", s.deleted, poison, v1, v2)
			}
			if got := extIDs(p.snapshot()); !slices.Equal(got, tc.calls) {
				t.Fatalf("chamadas = %v, want %v", got, tc.calls)
			}
			if len(s.visChanges) != 0 {
				t.Fatalf("nenhuma mensagem precisava voltar à fila: %+v", s.visChanges)
			}
		})
	}
}

// A seguinte de uma transitória que esgota as tentativas foi devolvida a
// cada falha (ApproximateReceiveCount subindo junto), mas não herda essas
// tentativas: é processada, e uma falha transitória dela ainda tem backoff
// em vez de DLQ imediata.
func TestConsumerValidMessagesBehindExhaustedTransientAreProcessed(t *testing.T) {
	f := newFakeSQS(4)
	badBody := envelopeJSON(t, "msg-bad", "tx-bad", nil)
	bad := f.send("G", badBody)
	v1 := f.send("G", envelopeJSON(t, "msg-v1", "tx-v1", nil))
	v2 := f.send("G", envelopeJSON(t, "msg-v2", "tx-v2", nil))
	var v1Failed atomic.Bool
	p := &fakeProcessor{fn: func(_ context.Context, cmd app.ProcessWagerCommand, _ int) (app.WagerResult, error) {
		switch cmd.ExternalTransactionID {
		case "tx-bad":
			return app.WagerResult{}, unavailable()
		case "tx-v1":
			if v1Failed.CompareAndSwap(false, true) {
				return app.WagerResult{}, unavailable()
			}
		}
		return result(cmd, domain.WagerStatusProcessed, false), nil
	}}
	cfg := testConfig(f)
	cfg.MaxAttempts = 3
	m := newTestMetrics(t)
	c := startConsumer(t, f, p, cfg, WithMetrics(m))
	waitFor(t, "fila vazia", func() bool { return f.drained() })
	_ = stop(t, c)

	s := f.state()
	if !slices.Equal(s.dlq, []string{bad}) {
		t.Fatalf("só a transitória esgotada vai à DLQ: %v", s.dlq)
	}
	requireExplicitDLQ(t, f, bad, "G", badBody, metrics.DLQReasonMaxReceives, app.CodeTemporarilyUnavailable)
	if !slices.Equal(s.deleted, []string{bad, v1, v2}) {
		t.Fatalf("deleted = %v", s.deleted)
	}
	want := []string{"tx-bad", "tx-bad", "tx-bad", "tx-v1", "tx-v1", "tx-v2"}
	if got := extIDs(p.snapshot()); !slices.Equal(got, want) {
		t.Fatalf("chamadas = %v, want %v", got, want)
	}
	// v1 chegou com ApproximateReceiveCount=3 (= MaxAttempts) depois de duas
	// devoluções sem tentativa: a falha dela é a tentativa 1 (backoff 1s).
	if !slices.Contains(s.visChanges, visChange{id: v1, seconds: 1}) {
		t.Fatalf("v1 deveria ter backoff da 1ª tentativa: %+v", s.visChanges)
	}
	if got := testutil.ToFloat64(m.DLQMessagesTotal.WithLabelValues(metrics.DLQReasonMaxReceives)); got != 1 {
		t.Fatalf("métrica DLQ = %v", got)
	}
}

// Falha no envio à DLQ é transitória: a mensagem não é apagada, volta com
// backoff e o envio é repetido; o grupo só segue depois.
func TestConsumerDLQSendFailureIsTransient(t *testing.T) {
	f := newFakeSQS(5)
	f.failSend = 1
	poison := f.send("G", "not json")
	valid := f.send("G", envelopeJSON(t, "msg-v", "tx-v", nil))
	m := newTestMetrics(t)
	p := &fakeProcessor{}
	c := startConsumer(t, f, p, testConfig(f), WithMetrics(m))
	waitFor(t, "fila vazia", func() bool { return f.drained() })
	_ = stop(t, c)

	s := f.state()
	if !slices.Equal(s.dlq, []string{poison}) || !slices.Equal(s.deleted, []string{poison, valid}) {
		t.Fatalf("estado inesperado: %+v", s)
	}
	requireExplicitDLQ(t, f, poison, "G", "not json", metrics.DLQReasonInvalidMessage, CodeInvalidMessage)
	// 1ª entrega: envio falha → backoff 1s na venenosa e a válida é liberada.
	if !slices.Equal(s.visChanges, []visChange{{poison, 1}, {valid, 0}}) {
		t.Fatalf("visChanges = %+v", s.visChanges)
	}
	if n := len(p.snapshot()); n != 1 {
		t.Fatalf("a válida deve ser processada uma vez: %d", n)
	}
	if got := testutil.ToFloat64(m.DLQMessagesTotal.WithLabelValues(metrics.DLQReasonInvalidMessage)); got != 1 {
		t.Fatalf("métrica DLQ = %v", got)
	}
}

// Se o delete falhar depois do envio, a original reaparece e o reenvio é
// deduplicado na DLQ FIFO: uma única cópia. A seguinte só é processada
// depois que a venenosa sai da fila.
func TestConsumerDLQSendIsDeduplicatedWhenDeleteFails(t *testing.T) {
	f := newFakeSQS(5)
	poison := f.send("G", "not json")
	valid := f.send("G", envelopeJSON(t, "msg-v", "tx-v", nil))
	f.failDelete[poison] = 1
	p := &fakeProcessor{}
	c := startConsumer(t, f, p, testConfig(f))
	waitFor(t, "fila vazia", func() bool { return f.drained() })
	_ = stop(t, c)

	s := f.state()
	if !slices.Equal(s.dlq, []string{poison}) || !slices.Equal(s.deleted, []string{poison, valid}) {
		t.Fatalf("estado inesperado: %+v", s)
	}
	f.mu.Lock()
	sends := f.sendCalls
	f.mu.Unlock()
	if sends != 2 {
		t.Fatalf("esperava 2 envios (o segundo deduplicado), obteve %d", sends)
	}
}

func TestConsumerStartRequiresMaxAttemptsBelowMaxReceiveCount(t *testing.T) {
	f := newFakeSQS(3)
	cfg := testConfig(f)
	cfg.MaxAttempts = 3
	c, err := NewConsumer(cfg, f, &fakeProcessor{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(t.Context()); !errors.Is(err, ErrMaxAttemptsTooHigh) {
		t.Fatalf("Start = %v, want ErrMaxAttemptsTooHigh", err)
	}
}

func TestConsumerStartResolvesDLQFromRedrivePolicy(t *testing.T) {
	f := newFakeSQS(3)
	f.getURLErr = 2 // indisponibilidade passageira: repetida no start
	c := startConsumer(t, f, &fakeProcessor{}, testConfig(f))
	if c.dlqTargetURL != f.dlqURL || !c.dlqTargetFIFO {
		t.Fatalf("DLQ resolvida = %q (fifo=%v)", c.dlqTargetURL, c.dlqTargetFIFO)
	}
	if f.getURLCalls != 3 {
		t.Fatalf("GetQueueUrl chamado %d vezes", f.getURLCalls)
	}

	// DLQ da RedrivePolicy inexistente: erro definitivo, sem esperar o prazo.
	g := newFakeSQS(3)
	g.dlqURL = "http://fake/000000000000/other-dlq.fifo"
	c2, err := NewConsumer(testConfig(g), g, &fakeProcessor{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	start := time.Now()
	err = c2.Start(ctx)
	var notFound *types.QueueDoesNotExist
	if !errors.As(err, &notFound) || time.Since(start) > time.Second {
		t.Fatalf("Start = %v (em %s), want QueueDoesNotExist imediato", err, time.Since(start))
	}
}

func TestReleaseTracker(t *testing.T) {
	r := newReleaseTracker()
	r.add("a")
	r.add("a")
	r.add("")
	if r.count("a") != 2 || r.count("b") != 0 || r.count("") != 0 {
		t.Fatalf("contagens: a=%d b=%d", r.count("a"), r.count("b"))
	}
	r.forget("a")
	if r.count("a") != 0 {
		t.Fatal("forget")
	}
	// Cheio: entradas antigas são podadas; sem antigas, o mapa é zerado.
	for i := range releaseTrackerMax {
		r.add(fmt.Sprintf("m%d", i))
	}
	r.mu.Lock()
	r.m["m0"] = releaseEntry{n: 1, at: time.Now().Add(-2 * releaseTTL)}
	r.mu.Unlock()
	r.add("new-1")
	if r.count("m0") != 0 || r.count("m1") != 1 || r.count("new-1") != 1 {
		t.Fatal("poda das entradas antigas")
	}
	r.add("new-2")
	if r.count("m1") != 0 || r.count("new-2") != 1 {
		t.Fatal("mapa cheio sem entradas antigas deve ser zerado")
	}
}

func TestConsumerPreservesOrderWithinGroupOnFailure(t *testing.T) {
	f := newFakeSQS(5)
	for _, ext := range []string{"a1", "a2", "a3"} {
		f.send("A", envelopeJSON(t, "msg-"+ext, ext, nil))
	}
	f.send("B", envelopeJSON(t, "msg-b1", "b1", nil))
	var failed atomic.Bool
	p := &fakeProcessor{fn: func(_ context.Context, cmd app.ProcessWagerCommand, _ int) (app.WagerResult, error) {
		if cmd.ExternalTransactionID == "a1" && failed.CompareAndSwap(false, true) {
			return app.WagerResult{}, unavailable()
		}
		return result(cmd, domain.WagerStatusProcessed, false), nil
	}}
	c := startConsumer(t, f, p, testConfig(f))
	waitFor(t, "todas apagadas", func() bool { return len(f.state().deleted) == 4 })
	_ = stop(t, c)

	var groupA []string
	for _, id := range extIDs(p.snapshot()) {
		if id[0] == 'a' {
			groupA = append(groupA, id)
		}
	}
	if !slices.Equal(groupA, []string{"a1", "a1", "a2", "a3"}) {
		t.Fatalf("ordem do grupo A = %v", groupA)
	}
	// a2 e a3 vieram no mesmo lote de a1 e foram devolvidos sem processar.
	s := f.state()
	released := 0
	for _, v := range s.visChanges {
		if v.seconds == 0 {
			released++
		}
	}
	if released != 2 {
		t.Fatalf("esperava a2 e a3 liberadas (visibilidade 0): %+v", s.visChanges)
	}
}

func TestConsumerParallelAcrossGroupsSequentialWithinGroup(t *testing.T) {
	f := newFakeSQS(5)
	groups := []string{"A", "B", "C"}
	for i := range 4 {
		for _, g := range groups {
			ext := fmt.Sprintf("%s%d", g, i)
			f.send(g, envelopeJSON(t, "msg-"+ext, ext, nil))
		}
	}
	var (
		mu           sync.Mutex
		activeByGrp  = map[byte]int{}
		maxByGrp     = map[byte]int{}
		active, peak int
		order        = map[byte][]string{}
	)
	barrier := make(chan struct{})
	var arrived atomic.Int32
	p := &fakeProcessor{fn: func(_ context.Context, cmd app.ProcessWagerCommand, _ int) (app.WagerResult, error) {
		g := cmd.ExternalTransactionID[0]
		mu.Lock()
		activeByGrp[g]++
		active++
		maxByGrp[g] = max(maxByGrp[g], activeByGrp[g])
		peak = max(peak, active)
		order[g] = append(order[g], cmd.ExternalTransactionID)
		mu.Unlock()
		// As primeiras mensagens de A e B só seguem quando ambas chegaram:
		// sem paralelismo entre grupos, isto travaria (timeout).
		if id := cmd.ExternalTransactionID; id == "A0" || id == "B0" {
			if arrived.Add(1) == 2 {
				close(barrier)
			}
			select {
			case <-barrier:
			case <-time.After(5 * time.Second):
				return app.WagerResult{}, errors.New("grupos não processados em paralelo")
			}
		}
		time.Sleep(2 * time.Millisecond)
		mu.Lock()
		activeByGrp[g]--
		active--
		mu.Unlock()
		return result(cmd, domain.WagerStatusProcessed, false), nil
	}}
	c := startConsumer(t, f, p, testConfig(f))
	waitFor(t, "todas apagadas", func() bool { return len(f.state().deleted) == 12 })
	_ = stop(t, c)

	mu.Lock()
	defer mu.Unlock()
	for _, g := range []byte("ABC") {
		if maxByGrp[g] != 1 {
			t.Fatalf("grupo %c processado em paralelo consigo mesmo (%d)", g, maxByGrp[g])
		}
		want := []string{fmt.Sprintf("%c0", g), fmt.Sprintf("%c1", g), fmt.Sprintf("%c2", g), fmt.Sprintf("%c3", g)}
		if !slices.Equal(order[g], want) {
			t.Fatalf("ordem do grupo %c = %v", g, order[g])
		}
	}
	if peak < 2 {
		t.Fatalf("grupos diferentes deveriam rodar em paralelo (pico %d)", peak)
	}
	if len(p.snapshot()) != 12 {
		t.Fatalf("chamadas = %d", len(p.snapshot()))
	}
}

func TestConsumerConcurrencyLimit(t *testing.T) {
	f := newFakeSQS(5)
	for i := range 6 {
		f.send(fmt.Sprintf("g%d", i), envelopeJSON(t, fmt.Sprintf("msg-%d", i), fmt.Sprintf("tx-%d", i), nil))
	}
	var active, peak atomic.Int32
	p := &fakeProcessor{fn: func(_ context.Context, cmd app.ProcessWagerCommand, _ int) (app.WagerResult, error) {
		n := active.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		active.Add(-1)
		return result(cmd, domain.WagerStatusProcessed, false), nil
	}}
	cfg := testConfig(f)
	cfg.Concurrency = 2
	c := startConsumer(t, f, p, cfg)
	waitFor(t, "todas apagadas", func() bool { return len(f.state().deleted) == 6 })
	_ = stop(t, c)
	if peak.Load() > 2 {
		t.Fatalf("concorrência %d acima do limite 2", peak.Load())
	}
}

func TestConsumerGracefulShutdownDrainsInFlight(t *testing.T) {
	f := newFakeSQS(5)
	f.unit = 100 * time.Millisecond // visibilidade de 3s: não expira durante o teste
	a1 := f.send("A", envelopeJSON(t, "msg-a1", "a1", nil))
	a2 := f.send("A", envelopeJSON(t, "msg-a2", "a2", nil))
	entered := make(chan struct{})
	release := make(chan struct{})
	p := &fakeProcessor{fn: func(ctx context.Context, cmd app.ProcessWagerCommand, call int) (app.WagerResult, error) {
		if call == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return app.WagerResult{}, ctx.Err()
			}
		}
		return result(cmd, domain.WagerStatusProcessed, false), nil
	}}
	cfg := testConfig(f)
	cfg.DrainTimeout = 5 * time.Second
	c := startConsumer(t, f, p, cfg)
	<-entered

	stopped := make(chan error, 1)
	go func() { stopped <- stop(t, c) }()
	select {
	case err := <-stopped:
		t.Fatalf("Stop não esperou a mensagem em processamento: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	receivesAtStop := f.state().receiveCalls
	close(release)
	if err := <-stopped; err != nil {
		t.Fatalf("Stop: %v", err)
	}

	s := f.state()
	if !slices.Equal(s.deleted, []string{a1}) {
		t.Fatalf("a mensagem em processamento deve concluir e ser apagada: %+v", s)
	}
	if n := len(p.snapshot()); n != 1 {
		t.Fatalf("a2 não pode ser processada depois do início do shutdown (chamadas %d)", n)
	}
	if !slices.Equal(s.pending, []string{a2}) || !slices.Contains(s.visChanges, visChange{id: a2, seconds: 0}) {
		t.Fatalf("a2 deve ser liberada (visibilidade 0) e continuar na fila: %+v", s)
	}
	if s.receiveCalls != receivesAtStop {
		t.Fatalf("nenhum ReceiveMessage depois do Stop: %d → %d", receivesAtStop, s.receiveCalls)
	}
}

func TestConsumerShutdownDeadlineReleasesVisibility(t *testing.T) {
	f := newFakeSQS(5)
	f.unit = 100 * time.Millisecond
	a1 := f.send("A", envelopeJSON(t, "msg-a1", "a1", nil))
	entered := make(chan struct{})
	p := &fakeProcessor{fn: func(ctx context.Context, _ app.ProcessWagerCommand, _ int) (app.WagerResult, error) {
		close(entered)
		<-ctx.Done() // simula transação lenta: só termina com o cancelamento
		return app.WagerResult{}, fmt.Errorf("process wager: %w", ctx.Err())
	}}
	cfg := testConfig(f)
	cfg.DrainTimeout = 50 * time.Millisecond
	c := startConsumer(t, f, p, cfg)
	<-entered
	err := stop(t, c)
	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("Stop = %v, want ErrDrainTimeout", err)
	}
	s := f.state()
	if len(s.deleted) != 0 || !slices.Equal(s.pending, []string{a1}) ||
		!slices.Equal(s.visChanges, []visChange{{id: a1, seconds: 0}}) {
		t.Fatalf("mensagem cancelada deve ser liberada para reentrega: %+v", s)
	}
}

// Como no SQS real e no LocalStack, um long polling cancelado no cliente
// continua no servidor (serverSidePolls). Se o Stop liberasse a mensagem
// interrompida com esse poll ainda aberto, o poll órfão a pegaria e ela
// ficaria invisível pela visibilidade inteira. O Stop espera o último
// receive e só então libera: a mensagem fica visível na hora.
func TestConsumerShutdownReleasesAfterLastLongPoll(t *testing.T) {
	f := newFakeSQS(5)
	f.unit = 100 * time.Millisecond // poll de 500ms; visibilidade de 3s
	f.serverSidePolls = true
	a1 := f.send("A", envelopeJSON(t, "msg-a1", "a1", nil))
	entered := make(chan struct{})
	p := &fakeProcessor{fn: func(ctx context.Context, _ app.ProcessWagerCommand, _ int) (app.WagerResult, error) {
		close(entered)
		<-ctx.Done()
		return app.WagerResult{}, fmt.Errorf("process wager: %w", ctx.Err())
	}}
	cfg := testConfig(f)
	cfg.WaitTime = 5 * time.Second
	cfg.DrainTimeout = 50 * time.Millisecond
	c := startConsumer(t, f, p, cfg)
	<-entered
	waitFor(t, "long polling em curso", func() bool { return f.state().receiveCalls >= 2 })

	start := time.Now()
	if err := stop(t, c); !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("Stop = %v, want ErrDrainTimeout", err)
	}
	if d := time.Since(start); d > cfg.WaitTime+cfg.CallTimeout+time.Second {
		t.Fatalf("Stop levou %s", d)
	}
	f.polls.Wait()
	s := f.state()
	f.mu.Lock()
	lost := slices.Clone(f.lostReceives)
	f.mu.Unlock()
	if len(lost) != 0 {
		t.Fatalf("nenhum poll órfão pode pegar a mensagem liberada: %v", lost)
	}
	if !slices.Equal(s.visChanges, []visChange{{id: a1, seconds: 0}}) || !f.visibleNow(a1) {
		t.Fatalf("a1 deve estar visível logo após o Stop: %+v", s)
	}
}

// As mensagens que o último long polling trouxer depois do início do Stop
// não são processadas: voltam à fila com visibilidade 0.
func TestConsumerStopReleasesMessagesFromLastLongPoll(t *testing.T) {
	f := newFakeSQS(5)
	f.unit = 200 * time.Millisecond // poll de 2s
	cfg := testConfig(f)
	cfg.WaitTime = 10 * time.Second
	p := &fakeProcessor{}
	c := startConsumer(t, f, p, cfg)
	waitFor(t, "long polling em curso", func() bool { return f.state().receiveCalls >= 1 })

	stopped := make(chan error, 1)
	go func() { stopped <- stop(t, c) }()
	waitFor(t, "Stop iniciado", c.stopping.Load)
	receivesAtStop := f.state().receiveCalls
	a1 := f.send("A", envelopeJSON(t, "msg-a1", "a1", nil))
	if err := <-stopped; err != nil {
		t.Fatalf("Stop: %v", err)
	}

	s := f.state()
	if n := len(p.snapshot()); n != 0 {
		t.Fatalf("nada pode ser processado depois do início do Stop (chamadas %d)", n)
	}
	if !slices.Equal(s.visChanges, []visChange{{id: a1, seconds: 0}}) || !f.visibleNow(a1) {
		t.Fatalf("a1, trazida pelo último receive, deve ser liberada: %+v", s)
	}
	if s.receiveCalls != receivesAtStop {
		t.Fatalf("nenhum ReceiveMessage novo depois do Stop: %d → %d", receivesAtStop, s.receiveCalls)
	}
	if err := c.Ready(t.Context()); err == nil {
		t.Fatal("readiness deve falhar depois do Stop")
	}
}

// Ocioso, o Stop só espera o último long polling; se ele durar mais que o
// DrainTimeout, isso não é estouro de drenagem (nada estava em processamento).
func TestConsumerIdleStopWaitingLongPollIsNotDrainTimeout(t *testing.T) {
	f := newFakeSQS(5)
	f.unit = 100 * time.Millisecond // poll de 500ms
	cfg := testConfig(f)
	cfg.WaitTime = 5 * time.Second
	cfg.DrainTimeout = 50 * time.Millisecond
	c := startConsumer(t, f, &fakeProcessor{}, cfg)
	waitFor(t, "long polling em curso", func() bool { return f.state().receiveCalls >= 1 })
	if err := stop(t, c); err != nil {
		t.Fatalf("Stop ocioso = %v, want nil", err)
	}
}

// Se o servidor não responde ao long polling em WaitTime + CallTimeout, ou o
// ctx do Stop vence antes, o receive é cancelado e o Stop não trava.
func TestConsumerStopCancelsUnresponsiveLongPolling(t *testing.T) {
	t.Run("prazo do receive", func(t *testing.T) {
		f := newFakeSQS(5)
		f.hangReceive = true
		cfg := testConfig(f)
		cfg.CallTimeout = 200 * time.Millisecond
		c := startConsumer(t, f, &fakeProcessor{}, cfg)
		waitFor(t, "receive em andamento", func() bool { return f.state().receiveCalls >= 1 })
		start := time.Now()
		if err := stop(t, c); err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d < cfg.WaitTime || d > cfg.WaitTime+cfg.CallTimeout+time.Second {
			t.Fatalf("Stop deve esperar WaitTime + CallTimeout e então cancelar; levou %s", d)
		}
		if err := c.Ready(t.Context()); err == nil {
			t.Fatal("readiness deve falhar depois do Stop")
		}
	})
	t.Run("prazo do ctx", func(t *testing.T) {
		f := newFakeSQS(5)
		f.hangReceive = true
		cfg := testConfig(f)
		cfg.WaitTime = 20 * time.Second
		c := startConsumer(t, f, &fakeProcessor{}, cfg)
		waitFor(t, "receive em andamento", func() bool { return f.state().receiveCalls >= 1 })
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		start := time.Now()
		_ = c.Stop(ctx)
		if d := time.Since(start); d > 2*time.Second {
			t.Fatalf("Stop deve respeitar o prazo do ctx; levou %s", d)
		}
	})
}

func TestConsumerDeleteFailureIsRedeliveredAsReplay(t *testing.T) {
	f := newFakeSQS(5)
	id := f.send("g", envelopeJSON(t, "msg-1", "tx-1", nil))
	f.failDelete[id] = 1
	p := &fakeProcessor{fn: func(_ context.Context, cmd app.ProcessWagerCommand, call int) (app.WagerResult, error) {
		// Segunda entrega: a inbox já está COMPLETED → replay.
		return result(cmd, domain.WagerStatusProcessed, call > 1), nil
	}}
	m := newTestMetrics(t)
	c := startConsumer(t, f, p, testConfig(f), WithMetrics(m))
	waitFor(t, "delete", func() bool { return len(f.state().deleted) == 1 })
	_ = stop(t, c)
	if n := len(p.snapshot()); n != 2 {
		t.Fatalf("esperava reentrega após falha do delete (chamadas %d)", n)
	}
	if got := testutil.ToFloat64(m.DuplicatesTotal.WithLabelValues("sqs", metrics.DuplicateInbox)); got != 1 {
		t.Fatalf("métrica de duplicata = %v", got)
	}
	if got := testutil.ToFloat64(m.TransactionsTotal.WithLabelValues("sqs", "BET", "PROCESSED")); got != 1 {
		t.Fatalf("resultado contado %v vezes", got)
	}
}

func TestConsumerRetriesReceiveErrors(t *testing.T) {
	f := newFakeSQS(5)
	f.failReceive = 3
	f.send("g", envelopeJSON(t, "msg-1", "tx-1", nil))
	c := startConsumer(t, f, &fakeProcessor{}, testConfig(f))
	waitFor(t, "delete", func() bool { return len(f.state().deleted) == 1 })
	_ = stop(t, c)
	if s := f.state(); s.receiveCalls < 4 {
		t.Fatalf("receiveCalls = %d", s.receiveCalls)
	}
}

func TestConsumerExtendsVisibilityBeforeLateGroupMessage(t *testing.T) {
	f := newFakeSQS(5)
	f.unit = time.Second // visibilidade real = configurada
	f.send("A", envelopeJSON(t, "msg-a1", "a1", nil))
	a2 := f.send("A", envelopeJSON(t, "msg-a2", "a2", nil))
	p := &fakeProcessor{fn: func(_ context.Context, cmd app.ProcessWagerCommand, call int) (app.WagerResult, error) {
		if call == 1 {
			time.Sleep(1200 * time.Millisecond)
		}
		return result(cmd, domain.WagerStatusProcessed, false), nil
	}}
	cfg := testConfig(f)
	cfg.VisibilityTimeout = 2 * time.Second
	cfg.ProcessTimeout = time.Second
	c := startConsumer(t, f, p, cfg)
	waitFor(t, "ambas apagadas", func() bool { return len(f.state().deleted) == 2 })
	_ = stop(t, c)
	s := f.state()
	if !slices.Equal(s.visChanges, []visChange{{id: a2, seconds: 2}}) {
		t.Fatalf("a2 deveria ter a visibilidade renovada antes do processamento: %+v", s.visChanges)
	}
}

func TestConsumerStartRequiresRedrivePolicy(t *testing.T) {
	f := newFakeSQS(0)
	c, err := NewConsumer(testConfig(f), f, &fakeProcessor{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(t.Context()); !errors.Is(err, ErrNoRedrivePolicy) {
		t.Fatalf("Start = %v, want ErrNoRedrivePolicy", err)
	}
	if err := stop(t, c); err != nil {
		t.Fatalf("Stop sem Start: %v", err)
	}
}

func TestConsumerStartFailsWhenQueueUnavailable(t *testing.T) {
	f := newFakeSQS(3)
	f.attrsErr = errors.New("connection refused")
	c, err := NewConsumer(testConfig(f), f, &fakeProcessor{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if err := c.Start(ctx); err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start = %v", err)
	}
}

func TestParseRedrivePolicy(t *testing.T) {
	const arn = "arn:aws:sqs:us-east-1:000000000000:in-dlq.fifo"
	dlq := queueARN{account: "000000000000", name: "in-dlq.fifo"}
	cases := []struct {
		raw  string
		want redrivePolicy
		ok   bool
	}{
		{`{"deadLetterTargetArn":"` + arn + `","maxReceiveCount":5}`, redrivePolicy{dlq: dlq, maxReceiveCount: 5}, true},
		{`{"deadLetterTargetArn":"` + arn + `","maxReceiveCount":"3"}`, redrivePolicy{dlq: dlq, maxReceiveCount: 3}, true},
		{``, redrivePolicy{}, false},
		{`{`, redrivePolicy{}, false},
		{`{"deadLetterTargetArn":"` + arn + `","maxReceiveCount":0}`, redrivePolicy{}, false},
		{`{"maxReceiveCount":3}`, redrivePolicy{}, false},
		{`{"deadLetterTargetArn":"arn:x","maxReceiveCount":3}`, redrivePolicy{}, false},
		{`{"deadLetterTargetArn":"arn:aws:sns:us-east-1:000000000000:t","maxReceiveCount":3}`, redrivePolicy{}, false},
	}
	for _, tc := range cases {
		got, err := parseRedrivePolicy(tc.raw)
		if tc.ok != (err == nil) || got != tc.want {
			t.Fatalf("%q: got %+v, %v", tc.raw, got, err)
		}
		if err != nil && !errors.Is(err, ErrNoRedrivePolicy) {
			t.Fatalf("%q: erro deveria casar com ErrNoRedrivePolicy: %v", tc.raw, err)
		}
	}
}

func TestConsumerBackoff(t *testing.T) {
	f := newFakeSQS(3)
	cfg := testConfig(f)
	cfg.RetryBaseDelay = 2 * time.Second
	cfg.RetryMaxDelay = 30 * time.Second
	c, err := NewConsumer(cfg, f, &fakeProcessor{})
	if err != nil {
		t.Fatal(err)
	}
	for rc, want := range map[int32]time.Duration{
		1: 2 * time.Second, 2: 4 * time.Second, 3: 8 * time.Second, 4: 16 * time.Second,
		5: 30 * time.Second, 1000: 30 * time.Second,
	} {
		if got := c.backoff(rc); got != want {
			t.Fatalf("backoff(%d) = %s, want %s", rc, got, want)
		}
	}
}

func TestNewConsumerValidatesConfig(t *testing.T) {
	f := newFakeSQS(3)
	bad := []func(*ConsumerConfig){
		func(c *ConsumerConfig) { c.QueueURL = "" },
		func(c *ConsumerConfig) { c.Name = "" },
		func(c *ConsumerConfig) { c.Concurrency = 0 },
		func(c *ConsumerConfig) { c.WaitTime = 21 * time.Second },
		func(c *ConsumerConfig) { c.VisibilityTimeout = 0 },
		func(c *ConsumerConfig) { c.ProcessTimeout = c.VisibilityTimeout },
		func(c *ConsumerConfig) { c.RetryBaseDelay = 0 },
		func(c *ConsumerConfig) { c.RetryMaxDelay = time.Millisecond },
		func(c *ConsumerConfig) { c.DrainTimeout = 0 },
		func(c *ConsumerConfig) { c.MaxAttempts = 0 },
	}
	for i, mutate := range bad {
		cfg := testConfig(f)
		mutate(&cfg)
		if _, err := NewConsumer(cfg, f, &fakeProcessor{}); err == nil {
			t.Fatalf("caso %d: esperava erro", i)
		}
	}
	if _, err := NewConsumer(testConfig(f), nil, &fakeProcessor{}); err == nil {
		t.Fatal("API nil")
	}
	if _, err := NewConsumer(testConfig(f), f, nil); err == nil {
		t.Fatal("processor nil")
	}
}

// TestConsumerStartHonorsStartReserve: com a fila indisponível, Start desiste
// no prazo do ctx menos a reserva, deixando o resto do prazo para o rollback
// do Fx (ADR-025).
func TestConsumerStartHonorsStartReserve(t *testing.T) {
	f := newFakeSQS(3)
	f.attrsErr = errors.New("connection refused")
	c, err := NewConsumer(testConfig(f), f, &fakeProcessor{}, WithStartReserve(startbudget.Reserve(5*time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second+300*time.Millisecond)
	defer cancel()
	begin := time.Now()
	if err := c.Start(ctx); err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start = %v", err)
	}
	if elapsed := time.Since(begin); elapsed > 3*time.Second {
		t.Fatalf("Start levou %s: não respeitou a reserva de rollback", elapsed)
	}
	if ctx.Err() != nil {
		t.Fatal("o ctx do start venceu: não sobrou tempo para o rollback")
	}
}
