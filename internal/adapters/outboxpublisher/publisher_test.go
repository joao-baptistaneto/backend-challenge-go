package outboxpublisher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

const testQueueURL = "http://fake/000000000000/wager-events.fifo"

func testConfig() Config {
	return Config{
		QueueURL: testQueueURL, Owner: "pub-test", PollInterval: 10 * time.Millisecond,
		BatchSize: 10, Concurrency: 3, Lease: time.Minute, PublishTimeout: time.Second,
		DrainTimeout: 2 * time.Second,
	}
}

var seqCounter atomic.Int64

// message cria um evento reivindicado do agregado agg.
func message(agg string) postgres.OutboxMessage {
	seq := seqCounter.Add(1)
	id := uuid.Must(uuid.NewV7())
	return postgres.OutboxMessage{
		Seq: seq, EventID: id, EventType: "WalletBalanceChanged", EventVersion: 1,
		AggregateID: agg, CorrelationID: "corr-" + agg,
		Payload:    []byte(fmt.Sprintf(`{"eventId":%q,"n":%d}`, id, seq)),
		OccurredAt: time.Now().Add(-2 * time.Second).UTC(),
		Lease:      postgres.OutboxLease{Seq: seq, EventID: id, Attempt: 1},
	}
}

func newTestPublisher(t *testing.T, cfg Config, s Store, q Sender, opts ...Option) *Publisher {
	t.Helper()
	p, err := New(cfg, s, q, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func newMetrics(t *testing.T) (*metrics.Metrics, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m, err := metrics.New(reg)
	if err != nil {
		t.Fatal(err)
	}
	return m, reg
}

// metricValue devolve a soma dos valores (contador/gauge) ou das contagens
// (histograma) das séries de name cujos rótulos contêm labels.
func metricValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var total float64
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if !hasLabels(m, labels) {
				continue
			}
			switch {
			case m.Counter != nil:
				total += m.Counter.GetValue()
			case m.Gauge != nil:
				total += m.Gauge.GetValue()
			case m.Histogram != nil:
				total += float64(m.Histogram.GetSampleCount())
			}
		}
	}
	return total
}

func hasLabels(m *dto.Metric, want map[string]string) bool {
	for k, v := range want {
		found := false
		for _, lp := range m.GetLabel() {
			if lp.GetName() == k && lp.GetValue() == v {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func TestConfigValidate(t *testing.T) {
	cases := map[string]func(*Config){
		"queue vazia":          func(c *Config) { c.QueueURL = " " },
		"owner longo":          func(c *Config) { c.Owner = strings.Repeat("o", 256) },
		"poll zero":            func(c *Config) { c.PollInterval = 0 },
		"batch zero":           func(c *Config) { c.BatchSize = 0 },
		"batch alto":           func(c *Config) { c.BatchSize = postgres.MaxOutboxClaimBatch + 1 },
		"concurrency zero":     func(c *Config) { c.Concurrency = 0 },
		"lease zero":           func(c *Config) { c.Lease = 0 },
		"lease alto":           func(c *Config) { c.Lease = 2 * time.Hour },
		"publish zero":         func(c *Config) { c.PublishTimeout = 0 },
		"publish x2 >= lease":  func(c *Config) { c.PublishTimeout = 30 * time.Second },
		"drain zero":           func(c *Config) { c.DrainTimeout = 0 },
		"publish negativo":     func(c *Config) { c.PublishTimeout = -time.Second },
		"lease abaixo de 1ms ": func(c *Config) { c.Lease = time.Microsecond },
	}
	for name, mutate := range cases {
		c := testConfig()
		mutate(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: esperava erro", name)
		}
		if _, err := New(c, &memStore{}, newFakeQueue()); err == nil {
			t.Errorf("%s: New deveria recusar", name)
		}
	}
	if err := testConfig().Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := New(testConfig(), nil, newFakeQueue()); err == nil {
		t.Error("store nil deveria ser recusado")
	}
	if _, err := New(testConfig(), &memStore{}, nil); err == nil {
		t.Error("sender nil deveria ser recusado")
	}
}

func TestDefaultOwnerIsUniquePerPublisher(t *testing.T) {
	c := testConfig()
	c.Owner = ""
	a := newTestPublisher(t, c, &memStore{}, newFakeQueue())
	b := newTestPublisher(t, c, &memStore{}, newFakeQueue())
	if a.Owner() == "" || a.Owner() == b.Owner() || len(a.Owner()) > maxOwnerLength {
		t.Fatalf("owners = %q, %q", a.Owner(), b.Owner())
	}
	if strings.ContainsAny(a.Owner(), " \t\n") {
		t.Fatalf("owner com espaço: %q", a.Owner())
	}
}

func TestSendInput(t *testing.T) {
	m := message("wallet-1")
	in := SendInput(testQueueURL, m)
	if aws.ToString(in.QueueUrl) != testQueueURL || aws.ToString(in.MessageGroupId) != "wallet-1" ||
		aws.ToString(in.MessageDeduplicationId) != m.EventID.String() || aws.ToString(in.MessageBody) != string(m.Payload) {
		t.Fatalf("input = %+v", in)
	}
	want := map[string]string{
		AttrEventType: "WalletBalanceChanged", AttrEventVersion: "1", AttrEventID: m.EventID.String(),
		AttrCorrelationID: "corr-wallet-1",
	}
	if len(in.MessageAttributes) != len(want) {
		t.Fatalf("atributos = %v", in.MessageAttributes)
	}
	for k, v := range want {
		if got := aws.ToString(in.MessageAttributes[k].StringValue); got != v {
			t.Errorf("atributo %s = %q, want %q", k, got, v)
		}
	}
	if aws.ToString(in.MessageAttributes[AttrEventVersion].DataType) != "Number" {
		t.Error("eventVersion deve ser Number")
	}
	m.CorrelationID = ""
	if _, ok := SendInput(testQueueURL, m).MessageAttributes[AttrCorrelationID]; ok {
		t.Error("correlationId vazio não deve virar atributo")
	}
}

func TestRunOncePublishesAndMarks(t *testing.T) {
	s := &memStore{}
	q := newFakeQueue()
	met, reg := newMetrics(t)
	batch := []postgres.OutboxMessage{message("a"), message("b"), message("c")}
	s.push(batch...)
	p := newTestPublisher(t, testConfig(), s, q, WithMetrics(met))
	n, err := p.RunOnce(t.Context())
	if err != nil || n != 3 {
		t.Fatalf("RunOnce = %d, %v", n, err)
	}
	_, accepted := q.snapshot()
	if len(accepted) != 3 {
		t.Fatalf("aceitas = %d", len(accepted))
	}
	byDedup := map[string]sentMessage{}
	for _, m := range accepted {
		byDedup[m.dedup] = m
	}
	for _, m := range batch {
		got, ok := byDedup[m.EventID.String()]
		if !ok || got.group != m.AggregateID || got.body != string(m.Payload) {
			t.Fatalf("mensagem de %s = %+v", m.EventID, got)
		}
	}
	if pub, failed, rel := s.counts(); pub != 3 || failed != 0 || rel != 0 {
		t.Fatalf("published/failed/released = %d/%d/%d", pub, failed, rel)
	}
	for _, l := range s.published {
		if l.Owner != "pub-test" {
			t.Fatalf("lease com owner %q", l.Owner)
		}
	}
	if got := metricValue(t, reg, "wallet_outbox_publish_delay_seconds", nil); got != 3 {
		t.Fatalf("observações de atraso = %v", got)
	}
	if got := metricValue(t, reg, "wallet_processing_duration_seconds", map[string]string{"operation": "outbox_publish", "outcome": "success"}); got != 3 {
		t.Fatalf("latência success = %v", got)
	}
	// Lote vazio.
	if n, err := p.RunOnce(t.Context()); n != 0 || err != nil {
		t.Fatalf("RunOnce vazio = %d, %v", n, err)
	}
}

func TestRunOnceSendFailureMarksFailed(t *testing.T) {
	s := &memStore{}
	q := newFakeQueue()
	met, reg := newMetrics(t)
	bad, good := message("a"), message("b")
	s.push(bad, good)
	q.setFail(func(in *awssqs.SendMessageInput) error {
		if aws.ToString(in.MessageDeduplicationId) == bad.EventID.String() {
			return errFakeSend
		}
		return nil
	})
	p := newTestPublisher(t, testConfig(), s, q, WithMetrics(met))
	if _, err := p.RunOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(s.failed) != 1 || s.failed[0].lease.EventID != bad.EventID || !strings.Contains(s.failed[0].cause, errFakeSend.Error()) {
		t.Fatalf("failed = %+v", s.failed)
	}
	if len(s.published) != 1 || s.published[0].EventID != good.EventID {
		t.Fatalf("published = %+v", s.published)
	}
	if got := metricValue(t, reg, "wallet_retries_total", map[string]string{"operation": "outbox_publish"}); got != 1 {
		t.Fatalf("retries = %v", got)
	}
}

func TestRunOnceMissingMessageIDIsFailure(t *testing.T) {
	s := &memStore{}
	s.push(message("a"))
	p := newTestPublisher(t, testConfig(), s, senderFunc(func(context.Context, *awssqs.SendMessageInput) (*awssqs.SendMessageOutput, error) {
		return &awssqs.SendMessageOutput{}, nil
	}))
	if _, err := p.RunOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, failed, _ := s.counts(); failed != 1 {
		t.Fatalf("failed = %d", failed)
	}
}

type senderFunc func(context.Context, *awssqs.SendMessageInput) (*awssqs.SendMessageOutput, error)

func (f senderFunc) SendMessage(ctx context.Context, in *awssqs.SendMessageInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	return f(ctx, in)
}

func TestRunOnceMarkLeaseLostIsReportedNotRetried(t *testing.T) {
	s := &memStore{markErr: postgres.ErrOutboxLeaseLost}
	q := newFakeQueue()
	met, reg := newMetrics(t)
	s.push(message("a"))
	p := newTestPublisher(t, testConfig(), s, q, WithMetrics(met))
	if _, err := p.RunOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, failed, _ := s.counts(); failed != 0 {
		t.Fatal("lease perdido após publicar não deve virar MarkFailed")
	}
	if got := metricValue(t, reg, "wallet_processing_duration_seconds", map[string]string{"outcome": "conflict"}); got != 1 {
		t.Fatalf("outcome conflict = %v", got)
	}
	if got := metricValue(t, reg, "wallet_outbox_publish_delay_seconds", nil); got != 0 {
		t.Fatal("atraso só é observado com a publicação confirmada")
	}
}

func TestRunOnceClaimErrorIsReturned(t *testing.T) {
	s := &memStore{claimErr: errors.New("db down")}
	p := newTestPublisher(t, testConfig(), s, newFakeQueue())
	if _, err := p.RunOnce(t.Context()); err == nil {
		t.Fatal("esperava erro do claim")
	}
}

// Itens cujo lease já não cobre envio + marcação são devolvidos (Release)
// sem publicar.
func TestRunOnceReleasesItemsNearLeaseEnd(t *testing.T) {
	s := &memStore{}
	q := newFakeQueue()
	s.push(message("a"), message("b"))
	var calls atomic.Int64
	base := time.Now()
	clock := func() time.Time {
		// 1ª leitura: claimedAt; as seguintes já passaram do prazo.
		if calls.Add(1) == 1 {
			return base
		}
		return base.Add(59 * time.Second)
	}
	p := newTestPublisher(t, testConfig(), s, q, WithClock(clock))
	if n, err := p.RunOnce(t.Context()); n != 2 || err != nil {
		t.Fatalf("RunOnce = %d, %v", n, err)
	}
	if pub, failed, rel := s.counts(); pub != 0 || failed != 0 || rel != 2 {
		t.Fatalf("published/failed/released = %d/%d/%d", pub, failed, rel)
	}
	if calls, _ := q.snapshot(); len(calls) != 0 {
		t.Fatal("nada deveria ser enviado")
	}
}

func TestStartStopPublishesThroughLoop(t *testing.T) {
	s := &memStore{}
	q := newFakeQueue()
	met, reg := newMetrics(t)
	s.backlog = postgres.OutboxBacklog{Pending: 5, OldestOccurredAt: time.Now().Add(-time.Minute), Now: time.Now()}
	for i := range 4 {
		s.push(message(fmt.Sprintf("g%d", i)))
	}
	p := newTestPublisher(t, testConfig(), s, q, WithMetrics(met))
	if err := p.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := p.Start(t.Context()); err == nil {
		t.Fatal("Start duplicado deveria falhar")
	}
	waitFor(t, func() bool { pub, _, _ := s.counts(); return pub == 4 })
	waitFor(t, func() bool { return metricValue(t, reg, "wallet_outbox_pending_events", nil) == 5 })
	if age := metricValue(t, reg, "wallet_outbox_oldest_pending_age_seconds", nil); age < 59 {
		t.Fatalf("idade = %v", age)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.Stop(ctx); err != nil {
		t.Fatal("Stop repetido deve ser no-op")
	}
}

func TestStopWithoutStartIsNoop(t *testing.T) {
	p := newTestPublisher(t, testConfig(), &memStore{}, newFakeQueue())
	if err := p.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// No shutdown, o envio em voo termina (drenagem) e os itens do lote ainda
// não iniciados são devolvidos.
func TestStopDrainsInFlightAndReleasesRest(t *testing.T) {
	s := &memStore{}
	q := newFakeQueue()
	bs := newBlockingSender(q, true)
	cfg := testConfig()
	cfg.Concurrency = 1
	first, second, third := message("a"), message("b"), message("c")
	s.push(first, second, third)
	p := newTestPublisher(t, cfg, s, bs)
	if err := p.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := <-bs.started; got != first.EventID.String() {
		t.Fatalf("primeiro envio = %s", got)
	}
	stopped := make(chan error, 1)
	go func() { stopped <- p.Stop(context.Background()) }()
	// Stop espera a drenagem: o envio em voo ainda não terminou.
	select {
	case err := <-stopped:
		t.Fatalf("Stop voltou antes da drenagem: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(bs.release)
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	pub, failed, rel := s.counts()
	if pub != 1 || failed != 0 || rel != 2 {
		t.Fatalf("published/failed/released = %d/%d/%d", pub, failed, rel)
	}
	if s.published[0].EventID != first.EventID {
		t.Fatal("o evento em voo deveria ter sido marcado")
	}
}

// Drenagem estourada: o envio é cancelado, nada é marcado (o lease expira
// e outra instância republica) e Stop devolve ErrDrainTimeout.
func TestStopDrainTimeoutCancelsInFlight(t *testing.T) {
	s := &memStore{}
	bs := newBlockingSender(newFakeQueue(), true)
	cfg := testConfig()
	cfg.DrainTimeout = 50 * time.Millisecond
	s.push(message("a"))
	p := newTestPublisher(t, cfg, s, bs)
	if err := p.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-bs.started
	err := p.Stop(context.Background())
	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("Stop = %v, want ErrDrainTimeout", err)
	}
	if pub, failed, _ := s.counts(); pub != 0 || failed != 0 {
		t.Fatalf("published/failed = %d/%d", pub, failed)
	}
}

// O ctx do OnStop vence antes da drenagem: Stop desiste de esperar.
func TestStopContextExpiresBeforeDrain(t *testing.T) {
	s := &memStore{}
	bs := newBlockingSender(newFakeQueue(), false) // ignora cancelamento
	cfg := testConfig()
	cfg.DrainTimeout = 20 * time.Millisecond
	s.push(message("a"))
	p := newTestPublisher(t, cfg, s, bs)
	if err := p.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-bs.started
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	err := p.Stop(ctx)
	if !errors.Is(err, ErrDrainTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v", err)
	}
	// Libera a goroutine presa (goleak no TestMain).
	close(bs.release)
	<-p.loopDone
}
