//go:build integration

package outboxpublisher

// Integração do publisher (T22) com PostgreSQL real e fila FIFO falsa
// (fakeQueue): disputa entre dois publishers sem perda, crash entre o commit
// e a publicação, crash entre a publicação e a marcação (mesmo eventId),
// lease perdido durante o envio (fencing), ordem por agregado sob falha com
// backoff persistido, métricas e ciclo de vida Fx.

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/goleak"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
)

// walletEvents cria n WalletBalanceChanged da MESMA carteira (mesmo
// aggregateId = MessageGroupId), na ordem dos movimentos.
func walletEvents(t *testing.T, n int) []domain.IntegrationEvent {
	t.Helper()
	at := time.Now().UTC().Truncate(time.Microsecond)
	initial, err := domain.Parse("10.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	one, err := domain.Parse("1.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	w, entry, err := domain.NewWallet(domain.NewWalletParams{
		ID: uuid.Must(uuid.NewV7()), PlayerID: uuid.Must(uuid.NewV7()), InitialBalance: initial,
		OpeningTransactionID: uuid.Must(uuid.NewV7()), Now: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	meta := domain.EventMetadata{CorrelationID: "corr-" + w.ID().String()}
	first, err := domain.NewWalletBalanceChangedEvent(*entry, w, meta)
	if err != nil {
		t.Fatal(err)
	}
	out := []domain.IntegrationEvent{first}
	for i := 1; i < n; i++ {
		e, err := w.Credit(one, uuid.Must(uuid.NewV7()), at.Add(time.Duration(i)*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		ev, err := domain.NewWalletBalanceChangedEvent(e, w, meta)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, ev)
	}
	return out
}

var outboxRepo = postgres.NewOutboxRepository()

// enqueue grava os eventos num commit próprio (como um caso de uso).
func enqueue(t *testing.T, db *postgres.DB, events ...domain.IntegrationEvent) {
	t.Helper()
	err := pgx.BeginFunc(t.Context(), db.Pool(), func(tx pgx.Tx) error {
		return outboxRepo.Enqueue(t.Context(), tx, events...)
	})
	if err != nil {
		t.Fatal(err)
	}
}

type outboxState struct {
	attempts      int
	lockedBy      *string
	nextAttemptAt time.Time
	lastError     *string
	publishedAt   *time.Time
	dbNow         time.Time
}

func stateOf(t *testing.T, db *postgres.DB, eventID uuid.UUID) outboxState {
	t.Helper()
	var s outboxState
	err := db.Pool().QueryRow(t.Context(), `SELECT attempts, locked_by, next_attempt_at, last_error, published_at, clock_timestamp()
FROM outbox WHERE event_id = $1`, eventID).Scan(&s.attempts, &s.lockedBy, &s.nextAttemptAt, &s.lastError, &s.publishedAt, &s.dbNow)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func unpublished(t *testing.T, db *postgres.DB) int {
	t.Helper()
	var n int
	if err := db.Pool().QueryRow(t.Context(), `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func integrationConfig(owner string) Config {
	return Config{
		QueueURL: testQueueURL, Owner: owner, PollInterval: 10 * time.Millisecond,
		BatchSize: 4, Concurrency: 3, Lease: 5 * time.Second, PublishTimeout: time.Second,
		DrainTimeout: 5 * time.Second,
	}
}

// fastBackoff mantém os testes rápidos (o backoff real é configurável).
var fastBackoff = postgres.OutboxBackoff{Base: 10 * time.Millisecond, Max: 50 * time.Millisecond}

func newStore(t *testing.T, db *postgres.DB, backoff postgres.OutboxBackoff) *PostgresStore {
	t.Helper()
	s, err := NewPostgresStore(db.Pool(), outboxRepo, backoff)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// stopOnCleanup registra o Stop do publisher no cleanup.
func stopOnCleanup(t *testing.T, p *Publisher) {
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = p.Stop(ctx)
	})
}

var errCrashed = errors.New("simulado: processo morreu")

// deadStore delega o Claim ao store real e "morre" logo depois: nenhuma
// marcação chega ao banco. O estado resultante é o de um processo encerrado
// abruptamente depois do claim (lease gravado, nada marcado).
type deadStore struct{ Store }

func (deadStore) MarkPublished(context.Context, postgres.OutboxLease) error { return errCrashed }
func (deadStore) MarkFailed(context.Context, postgres.OutboxLease, string) error {
	return errCrashed
}
func (deadStore) Release(context.Context, postgres.OutboxLease) error { return errCrashed }

// countingSender conta os envios de uma instância.
type countingSender struct {
	inner Sender
	n     atomic.Int64
}

func (c *countingSender) SendMessage(ctx context.Context, in *awssqs.SendMessageInput, opts ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	c.n.Add(1)
	return c.inner.SendMessage(ctx, in, opts...)
}

// Dois publishers (cada um com seu pool, como instâncias distintas) disputam
// a mesma outbox enquanto novos eventos são confirmados e o SQS falha de vez
// em quando. Nenhum evento se perde, cada eventId entra na fila uma única
// vez, a ordem por agregado (MessageGroupId) é a de seq e todo evento termina
// marcado como publicado.
func TestIntegrationTwoPublishersNoLoss(t *testing.T) {
	dsn := migratedDSN(t)
	producer := startDB(t, dsn)
	q := newFakeQueue()
	var calls atomic.Int64
	q.setFail(func(*awssqs.SendMessageInput) error {
		if calls.Add(1)%5 == 0 { // 20% dos envios falham
			return errFakeSend
		}
		return nil
	})

	const wallets, perWallet = 12, 8
	want := map[string][]string{}
	batches := make([][]domain.IntegrationEvent, perWallet)
	for range wallets {
		evs := walletEvents(t, perWallet)
		for i, e := range evs {
			want[e.AggregateID().String()] = append(want[e.AggregateID().String()], e.ID().String())
			batches[i] = append(batches[i], e)
		}
	}
	// Metade confirmada antes de os publishers subirem.
	for _, round := range batches[:perWallet/2] {
		for _, e := range round {
			enqueue(t, producer, e)
		}
	}

	senders := make([]*countingSender, 2)
	for i := range senders {
		db := startDB(t, dsn)
		senders[i] = &countingSender{inner: q}
		p, err := New(integrationConfig("pub-"+string(rune('a'+i))), newStore(t, db, fastBackoff), senders[i])
		if err != nil {
			t.Fatal(err)
		}
		if err := p.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		stopOnCleanup(t, p)
	}
	// A outra metade é confirmada com os publishers rodando.
	for _, round := range batches[perWallet/2:] {
		for _, e := range round {
			enqueue(t, producer, e)
		}
	}

	waitForLong(t, 60*time.Second, func() bool { return unpublished(t, producer) == 0 })

	_, accepted := q.snapshot()
	if len(accepted) != wallets*perWallet {
		t.Fatalf("mensagens na fila = %d, want %d", len(accepted), wallets*perWallet)
	}
	got := q.groupOrder()
	for agg, ids := range want {
		if !slices.Equal(got[agg], ids) {
			t.Fatalf("grupo %s na fila = %v, want %v", agg, got[agg], ids)
		}
	}
	for i, s := range senders {
		if s.n.Load() == 0 {
			t.Fatalf("publisher %d não publicou nada: não houve disputa", i)
		}
	}
	if calls.Load() <= int64(wallets*perWallet) {
		t.Fatalf("envios = %d: as falhas injetadas deveriam forçar novas tentativas", calls.Load())
	}
	var failedOnce int
	if err := producer.Pool().QueryRow(t.Context(), `SELECT count(*) FROM outbox WHERE last_error IS NOT NULL`).Scan(&failedOnce); err != nil {
		t.Fatal(err)
	}
	if failedOnce == 0 {
		t.Fatal("nenhuma falha registrada em last_error")
	}
	t.Logf("envios: total %d (pub-a %d, pub-b %d), eventos com falha registrada: %d",
		calls.Load(), senders[0].n.Load(), senders[1].n.Load(), failedOnce)
}

func waitForLong(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condição não atingida em %v", d)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Crash entre o commit e a publicação: (1) evento confirmado e nenhum
// publisher vivo; (2) publisher A reivindica e morre antes de enviar. Outra
// instância publica os dois, o (2) só depois que o lease de A expira, com o
// mesmo eventId e o mesmo corpo.
func TestIntegrationCrashBetweenCommitAndPublish(t *testing.T) {
	dsn := migratedDSN(t)
	dbA, dbB := startDB(t, dsn), startDB(t, dsn)
	claimedByA := walletEvents(t, 1)[0]
	enqueue(t, dbA, claimedByA)

	// A reivindica e "morre": o envio nunca acontece e nada é marcado.
	cfgA := integrationConfig("pub-a")
	cfgA.Lease, cfgA.PublishTimeout = 1500*time.Millisecond, 300*time.Millisecond
	a, err := New(cfgA, deadStore{newStore(t, dbA, fastBackoff)}, senderFunc(
		func(context.Context, *awssqs.SendMessageInput) (*awssqs.SendMessageOutput, error) {
			return nil, errCrashed
		}))
	if err != nil {
		t.Fatal(err)
	}
	if n, err := a.RunOnce(t.Context()); n != 1 || err != nil {
		t.Fatalf("A.RunOnce = %d, %v", n, err)
	}
	if s := stateOf(t, dbA, claimedByA.ID()); s.lockedBy == nil || *s.lockedBy != "pub-a" || s.publishedAt != nil {
		t.Fatalf("estado após o crash de A = %+v", s)
	}

	// Evento confirmado depois, sem publisher vivo.
	committedOnly := walletEvents(t, 1)[0]
	enqueue(t, dbA, committedOnly)

	q := newFakeQueue()
	b, err := New(integrationConfig("pub-b"), newStore(t, dbB, fastBackoff), q)
	if err != nil {
		t.Fatal(err)
	}
	// B publica o que não está em lease; o evento de A segue reservado.
	if n, err := b.RunOnce(t.Context()); n != 1 || err != nil {
		t.Fatalf("B.RunOnce = %d, %v", n, err)
	}
	if _, acc := q.snapshot(); len(acc) != 1 || acc[0].dedup != committedOnly.ID().String() {
		t.Fatalf("fila = %+v", acc)
	}
	// Expirado o lease de A, B reassume e publica.
	waitForLong(t, 10*time.Second, func() bool {
		n, err := b.RunOnce(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		return n == 1
	})
	_, acc := q.snapshot()
	if len(acc) != 2 || acc[1].dedup != claimedByA.ID().String() || acc[1].body != string(claimedByA.Payload()) ||
		acc[1].group != claimedByA.AggregateID().String() {
		t.Fatalf("fila = %+v", acc)
	}
	for _, e := range []domain.IntegrationEvent{claimedByA, committedOnly} {
		if s := stateOf(t, dbB, e.ID()); s.publishedAt == nil || s.lockedBy != nil {
			t.Fatalf("evento %s não marcado: %+v", e.ID(), s)
		}
	}
	if s := stateOf(t, dbB, claimedByA.ID()); s.attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (claim de A + claim de B)", s.attempts)
	}
}

// Crash entre a publicação e a marcação: A publica e morre antes do
// MarkPublished. Enquanto o lease vale, ninguém republica; expirado, B
// republica com o MESMO eventId (MessageDeduplicationId) e o mesmo corpo, a
// fila FIFO descarta a duplicata e o evento fica marcado por B. A marcação
// tardia de A é recusada pelo fencing.
func TestIntegrationCrashBetweenPublishAndMark(t *testing.T) {
	dsn := migratedDSN(t)
	dbA, dbB := startDB(t, dsn), startDB(t, dsn)
	evs := walletEvents(t, 2)
	enqueue(t, dbA, evs...)
	q := newFakeQueue()

	cfgA := integrationConfig("pub-a")
	cfgA.Lease, cfgA.PublishTimeout = 1500*time.Millisecond, 300*time.Millisecond
	storeA := newStore(t, dbA, fastBackoff)
	var claimedByA []postgres.OutboxMessage
	a, err := New(cfgA, deadStore{&recordingStore{Store: storeA, claimed: &claimedByA}}, q)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := a.RunOnce(t.Context()); n != 1 || err != nil {
		t.Fatalf("A.RunOnce = %d, %v", n, err)
	}
	if _, acc := q.snapshot(); len(acc) != 1 || acc[0].dedup != evs[0].ID().String() {
		t.Fatalf("fila após A = %+v", acc)
	}
	if s := stateOf(t, dbA, evs[0].ID()); s.publishedAt != nil || s.lockedBy == nil {
		t.Fatalf("evento deveria seguir não publicado e em lease: %+v", s)
	}

	b, err := New(integrationConfig("pub-b"), newStore(t, dbB, fastBackoff), q)
	if err != nil {
		t.Fatal(err)
	}
	// Lease de A vigente: nada a fazer (e o 2º evento do agregado espera).
	if n, err := b.RunOnce(t.Context()); n != 0 || err != nil {
		t.Fatalf("B.RunOnce com lease vigente = %d, %v", n, err)
	}
	waitForLong(t, 10*time.Second, func() bool {
		n, err := b.RunOnce(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		return n == 1
	})
	calls, acc := q.snapshot()
	if len(calls) != 2 || calls[0].dedup != calls[1].dedup || calls[0].body != calls[1].body ||
		calls[0].group != calls[1].group || !calls[1].duplicate {
		t.Fatalf("envios = %+v: a republicação deve repetir eventId e corpo", calls)
	}
	if len(acc) != 1 {
		t.Fatalf("fila = %+v: a duplicata deveria ter sido descartada", acc)
	}
	s := stateOf(t, dbB, evs[0].ID())
	if s.publishedAt == nil || s.lockedBy != nil || s.attempts != 2 {
		t.Fatalf("estado final = %+v", s)
	}
	// A volta e tenta marcar com o lease antigo: recusado, nada muda.
	if len(claimedByA) != 1 {
		t.Fatalf("claims de A = %d", len(claimedByA))
	}
	if err := storeA.MarkPublished(t.Context(), claimedByA[0].Lease); !errors.Is(err, postgres.ErrOutboxLeaseLost) {
		t.Fatalf("marcação tardia de A: err = %v, want ErrOutboxLeaseLost", err)
	}
	// O 2º evento do agregado só sai depois do 1º marcado, em ordem.
	if n, err := b.RunOnce(t.Context()); n != 1 || err != nil {
		t.Fatalf("B.RunOnce do 2º evento = %d, %v", n, err)
	}
	if got := q.groupOrder()[evs[0].AggregateID().String()]; !slices.Equal(got, []string{evs[0].ID().String(), evs[1].ID().String()}) {
		t.Fatalf("ordem do grupo = %v", got)
	}
}

// recordingStore guarda os lotes reivindicados.
type recordingStore struct {
	Store
	mu      sync.Mutex
	claimed *[]postgres.OutboxMessage
}

func (r *recordingStore) Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]postgres.OutboxMessage, error) {
	msgs, err := r.Store.Claim(ctx, owner, lease, limit)
	r.mu.Lock()
	*r.claimed = append(*r.claimed, msgs...)
	r.mu.Unlock()
	return msgs, err
}

// Lease perdido durante o envio: o SendMessage de A "chega" depois que o
// lease expirou e B já republicou e marcou. A marcação de A é recusada
// (fencing) e não altera o registro de B.
func TestIntegrationLeaseLostDuringSend(t *testing.T) {
	dsn := migratedDSN(t)
	dbA, dbB := startDB(t, dsn), startDB(t, dsn)
	e := walletEvents(t, 1)[0]
	enqueue(t, dbA, e)
	q := newFakeQueue()

	slow := newBlockingSender(q, false) // ignora o prazo da chamada
	cfgA := integrationConfig("pub-a")
	cfgA.Lease, cfgA.PublishTimeout = 1500*time.Millisecond, 300*time.Millisecond
	a, err := New(cfgA, newStore(t, dbA, fastBackoff), slow)
	if err != nil {
		t.Fatal(err)
	}
	aDone := make(chan error, 1)
	go func() {
		_, err := a.RunOnce(context.Background())
		aDone <- err
	}()
	<-slow.started

	b, err := New(integrationConfig("pub-b"), newStore(t, dbB, fastBackoff), q)
	if err != nil {
		t.Fatal(err)
	}
	waitForLong(t, 10*time.Second, func() bool {
		n, err := b.RunOnce(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		return n == 1
	})
	before := stateOf(t, dbB, e.ID())
	if before.publishedAt == nil {
		t.Fatalf("B deveria ter publicado: %+v", before)
	}
	close(slow.release)
	if err := <-aDone; err != nil {
		t.Fatal(err)
	}
	after := stateOf(t, dbB, e.ID())
	if !after.publishedAt.Equal(*before.publishedAt) || after.attempts != before.attempts || after.lastError != nil {
		t.Fatalf("A alterou o registro de B: antes %+v, depois %+v", before, after)
	}
	calls, acc := q.snapshot()
	if len(calls) != 2 || len(acc) != 1 || calls[0].dedup != calls[1].dedup {
		t.Fatalf("envios = %+v, fila = %+v", calls, acc)
	}
}

// Ordem por agregado sob falha: o 1º evento do agregado X falha e entra em
// backoff persistido (last_error, next_attempt_at); o 2º de X fica retido,
// enquanto o agregado Y segue. Quando o SQS volta, X sai em ordem.
func TestIntegrationHeadOfLineAndBackoffUnderFailure(t *testing.T) {
	dsn := migratedDSN(t)
	db := startDB(t, dsn)
	x := walletEvents(t, 2)
	y := walletEvents(t, 1)
	enqueue(t, db, x[0])
	enqueue(t, db, y[0])
	enqueue(t, db, x[1])
	q := newFakeQueue()
	var failing atomic.Bool
	failing.Store(true)
	q.setFail(func(in *awssqs.SendMessageInput) error {
		if failing.Load() && aws.ToString(in.MessageDeduplicationId) == x[0].ID().String() {
			return errFakeSend
		}
		return nil
	})
	backoff := postgres.OutboxBackoff{Base: time.Second, Max: 2 * time.Second}
	p, err := New(integrationConfig("pub-a"), newStore(t, db, backoff), q)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := p.RunOnce(t.Context()); n != 2 || err != nil {
		t.Fatalf("RunOnce = %d, %v (want x1 e y1)", n, err)
	}
	s := stateOf(t, db, x[0].ID())
	if s.publishedAt != nil || s.lockedBy != nil || s.lastError == nil || s.attempts != 1 {
		t.Fatalf("x1 após falha = %+v", s)
	}
	if s.nextAttemptAt.Sub(s.dbNow) < 500*time.Millisecond {
		t.Fatalf("backoff não persistido: next_attempt_at %v, agora %v", s.nextAttemptAt, s.dbNow)
	}
	// Em backoff, x1 não sai e x2 continua retido.
	if n, err := p.RunOnce(t.Context()); n != 0 || err != nil {
		t.Fatalf("RunOnce em backoff = %d, %v", n, err)
	}
	if got := q.groupOrder()[x[0].AggregateID().String()]; len(got) != 0 {
		t.Fatalf("nada de X deveria ter saído: %v", got)
	}
	if got := q.groupOrder()[y[0].AggregateID().String()]; len(got) != 1 {
		t.Fatalf("Y deveria ter saído: %v", got)
	}
	failing.Store(false)
	waitForLong(t, 10*time.Second, func() bool {
		if _, err := p.RunOnce(t.Context()); err != nil {
			t.Fatal(err)
		}
		return unpublished(t, db) == 0
	})
	if got := q.groupOrder()[x[0].AggregateID().String()]; !slices.Equal(got, []string{x[0].ID().String(), x[1].ID().String()}) {
		t.Fatalf("ordem de X = %v", got)
	}
	if s := stateOf(t, db, x[0].ID()); s.attempts != 2 || s.lastError == nil {
		t.Fatalf("x1 final = %+v (last_error fica como histórico)", s)
	}
}

// Métricas: pendentes e idade do mais antigo antes, atraso na publicação e
// zero pendentes depois.
func TestIntegrationBacklogAndDelayMetrics(t *testing.T) {
	db := startDB(t, migratedDSN(t))
	enqueue(t, db, walletEvents(t, 3)...)
	enqueue(t, db, walletEvents(t, 1)...)
	met, reg := newMetrics(t)
	p, err := New(integrationConfig("pub-a"), newStore(t, db, fastBackoff), newFakeQueue(), WithMetrics(met))
	if err != nil {
		t.Fatal(err)
	}
	p.RefreshBacklog(t.Context())
	if got := metricValue(t, reg, "wallet_outbox_pending_events", nil); got != 4 {
		t.Fatalf("pendentes = %v", got)
	}
	waitForLong(t, 10*time.Second, func() bool {
		if _, err := p.RunOnce(t.Context()); err != nil {
			t.Fatal(err)
		}
		return unpublished(t, db) == 0
	})
	p.RefreshBacklog(t.Context())
	if got := metricValue(t, reg, "wallet_outbox_pending_events", nil); got != 0 {
		t.Fatalf("pendentes após publicar = %v", got)
	}
	if got := metricValue(t, reg, "wallet_outbox_oldest_pending_age_seconds", nil); got != 0 {
		t.Fatalf("idade após publicar = %v", got)
	}
	if got := metricValue(t, reg, "wallet_outbox_publish_delay_seconds", nil); got != 4 {
		t.Fatalf("observações de atraso = %v", got)
	}
}

// Composição Fx real (pool do postgres.Module + PostgresStore + publisher):
// start publica a outbox, stop encerra tudo sem goroutines sobrando.
func TestIntegrationFxLifecycleNoLeak(t *testing.T) {
	dsn := migratedDSN(t)
	seed := startDB(t, dsn)
	evs := walletEvents(t, 3)
	enqueue(t, seed, evs...)
	ignore := goleak.IgnoreCurrent()

	var cfg config.Config
	cfg.Database = dbConfig(dsn)
	cfg.SQS.EventsQueueURL = testQueueURL
	cfg.Workers.OutboxPollInterval = 10 * time.Millisecond
	cfg.Workers.OutboxBatchSize = 10
	cfg.Workers.OutboxLease = 5 * time.Second
	cfg.Workers.OutboxConcurrency = 2
	cfg.Workers.OutboxPublishTimeout = time.Second
	cfg.Workers.OutboxDrainTimeout = time.Second
	cfg.Workers.OutboxRetryBaseDelay = 10 * time.Millisecond
	cfg.Workers.OutboxRetryMaxDelay = 50 * time.Millisecond

	q := newFakeQueue()
	app := fxtest.New(t,
		fx.Supply(cfg),
		postgres.Module,
		fx.Provide(
			NewStoreFx,
			func(s *PostgresStore) Store { return s },
			func() Sender { return q },
			NewPublisherFx,
		),
		fx.Invoke(func(*Publisher) {}),
	)
	app.RequireStart()
	waitForLong(t, 10*time.Second, func() bool { return unpublished(t, seed) == 0 })
	app.RequireStop()
	if got := q.groupOrder()[evs[0].AggregateID().String()]; len(got) != 3 {
		t.Fatalf("grupo = %v", got)
	}
	goleak.VerifyNone(t, ignore)
}
