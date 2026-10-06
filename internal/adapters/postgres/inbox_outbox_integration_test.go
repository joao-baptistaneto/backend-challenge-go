//go:build integration

package postgres_test

// Integração dos repositórios de inbox e outbox (T13) contra PostgreSQL real
// (TEST_DATABASE_URL ou testcontainers; banco novo por teste).

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

type inboxOutbox struct {
	repos
	inbox  *postgres.InboxRepository
	outbox *postgres.OutboxRepository
}

func newInboxOutbox(t *testing.T) inboxOutbox {
	t.Helper()
	return inboxOutbox{repos: newRepos(t), inbox: postgres.NewInboxRepository(), outbox: postgres.NewOutboxRepository()}
}

// newEvent cria um WalletBalanceChanged de uma carteira em memória (a outbox
// não tem FK), com occurredAt = at.
func newEvent(t *testing.T, at time.Time) domain.IntegrationEvent {
	t.Helper()
	w, entry, err := domain.NewWallet(domain.NewWalletParams{
		ID: newID(t), PlayerID: newID(t), InitialBalance: money(t, "10.00"),
		OpeningTransactionID: newID(t), Now: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	e, err := domain.NewWalletBalanceChangedEvent(*entry, w, domain.EventMetadata{CorrelationID: "corr-" + w.ID().String()})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// dbNow lê o relógio do PostgreSQL (o mesmo usado por leases e backoff).
func (r inboxOutbox) dbNow(t *testing.T) time.Time {
	t.Helper()
	var now time.Time
	if err := r.db.Pool().QueryRow(t.Context(), `SELECT clock_timestamp()`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	return now
}

type outboxRow struct {
	attempts      int
	lockedBy      *string
	lockedUntil   *time.Time
	nextAttemptAt time.Time
	lastError     *string
	publishedAt   *time.Time
}

func (r inboxOutbox) outboxRow(t *testing.T, seq int64) outboxRow {
	t.Helper()
	var o outboxRow
	err := r.db.Pool().QueryRow(t.Context(), `SELECT attempts, locked_by, locked_until, next_attempt_at, last_error, published_at
FROM outbox WHERE seq = $1`, seq).Scan(&o.attempts, &o.lockedBy, &o.lockedUntil, &o.nextAttemptAt, &o.lastError, &o.publishedAt)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func (r inboxOutbox) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := r.db.Pool().Exec(t.Context(), sql, args...); err != nil {
		t.Fatal(err)
	}
}

func (r inboxOutbox) enqueue(t *testing.T, events ...domain.IntegrationEvent) {
	t.Helper()
	if err := r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		return r.outbox.Enqueue(ctx, tx, events...)
	}); err != nil {
		t.Fatal(err)
	}
}

func (r inboxOutbox) claim(t *testing.T, owner string, lease time.Duration, limit int) []postgres.OutboxMessage {
	t.Helper()
	msgs, err := r.outbox.Claim(t.Context(), r.db.Pool(), owner, lease, limit)
	if err != nil {
		t.Fatal(err)
	}
	return msgs
}

const testConsumer = "wager-transactions"

// --- Inbox ---------------------------------------------------------------

func TestIntegrationInboxRepository_RegisterAndComplete(t *testing.T) {
	t.Parallel()
	r := newInboxOutbox(t)
	ctx := t.Context()
	now := baseTime()
	h1, h2 := hashOf("payload-1"), hashOf("payload-2")

	// Rollback do caso de uso desfaz o registro (mesma transação).
	errBoom := errors.New("boom")
	err := r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		reg, err := r.inbox.Register(ctx, tx, testConsumer, "msg-1", h1, now)
		if err != nil || reg.Status != postgres.InboxNew {
			t.Fatalf("register: %+v %v", reg, err)
		}
		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatal(err)
	}
	if _, err := r.inbox.Get(ctx, r.db.Pool(), testConsumer, "msg-1"); !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("after rollback Get = %v, want ErrNotFound", err)
	}

	// Registro confirmado sem conclusão.
	if err := r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		reg, err := r.inbox.Register(ctx, tx, testConsumer, "msg-1", h1, now)
		if err != nil {
			return err
		}
		if reg.Status != postgres.InboxNew || reg.Message.RequestHash != h1 || !reg.Message.ReceivedAt.Equal(now) || reg.Message.Completed() {
			t.Fatalf("new registration: %+v", reg)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Mesmo hash, ainda não concluída → IN_PROGRESS.
	if err := r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		reg, err := r.inbox.Register(ctx, tx, testConsumer, "msg-1", h1, now.Add(time.Second))
		if err != nil {
			return err
		}
		if reg.Status != postgres.InboxInProgress || !reg.Message.ReceivedAt.Equal(now) {
			t.Fatalf("re-register: %+v", reg)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Hash diferente → conflito, linha intacta.
	err = r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		reg, err := r.inbox.Register(ctx, tx, testConsumer, "msg-1", h2, now)
		if reg.Message.RequestHash != h1 {
			t.Fatalf("mismatch should return the stored message: %+v", reg)
		}
		return err
	})
	if !errors.Is(err, postgres.ErrInboxHashMismatch) {
		t.Fatalf("register with other hash = %v", err)
	}
	// Complete com hash diferente também é recusado.
	err = r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		return r.inbox.Complete(ctx, tx, testConsumer, "msg-1", h2, uuid.Nil, now)
	})
	if !errors.Is(err, postgres.ErrInboxHashMismatch) {
		t.Fatalf("complete with other hash = %v", err)
	}

	// Conclusão associando a transação de domínio (OPENING de uma carteira).
	w := r.openWallet(t, "50.00", now)
	var openingID uuid.UUID
	if err := r.db.Pool().QueryRow(ctx, `SELECT id FROM wager_transactions WHERE wallet_id = $1`, w.ID()).Scan(&openingID); err != nil {
		t.Fatal(err)
	}
	// Relógio desta instância "atrás" do registro: completed_at não pode
	// violar o CHECK completed_at >= received_at.
	if err := r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		return r.inbox.Complete(ctx, tx, testConsumer, "msg-1", h1, openingID, now.Add(-time.Minute))
	}); err != nil {
		t.Fatal(err)
	}
	got, err := r.inbox.Get(ctx, r.db.Pool(), testConsumer, "msg-1")
	if err != nil || got.TransactionID != openingID || !got.CompletedAt.Equal(now) {
		t.Fatalf("completed: %+v %v", got, err)
	}

	// Reentrega após concluída → COMPLETED com o resultado.
	if err := r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		reg, err := r.inbox.Register(ctx, tx, testConsumer, "msg-1", h1, now.Add(time.Hour))
		if err != nil {
			return err
		}
		if reg.Status != postgres.InboxCompleted || reg.Message.TransactionID != openingID {
			t.Fatalf("redelivery: %+v", reg)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Segunda conclusão, mensagem desconhecida e FK inválida.
	for name, c := range map[string]struct {
		msg  string
		txID uuid.UUID
		want error
	}{
		"already completed": {"msg-1", uuid.Nil, postgres.ErrInboxAlreadyCompleted},
		"unknown message":   {"msg-unknown", uuid.Nil, postgres.ErrNotFound},
	} {
		err := r.within(t, func(ctx context.Context, tx pgx.Tx) error {
			return r.inbox.Complete(ctx, tx, testConsumer, c.msg, h1, c.txID, now)
		})
		if !errors.Is(err, c.want) {
			t.Fatalf("%s: %v, want %v", name, err, c.want)
		}
	}
	err = r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := r.inbox.Register(ctx, tx, testConsumer, "msg-fk", h1, now); err != nil {
			return err
		}
		return r.inbox.Complete(ctx, tx, testConsumer, "msg-fk", h1, newID(t), now)
	})
	if !errors.Is(err, postgres.ErrForeignKeyViolation) {
		t.Fatalf("complete with unknown transaction = %v", err)
	}

	// Sem transação de domínio (rejeição de entrada) e consumidores isolados.
	if err := r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		reg, err := r.inbox.Register(ctx, tx, "other-consumer", "msg-1", h2, now)
		if err != nil || reg.Status != postgres.InboxNew {
			t.Fatalf("other consumer: %+v %v", reg, err)
		}
		return r.inbox.Complete(ctx, tx, "other-consumer", "msg-1", h2, uuid.Nil, now)
	}); err != nil {
		t.Fatal(err)
	}
	other, err := r.inbox.Get(ctx, r.db.Pool(), "other-consumer", "msg-1")
	if err != nil || other.TransactionID != uuid.Nil || !other.Completed() {
		t.Fatalf("other consumer: %+v %v", other, err)
	}
}

// Entregas concorrentes da mesma mensagem: exatamente uma processa.
func TestIntegrationInboxRepository_ConcurrentRegister(t *testing.T) {
	t.Parallel()
	r := newInboxOutbox(t)
	now := baseTime()
	h := hashOf("same")
	const workers = 10
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		statuses = map[postgres.InboxStatus]int{}
	)
	start := make(chan struct{})
	for range workers {
		wg.Go(func() {
			<-start
			err := r.tm.WithinTx(t.Context(), func(ctx context.Context, tx pgx.Tx) error {
				reg, err := r.inbox.Register(ctx, tx, testConsumer, "msg-race", h, now)
				if err != nil {
					return err
				}
				if reg.Status == postgres.InboxNew {
					// "Processa" segurando a linha, depois conclui no mesmo commit.
					time.Sleep(50 * time.Millisecond)
					if err := r.inbox.Complete(ctx, tx, testConsumer, "msg-race", h, uuid.Nil, now); err != nil {
						return err
					}
				}
				mu.Lock()
				statuses[reg.Status]++
				mu.Unlock()
				return nil
			})
			if err != nil {
				t.Error(err)
			}
		})
	}
	close(start)
	wg.Wait()
	if statuses[postgres.InboxNew] != 1 || statuses[postgres.InboxCompleted] != workers-1 {
		t.Fatalf("statuses = %v, want 1 NEW and %d COMPLETED", statuses, workers-1)
	}
}

// --- Outbox --------------------------------------------------------------

func TestIntegrationOutboxRepository_EnqueueAndClaimOrderBySeq(t *testing.T) {
	t.Parallel()
	r := newInboxOutbox(t)
	ctx := t.Context()
	now := baseTime()

	// occurred_at decrescente: a ordem do claim precisa ser a de inserção (seq).
	events := []domain.IntegrationEvent{newEvent(t, now), newEvent(t, now.Add(-time.Hour)), newEvent(t, now.Add(-2*time.Hour))}
	r.enqueue(t, events[0], events[1])
	r.enqueue(t, events[2])

	// Rollback descarta o evento junto com o resto da transação.
	errBoom := errors.New("boom")
	if err := r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		if err := r.outbox.Enqueue(ctx, tx, newEvent(t, now.Add(-3*time.Hour))); err != nil {
			return err
		}
		return errBoom
	}); !errors.Is(err, errBoom) {
		t.Fatal(err)
	}
	// eventId repetido.
	err := r.within(t, func(ctx context.Context, tx pgx.Tx) error { return r.outbox.Enqueue(ctx, tx, events[1]) })
	if !errors.Is(err, postgres.ErrDuplicateEvent) {
		t.Fatalf("duplicate enqueue = %v", err)
	}

	before := r.dbNow(t)
	msgs := r.claim(t, "pub-1", time.Minute, 10)
	if len(msgs) != len(events) {
		t.Fatalf("claimed %d, want %d", len(msgs), len(events))
	}
	for i, m := range msgs {
		e := events[i]
		if i > 0 && m.Seq <= msgs[i-1].Seq {
			t.Fatalf("not ordered by seq: %d after %d", m.Seq, msgs[i-1].Seq)
		}
		if m.EventID != e.ID() || m.EventType != e.Type().String() || m.EventVersion != e.Version() ||
			m.AggregateID != e.AggregateID().String() || m.CorrelationID != e.CorrelationID() ||
			!m.OccurredAt.Equal(e.OccurredAt()) || m.LastError != "" {
			t.Fatalf("message %d mismatch: %+v", i, m)
		}
		if !bytes.Equal(m.Payload, e.Payload()) {
			t.Fatalf("payload not byte-identical:\n%s\n%s", m.Payload, e.Payload())
		}
		l := m.Lease
		if l.Seq != m.Seq || l.EventID != e.ID() || l.Owner != "pub-1" || l.Attempt != 1 ||
			l.Until.Before(before.Add(time.Minute)) || l.Until.After(r.dbNow(t).Add(time.Minute)) {
			t.Fatalf("lease %d: %+v (db now before claim %v)", i, l, before)
		}
	}
	// Com lease vigente, ninguém reassume.
	if again := r.claim(t, "pub-2", time.Minute, 10); len(again) != 0 {
		t.Fatalf("claimed %d events under active lease", len(again))
	}
	// Lote limitado respeita a ordem: publicados os dois primeiros, sobra o terceiro.
	for _, m := range msgs[:2] {
		if err := r.outbox.MarkPublished(ctx, r.db.Pool(), m.Lease); err != nil {
			t.Fatal(err)
		}
	}
	row := r.outboxRow(t, msgs[0].Seq)
	if row.publishedAt == nil || row.lockedBy != nil || row.lockedUntil != nil || row.attempts != 1 {
		t.Fatalf("published row: %+v", row)
	}
}

// Claimers concorrentes (vários processos/instâncias) nunca recebem o mesmo
// evento enquanto o lease vale, e juntos drenam a outbox.
func TestIntegrationOutboxRepository_ConcurrentClaimersDoNotDuplicate(t *testing.T) {
	t.Parallel()
	r := newInboxOutbox(t)
	now := baseTime()
	const total = 300
	events := make([]domain.IntegrationEvent, total)
	for i := range events {
		events[i] = newEvent(t, now)
	}
	r.enqueue(t, events...)

	const claimers = 8
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		seen = map[uuid.UUID]string{}
		dups []string
	)
	start := make(chan struct{})
	for c := range claimers {
		owner := fmt.Sprintf("pub-%d", c)
		wg.Go(func() {
			<-start
			for {
				msgs, err := r.outbox.Claim(t.Context(), r.db.Pool(), owner, time.Minute, 7)
				if err != nil {
					t.Error(err)
					return
				}
				if len(msgs) == 0 {
					return
				}
				mu.Lock()
				for _, m := range msgs {
					if prev, ok := seen[m.EventID]; ok {
						dups = append(dups, fmt.Sprintf("%s by %s and %s", m.EventID, prev, owner))
					}
					seen[m.EventID] = owner
				}
				mu.Unlock()
			}
		})
	}
	close(start)
	wg.Wait()
	if len(dups) > 0 {
		t.Fatalf("duplicated claims: %v", dups)
	}
	if len(seen) != total {
		t.Fatalf("claimed %d distinct events, want %d", len(seen), total)
	}
	var attempts int
	if err := r.db.Pool().QueryRow(t.Context(), `SELECT sum(attempts) FROM outbox`).Scan(&attempts); err != nil || attempts != total {
		t.Fatalf("sum(attempts) = %d (%v), want %d (one claim each)", attempts, err, total)
	}
}

// Um claim em andamento (linhas travadas, lease ainda não confirmado) não
// bloqueia nem é duplicado por outro claimer: SKIP LOCKED.
func TestIntegrationOutboxRepository_ClaimSkipsLockedRows(t *testing.T) {
	t.Parallel()
	r := newInboxOutbox(t)
	ctx := t.Context()
	now := baseTime()
	events := []domain.IntegrationEvent{newEvent(t, now), newEvent(t, now), newEvent(t, now)}
	r.enqueue(t, events...)

	txA, err := r.db.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer txA.Rollback(context.Background())
	a, err := r.outbox.Claim(ctx, txA, "pub-a", time.Minute, 2)
	if err != nil || len(a) != 2 || a[0].EventID != events[0].ID() || a[1].EventID != events[1].ID() {
		t.Fatalf("A claimed %d: %v", len(a), err)
	}
	var b []postgres.OutboxMessage
	if err := r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '150ms'`); err != nil {
			return err
		}
		var err error
		b, err = r.outbox.Claim(ctx, tx, "pub-b", time.Minute, 10)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(b) != 1 || b[0].EventID != events[2].ID() {
		t.Fatalf("B claimed %d, want only the third event", len(b))
	}
	// A desiste (rollback): o claim dele não vale e as linhas voltam.
	if err := txA.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	c := r.claim(t, "pub-c", time.Minute, 10)
	if len(c) != 2 || c[0].EventID != events[0].ID() || c[0].Lease.Attempt != 1 {
		t.Fatalf("after rollback claimed %+v", c)
	}
}

// Lease expirado é reassumido; o claimer antigo não consegue mais marcar nada
// (nem com o mesmo owner), e um lease expirado não reassumido também não vale.
func TestIntegrationOutboxRepository_ExpiredLeaseReclaimedAndFenced(t *testing.T) {
	t.Parallel()
	r := newInboxOutbox(t)
	ctx := t.Context()
	pool := r.db.Pool()
	now := baseTime()
	e := newEvent(t, now)
	r.enqueue(t, e)
	backoff := postgres.OutboxBackoff{Base: time.Second, Max: time.Minute}

	const lease = 300 * time.Millisecond
	a := r.claim(t, "pub-a", lease, 10)
	if len(a) != 1 {
		t.Fatalf("A claimed %d", len(a))
	}
	if got := r.claim(t, "pub-b", time.Minute, 10); len(got) != 0 {
		t.Fatal("B claimed under A's active lease")
	}

	// Espera o lease de A vencer (relógio do banco) e B reassume.
	var b []postgres.OutboxMessage
	deadline := time.Now().Add(10 * time.Second)
	for len(b) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("expired lease was not reclaimed")
		}
		time.Sleep(50 * time.Millisecond)
		b = r.claim(t, "pub-b", time.Minute, 10)
	}
	if b[0].EventID != e.ID() || b[0].Lease.Owner != "pub-b" || b[0].Lease.Attempt != 2 || b[0].Seq != a[0].Seq {
		t.Fatalf("reclaimed lease: %+v", b[0].Lease)
	}
	if !bytes.Equal(b[0].Payload, e.Payload()) {
		t.Fatal("reclaimed payload changed (eventId/payload must be preserved)")
	}

	// A (lease antigo) não sobrescreve B.
	if err := r.outbox.MarkPublished(ctx, pool, a[0].Lease); !errors.Is(err, postgres.ErrOutboxLeaseLost) {
		t.Fatalf("stale MarkPublished = %v", err)
	}
	if err := r.outbox.MarkFailed(ctx, pool, a[0].Lease, "late failure", backoff); !errors.Is(err, postgres.ErrOutboxLeaseLost) {
		t.Fatalf("stale MarkFailed = %v", err)
	}
	row := r.outboxRow(t, b[0].Seq)
	if row.publishedAt != nil || row.lockedBy == nil || *row.lockedBy != "pub-b" || row.attempts != 2 || row.lastError != nil {
		t.Fatalf("stale owner changed the row: %+v", row)
	}
	// Mesmo owner com token antigo (outro worker do mesmo processo) também não.
	stale := b[0].Lease
	stale.Attempt = 1
	if err := r.outbox.MarkPublished(ctx, pool, stale); !errors.Is(err, postgres.ErrOutboxLeaseLost) {
		t.Fatalf("same owner, old attempt = %v", err)
	}

	// B, dono do lease vigente, publica; repetir não tem efeito.
	if err := r.outbox.MarkPublished(ctx, pool, b[0].Lease); err != nil {
		t.Fatal(err)
	}
	if err := r.outbox.MarkPublished(ctx, pool, b[0].Lease); !errors.Is(err, postgres.ErrOutboxLeaseLost) {
		t.Fatalf("second MarkPublished = %v", err)
	}
	if err := r.outbox.MarkFailed(ctx, pool, b[0].Lease, "x", backoff); !errors.Is(err, postgres.ErrOutboxLeaseLost) {
		t.Fatalf("MarkFailed after publish = %v", err)
	}
	if got := r.claim(t, "pub-c", time.Minute, 10); len(got) != 0 {
		t.Fatal("published event claimed again")
	}

	// Lease expirado sem ninguém reassumir também não é mais vigente.
	e2 := newEvent(t, now)
	r.enqueue(t, e2)
	c := r.claim(t, "pub-c", 100*time.Millisecond, 10)
	if len(c) != 1 {
		t.Fatalf("claimed %d", len(c))
	}
	time.Sleep(300 * time.Millisecond)
	if err := r.outbox.MarkPublished(ctx, pool, c[0].Lease); !errors.Is(err, postgres.ErrOutboxLeaseLost) {
		t.Fatalf("expired MarkPublished = %v", err)
	}
	if row := r.outboxRow(t, c[0].Seq); row.publishedAt != nil {
		t.Fatal("expired lease published the event")
	}
}

// MarkFailed: backoff exponencial com teto, persistido em next_attempt_at
// pelo relógio do banco; o evento só volta a ser reivindicável quando vence.
func TestIntegrationOutboxRepository_MarkFailedBackoff(t *testing.T) {
	t.Parallel()
	r := newInboxOutbox(t)
	ctx := t.Context()
	pool := r.db.Pool()
	e := newEvent(t, baseTime())
	r.enqueue(t, e)
	backoff := postgres.OutboxBackoff{Base: time.Second, Max: 10 * time.Second}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second}

	var seq int64
	for i, delay := range want {
		msgs := r.claim(t, "pub-1", time.Minute, 10)
		if len(msgs) != 1 || msgs[0].Lease.Attempt != i+1 {
			t.Fatalf("round %d: claimed %+v", i, msgs)
		}
		seq = msgs[0].Seq
		if i > 0 && msgs[0].LastError != fmt.Sprintf("sqs unavailable %d", i-1) {
			t.Fatalf("round %d: last error %q", i, msgs[0].LastError)
		}
		before := r.dbNow(t)
		if err := r.outbox.MarkFailed(ctx, pool, msgs[0].Lease, fmt.Sprintf("sqs unavailable %d", i), backoff); err != nil {
			t.Fatal(err)
		}
		after := r.dbNow(t)
		row := r.outboxRow(t, seq)
		if row.nextAttemptAt.Before(before.Add(delay)) || row.nextAttemptAt.After(after.Add(delay)) {
			t.Fatalf("attempt %d: next_attempt_at %v not in [%v, %v] (+%v)", i+1, row.nextAttemptAt, before.Add(delay), after.Add(delay), delay)
		}
		if row.lockedBy != nil || row.lockedUntil != nil || row.publishedAt != nil || row.attempts != i+1 {
			t.Fatalf("attempt %d: row after failure %+v", i+1, row)
		}
		// Em backoff: não é reivindicável.
		if got := r.claim(t, "pub-2", time.Minute, 10); len(got) != 0 {
			t.Fatalf("attempt %d: claimed during backoff", i+1)
		}
		// Simula a passagem do tempo (a guarda da outbox permite mexer no
		// agendamento; snapshot e attempts continuam protegidos).
		r.exec(t, `UPDATE outbox SET next_attempt_at = statement_timestamp() - interval '1 second' WHERE seq = $1`, seq)
	}

	// Tentativas altíssimas: atraso = teto, sem overflow no Go nem no banco.
	r.exec(t, `UPDATE outbox SET attempts = 2000000000 WHERE seq = $1`, seq)
	msgs := r.claim(t, "pub-1", time.Minute, 10)
	if len(msgs) != 1 || msgs[0].Lease.Attempt != 2000000001 {
		t.Fatalf("claimed %+v", msgs)
	}
	before := r.dbNow(t)
	long := strings.Repeat("é", 3000) + "\x00"
	if err := r.outbox.MarkFailed(ctx, pool, msgs[0].Lease, long, backoff); err != nil {
		t.Fatal(err)
	}
	row := r.outboxRow(t, seq)
	if d := row.nextAttemptAt.Sub(before); d < backoff.Max || d > backoff.Max+5*time.Second {
		t.Fatalf("huge attempt delay %v, want ~%v", d, backoff.Max)
	}
	if row.lastError == nil || len(*row.lastError) > 2048 {
		t.Fatalf("last_error not truncated: %v", row.lastError)
	}

	// Por fim publica.
	r.exec(t, `UPDATE outbox SET next_attempt_at = statement_timestamp() - interval '1 second' WHERE seq = $1`, seq)
	msgs = r.claim(t, "pub-1", time.Minute, 10)
	if len(msgs) != 1 {
		t.Fatalf("claimed %d", len(msgs))
	}
	if err := r.outbox.MarkPublished(ctx, pool, msgs[0].Lease); err != nil {
		t.Fatal(err)
	}
	if row := r.outboxRow(t, seq); row.publishedAt == nil || row.lastError == nil {
		t.Fatalf("published row: %+v", row)
	}
}
