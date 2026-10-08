//go:build integration

package postgres_test

// Claim da outbox com ordem por agregado (head-of-line, ADR-024),
// Release e Backlog.

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// walletEvents cria n WalletBalanceChanged da MESMA carteira (mesmo
// aggregateId), na ordem dos movimentos.
func walletEvents(t *testing.T, n int) []domain.IntegrationEvent {
	t.Helper()
	at := baseTime()
	w, entry, err := domain.NewWallet(domain.NewWalletParams{
		ID: newID(t), PlayerID: newID(t), InitialBalance: money(t, "10.00"),
		OpeningTransactionID: newID(t), Now: at,
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
		e, err := w.Credit(money(t, "1.00"), newID(t), at.Add(time.Duration(i)*time.Millisecond))
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

func eventIDs(msgs []postgres.OutboxMessage) []uuid.UUID {
	ids := make([]uuid.UUID, len(msgs))
	for i, m := range msgs {
		ids[i] = m.EventID
	}
	return ids
}

func TestIntegrationOutboxRepository_ClaimHeadOfLinePerAggregate(t *testing.T) {
	t.Parallel()
	r := newInboxOutbox(t)
	ctx := t.Context()
	a := walletEvents(t, 3)
	b := walletEvents(t, 2)
	// Intercalados em commits separados: a1, b1, a2, b2, a3.
	for _, e := range []domain.IntegrationEvent{a[0], b[0], a[1], b[1], a[2]} {
		r.enqueue(t, e)
	}

	// Só a cabeça de cada agregado.
	got := r.claim(t, "pub-1", time.Minute, 10)
	if want := []uuid.UUID{a[0].ID(), b[0].ID()}; !slices.Equal(eventIDs(got), want) {
		t.Fatalf("claim 1 = %v, want %v", eventIDs(got), want)
	}
	// Cabeças em lease bloqueiam os seguintes do mesmo agregado.
	if got2 := r.claim(t, "pub-2", time.Minute, 10); len(got2) != 0 {
		t.Fatalf("claim com cabeças em lease = %v, want vazio", eventIDs(got2))
	}

	// a1 falha (backoff longo) e b1 é publicado.
	if err := r.outbox.MarkFailed(ctx, r.db.Pool(), got[0].Lease, "sqs down", postgres.OutboxBackoff{Base: time.Hour, Max: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if err := r.outbox.MarkPublished(ctx, r.db.Pool(), got[1].Lease); err != nil {
		t.Fatal(err)
	}
	// a2 continua bloqueado pelo a1 em backoff; b2 virou a cabeça de b.
	got3 := r.claim(t, "pub-2", time.Minute, 10)
	if want := []uuid.UUID{b[1].ID()}; !slices.Equal(eventIDs(got3), want) {
		t.Fatalf("claim com a1 em backoff = %v, want %v", eventIDs(got3), want)
	}

	// Release devolve sem backoff: elegível de novo na hora, attempts mantido.
	if err := r.outbox.Release(ctx, r.db.Pool(), got3[0].Lease); err != nil {
		t.Fatal(err)
	}
	if err := r.outbox.Release(ctx, r.db.Pool(), got3[0].Lease); !errors.Is(err, postgres.ErrOutboxLeaseLost) {
		t.Fatalf("release repetido: err = %v, want ErrOutboxLeaseLost", err)
	}
	got4 := r.claim(t, "pub-3", time.Minute, 10)
	if len(got4) != 1 || got4[0].EventID != b[1].ID() || got4[0].Lease.Attempt != 2 {
		t.Fatalf("claim após release = %+v", got4)
	}
	row := r.outboxRow(t, got4[0].Seq)
	if row.lastError != nil {
		t.Fatalf("release não deveria gravar erro: %v", *row.lastError)
	}

	// Backlog: a1, a2, a3 e b2 pendentes; o mais antigo é a1.
	bl, err := r.outbox.Backlog(ctx, r.db.Pool())
	if err != nil {
		t.Fatal(err)
	}
	if bl.Pending != 4 || !bl.OldestOccurredAt.Equal(a[0].OccurredAt()) || bl.OldestAge() < 0 || bl.Now.IsZero() {
		t.Fatalf("backlog = %+v (oldest want %v)", bl, a[0].OccurredAt())
	}

	// Depois que a1 é reassumido (backoff vencido) e publicado, a2 vira a
	// cabeça.
	r.exec(t, `UPDATE outbox SET next_attempt_at = statement_timestamp() WHERE event_id = $1`, a[0].ID())
	got5 := r.claim(t, "pub-1", time.Minute, 10)
	if len(got5) != 1 || got5[0].EventID != a[0].ID() {
		t.Fatalf("claim de a1 após backoff = %v", eventIDs(got5))
	}
	if err := r.outbox.MarkPublished(ctx, r.db.Pool(), got5[0].Lease); err != nil {
		t.Fatal(err)
	}
	got6 := r.claim(t, "pub-1", time.Minute, 10)
	if want := []uuid.UUID{a[1].ID()}; !slices.Equal(eventIDs(got6), want) {
		t.Fatalf("claim após publicar a1 = %v, want %v", eventIDs(got6), want)
	}
}

// Backlog vazio: zero pendentes e idade zero.
func TestIntegrationOutboxRepository_BacklogEmpty(t *testing.T) {
	t.Parallel()
	r := newInboxOutbox(t)
	bl, err := r.outbox.Backlog(t.Context(), r.db.Pool())
	if err != nil {
		t.Fatal(err)
	}
	if bl.Pending != 0 || !bl.OldestOccurredAt.IsZero() || bl.OldestAge() != 0 {
		t.Fatalf("backlog = %+v", bl)
	}
}

// Claimers concorrentes sobre agregados com vários eventos: nunca dois
// eventos do mesmo agregado em voo ao mesmo tempo, e cada agregado é
// publicado em ordem de seq, sem perda nem duplicidade.
func TestIntegrationOutboxRepository_ConcurrentClaimersPreserveAggregateOrder(t *testing.T) {
	t.Parallel()
	r := newInboxOutbox(t)
	const aggregates, perAggregate, claimers = 8, 6, 6
	want := map[string][]uuid.UUID{}
	var all []domain.IntegrationEvent
	for range aggregates {
		evs := walletEvents(t, perAggregate)
		for _, e := range evs {
			want[e.AggregateID().String()] = append(want[e.AggregateID().String()], e.ID())
		}
		all = append(all, evs...)
	}
	// Intercala os agregados em commits separados.
	for i := range perAggregate {
		for g := range aggregates {
			r.enqueue(t, all[g*perAggregate+i])
		}
	}

	var (
		mu       sync.Mutex
		inFlight = map[string]bool{}
		order    = map[string][]uuid.UUID{}
		done     int
		failures []string
	)
	var wg sync.WaitGroup
	for c := range claimers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			owner := "pub-" + string(rune('a'+c))
			deadline := time.Now().Add(30 * time.Second)
			for time.Now().Before(deadline) {
				mu.Lock()
				finished := done == aggregates*perAggregate
				mu.Unlock()
				if finished {
					return
				}
				msgs, err := r.outbox.Claim(t.Context(), r.db.Pool(), owner, time.Minute, 3)
				if err != nil {
					mu.Lock()
					failures = append(failures, err.Error())
					mu.Unlock()
					return
				}
				mu.Lock()
				for _, m := range msgs {
					if inFlight[m.AggregateID] {
						failures = append(failures, "dois eventos em voo no agregado "+m.AggregateID)
					}
					inFlight[m.AggregateID] = true
				}
				mu.Unlock()
				for _, m := range msgs {
					mu.Lock()
					order[m.AggregateID] = append(order[m.AggregateID], m.EventID)
					// Sai do conjunto ANTES do commit da marcação: o próximo
					// do agregado só fica elegível depois dela.
					inFlight[m.AggregateID] = false
					mu.Unlock()
					if err := r.outbox.MarkPublished(t.Context(), r.db.Pool(), m.Lease); err != nil {
						mu.Lock()
						failures = append(failures, err.Error())
						mu.Unlock()
						return
					}
					mu.Lock()
					done++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	if len(failures) > 0 {
		t.Fatalf("falhas: %v", failures)
	}
	if done != aggregates*perAggregate {
		t.Fatalf("publicados %d, want %d", done, aggregates*perAggregate)
	}
	for agg, ids := range want {
		if !slices.Equal(order[agg], ids) {
			t.Fatalf("agregado %s publicado em %v, want %v", agg, order[agg], ids)
		}
	}
}
