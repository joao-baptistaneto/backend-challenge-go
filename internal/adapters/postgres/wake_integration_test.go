//go:build integration

package postgres_test

// Integração de WagerTransactionRepository.WakePendingReferences (T31,
// ADR-032): quais linhas são acordadas, o que muda nelas e o uso do índice.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

func TestIntegrationWagerTransactionRepository_WakePendingReferences(t *testing.T) {
	t.Parallel()
	r := newRepos(t)
	ctx := t.Context()
	pool := r.db.Pool()
	now := baseTime()
	created := now.Add(-time.Hour)
	w := r.openWallet(t, "100.00", created)

	pending := func(provider, external, reference string, createdAt, next time.Time) *domain.WagerTransaction {
		t.Helper()
		x := newWager(t, w, wagerSpec{provider: provider, external: external, kind: domain.WagerKindRefund,
			amount: "10.00", reference: reference, now: createdAt})
		if err := x.MarkPendingReference(domain.MarkWagerPendingReferenceParams{NextAttemptAt: next, Now: createdAt}); err != nil {
			t.Fatal(err)
		}
		if err := r.wagers.Insert(ctx, pool, x); err != nil {
			t.Fatal(err)
		}
		return x
	}
	future := pending("provider-a", "rf-future", "bet-1", created, now.Add(time.Hour))
	alreadyDue := pending("provider-a", "rf-due", "bet-1", created, now.Add(-time.Minute))
	// Criada "depois" de now (relógio de outra instância adiantado): acorda
	// em created_at, nunca antes (CHECK next_attempt_at >= created_at).
	ahead := pending("provider-a", "rf-ahead", "bet-1", now.Add(10*time.Second), now.Add(time.Hour))
	otherProvider := pending("provider-b", "rf-b", "bet-1", created, now.Add(time.Hour))
	otherReference := pending("provider-a", "rf-other", "bet-2", created, now.Add(time.Hour))

	type sched struct {
		next, updated time.Time
		attempts      int32
	}
	read := func(id domain.WagerTransaction) sched {
		t.Helper()
		var s sched
		if err := pool.QueryRow(ctx, `SELECT next_attempt_at, updated_at, attempts FROM wager_transactions WHERE id = $1`,
			id.ID()).Scan(&s.next, &s.updated, &s.attempts); err != nil {
			t.Fatal(err)
		}
		return s
	}
	before := map[string]sched{}
	all := []*domain.WagerTransaction{future, alreadyDue, ahead, otherProvider, otherReference}
	for _, x := range all {
		before[x.ExternalTransactionID()] = read(*x)
	}

	// Fora de transação e com parâmetros vazios: recusado.
	if _, err := r.wagers.WakePendingReferences(ctx, pool, "provider-a", "bet-1", now); !errors.Is(err, postgres.ErrTxRequired) {
		t.Fatalf("pool: %v", err)
	}
	err := r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		for _, args := range [][2]string{{"", "bet-1"}, {"provider-a", ""}} {
			if _, err := r.wagers.WakePendingReferences(ctx, tx, args[0], args[1], now); !errors.Is(err, postgres.ErrInvalidWake) {
				t.Errorf("invalid %v: %v", args, err)
			}
		}
		if _, err := r.wagers.WakePendingReferences(ctx, tx, "provider-a", "bet-1", time.Time{}); !errors.Is(err, postgres.ErrInvalidWake) {
			t.Errorf("zero now: %v", err)
		}
		n, err := r.wagers.WakePendingReferences(ctx, tx, "provider-a", "bet-1", now)
		if err != nil {
			return err
		}
		if n != 2 {
			t.Errorf("woken = %d, want 2 (future, ahead)", n)
		}
		// Idempotente: já acordadas não contam de novo.
		if n, err := r.wagers.WakePendingReferences(ctx, tx, "provider-a", "bet-1", now); err != nil || n != 0 {
			t.Errorf("second wake = %d, %v", n, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	want := map[string]time.Time{
		"rf-future": now,
		"rf-due":    before["rf-due"].next,
		"rf-ahead":  ahead.CreatedAt(),
		"rf-b":      before["rf-b"].next,
		"rf-other":  before["rf-other"].next,
	}
	for _, x := range all {
		got, b := read(*x), before[x.ExternalTransactionID()]
		if !got.next.Equal(want[x.ExternalTransactionID()]) {
			t.Errorf("%s next_attempt_at = %v, want %v", x.ExternalTransactionID(), got.next, want[x.ExternalTransactionID()])
		}
		if got.attempts != b.attempts || !got.updated.Equal(b.updated) {
			t.Errorf("%s attempts/updated_at changed: %+v -> %+v", x.ExternalTransactionID(), b, got)
		}
	}
	// A linha acordada continua reidratável e PENDING_REFERENCE.
	got, err := r.wagers.GetByID(ctx, pool, future.ID())
	if err != nil || got.Status() != domain.WagerStatusPendingReference || !got.NextAttemptAt().Equal(now) {
		t.Fatalf("rehydrated = %v, %v", got, err)
	}
}

// O comando usa o índice parcial (provider_id,
// reference_external_transaction_id) da migration 0002. Com a tabela vazia
// o planejador preferiria seq scan, então ele é desligado só nesta transação.
func TestIntegrationWagerTransactionRepository_WakeUsesReferenceIndex(t *testing.T) {
	t.Parallel()
	r := newRepos(t)
	err := r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `EXPLAIN `+postgres.WakePendingReferencesSQL, "provider-a", "bet-1", time.Now())
		if err != nil {
			return err
		}
		plan, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		if text := strings.Join(plan, "\n"); !strings.Contains(text, "wager_transactions_reference_external_idx") {
			t.Errorf("plan does not use the reference index:\n%s", text)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
