package outboxpublisher

import (
	"context"
	"errors"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
)

// PostgresStore liga o publisher ao postgres.OutboxRepository. Todas as
// chamadas usam o Querier recebido (o pool), em autocommit: o claim
// confirma o lease no próprio comando, e nenhuma transação fica aberta
// durante o envio ao SQS (ADR-015/024).
type PostgresStore struct {
	q       postgres.Querier
	repo    *postgres.OutboxRepository
	backoff postgres.OutboxBackoff
}

var _ Store = (*PostgresStore)(nil)

// NewPostgresStore cria o store. backoff é a política de MarkFailed.
func NewPostgresStore(q postgres.Querier, repo *postgres.OutboxRepository, backoff postgres.OutboxBackoff) (*PostgresStore, error) {
	if q == nil || repo == nil {
		return nil, errors.New("outboxpublisher: nil querier or repository")
	}
	if err := backoff.Validate(); err != nil {
		return nil, err
	}
	return &PostgresStore{q: q, repo: repo, backoff: backoff}, nil
}

// Claim implementa Store.
func (s *PostgresStore) Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]postgres.OutboxMessage, error) {
	return s.repo.Claim(ctx, s.q, owner, lease, limit)
}

// MarkPublished implementa Store.
func (s *PostgresStore) MarkPublished(ctx context.Context, lease postgres.OutboxLease) error {
	return s.repo.MarkPublished(ctx, s.q, lease)
}

// MarkFailed implementa Store.
func (s *PostgresStore) MarkFailed(ctx context.Context, lease postgres.OutboxLease, cause string) error {
	return s.repo.MarkFailed(ctx, s.q, lease, cause, s.backoff)
}

// Release implementa Store.
func (s *PostgresStore) Release(ctx context.Context, lease postgres.OutboxLease) error {
	return s.repo.Release(ctx, s.q, lease)
}

// Backlog implementa Store.
func (s *PostgresStore) Backlog(ctx context.Context) (postgres.OutboxBacklog, error) {
	return s.repo.Backlog(ctx, s.q)
}
