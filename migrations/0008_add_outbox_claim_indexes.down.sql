DROP INDEX IF EXISTS outbox_unpublished_aggregate_seq_idx;
DROP INDEX IF EXISTS outbox_unpublished_seq_idx;

CREATE INDEX outbox_unpublished_due_idx
    ON outbox (next_attempt_at, seq)
    WHERE published_at IS NULL;
