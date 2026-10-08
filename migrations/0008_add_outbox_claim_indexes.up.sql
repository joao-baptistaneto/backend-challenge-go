-- 0008: índices do claim da outbox com ordem por agregado. ADR-024.
--
-- O claim do publisher (OutboxRepository.Claim) mudou em dois pontos:
--   * percorre os não publicados em ORDER BY seq (não por next_attempt_at);
--   * só aceita o evento não publicado mais antigo de cada agregado
--     (MessageGroupId), com NOT EXISTS (evento anterior não publicado do mesmo
--     aggregate_id).
-- O índice outbox_unpublished_due_idx (0005, em (next_attempt_at, seq)) não
-- serve a nenhum dos dois e é substituído por:
--   * outbox_unpublished_seq_idx: varredura em ordem de seq com LIMIT, e
--     count/min do backlog (métricas) só sobre os não publicados;
--   * outbox_unpublished_aggregate_seq_idx: o NOT EXISTS por agregado.
-- Ambos parciais (published_at IS NULL): eventos publicados saem do índice e
-- a tabela pode crescer (retenção) sem pesar no claim.

DROP INDEX IF EXISTS outbox_unpublished_due_idx;

CREATE INDEX outbox_unpublished_seq_idx
    ON outbox (seq)
    WHERE published_at IS NULL;

CREATE INDEX outbox_unpublished_aggregate_seq_idx
    ON outbox (aggregate_id, seq)
    WHERE published_at IS NULL;
