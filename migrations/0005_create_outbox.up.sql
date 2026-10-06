-- 0005: transactional outbox (README §6.5 e §11). ADR-011.
--
-- Eventos são gravados na mesma transação SQL que os originou e publicados
-- depois do commit por um worker (T22). Disputa entre publishers: claim em lote
-- com FOR UPDATE SKIP LOCKED + lease (locked_until/locked_by); lease vencido é
-- reassumido por outra instância. Backoff exponencial via attempts/next_attempt_at.
--
--  * seq: ordem de inserção (IDENTITY). Dentro de um agregado as escritas são
--    serializadas pelo lock da carteira, então seq preserva a ordem por
--    MessageGroupId.
--  * event_id: identidade estável do evento = MessageDeduplicationId; preservada
--    em republicações.
--  * payload: tipo JSON (não JSONB) para guardar o snapshot byte a byte, sem
--    reordenação de chaves; valores monetários são strings decimais (T07).

CREATE TABLE outbox (
    seq             BIGINT GENERATED ALWAYS AS IDENTITY,
    event_id        UUID        NOT NULL,
    event_type      TEXT        NOT NULL,
    event_version   INTEGER     NOT NULL,
    aggregate_id    TEXT        NOT NULL,
    correlation_id  TEXT,
    payload         JSON        NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts        INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until    TIMESTAMPTZ,
    locked_by       TEXT,
    last_error      TEXT,
    published_at    TIMESTAMPTZ,

    CONSTRAINT outbox_pkey PRIMARY KEY (seq),
    CONSTRAINT outbox_event_id_key UNIQUE (event_id),
    CONSTRAINT outbox_event_type_length CHECK (octet_length(event_type) BETWEEN 1 AND 255),
    CONSTRAINT outbox_event_version_positive CHECK (event_version >= 1),
    CONSTRAINT outbox_aggregate_id_length CHECK (octet_length(aggregate_id) BETWEEN 1 AND 128),
    CONSTRAINT outbox_correlation_id_length
        CHECK (correlation_id IS NULL OR octet_length(correlation_id) BETWEEN 1 AND 255),
    CONSTRAINT outbox_attempts_non_negative CHECK (attempts >= 0),
    -- Lease: dono e prazo andam juntos.
    CONSTRAINT outbox_lease_pair CHECK ((locked_until IS NULL) = (locked_by IS NULL)),
    CONSTRAINT outbox_last_error_length CHECK (last_error IS NULL OR octet_length(last_error) <= 2048)
);

-- Claim do publisher: não publicados, vencidos, em ordem de seq.
CREATE INDEX outbox_unpublished_due_idx
    ON outbox (next_attempt_at, seq)
    WHERE published_at IS NULL;

-- Guarda: o snapshot do evento é imutável; evento publicado não volta a
-- pendente nem muda; evento não publicado não pode ser apagado (seria perda de
-- um evento confirmado). Eventos publicados podem ser apagados (retenção).
CREATE FUNCTION outbox_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.published_at IS NULL THEN
            RAISE EXCEPTION 'outbox: unpublished event % cannot be deleted', OLD.event_id
                USING ERRCODE = 'restrict_violation', CONSTRAINT = 'outbox_unpublished_not_deletable';
        END IF;
        RETURN OLD;
    END IF;
    IF ROW(NEW.seq, NEW.event_id, NEW.event_type, NEW.event_version, NEW.aggregate_id,
           NEW.correlation_id, NEW.payload::TEXT, NEW.occurred_at, NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.seq, OLD.event_id, OLD.event_type, OLD.event_version, OLD.aggregate_id,
           OLD.correlation_id, OLD.payload::TEXT, OLD.occurred_at, OLD.created_at) THEN
        RAISE EXCEPTION 'outbox: event snapshot is immutable (event %)', OLD.event_id
            USING ERRCODE = 'restrict_violation', CONSTRAINT = 'outbox_snapshot_immutable';
    END IF;
    IF OLD.published_at IS NOT NULL THEN
        RAISE EXCEPTION 'outbox: published event % is immutable', OLD.event_id
            USING ERRCODE = 'restrict_violation', CONSTRAINT = 'outbox_published_immutable';
    END IF;
    IF NEW.attempts < OLD.attempts THEN
        RAISE EXCEPTION 'outbox: attempts must not decrease (event %)', OLD.event_id
            USING ERRCODE = 'restrict_violation', CONSTRAINT = 'outbox_attempts_monotonic';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER outbox_guard
    BEFORE UPDATE OR DELETE ON outbox
    FOR EACH ROW EXECUTE FUNCTION outbox_guard();
