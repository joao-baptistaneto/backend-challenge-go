-- 0002: operações financeiras (WagerTransaction). ADR-008/ADR-011.
--
-- Os CHECKs espelham as invariantes de domain.WagerTransaction.validate(): o
-- domínio continua sendo a primeira barreira, e o schema garante as mesmas regras
-- mesmo para escritas fora do caminho feliz (README §5.3 e §5.8).
--
-- Cuidado com NULL em CHECK: uma expressão NULL é aceita pelo PostgreSQL. Por
-- isso toda comparação com coluna anulável vem acompanhada de IS [NOT] NULL ou
-- IS DISTINCT FROM.
--
-- Decisões que afetam os repositórios e o caso de uso (ADR-014/ADR-017):
--  * wallet_id/player_id NÃO têm FK para wallets: rejeições WALLET_NOT_FOUND,
--    WALLET_PLAYER_MISMATCH e CURRENCY_MISMATCH precisam ser persistidas para
--    replay idempotente e apontam para carteira inexistente ou divergente. A
--    integridade financeira é garantida pelas FKs do ledger (0003), que exigem
--    carteira existente, mesma carteira da transação e mesma moeda.
--  * Identificadores externos são TEXT com 1..255 BYTES (octet_length), igual a
--    domain.MaxWagerIdentifierLength; VARCHAR(255) contaria caracteres.
--  * balance_after usa a moeda da operação (currency); o domínio exige isso.

CREATE TABLE wager_transactions (
    id                                UUID        NOT NULL,
    origin                            TEXT        NOT NULL,
    kind                              TEXT        NOT NULL,
    status                            TEXT        NOT NULL,
    wallet_id                         UUID        NOT NULL,
    player_id                         UUID        NOT NULL,
    amount                            BIGINT      NOT NULL,
    currency                          CHAR(3)     NOT NULL,

    -- Metadados externos: NULL em INTERNAL (OPENING), obrigatórios em EXTERNAL.
    provider_id                       TEXT,
    external_transaction_id           TEXT,
    idempotency_key                   TEXT,
    request_hash                      CHAR(64),
    round_id                          TEXT,
    game_id                           TEXT,
    reference_external_transaction_id TEXT,

    -- Resultado.
    reference_transaction_id          UUID,
    failure_code                      TEXT,
    balance_after                     BIGINT,

    -- Retomada durável (worker de referências/PENDING órfãos, ADR-023).
    attempts                          INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at                   TIMESTAMPTZ,

    created_at                        TIMESTAMPTZ NOT NULL,
    updated_at                        TIMESTAMPTZ NOT NULL,
    completed_at                      TIMESTAMPTZ,

    CONSTRAINT wager_transactions_pkey PRIMARY KEY (id),

    -- Idempotência persistente (README §9): a operação é identificada por
    -- (provider, externalTransactionId) e a chave por (provider, idempotencyKey).
    -- NULLs (OPENING) são distintos entre si, então não colidem.
    CONSTRAINT wager_transactions_provider_external_key UNIQUE (provider_id, external_transaction_id),
    CONSTRAINT wager_transactions_provider_idempotency_key UNIQUE (provider_id, idempotency_key),
    -- Alvo da FK composta do ledger (lançamento pertence à mesma carteira e moeda).
    CONSTRAINT wager_transactions_id_wallet_currency_key UNIQUE (id, wallet_id, currency),
    -- Referência interna resolvida aponta para uma transação existente.
    CONSTRAINT wager_transactions_reference_fkey FOREIGN KEY (reference_transaction_id)
        REFERENCES wager_transactions (id),

    -- Domínios fechados (mesmos valores de internal/domain).
    CONSTRAINT wager_transactions_origin_valid CHECK (origin IN ('INTERNAL', 'EXTERNAL')),
    CONSTRAINT wager_transactions_kind_valid
        CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    CONSTRAINT wager_transactions_status_valid
        CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    CONSTRAINT wager_transactions_failure_code_valid CHECK (failure_code IS NULL OR failure_code IN (
        'INVALID_REQUEST', 'INVALID_KIND', 'INVALID_AMOUNT', 'INVALID_CURRENCY',
        'REFERENCE_REQUIRED', 'REFERENCE_NOT_ALLOWED', 'WALLET_NOT_FOUND',
        'WALLET_PLAYER_MISMATCH', 'CURRENCY_MISMATCH', 'REFERENCE_MISMATCH', 'AMOUNT_MISMATCH',
        'INSUFFICIENT_FUNDS', 'REVERSAL_INSUFFICIENT_FUNDS', 'REFERENCE_NOT_FOUND',
        'REFERENCE_NOT_REVERSIBLE', 'REFERENCE_NOT_PROCESSED', 'ALREADY_REVERSED',
        'PROCESSING_FAILED', 'RETRIES_EXHAUSTED')),
    CONSTRAINT wager_transactions_currency_format CHECK (currency ~ '^[A-Z]{3}$'),

    -- Origem x tipo: OPENING é a única operação INTERNAL.
    CONSTRAINT wager_transactions_origin_kind CHECK ((origin = 'INTERNAL') = (kind = 'OPENING')),
    -- INTERNAL: sem metadados externos, sempre PROCESSED, balance_after = amount
    -- (a carteira parte de zero), sem agendamento.
    CONSTRAINT wager_transactions_internal_shape CHECK (origin <> 'INTERNAL' OR (
        provider_id IS NULL AND external_transaction_id IS NULL AND idempotency_key IS NULL
        AND request_hash IS NULL AND round_id IS NULL AND game_id IS NULL
        AND reference_external_transaction_id IS NULL AND reference_transaction_id IS NULL
        AND status = 'PROCESSED' AND failure_code IS NULL
        AND balance_after IS NOT NULL AND balance_after = amount
        AND attempts = 0 AND next_attempt_at IS NULL)),
    -- EXTERNAL: metadados obrigatórios (a referência é opcional e validada abaixo).
    CONSTRAINT wager_transactions_external_shape CHECK (origin <> 'EXTERNAL' OR (
        provider_id IS NOT NULL AND external_transaction_id IS NOT NULL
        AND idempotency_key IS NOT NULL AND request_hash IS NOT NULL
        AND round_id IS NOT NULL AND game_id IS NOT NULL)),
    CONSTRAINT wager_transactions_identifiers_length CHECK (
        (provider_id IS NULL OR octet_length(provider_id) BETWEEN 1 AND 255)
        AND (external_transaction_id IS NULL OR octet_length(external_transaction_id) BETWEEN 1 AND 255)
        AND (idempotency_key IS NULL OR octet_length(idempotency_key) BETWEEN 1 AND 255)
        AND (round_id IS NULL OR octet_length(round_id) BETWEEN 1 AND 255)
        AND (game_id IS NULL OR octet_length(game_id) BETWEEN 1 AND 255)
        AND (reference_external_transaction_id IS NULL
             OR octet_length(reference_external_transaction_id) BETWEEN 1 AND 255)),
    -- SHA-256 em hex minúsculo (ADR-010).
    CONSTRAINT wager_transactions_request_hash_format
        CHECK (request_hash IS NULL OR request_hash ~ '^[0-9a-f]{64}$'),

    -- Política de zero: LOSS = 0; demais > 0. Nunca negativo.
    CONSTRAINT wager_transactions_amount_policy
        CHECK ((kind = 'LOSS' AND amount = 0) OR (kind <> 'LOSS' AND amount > 0)),

    -- Referências: obrigatória em REFUND/ROLLBACK, opcional em WIN, proibida em
    -- BET/LOSS/OPENING; nunca a si mesma.
    CONSTRAINT wager_transactions_reference_required
        CHECK (kind NOT IN ('REFUND', 'ROLLBACK') OR reference_external_transaction_id IS NOT NULL),
    CONSTRAINT wager_transactions_reference_not_allowed
        CHECK (kind NOT IN ('BET', 'LOSS', 'OPENING') OR reference_external_transaction_id IS NULL),
    CONSTRAINT wager_transactions_reference_not_self
        CHECK (reference_external_transaction_id IS NULL
               OR reference_external_transaction_id <> external_transaction_id),
    -- Referência interna: só com referência externa, nunca a própria linha, e
    -- obrigatória quando PROCESSED com referência externa.
    CONSTRAINT wager_transactions_resolved_reference CHECK (reference_transaction_id IS NULL OR (
        reference_external_transaction_id IS NOT NULL AND reference_transaction_id <> id)),
    CONSTRAINT wager_transactions_processed_reference_resolved CHECK (
        status <> 'PROCESSED' OR reference_external_transaction_id IS NULL
        OR reference_transaction_id IS NOT NULL),

    -- Status x failure_code: REJECTED exige código INPUT/BUSINESS; FAILED exige
    -- código INFRASTRUCTURE; demais estados sem código.
    CONSTRAINT wager_transactions_status_failure_code CHECK (
        (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED') AND failure_code IS NULL)
        OR (status = 'REJECTED' AND failure_code IS NOT NULL
            AND failure_code NOT IN ('PROCESSING_FAILED', 'RETRIES_EXHAUSTED'))
        OR (status = 'FAILED' AND failure_code IS NOT NULL
            AND failure_code IN ('PROCESSING_FAILED', 'RETRIES_EXHAUSTED'))),
    -- Restrições de aplicabilidade do código (FailureCode.allowedForKind e
    -- requiresReference).
    CONSTRAINT wager_transactions_failure_code_kind CHECK (
        (failure_code IS DISTINCT FROM 'INSUFFICIENT_FUNDS' OR kind = 'BET')
        AND (failure_code IS DISTINCT FROM 'REVERSAL_INSUFFICIENT_FUNDS' OR kind = 'ROLLBACK')
        AND (failure_code IS NULL OR failure_code NOT IN ('ALREADY_REVERSED', 'AMOUNT_MISMATCH')
             OR kind IN ('REFUND', 'ROLLBACK'))),
    -- REFERENCE_NOT_FOUND significa que a referência nunca foi resolvida, logo
    -- reference_transaction_id precisa ser NULL (WagerTransaction.validate).
    CONSTRAINT wager_transactions_failure_code_reference CHECK (
        (failure_code IS NULL
         OR failure_code NOT IN ('REFERENCE_NOT_FOUND', 'REFERENCE_MISMATCH', 'REFERENCE_NOT_REVERSIBLE',
                                 'REFERENCE_NOT_PROCESSED', 'ALREADY_REVERSED', 'AMOUNT_MISMATCH')
         OR reference_external_transaction_id IS NOT NULL)
        AND (failure_code IS DISTINCT FROM 'REFERENCE_NOT_FOUND' OR reference_transaction_id IS NULL)),

    -- Status x balance_after: obrigatório em PROCESSED (saldo devolvido no replay;
    -- em LOSS é o saldo inalterado), opcional em REJECTED (saldo observado),
    -- proibido nos demais. Nunca negativo.
    CONSTRAINT wager_transactions_balance_after_non_negative
        CHECK (balance_after IS NULL OR balance_after >= 0),
    CONSTRAINT wager_transactions_processed_balance_after
        CHECK (status <> 'PROCESSED' OR balance_after IS NOT NULL),
    CONSTRAINT wager_transactions_balance_after_status
        CHECK (status IN ('PROCESSED', 'REJECTED') OR balance_after IS NULL),

    -- Status x completed_at: preenchido exatamente nos estados terminais, dentro
    -- de [created_at, updated_at].
    CONSTRAINT wager_transactions_completed_at_status CHECK (
        (status IN ('PROCESSED', 'REJECTED', 'FAILED')) = (completed_at IS NOT NULL)),
    CONSTRAINT wager_transactions_timestamps CHECK (
        updated_at >= created_at
        AND (completed_at IS NULL OR (completed_at >= created_at AND completed_at <= updated_at))),

    -- Agendamento: PENDING_REFERENCE exige referência, attempts >= 1 e
    -- next_attempt_at; terminais não ficam agendados; PENDING pode ter
    -- next_attempt_at (PENDING órfão).
    CONSTRAINT wager_transactions_attempts_non_negative CHECK (attempts >= 0),
    -- attempts conta só buscas de referência sem sucesso (ADR-008): > 0 exige
    -- referência externa e é proibido em PENDING (não há volta para PENDING).
    CONSTRAINT wager_transactions_attempts_reference CHECK (
        attempts = 0 OR (reference_external_transaction_id IS NOT NULL AND status <> 'PENDING')),
    CONSTRAINT wager_transactions_pending_reference_schedule CHECK (
        status <> 'PENDING_REFERENCE' OR (
            reference_external_transaction_id IS NOT NULL AND attempts >= 1
            AND next_attempt_at IS NOT NULL)),
    CONSTRAINT wager_transactions_terminal_unscheduled CHECK (
        status NOT IN ('PROCESSED', 'REJECTED', 'FAILED') OR next_attempt_at IS NULL),
    CONSTRAINT wager_transactions_next_attempt_after_creation
        CHECK (next_attempt_at IS NULL OR next_attempt_at >= created_at)
);

-- No máximo um crédito inicial por carteira (README §6.3: "impedir crédito
-- inicial duplicado").
CREATE UNIQUE INDEX wager_transactions_opening_per_wallet_key
    ON wager_transactions (wallet_id)
    WHERE kind = 'OPENING';

-- Uma referência recebe no máximo UMA reversão bem-sucedida, de qualquer tipo
-- (política REFUND x ROLLBACK do ADR-009): REFUND + ROLLBACK da mesma BET
-- devolveriam o mesmo débito duas vezes. Isso implica a exigência do README §7
-- ("não duas reversões bem-sucedidas do mesmo tipo"), que seria um índice em
-- (reference_transaction_id, kind), e é mais restritivo. ROLLBACK de um REFUND
-- referencia o REFUND, não a BET, então continua permitido. Usa a referência
-- interna resolvida, que já é escopo do provedor. É a barreira final contra
-- corrida mesmo que a regra de domínio (ALREADY_REVERSED) falhe.
CREATE UNIQUE INDEX wager_transactions_reversal_once_key
    ON wager_transactions (reference_transaction_id)
    WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');

-- Worker de referências (ADR-023): PENDING_REFERENCE vencidos por next_attempt_at.
CREATE INDEX wager_transactions_pending_reference_due_idx
    ON wager_transactions (next_attempt_at, id)
    WHERE status = 'PENDING_REFERENCE';

-- Worker de PENDING órfãos (ADR-023): PENDING mais antigos primeiro.
CREATE INDEX wager_transactions_pending_idx
    ON wager_transactions (created_at, id)
    WHERE status = 'PENDING';

-- Operações que citam uma referência externa: detecção de ALREADY_REVERSED e
-- "acordar" pendências quando a referência chega.
CREATE INDEX wager_transactions_reference_external_idx
    ON wager_transactions (provider_id, reference_external_transaction_id)
    WHERE reference_external_transaction_id IS NOT NULL;

-- Consultas por carteira (auditoria/reconciliação).
CREATE INDEX wager_transactions_wallet_idx ON wager_transactions (wallet_id, created_at, id);

-- Guarda de UPDATE (máquina de estados no banco, ADR-008):
--  * estados terminais são imutáveis;
--  * não há aresta PENDING_REFERENCE -> PENDING;
--  * identidade, metadados de entrada e created_at são imutáveis;
--  * attempts nunca diminui.
CREATE FUNCTION wager_transactions_guard_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'wager_transactions: transaction % is terminal (%)', OLD.id, OLD.status
            USING ERRCODE = 'restrict_violation', CONSTRAINT = 'wager_transactions_terminal_immutable';
    END IF;
    IF OLD.status = 'PENDING_REFERENCE' AND NEW.status = 'PENDING' THEN
        RAISE EXCEPTION 'wager_transactions: transition PENDING_REFERENCE -> PENDING is not allowed (transaction %)', OLD.id
            USING ERRCODE = 'restrict_violation', CONSTRAINT = 'wager_transactions_status_transition';
    END IF;
    IF ROW(NEW.id, NEW.origin, NEW.kind, NEW.wallet_id, NEW.player_id, NEW.amount, NEW.currency,
           NEW.provider_id, NEW.external_transaction_id, NEW.idempotency_key, NEW.request_hash,
           NEW.round_id, NEW.game_id, NEW.reference_external_transaction_id, NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.id, OLD.origin, OLD.kind, OLD.wallet_id, OLD.player_id, OLD.amount, OLD.currency,
           OLD.provider_id, OLD.external_transaction_id, OLD.idempotency_key, OLD.request_hash,
           OLD.round_id, OLD.game_id, OLD.reference_external_transaction_id, OLD.created_at) THEN
        RAISE EXCEPTION 'wager_transactions: input columns are immutable (transaction %)', OLD.id
            USING ERRCODE = 'restrict_violation', CONSTRAINT = 'wager_transactions_immutable_columns';
    END IF;
    IF NEW.attempts < OLD.attempts THEN
        RAISE EXCEPTION 'wager_transactions: attempts must not decrease (transaction %)', OLD.id
            USING ERRCODE = 'restrict_violation', CONSTRAINT = 'wager_transactions_attempts_monotonic';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER wager_transactions_guard_update
    BEFORE UPDATE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION wager_transactions_guard_update();
