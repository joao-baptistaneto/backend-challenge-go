-- 0004: inbox do consumidor SQS (README §6.5 e §10). ADR-011.
--
-- O registro e a conclusão compartilham a transação SQL das alterações de
-- domínio (T15/T20). (consumer_name, message_id) é a identidade durável da
-- mensagem; request_hash (SHA-256 hex do JSON canônico, T08) detecta reentrega
-- com o mesmo messageId e conteúdo diferente.

CREATE TABLE inbox (
    consumer_name  TEXT        NOT NULL,
    message_id     TEXT        NOT NULL,
    request_hash   CHAR(64)    NOT NULL,
    -- Transação de domínio resultante (NULL quando a mensagem não gerou
    -- transação, ex.: rejeição de entrada sem registro).
    transaction_id UUID,
    received_at    TIMESTAMPTZ NOT NULL,
    completed_at   TIMESTAMPTZ,

    CONSTRAINT inbox_consumer_message_key PRIMARY KEY (consumer_name, message_id),
    CONSTRAINT inbox_transaction_fkey FOREIGN KEY (transaction_id) REFERENCES wager_transactions (id),
    CONSTRAINT inbox_consumer_name_length CHECK (octet_length(consumer_name) BETWEEN 1 AND 255),
    CONSTRAINT inbox_message_id_length CHECK (octet_length(message_id) BETWEEN 1 AND 255),
    CONSTRAINT inbox_request_hash_format CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT inbox_completed_after_received CHECK (completed_at IS NULL OR completed_at >= received_at)
);

-- Retenção/limpeza de mensagens concluídas.
CREATE INDEX inbox_completed_at_idx ON inbox (completed_at) WHERE completed_at IS NOT NULL;
