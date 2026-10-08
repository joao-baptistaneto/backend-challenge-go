-- 0003: ledger append-only (WalletLedgerEntry). ADR-006/ADR-011.
--
-- Integridade imposta pelo banco (README §5.5 e §5.8):
--  * no máximo um lançamento por (carteira, transação);
--  * aritmética balance_after = balance_before ± amount conforme a direção,
--    calculada em NUMERIC (exato, sem overflow nem ponto flutuante);
--  * FKs compostas: a carteira existe e tem a mesma moeda; a transação existe e
--    pertence à mesma carteira e moeda;
--  * UPDATE, DELETE e TRUNCATE (inclusive TRUNCATE ... CASCADE vindo de wallets
--    ou wager_transactions) são bloqueados por triggers. Correções exigem novos
--    lançamentos.
--
-- Limitação: o dono da tabela/superusuário pode desabilitar triggers
-- (ALTER TABLE ... DISABLE TRIGGER, session_replication_role = replica). Em
-- produção a aplicação deve usar um papel que não seja dono do schema (ADR-026).

CREATE TABLE wallet_ledger_entries (
    id             UUID        NOT NULL,
    wallet_id      UUID        NOT NULL,
    transaction_id UUID        NOT NULL,
    direction      TEXT        NOT NULL,
    amount         BIGINT      NOT NULL,
    currency       CHAR(3)     NOT NULL,
    balance_before BIGINT      NOT NULL,
    balance_after  BIGINT      NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,

    CONSTRAINT wallet_ledger_entries_pkey PRIMARY KEY (id),
    CONSTRAINT wallet_ledger_entries_wallet_transaction_key UNIQUE (wallet_id, transaction_id),
    CONSTRAINT wallet_ledger_entries_wallet_fkey FOREIGN KEY (wallet_id, currency)
        REFERENCES wallets (id, currency),
    CONSTRAINT wallet_ledger_entries_transaction_fkey FOREIGN KEY (transaction_id, wallet_id, currency)
        REFERENCES wager_transactions (id, wallet_id, currency),

    CONSTRAINT wallet_ledger_entries_direction_valid CHECK (direction IN ('DEBIT', 'CREDIT')),
    -- LOSS e rejeições não geram lançamento: valor sempre > 0.
    CONSTRAINT wallet_ledger_entries_amount_positive CHECK (amount > 0),
    CONSTRAINT wallet_ledger_entries_balances_non_negative CHECK (balance_before >= 0 AND balance_after >= 0),
    CONSTRAINT wallet_ledger_entries_currency_format CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT wallet_ledger_entries_arithmetic CHECK (
        (direction = 'CREDIT' AND balance_after::NUMERIC = balance_before::NUMERIC + amount::NUMERIC)
        OR (direction = 'DEBIT' AND balance_after::NUMERIC = balance_before::NUMERIC - amount::NUMERIC))
);

-- Paginação keyset do extrato (ADR-014): ordem estável (created_at, id) por carteira.
CREATE INDEX wallet_ledger_entries_wallet_page_idx
    ON wallet_ledger_entries (wallet_id, created_at, id);

-- Índice das FKs para wager_transactions (consultas por transação).
CREATE INDEX wallet_ledger_entries_transaction_idx ON wallet_ledger_entries (transaction_id);

CREATE FUNCTION wallet_ledger_entries_reject_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries is append-only: % is not allowed', TG_OP
        USING ERRCODE = 'restrict_violation',
              CONSTRAINT = 'wallet_ledger_entries_append_only',
              HINT = 'post a compensating ledger entry instead';
    RETURN NULL;
END;
$$;

CREATE TRIGGER wallet_ledger_entries_no_update_delete
    BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION wallet_ledger_entries_reject_mutation();

CREATE TRIGGER wallet_ledger_entries_no_truncate
    BEFORE TRUNCATE ON wallet_ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION wallet_ledger_entries_reject_mutation();
