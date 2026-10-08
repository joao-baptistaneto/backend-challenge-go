-- 0001: carteiras (raiz do agregado financeiro). ADR-004/ADR-005/ADR-011.
--
-- Money: BIGINT em centavos (escala fixa 2) + CHAR(3) ISO 4217. Nenhum tipo de
-- ponto flutuante é usado no schema.
-- Concorrência: SELECT ... FOR UPDATE + UPDATE ... WHERE version = $old (ADR-014). O
-- trigger abaixo garante no banco que toda mudança de saldo incrementa a versão
-- em exatamente 1 (e que a versão não muda sem mudança de saldo), de modo que um
-- UPDATE que "esqueça" a versão não passa e o controle otimista continua válido.

CREATE TABLE wallets (
    id         UUID        NOT NULL,
    player_id  UUID        NOT NULL,
    currency   CHAR(3)     NOT NULL,
    balance    BIGINT      NOT NULL,
    version    BIGINT      NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT wallets_pkey PRIMARY KEY (id),
    -- (playerId, currency) identifica uma única carteira (README §6.2).
    CONSTRAINT wallets_player_currency_key UNIQUE (player_id, currency),
    -- Alvo das FKs compostas do ledger: garante que o lançamento usa a moeda da
    -- carteira.
    CONSTRAINT wallets_id_currency_key UNIQUE (id, currency),
    CONSTRAINT wallets_balance_non_negative CHECK (balance >= 0),
    CONSTRAINT wallets_version_positive CHECK (version >= 1),
    CONSTRAINT wallets_currency_format CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT wallets_timestamps CHECK (updated_at >= created_at)
);

-- Guarda de UPDATE: identidade imutável, versão acoplada ao saldo e updated_at
-- monotônico. SQLSTATE 23001 (restrict_violation) com nome de constraint
-- explícito, para que a camada de persistência (ADR-013) classifique o erro.
CREATE FUNCTION wallets_guard_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.id <> OLD.id OR NEW.player_id <> OLD.player_id
       OR NEW.currency <> OLD.currency OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'wallets: id, player_id, currency and created_at are immutable (wallet %)', OLD.id
            USING ERRCODE = 'restrict_violation', CONSTRAINT = 'wallets_immutable_columns';
    END IF;
    IF NEW.version <> OLD.version + (CASE WHEN NEW.balance <> OLD.balance THEN 1 ELSE 0 END) THEN
        RAISE EXCEPTION 'wallets: version must increase by exactly 1 per balance change (wallet %, version % -> %)',
            OLD.id, OLD.version, NEW.version
            USING ERRCODE = 'restrict_violation', CONSTRAINT = 'wallets_version_increment';
    END IF;
    IF NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'wallets: updated_at must not go backwards (wallet %)', OLD.id
            USING ERRCODE = 'restrict_violation', CONSTRAINT = 'wallets_updated_at_monotonic';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER wallets_guard_update
    BEFORE UPDATE ON wallets
    FOR EACH ROW EXECUTE FUNCTION wallets_guard_update();
