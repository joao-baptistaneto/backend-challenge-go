-- 0007: lease persistido do worker de referências pendentes e PENDING órfãos
-- (ADR-023).
--
-- O worker reivindica linhas vencidas num comando curto (UPDATE ... FROM
-- (SELECT ... FOR NO KEY UPDATE SKIP LOCKED)) e as processa depois, uma a uma,
-- cada uma na sua transação. O lease fica gravado na linha:
--   * claimed_by: token do lote (fencing). O processamento só grava se a linha
--     ainda estiver reivindicada pelo MESMO token (SELECT ... WHERE claimed_by =
--     $token FOR NO KEY UPDATE). Um worker atrasado, cujo lease expirou e foi
--     reassumido, encontra outro token e desiste sem efeito;
--   * claimed_until: fim do lease. Um worker que morre (crash, kill) não perde a
--     linha: ela volta a ser elegível quando claimed_until passa.
-- A transição que o worker grava (WagerTransactionRepository.Update) limpa as
-- duas colunas no mesmo UPDATE.

ALTER TABLE wager_transactions
    ADD COLUMN claimed_by    UUID,
    ADD COLUMN claimed_until TIMESTAMPTZ;

-- As duas colunas andam juntas.
ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_claim_pair
        CHECK ((claimed_by IS NULL) = (claimed_until IS NULL));

-- Só estados não terminais podem estar reivindicados (a transição terminal
-- solta o lease; o trigger de guarda impede mexer na linha terminal depois).
ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_terminal_unclaimed
        CHECK (status IN ('PENDING', 'PENDING_REFERENCE') OR claimed_by IS NULL);
