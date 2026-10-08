-- 0009: contador de falhas consecutivas da retomada pelo worker de pendências
-- (ADR-031).
--
-- resume_failures conta as retomadas (ResumeWagerTransaction.Resume) que
-- falharam por erro NÃO transitório desde a última retomada bem-sucedida. É um
-- dado operacional do worker, como claimed_by/claimed_until (0007), e não faz
-- parte do agregado WagerTransaction: o domínio não o lê nem o valida.
--   * Uma falha contada grava resume_failures + 1 e estende o lease
--     (claimed_until = agora + backoff exponencial), sem mexer em status,
--     attempts nem next_attempt_at: o item só volta a ser elegível depois do
--     backoff, e o fencing (claimed_by) continua valendo.
--   * Uma retomada que grava a transição (Update) zera o contador ("falhas
--     consecutivas"), salvo quando grava FAILED: aí o valor fica para
--     auditoria.
--   * Esgotado o limite (PENDING_WORKER_MAX_FAILURES) ou o TTL, a linha vira
--     FAILED RETRIES_EXHAUSTED, sem movimentar saldo.
-- ADD COLUMN com DEFAULT constante não reescreve a tabela (PostgreSQL 11+).

ALTER TABLE wager_transactions
    ADD COLUMN resume_failures INTEGER NOT NULL DEFAULT 0;

ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_resume_failures_non_negative
        CHECK (resume_failures >= 0);
