-- 0006: ordem estável do extrato (paginação keyset, T12). ADR-014.
--
-- (created_at, id) não é uma ordem segura para cursor: created_at vem do relógio
-- de cada instância e o id (UUIDv7) também. Com relógios levemente divergentes,
-- um lançamento confirmado DEPOIS de um cursor emitido poderia ordenar ANTES
-- dele e nunca aparecer na próxima página.
--
-- seq é atribuído pelo banco no INSERT. Todo lançamento de uma carteira é
-- inserido com a linha da carteira travada (SELECT ... FOR UPDATE / UPDATE, ou
-- o INSERT da própria carteira na abertura), e o lock só é liberado no commit.
-- Logo, dentro de uma carteira, seq cresce na ordem de commit: um lançamento
-- novo sempre ordena depois de qualquer cursor já emitido. Lacunas (rollback)
-- não importam. GENERATED ALWAYS impede que a aplicação escolha o valor.
--
-- ADD COLUMN reescreve a tabela sem disparar os triggers de UPDATE do ledger e
-- preenche as linhas existentes.

ALTER TABLE wallet_ledger_entries
    ADD COLUMN seq BIGINT GENERATED ALWAYS AS IDENTITY;

-- Paginação keyset: WHERE wallet_id = $1 AND seq > $cursor ORDER BY seq.
CREATE UNIQUE INDEX wallet_ledger_entries_wallet_seq_key
    ON wallet_ledger_entries (wallet_id, seq);
