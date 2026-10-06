-- DROP TABLE não dispara os triggers de UPDATE/DELETE/TRUNCATE: reverter a
-- migration descarta o ledger inteiro. Só use em ambientes descartáveis.
DROP TRIGGER wallet_ledger_entries_no_truncate ON wallet_ledger_entries;
DROP TRIGGER wallet_ledger_entries_no_update_delete ON wallet_ledger_entries;
DROP FUNCTION wallet_ledger_entries_reject_mutation();
DROP TABLE wallet_ledger_entries;
