DROP INDEX IF EXISTS wallet_ledger_entries_wallet_seq_key;
ALTER TABLE wallet_ledger_entries DROP COLUMN IF EXISTS seq;
