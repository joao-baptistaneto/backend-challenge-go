-- Recria o índice exatamente como na 0003.
CREATE INDEX wallet_ledger_entries_wallet_page_idx
    ON wallet_ledger_entries (wallet_id, created_at, id);
