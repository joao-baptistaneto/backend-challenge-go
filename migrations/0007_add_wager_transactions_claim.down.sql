ALTER TABLE wager_transactions DROP CONSTRAINT IF EXISTS wager_transactions_terminal_unclaimed;
ALTER TABLE wager_transactions DROP CONSTRAINT IF EXISTS wager_transactions_claim_pair;
ALTER TABLE wager_transactions DROP COLUMN IF EXISTS claimed_until;
ALTER TABLE wager_transactions DROP COLUMN IF EXISTS claimed_by;
