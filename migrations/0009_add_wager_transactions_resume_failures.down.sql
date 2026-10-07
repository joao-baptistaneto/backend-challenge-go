ALTER TABLE wager_transactions DROP CONSTRAINT IF EXISTS wager_transactions_resume_failures_non_negative;
ALTER TABLE wager_transactions DROP COLUMN IF EXISTS resume_failures;
