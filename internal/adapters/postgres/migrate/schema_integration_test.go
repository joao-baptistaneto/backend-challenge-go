//go:build integration

package migrate_test

// Testes das constraints, índices e triggers do schema (T10). Cada caso de
// CHECK é construído para violar exatamente uma constraint, de modo que o nome
// reportado pelo PostgreSQL seja determinístico.

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var t0 = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

const validHash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

type fixture struct {
	conn *pgx.Conn
	seq  int
}

func newFixture(t *testing.T, conn *pgx.Conn) *fixture {
	t.Helper()
	return &fixture{conn: conn}
}

type walletRef struct {
	id, player uuid.UUID
	currency   string
}

func newID() uuid.UUID { return uuid.Must(uuid.NewV7()) }

func walletRow(id, player uuid.UUID, balance int64) row {
	return row{
		"id": id, "player_id": player, "currency": "BRL", "balance": balance,
		"version": int64(1), "created_at": t0, "updated_at": t0,
	}
}

func (f *fixture) wallet(t *testing.T, balance int64) walletRef {
	t.Helper()
	w := walletRef{id: newID(), player: newID(), currency: "BRL"}
	mustInsert(t, f.conn, "wallets", walletRow(w.id, w.player, balance))
	return w
}

// externalRow é uma BET PENDING válida; extID distingue operações.
func (f *fixture) externalRow(w walletRef) row {
	f.seq++
	ext := "tx-" + uuid.NewString()
	return row{
		"id": newID(), "origin": "EXTERNAL", "kind": "BET", "status": "PENDING",
		"wallet_id": w.id, "player_id": w.player, "amount": int64(2_500), "currency": w.currency,
		"provider_id": "provider-a", "external_transaction_id": ext,
		"idempotency_key": "provider-a:" + ext, "request_hash": validHash,
		"round_id": "round-1", "game_id": "game-1",
		"created_at": t0, "updated_at": t0,
	}
}

// processed devolve r em PROCESSED com saldo resultante.
func processed(r row, balanceAfter int64) row {
	return r.with(row{"status": "PROCESSED", "balance_after": balanceAfter, "completed_at": t0})
}

func openingRow(w walletRef, amount int64) row {
	return row{
		"id": newID(), "origin": "INTERNAL", "kind": "OPENING", "status": "PROCESSED",
		"wallet_id": w.id, "player_id": w.player, "amount": amount, "currency": w.currency,
		"balance_after": amount, "created_at": t0, "updated_at": t0, "completed_at": t0,
	}
}

// openingWithLedger grava OPENING + lançamento CREDIT 0 -> amount e devolve o
// id da transação.
func (f *fixture) openingWithLedger(t *testing.T, w walletRef, amount int64) uuid.UUID {
	t.Helper()
	op := openingRow(w, amount)
	mustInsert(t, f.conn, "wager_transactions", op)
	mustInsert(t, f.conn, "wallet_ledger_entries", ledgerRow(w, op["id"].(uuid.UUID), "CREDIT", amount, 0, amount))
	return op["id"].(uuid.UUID)
}

func ledgerRow(w walletRef, txID uuid.UUID, dir string, amount, before, after int64) row {
	return row{
		"id": newID(), "wallet_id": w.id, "transaction_id": txID, "direction": dir,
		"amount": amount, "currency": w.currency, "balance_before": before, "balance_after": after,
		"created_at": t0,
	}
}

// --- wallets ---------------------------------------------------------------------

func TestWalletsConstraints(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	conn := migratedDB(t)
	f := newFixture(t, conn)
	w := f.wallet(t, 10_000)

	t.Run("unique player and currency", func(t *testing.T) {
		err := insert(ctx, conn, "wallets", walletRow(newID(), w.player, 0))
		requirePgError(t, err, codeUniqueViolation, "wallets_player_currency_key")
	})
	t.Run("same player other currency is allowed", func(t *testing.T) {
		mustInsert(t, conn, "wallets", walletRow(newID(), w.player, 0).with(row{"currency": "USD"}))
	})
	t.Run("negative balance on insert", func(t *testing.T) {
		err := insert(ctx, conn, "wallets", walletRow(newID(), newID(), -1))
		requirePgError(t, err, codeCheckViolation, "wallets_balance_non_negative")
	})
	t.Run("version below 1", func(t *testing.T) {
		err := insert(ctx, conn, "wallets", walletRow(newID(), newID(), 0).with(row{"version": int64(0)}))
		requirePgError(t, err, codeCheckViolation, "wallets_version_positive")
	})
	t.Run("lowercase currency", func(t *testing.T) {
		err := insert(ctx, conn, "wallets", walletRow(newID(), newID(), 0).with(row{"currency": "brl"}))
		requirePgError(t, err, codeCheckViolation, "wallets_currency_format")
	})
	t.Run("short currency", func(t *testing.T) {
		err := insert(ctx, conn, "wallets", walletRow(newID(), newID(), 0).with(row{"currency": "BR"}))
		requirePgError(t, err, codeCheckViolation, "wallets_currency_format")
	})
	t.Run("updated before created", func(t *testing.T) {
		err := insert(ctx, conn, "wallets", walletRow(newID(), newID(), 0).with(row{"updated_at": t0.Add(-time.Second)}))
		requirePgError(t, err, codeCheckViolation, "wallets_timestamps")
	})

	t.Run("debit with version bump", func(t *testing.T) {
		tag, err := conn.Exec(ctx, `UPDATE wallets SET balance = balance - 2500, version = version + 1, updated_at = $2
			WHERE id = $1 AND version = 1`, w.id, t0.Add(time.Second))
		if err != nil || tag.RowsAffected() != 1 {
			t.Fatalf("debit: rows %d, err %v", tag.RowsAffected(), err)
		}
	})
	t.Run("stale optimistic version updates nothing", func(t *testing.T) {
		tag, err := conn.Exec(ctx, `UPDATE wallets SET balance = 0, version = 2 WHERE id = $1 AND version = 1`, w.id)
		if err != nil || tag.RowsAffected() != 0 {
			t.Fatalf("stale update: rows %d, err %v", tag.RowsAffected(), err)
		}
	})
	t.Run("negative balance on update", func(t *testing.T) {
		_, err := conn.Exec(ctx, `UPDATE wallets SET balance = -1, version = version + 1 WHERE id = $1`, w.id)
		requirePgError(t, err, codeCheckViolation, "wallets_balance_non_negative")
	})
	t.Run("balance change without version bump", func(t *testing.T) {
		_, err := conn.Exec(ctx, `UPDATE wallets SET balance = balance + 1 WHERE id = $1`, w.id)
		requirePgError(t, err, codeRestrictViolation, "wallets_version_increment")
	})
	t.Run("version jump", func(t *testing.T) {
		_, err := conn.Exec(ctx, `UPDATE wallets SET balance = balance + 1, version = version + 2 WHERE id = $1`, w.id)
		requirePgError(t, err, codeRestrictViolation, "wallets_version_increment")
	})
	t.Run("version bump without balance change", func(t *testing.T) {
		_, err := conn.Exec(ctx, `UPDATE wallets SET version = version + 1 WHERE id = $1`, w.id)
		requirePgError(t, err, codeRestrictViolation, "wallets_version_increment")
	})
	t.Run("currency is immutable", func(t *testing.T) {
		_, err := conn.Exec(ctx, `UPDATE wallets SET currency = 'EUR' WHERE id = $1`, w.id)
		requirePgError(t, err, codeRestrictViolation, "wallets_immutable_columns")
	})
	t.Run("player is immutable", func(t *testing.T) {
		_, err := conn.Exec(ctx, `UPDATE wallets SET player_id = $2 WHERE id = $1`, w.id, newID())
		requirePgError(t, err, codeRestrictViolation, "wallets_immutable_columns")
	})
	t.Run("updated_at must not go backwards", func(t *testing.T) {
		_, err := conn.Exec(ctx, `UPDATE wallets SET updated_at = $2 WHERE id = $1`, w.id, t0)
		requirePgError(t, err, codeRestrictViolation, "wallets_updated_at_monotonic")
	})

	var balance, version int64
	if err := conn.QueryRow(ctx, `SELECT balance, version FROM wallets WHERE id = $1`, w.id).Scan(&balance, &version); err != nil {
		t.Fatal(err)
	}
	if balance != 7_500 || version != 2 {
		t.Fatalf("wallet = (%d, v%d), want (7500, v2)", balance, version)
	}
}

// --- wager_transactions ----------------------------------------------------------

func TestWagerTransactionsCheckConstraints(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	conn := migratedDB(t)
	f := newFixture(t, conn)
	w := f.wallet(t, 10_000)
	refTarget := processed(f.externalRow(w), 7_500)
	mustInsert(t, conn, "wager_transactions", refTarget)
	refID := refTarget["id"].(uuid.UUID)
	refExt := refTarget["external_transaction_id"].(string)
	long := strings.Repeat("x", 256)

	// Linhas válidas que precisam ser aceitas (sanidade dos CHECKs).
	valid := []struct {
		name string
		r    row
	}{
		{"pending BET", f.externalRow(w)},
		{"LOSS zero processed", processed(f.externalRow(w).with(row{"kind": "LOSS", "amount": int64(0)}), 7_500)},
		{"WIN without reference", processed(f.externalRow(w).with(row{"kind": "WIN"}), 10_000)},
		{"WIN with resolved reference", processed(f.externalRow(w).with(row{
			"kind": "WIN", "reference_external_transaction_id": refExt, "reference_transaction_id": refID,
		}), 10_000)},
		{"REFUND pending reference", f.externalRow(w).with(row{
			"kind": "REFUND", "status": "PENDING_REFERENCE", "reference_external_transaction_id": "missing-ref",
			"attempts": int32(1), "next_attempt_at": t0.Add(time.Second),
		})},
		{"PENDING orphan scheduled", f.externalRow(w).with(row{"next_attempt_at": t0.Add(time.Minute)})},
		{"BET rejected insufficient funds with observed balance", f.externalRow(w).with(row{
			"status": "REJECTED", "failure_code": "INSUFFICIENT_FUNDS", "balance_after": int64(100), "completed_at": t0,
		})},
		{"ROLLBACK rejected reversal insufficient funds", f.externalRow(w).with(row{
			"kind": "ROLLBACK", "reference_external_transaction_id": refExt, "status": "REJECTED",
			"failure_code": "REVERSAL_INSUFFICIENT_FUNDS", "completed_at": t0,
		})},
		{"REFUND rejected reference not found", f.externalRow(w).with(row{
			"kind": "REFUND", "reference_external_transaction_id": "never-arrived", "status": "REJECTED",
			"failure_code": "REFERENCE_NOT_FOUND", "attempts": int32(5), "completed_at": t0,
		})},
		{"BET failed", f.externalRow(w).with(row{"status": "FAILED", "failure_code": "RETRIES_EXHAUSTED", "completed_at": t0})},
		{"rejection for unknown wallet (no FK)", f.externalRow(walletRef{id: newID(), player: newID(), currency: "BRL"}).with(row{
			"status": "REJECTED", "failure_code": "WALLET_NOT_FOUND", "completed_at": t0,
		})},
		{"identifier with 255 bytes", f.externalRow(w).with(row{"round_id": strings.Repeat("r", 255)})},
	}
	for _, tt := range valid {
		t.Run("valid/"+tt.name, func(t *testing.T) {
			mustInsert(t, conn, "wager_transactions", tt.r)
		})
	}

	// Casos que violam duas constraints ao mesmo tempo aceitam qualquer uma.
	multi := []struct {
		name        string
		r           row
		constraints []string
	}{
		{"OPENING with attempts", openingRow(w, 100).with(row{"attempts": int32(1)}),
			[]string{"wager_transactions_internal_shape", "wager_transactions_attempts_reference"}},
		{"negative attempts", f.externalRow(w).with(row{"attempts": int32(-1)}),
			[]string{"wager_transactions_attempts_non_negative", "wager_transactions_attempts_reference"}},
		// Estado desconhecido também não satisfaz status x failure_code.
		{"unknown status", f.externalRow(w).with(row{"status": "DONE"}),
			[]string{"wager_transactions_status_valid", "wager_transactions_status_failure_code"}},
	}
	for _, tt := range multi {
		t.Run("invalid/"+tt.name, func(t *testing.T) {
			requirePgError(t, insert(ctx, conn, "wager_transactions", tt.r), codeCheckViolation, tt.constraints...)
		})
	}

	invalid := []struct {
		name       string
		r          row
		constraint string
	}{
		{"BET with attempts", processed(f.externalRow(w), 1).with(row{"attempts": int32(1)}), "wager_transactions_attempts_reference"},
		{"PENDING reversal with attempts", f.externalRow(w).with(row{
			"kind": "REFUND", "reference_external_transaction_id": "x", "attempts": int32(1),
		}), "wager_transactions_attempts_reference"},
		{"EXTERNAL OPENING", f.externalRow(w).with(row{"kind": "OPENING"}), "wager_transactions_origin_kind"},
		{"INTERNAL BET", openingRow(w, 100).with(row{"kind": "BET"}), "wager_transactions_origin_kind"},
		{"OPENING with provider", openingRow(w, 100).with(row{"provider_id": "provider-a"}), "wager_transactions_internal_shape"},
		{"OPENING with round", openingRow(w, 100).with(row{"round_id": "r"}), "wager_transactions_internal_shape"},
		{"OPENING balance_after differs", openingRow(w, 100).with(row{"balance_after": int64(99)}), "wager_transactions_internal_shape"},
		{"external without provider", f.externalRow(w).with(row{"provider_id": nil}), "wager_transactions_external_shape"},
		{"external without idempotency key", f.externalRow(w).with(row{"idempotency_key": nil}), "wager_transactions_external_shape"},
		{"external without hash", f.externalRow(w).with(row{"request_hash": nil}), "wager_transactions_external_shape"},
		{"external without game", f.externalRow(w).with(row{"game_id": nil}), "wager_transactions_external_shape"},
		{"identifier over 255 bytes", f.externalRow(w).with(row{"round_id": long}), "wager_transactions_identifiers_length"},
		{"multibyte identifier over 255 bytes", f.externalRow(w).with(row{"game_id": strings.Repeat("é", 128)}), "wager_transactions_identifiers_length"},
		{"empty identifier", f.externalRow(w).with(row{"game_id": ""}), "wager_transactions_identifiers_length"},
		{"uppercase hash", f.externalRow(w).with(row{"request_hash": strings.ToUpper(validHash)}), "wager_transactions_request_hash_format"},
		{"LOSS with amount", f.externalRow(w).with(row{"kind": "LOSS", "amount": int64(1)}), "wager_transactions_amount_policy"},
		{"BET zero", f.externalRow(w).with(row{"amount": int64(0)}), "wager_transactions_amount_policy"},
		{"BET negative", f.externalRow(w).with(row{"amount": int64(-1)}), "wager_transactions_amount_policy"},
		{"REFUND without reference", f.externalRow(w).with(row{"kind": "REFUND"}), "wager_transactions_reference_required"},
		{"ROLLBACK without reference", f.externalRow(w).with(row{"kind": "ROLLBACK"}), "wager_transactions_reference_required"},
		{"BET with reference", f.externalRow(w).with(row{"reference_external_transaction_id": refExt}), "wager_transactions_reference_not_allowed"},
		{"LOSS with reference", f.externalRow(w).with(row{"kind": "LOSS", "amount": int64(0), "reference_external_transaction_id": refExt}), "wager_transactions_reference_not_allowed"},
		{"self reference", func() row {
			r := f.externalRow(w).with(row{"kind": "WIN"})
			return r.with(row{"reference_external_transaction_id": r["external_transaction_id"]})
		}(), "wager_transactions_reference_not_self"},
		{"resolved reference without external reference", f.externalRow(w).with(row{"reference_transaction_id": refID}), "wager_transactions_resolved_reference"},
		{"PROCESSED reversal without resolved reference", processed(f.externalRow(w).with(row{
			"kind": "REFUND", "reference_external_transaction_id": refExt,
		}), 10_000), "wager_transactions_processed_reference_resolved"},
		{"REJECTED without code", f.externalRow(w).with(row{"status": "REJECTED", "completed_at": t0}), "wager_transactions_status_failure_code"},
		{"REJECTED with infrastructure code", f.externalRow(w).with(row{
			"status": "REJECTED", "failure_code": "PROCESSING_FAILED", "completed_at": t0,
		}), "wager_transactions_status_failure_code"},
		{"FAILED with business code", f.externalRow(w).with(row{
			"status": "FAILED", "failure_code": "INSUFFICIENT_FUNDS", "completed_at": t0,
		}), "wager_transactions_status_failure_code"},
		{"PENDING with code", f.externalRow(w).with(row{"failure_code": "INVALID_AMOUNT"}), "wager_transactions_status_failure_code"},
		{"unknown failure code", f.externalRow(w).with(row{"status": "REJECTED", "failure_code": "BOGUS", "completed_at": t0}), "wager_transactions_failure_code_valid"},
		{"INSUFFICIENT_FUNDS on WIN", f.externalRow(w).with(row{
			"kind": "WIN", "status": "REJECTED", "failure_code": "INSUFFICIENT_FUNDS", "completed_at": t0,
		}), "wager_transactions_failure_code_kind"},
		{"REVERSAL_INSUFFICIENT_FUNDS on REFUND", f.externalRow(w).with(row{
			"kind": "REFUND", "reference_external_transaction_id": refExt, "status": "REJECTED",
			"failure_code": "REVERSAL_INSUFFICIENT_FUNDS", "completed_at": t0,
		}), "wager_transactions_failure_code_kind"},
		{"ALREADY_REVERSED on WIN", f.externalRow(w).with(row{
			"kind": "WIN", "reference_external_transaction_id": refExt, "status": "REJECTED",
			"failure_code": "ALREADY_REVERSED", "completed_at": t0,
		}), "wager_transactions_failure_code_kind"},
		{"REFERENCE_NOT_FOUND without reference", f.externalRow(w).with(row{
			"status": "REJECTED", "failure_code": "REFERENCE_NOT_FOUND", "completed_at": t0,
		}), "wager_transactions_failure_code_reference"},
		{"REFERENCE_NOT_FOUND with resolved reference", f.externalRow(w).with(row{
			"kind": "REFUND", "reference_external_transaction_id": refExt, "reference_transaction_id": refID,
			"status": "REJECTED", "failure_code": "REFERENCE_NOT_FOUND", "completed_at": t0,
		}), "wager_transactions_failure_code_reference"},
		{"PROCESSED without balance_after", f.externalRow(w).with(row{"status": "PROCESSED", "completed_at": t0}), "wager_transactions_processed_balance_after"},
		{"PENDING with balance_after", f.externalRow(w).with(row{"balance_after": int64(1)}), "wager_transactions_balance_after_status"},
		{"negative balance_after", processed(f.externalRow(w), -1), "wager_transactions_balance_after_non_negative"},
		{"PENDING with completed_at", f.externalRow(w).with(row{"completed_at": t0}), "wager_transactions_completed_at_status"},
		{"PROCESSED without completed_at", processed(f.externalRow(w), 1).with(row{"completed_at": nil}), "wager_transactions_completed_at_status"},
		{"completed after updated", processed(f.externalRow(w), 1).with(row{"completed_at": t0.Add(time.Second)}), "wager_transactions_timestamps"},
		{"updated before created", f.externalRow(w).with(row{"updated_at": t0.Add(-time.Second)}), "wager_transactions_timestamps"},
		{"PENDING_REFERENCE without attempts", f.externalRow(w).with(row{
			"kind": "REFUND", "status": "PENDING_REFERENCE", "reference_external_transaction_id": "x",
			"next_attempt_at": t0,
		}), "wager_transactions_pending_reference_schedule"},
		{"PENDING_REFERENCE without next_attempt_at", f.externalRow(w).with(row{
			"kind": "REFUND", "status": "PENDING_REFERENCE", "reference_external_transaction_id": "x",
			"attempts": int32(1),
		}), "wager_transactions_pending_reference_schedule"},
		{"terminal still scheduled", processed(f.externalRow(w), 1).with(row{"next_attempt_at": t0}), "wager_transactions_terminal_unscheduled"},
		{"next_attempt_at before creation", f.externalRow(w).with(row{"next_attempt_at": t0.Add(-time.Second)}), "wager_transactions_next_attempt_after_creation"},
		{"unknown kind", f.externalRow(w).with(row{"kind": "bet"}), "wager_transactions_kind_valid"},
		{"unknown origin", f.externalRow(w).with(row{"origin": "OTHER"}), "wager_transactions_origin_valid"},
		{"lowercase currency", f.externalRow(w).with(row{"currency": "brl"}), "wager_transactions_currency_format"},
	}
	for _, tt := range invalid {
		t.Run("invalid/"+tt.name, func(t *testing.T) {
			requirePgError(t, insert(ctx, conn, "wager_transactions", tt.r), codeCheckViolation, tt.constraint)
		})
	}
}

func TestWagerTransactionsUniqueness(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	conn := migratedDB(t)
	f := newFixture(t, conn)
	w := f.wallet(t, 10_000)

	base := f.externalRow(w)
	mustInsert(t, conn, "wager_transactions", base)

	t.Run("same provider and external id with another key", func(t *testing.T) {
		r := f.externalRow(w).with(row{"external_transaction_id": base["external_transaction_id"]})
		requirePgError(t, insert(ctx, conn, "wager_transactions", r), codeUniqueViolation, "wager_transactions_provider_external_key")
	})
	t.Run("same provider and idempotency key with another external id", func(t *testing.T) {
		r := f.externalRow(w).with(row{"idempotency_key": base["idempotency_key"]})
		requirePgError(t, insert(ctx, conn, "wager_transactions", r), codeUniqueViolation, "wager_transactions_provider_idempotency_key")
	})
	t.Run("same ids under another provider", func(t *testing.T) {
		mustInsert(t, conn, "wager_transactions", f.externalRow(w).with(row{
			"provider_id":             "provider-b",
			"external_transaction_id": base["external_transaction_id"],
			"idempotency_key":         base["idempotency_key"],
		}))
	})
	t.Run("duplicate primary key", func(t *testing.T) {
		r := f.externalRow(w).with(row{"id": base["id"]})
		requirePgError(t, insert(ctx, conn, "wager_transactions", r), codeUniqueViolation, "wager_transactions_pkey")
	})

	t.Run("OPENING once per wallet", func(t *testing.T) {
		mustInsert(t, conn, "wager_transactions", openingRow(w, 10_000))
		requirePgError(t, insert(ctx, conn, "wager_transactions", openingRow(w, 10_000)), codeUniqueViolation, "wager_transactions_opening_per_wallet_key")
		// Outra carteira pode ter sua própria abertura.
		other := f.wallet(t, 500)
		mustInsert(t, conn, "wager_transactions", openingRow(other, 500))
	})

	t.Run("one successful reversal per reference and kind", func(t *testing.T) {
		bet := processed(f.externalRow(w), 7_500)
		mustInsert(t, conn, "wager_transactions", bet)
		reversal := func(kind, status string) row {
			r := f.externalRow(w).with(row{
				"kind": kind, "reference_external_transaction_id": bet["external_transaction_id"],
			})
			switch status {
			case "PROCESSED":
				r = processed(r, 10_000).with(row{"reference_transaction_id": bet["id"]})
			case "REJECTED":
				r = r.with(row{"status": "REJECTED", "failure_code": "ALREADY_REVERSED", "completed_at": t0, "reference_transaction_id": bet["id"]})
			}
			return r
		}
		mustInsert(t, conn, "wager_transactions", reversal("REFUND", "PROCESSED"))
		requirePgError(t, insert(ctx, conn, "wager_transactions", reversal("REFUND", "PROCESSED")),
			codeUniqueViolation, "wager_transactions_reversal_once_key")
		// Rejeições e pendências do mesmo tipo não colidem.
		mustInsert(t, conn, "wager_transactions", reversal("REFUND", "REJECTED"))
		mustInsert(t, conn, "wager_transactions", reversal("REFUND", "PENDING"))
		// Política REFUND x ROLLBACK (ADR-009): uma única reversão bem-sucedida
		// por referência, de qualquer tipo (evita devolver o débito duas vezes).
		requirePgError(t, insert(ctx, conn, "wager_transactions", reversal("ROLLBACK", "PROCESSED")),
			codeUniqueViolation, "wager_transactions_reversal_once_key")

		// ROLLBACK do REFUND referencia o REFUND, não a BET: permitido uma vez.
		var refundID uuid.UUID
		var refundExt string
		if err := conn.QueryRow(ctx, `SELECT id, external_transaction_id FROM wager_transactions
			WHERE reference_transaction_id = $1 AND kind = 'REFUND' AND status = 'PROCESSED'`, bet["id"]).Scan(&refundID, &refundExt); err != nil {
			t.Fatal(err)
		}
		rollbackOfRefund := func() row {
			return processed(f.externalRow(w).with(row{
				"kind": "ROLLBACK", "reference_external_transaction_id": refundExt, "reference_transaction_id": refundID,
			}), 7_500)
		}
		mustInsert(t, conn, "wager_transactions", rollbackOfRefund())
		requirePgError(t, insert(ctx, conn, "wager_transactions", rollbackOfRefund()),
			codeUniqueViolation, "wager_transactions_reversal_once_key")
	})

	t.Run("resolved reference must exist", func(t *testing.T) {
		r := processed(f.externalRow(w).with(row{
			"kind": "REFUND", "reference_external_transaction_id": "ghost", "reference_transaction_id": newID(),
		}), 10_000)
		requirePgError(t, insert(ctx, conn, "wager_transactions", r), codeForeignKeyViolation, "wager_transactions_reference_fkey")
	})
}

func TestWagerTransactionsStateMachineTrigger(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	conn := migratedDB(t)
	f := newFixture(t, conn)
	w := f.wallet(t, 10_000)

	t.Run("PENDING to PROCESSED", func(t *testing.T) {
		r := f.externalRow(w)
		mustInsert(t, conn, "wager_transactions", r)
		mustExec(t, conn, `UPDATE wager_transactions SET status = 'PROCESSED', balance_after = 7500,
			updated_at = $2, completed_at = $2 WHERE id = $1`, r["id"], t0.Add(time.Second))

		_, err := conn.Exec(ctx, `UPDATE wager_transactions SET updated_at = $2 WHERE id = $1`, r["id"], t0.Add(time.Hour))
		requirePgError(t, err, codeRestrictViolation, "wager_transactions_terminal_immutable")
		_, err = conn.Exec(ctx, `UPDATE wager_transactions SET status = 'REJECTED', failure_code = 'INVALID_REQUEST',
			balance_after = NULL WHERE id = $1`, r["id"])
		requirePgError(t, err, codeRestrictViolation, "wager_transactions_terminal_immutable")
	})

	t.Run("PENDING_REFERENCE reschedule then REJECTED", func(t *testing.T) {
		r := f.externalRow(w).with(row{
			"kind": "REFUND", "status": "PENDING_REFERENCE", "reference_external_transaction_id": "late",
			"attempts": int32(1), "next_attempt_at": t0.Add(time.Second),
		})
		mustInsert(t, conn, "wager_transactions", r)
		mustExec(t, conn, `UPDATE wager_transactions SET attempts = attempts + 1, next_attempt_at = $2, updated_at = $2
			WHERE id = $1`, r["id"], t0.Add(2*time.Second))

		_, err := conn.Exec(ctx, `UPDATE wager_transactions SET status = 'PENDING', next_attempt_at = NULL WHERE id = $1`, r["id"])
		requirePgError(t, err, codeRestrictViolation, "wager_transactions_status_transition")
		_, err = conn.Exec(ctx, `UPDATE wager_transactions SET attempts = 0 WHERE id = $1`, r["id"])
		requirePgError(t, err, codeRestrictViolation, "wager_transactions_attempts_monotonic")

		mustExec(t, conn, `UPDATE wager_transactions SET status = 'REJECTED', failure_code = 'REFERENCE_NOT_FOUND',
			next_attempt_at = NULL, updated_at = $2, completed_at = $2 WHERE id = $1`, r["id"], t0.Add(3*time.Second))
	})

	t.Run("input columns are immutable", func(t *testing.T) {
		r := f.externalRow(w)
		mustInsert(t, conn, "wager_transactions", r)
		for _, set := range []string{
			"amount = 1", "currency = 'USD'", "kind = 'WIN'", "request_hash = repeat('f', 64)",
			"idempotency_key = 'other'", "wallet_id = gen_random_uuid()", "created_at = created_at - interval '1 second'",
		} {
			_, err := conn.Exec(ctx, `UPDATE wager_transactions SET `+set+` WHERE id = $1`, r["id"])
			requirePgError(t, err, codeRestrictViolation, "wager_transactions_immutable_columns")
		}
	})

	t.Run("CHECKs still apply on update", func(t *testing.T) {
		r := f.externalRow(w)
		mustInsert(t, conn, "wager_transactions", r)
		_, err := conn.Exec(ctx, `UPDATE wager_transactions SET status = 'PROCESSED', completed_at = updated_at WHERE id = $1`, r["id"])
		requirePgError(t, err, codeCheckViolation, "wager_transactions_processed_balance_after")
	})
}

// --- wallet_ledger_entries -------------------------------------------------------

func TestLedgerConstraints(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	conn := migratedDB(t)
	f := newFixture(t, conn)
	w := f.wallet(t, 10_000)
	openingID := f.openingWithLedger(t, w, 10_000)

	bet := processed(f.externalRow(w), 7_500)
	mustInsert(t, conn, "wager_transactions", bet)
	betID := bet["id"].(uuid.UUID)
	mustInsert(t, conn, "wallet_ledger_entries", ledgerRow(w, betID, "DEBIT", 2_500, 10_000, 7_500))

	t.Run("one entry per wallet and transaction", func(t *testing.T) {
		err := insert(ctx, conn, "wallet_ledger_entries", ledgerRow(w, betID, "DEBIT", 2_500, 7_500, 5_000))
		requirePgError(t, err, codeUniqueViolation, "wallet_ledger_entries_wallet_transaction_key")
	})

	tx := func() uuid.UUID {
		r := f.externalRow(w)
		mustInsert(t, conn, "wager_transactions", r)
		return r["id"].(uuid.UUID)
	}
	checks := []struct {
		name        string
		r           row
		constraints []string
	}{
		{"credit arithmetic", ledgerRow(w, tx(), "CREDIT", 100, 7_500, 7_501), []string{"wallet_ledger_entries_arithmetic"}},
		{"debit arithmetic", ledgerRow(w, tx(), "DEBIT", 100, 7_500, 7_500), []string{"wallet_ledger_entries_arithmetic"}},
		{"zero amount", ledgerRow(w, tx(), "CREDIT", 0, 7_500, 7_500), []string{"wallet_ledger_entries_amount_positive"}},
		{"negative amount", ledgerRow(w, tx(), "DEBIT", -100, 7_500, 7_600), []string{"wallet_ledger_entries_amount_positive"}},
		{"negative balance after", ledgerRow(w, tx(), "DEBIT", 100, 50, -50), []string{"wallet_ledger_entries_balances_non_negative"}},
		// Direção inválida também viola a aritmética; ambas são aceitas.
		{"unknown direction", ledgerRow(w, tx(), "debit", 100, 7_500, 7_400),
			[]string{"wallet_ledger_entries_direction_valid", "wallet_ledger_entries_arithmetic"}},
	}
	for _, tt := range checks {
		t.Run(tt.name, func(t *testing.T) {
			requirePgError(t, insert(ctx, conn, "wallet_ledger_entries", tt.r), codeCheckViolation, tt.constraints...)
		})
	}

	t.Run("arithmetic near int64 limit does not overflow", func(t *testing.T) {
		big := f.wallet(t, 0)
		op := openingRow(big, math.MaxInt64-1)
		mustInsert(t, conn, "wager_transactions", op)
		mustInsert(t, conn, "wallet_ledger_entries", ledgerRow(big, op["id"].(uuid.UUID), "CREDIT", math.MaxInt64-1, 0, math.MaxInt64-1))
		win := processed(f.externalRow(big).with(row{"kind": "WIN", "amount": int64(1)}), math.MaxInt64)
		mustInsert(t, conn, "wager_transactions", win)
		mustInsert(t, conn, "wallet_ledger_entries", ledgerRow(big, win["id"].(uuid.UUID), "CREDIT", 1, math.MaxInt64-1, math.MaxInt64))
	})

	t.Run("currency must match wallet", func(t *testing.T) {
		usd := walletRef{id: w.id, player: w.player, currency: "USD"}
		err := insert(ctx, conn, "wallet_ledger_entries", ledgerRow(usd, tx(), "CREDIT", 100, 7_500, 7_600))
		requirePgError(t, err, codeForeignKeyViolation, "wallet_ledger_entries_wallet_fkey", "wallet_ledger_entries_transaction_fkey")
	})
	t.Run("transaction must belong to the wallet", func(t *testing.T) {
		other := f.wallet(t, 0)
		otherTx := f.externalRow(other)
		mustInsert(t, conn, "wager_transactions", otherTx)
		err := insert(ctx, conn, "wallet_ledger_entries", ledgerRow(w, otherTx["id"].(uuid.UUID), "CREDIT", 100, 7_500, 7_600))
		requirePgError(t, err, codeForeignKeyViolation, "wallet_ledger_entries_transaction_fkey")
	})
	t.Run("transaction must exist", func(t *testing.T) {
		err := insert(ctx, conn, "wallet_ledger_entries", ledgerRow(w, newID(), "CREDIT", 100, 7_500, 7_600))
		requirePgError(t, err, codeForeignKeyViolation, "wallet_ledger_entries_transaction_fkey")
	})

	// Append-only.
	mutations := []struct{ name, sql string }{
		{"UPDATE", `UPDATE wallet_ledger_entries SET amount = amount WHERE transaction_id = '` + openingID.String() + `'`},
		{"UPDATE all", `UPDATE wallet_ledger_entries SET created_at = now()`},
		{"DELETE", `DELETE FROM wallet_ledger_entries WHERE transaction_id = '` + betID.String() + `'`},
		{"DELETE all", `DELETE FROM wallet_ledger_entries`},
		{"TRUNCATE", `TRUNCATE wallet_ledger_entries`},
		{"TRUNCATE wallets CASCADE", `TRUNCATE wallets CASCADE`},
		{"TRUNCATE wager_transactions CASCADE", `TRUNCATE wager_transactions CASCADE`},
	}
	for _, mu := range mutations {
		t.Run("append-only/"+mu.name, func(t *testing.T) {
			_, err := conn.Exec(ctx, mu.sql)
			requirePgError(t, err, codeRestrictViolation, "wallet_ledger_entries_append_only")
		})
	}

	t.Run("deleting a wallet with entries is blocked", func(t *testing.T) {
		// A guarda de linha do ledger dispara antes da ação da FK; qualquer
		// das duas proteções é aceitável.
		_, err := conn.Exec(ctx, `DELETE FROM wallets WHERE id = $1`, w.id)
		if err == nil {
			t.Fatal("wallet with ledger entries was deleted")
		}
	})

	var n int
	var sum int64
	if err := conn.QueryRow(ctx, `SELECT count(*), COALESCE(sum(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END), 0)
		FROM wallet_ledger_entries WHERE wallet_id = $1`, w.id).Scan(&n, &sum); err != nil {
		t.Fatal(err)
	}
	if n != 2 || sum != 7_500 {
		t.Fatalf("ledger changed: %d entries, sum %d; want 2 entries, 7500", n, sum)
	}
}

// --- inbox -----------------------------------------------------------------------

func TestInboxConstraints(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	conn := migratedDB(t)
	msg := row{"consumer_name": "wager-transactions", "message_id": "msg-123", "request_hash": validHash, "received_at": t0}
	mustInsert(t, conn, "inbox", msg)

	t.Run("duplicate consumer and message", func(t *testing.T) {
		err := insert(ctx, conn, "inbox", msg.with(row{"request_hash": strings.Repeat("a", 64)}))
		requirePgError(t, err, codeUniqueViolation, "inbox_consumer_message_key")
	})
	t.Run("same message for another consumer", func(t *testing.T) {
		mustInsert(t, conn, "inbox", msg.with(row{"consumer_name": "audit"}))
	})
	t.Run("ON CONFLICT DO NOTHING registers once", func(t *testing.T) {
		tag, err := conn.Exec(ctx, `INSERT INTO inbox (consumer_name, message_id, request_hash, received_at)
			VALUES ($1, $2, $3, $4) ON CONFLICT (consumer_name, message_id) DO NOTHING`,
			msg["consumer_name"], msg["message_id"], msg["request_hash"], t0)
		if err != nil || tag.RowsAffected() != 0 {
			t.Fatalf("rows %d, err %v", tag.RowsAffected(), err)
		}
	})
	t.Run("invalid hash", func(t *testing.T) {
		err := insert(ctx, conn, "inbox", msg.with(row{"message_id": "m2", "request_hash": strings.Repeat("Z", 64)}))
		requirePgError(t, err, codeCheckViolation, "inbox_request_hash_format")
	})
	t.Run("empty message id", func(t *testing.T) {
		err := insert(ctx, conn, "inbox", msg.with(row{"message_id": ""}))
		requirePgError(t, err, codeCheckViolation, "inbox_message_id_length")
	})
	t.Run("completed before received", func(t *testing.T) {
		err := insert(ctx, conn, "inbox", msg.with(row{"message_id": "m3", "completed_at": t0.Add(-time.Second)}))
		requirePgError(t, err, codeCheckViolation, "inbox_completed_after_received")
	})
	t.Run("transaction must exist", func(t *testing.T) {
		err := insert(ctx, conn, "inbox", msg.with(row{"message_id": "m4", "transaction_id": newID()}))
		requirePgError(t, err, codeForeignKeyViolation, "inbox_transaction_fkey")
	})
	t.Run("complete", func(t *testing.T) {
		mustExec(t, conn, `UPDATE inbox SET completed_at = $3 WHERE consumer_name = $1 AND message_id = $2`,
			msg["consumer_name"], msg["message_id"], t0.Add(time.Second))
	})
}

// --- outbox ----------------------------------------------------------------------

func TestOutboxConstraints(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	conn := migratedDB(t)
	payload := []byte(`{"eventId":"x","data":{"money":{"amount":"25.00","currency":"BRL"}}}`)
	ev := func() row {
		return row{
			"event_id": newID(), "event_type": "WagerTransactionProcessed", "event_version": int32(1),
			"aggregate_id": newID().String(), "correlation_id": "corr-1", "payload": string(payload),
			"occurred_at": t0,
		}
	}
	first := ev()
	mustInsert(t, conn, "outbox", first)
	id := first["event_id"]

	t.Run("payload snapshot preserved byte for byte", func(t *testing.T) {
		var got string
		if err := conn.QueryRow(ctx, `SELECT payload::text FROM outbox WHERE event_id = $1`, id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != string(payload) {
			t.Fatalf("payload = %s, want %s", got, payload)
		}
	})
	t.Run("defaults", func(t *testing.T) {
		var attempts int32
		var published, locked *time.Time
		if err := conn.QueryRow(ctx, `SELECT attempts, published_at, locked_until FROM outbox WHERE event_id = $1`, id).
			Scan(&attempts, &published, &locked); err != nil {
			t.Fatal(err)
		}
		if attempts != 0 || published != nil || locked != nil {
			t.Fatalf("unexpected defaults: attempts %d, published %v, locked %v", attempts, published, locked)
		}
	})
	t.Run("duplicate event id", func(t *testing.T) {
		requirePgError(t, insert(ctx, conn, "outbox", ev().with(row{"event_id": id})), codeUniqueViolation, "outbox_event_id_key")
	})
	t.Run("invalid JSON payload", func(t *testing.T) {
		requirePgError(t, insert(ctx, conn, "outbox", ev().with(row{"payload": "{not json"})), codeInvalidText)
	})
	t.Run("version below 1", func(t *testing.T) {
		requirePgError(t, insert(ctx, conn, "outbox", ev().with(row{"event_version": int32(0)})), codeCheckViolation, "outbox_event_version_positive")
	})
	t.Run("lease owner without deadline", func(t *testing.T) {
		requirePgError(t, insert(ctx, conn, "outbox", ev().with(row{"locked_by": "publisher-1"})), codeCheckViolation, "outbox_lease_pair")
	})
	t.Run("snapshot is immutable", func(t *testing.T) {
		_, err := conn.Exec(ctx, `UPDATE outbox SET payload = '{}' WHERE event_id = $1`, id)
		requirePgError(t, err, codeRestrictViolation, "outbox_snapshot_immutable")
		_, err = conn.Exec(ctx, `UPDATE outbox SET event_type = 'Other' WHERE event_id = $1`, id)
		requirePgError(t, err, codeRestrictViolation, "outbox_snapshot_immutable")
	})
	t.Run("unpublished event cannot be deleted", func(t *testing.T) {
		_, err := conn.Exec(ctx, `DELETE FROM outbox WHERE event_id = $1`, id)
		requirePgError(t, err, codeRestrictViolation, "outbox_unpublished_not_deletable")
	})
	t.Run("claim, fail with backoff, publish", func(t *testing.T) {
		mustExec(t, conn, `UPDATE outbox SET locked_by = 'p1', locked_until = now() + interval '30 seconds' WHERE event_id = $1`, id)
		mustExec(t, conn, `UPDATE outbox SET attempts = attempts + 1, next_attempt_at = now() + interval '2 seconds',
			last_error = 'timeout', locked_by = NULL, locked_until = NULL WHERE event_id = $1`, id)
		_, err := conn.Exec(ctx, `UPDATE outbox SET attempts = 0 WHERE event_id = $1`, id)
		requirePgError(t, err, codeRestrictViolation, "outbox_attempts_monotonic")
		mustExec(t, conn, `UPDATE outbox SET published_at = now(), locked_by = NULL, locked_until = NULL WHERE event_id = $1`, id)
	})
	t.Run("published event is immutable", func(t *testing.T) {
		_, err := conn.Exec(ctx, `UPDATE outbox SET published_at = NULL WHERE event_id = $1`, id)
		requirePgError(t, err, codeRestrictViolation, "outbox_published_immutable")
	})
	t.Run("published event can be deleted (retention)", func(t *testing.T) {
		mustExec(t, conn, `DELETE FROM outbox WHERE event_id = $1`, id)
	})
}

// Os índices usados pelos workers existem com os predicados esperados.
func TestWorkerIndexes(t *testing.T) {
	t.Parallel()
	conn := migratedDB(t)
	want := map[string]string{
		"wager_transactions_pending_reference_due_idx": "PENDING_REFERENCE",
		"wager_transactions_pending_idx":               "PENDING",
		"wager_transactions_opening_per_wallet_key":    "OPENING",
		"wager_transactions_reversal_once_key":         "PROCESSED",
		"outbox_unpublished_due_idx":                   "published_at IS NULL",
		"wallet_ledger_entries_wallet_page_idx":        "created_at",
		"wallet_ledger_entries_wallet_seq_key":         "(wallet_id, seq)",
	}
	for name, fragment := range want {
		var def string
		if err := conn.QueryRow(t.Context(), `SELECT indexdef FROM pg_indexes WHERE indexname = $1`, name).Scan(&def); err != nil {
			t.Errorf("index %s: %v", name, err)
			continue
		}
		if !strings.Contains(def, fragment) {
			t.Errorf("index %s = %q, want fragment %q", name, def, fragment)
		}
	}
}
