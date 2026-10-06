package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// WagerTransactionRepository persiste WagerTransaction (ARCHITECTURE.md,
// ADR-014).
//
//   - Insert grava qualquer estado: PENDING na entrada, PROCESSED na abertura e
//     rejeições já decididas. As unicidades de idempotência viram sentinelas
//     (ErrDuplicateExternalTransaction, ErrDuplicateIdempotencyKey,
//     ErrOpeningAlreadyExists, ErrReferenceAlreadyReversed); o caso de uso
//     (T15) relê a linha vencedora e devolve replay ou conflito.
//   - Update grava só as colunas de resultado e agendamento, e só se a linha
//     ainda não é terminal (WHERE status IN ('PENDING','PENDING_REFERENCE')).
//     Transação já terminal devolve ErrTransactionFinalized; o trigger
//     wager_transactions_guard_update é a barreira final. Quem atualiza deve
//     ter lido a linha sob lock (GetByIDForUpdate, ClaimDuePendingReferences
//     ou o lock da carteira, que serializa as operações da mesma carteira).
//   - ClaimDuePendingReferences usa FOR UPDATE SKIP LOCKED: várias instâncias
//     do worker (T21) pegam lotes disjuntos sem esperar umas pelas outras.
type WagerTransactionRepository struct{}

// NewWagerTransactionRepository cria o repositório (stateless).
func NewWagerTransactionRepository() *WagerTransactionRepository {
	return &WagerTransactionRepository{}
}

// MaxClaimBatch é o maior lote aceito por ClaimDuePendingReferences.
const MaxClaimBatch = 1000

const wagerColumns = `id, origin, kind, status, wallet_id, player_id, amount, currency,
provider_id, external_transaction_id, idempotency_key, request_hash, round_id, game_id,
reference_external_transaction_id, reference_transaction_id, failure_code, balance_after,
attempts, next_attempt_at, created_at, updated_at, completed_at`

// Insert grava uma transação nova.
func (WagerTransactionRepository) Insert(ctx context.Context, q Querier, tx *domain.WagerTransaction) error {
	const op = "postgres: inserir transação"
	if tx == nil || !tx.IsInitialized() {
		return fmt.Errorf("%s: %w: %w", op, ErrInvalidAggregate, domain.ErrUninitializedWagerTransaction)
	}
	amount, currency, err := moneyColumns("amount", tx.Money())
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	balanceAfter, err := balanceAfterColumn(tx)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	_, err = q.Exec(ctx, `INSERT INTO wager_transactions (`+wagerColumns+`)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)`,
		tx.ID(), tx.Origin().String(), tx.Kind().String(), tx.Status().String(), tx.WalletID(), tx.PlayerID(),
		amount, currency,
		nullText(tx.ProviderID()), nullText(tx.ExternalTransactionID()), nullText(tx.IdempotencyKey()),
		nullText(tx.RequestHash()), nullText(tx.RoundID()), nullText(tx.GameID()),
		nullText(tx.ReferenceExternalTransactionID()), nullUUID(tx.ReferenceTransactionID()),
		nullText(tx.FailureCode().String()), balanceAfter,
		tx.Attempts(), nullTime(tx.NextAttemptAt()), tx.CreatedAt(), tx.UpdatedAt(), nullTime(tx.CompletedAt()))
	return mapWriteError(op, err)
}

// Update grava a transição feita em memória (status, referência resolvida,
// failureCode, balanceAfter, attempts, nextAttemptAt e timestamps).
func (WagerTransactionRepository) Update(ctx context.Context, q Querier, tx *domain.WagerTransaction) error {
	const op = "postgres: atualizar transação"
	if tx == nil || !tx.IsInitialized() {
		return fmt.Errorf("%s: %w: %w", op, ErrInvalidAggregate, domain.ErrUninitializedWagerTransaction)
	}
	balanceAfter, err := balanceAfterColumn(tx)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	tag, err := q.Exec(ctx, `UPDATE wager_transactions
SET status = $2, reference_transaction_id = $3, failure_code = $4, balance_after = $5,
    attempts = $6, next_attempt_at = $7, updated_at = $8, completed_at = $9
WHERE id = $1 AND status IN ('PENDING', 'PENDING_REFERENCE')`,
		tx.ID(), tx.Status().String(), nullUUID(tx.ReferenceTransactionID()), nullText(tx.FailureCode().String()),
		balanceAfter, tx.Attempts(), nullTime(tx.NextAttemptAt()), tx.UpdatedAt(), nullTime(tx.CompletedAt()))
	if err != nil {
		return mapWriteError(op, err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var status string
	err = q.QueryRow(ctx, `SELECT status FROM wager_transactions WHERE id = $1`, tx.ID()).Scan(&status)
	if err != nil {
		return mapReadError(op, err)
	}
	return fmt.Errorf("%s: %w: transação %s está %s", op, ErrTransactionFinalized, tx.ID(), status)
}

// GetByID lê a transação pelo id interno, sem lock.
func (WagerTransactionRepository) GetByID(ctx context.Context, q Querier, id uuid.UUID) (*domain.WagerTransaction, error) {
	return getWager(ctx, q, "postgres: ler transação", `SELECT `+wagerColumns+` FROM wager_transactions WHERE id = $1`, id)
}

// GetByIDForUpdate lê e trava a transação até o fim da transação SQL.
func (WagerTransactionRepository) GetByIDForUpdate(ctx context.Context, q Querier, id uuid.UUID) (*domain.WagerTransaction, error) {
	const op = "postgres: travar transação"
	if err := requireTx(op, q); err != nil {
		return nil, err
	}
	return getWager(ctx, q, op, `SELECT `+wagerColumns+` FROM wager_transactions WHERE id = $1 FOR UPDATE`, id)
}

// GetByProviderExternalID lê por (providerId, externalTransactionId): replay
// e resolução de referências.
func (WagerTransactionRepository) GetByProviderExternalID(ctx context.Context, q Querier, providerID, externalTransactionID string) (*domain.WagerTransaction, error) {
	return getWager(ctx, q, "postgres: ler transação por id externo",
		`SELECT `+wagerColumns+` FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`,
		providerID, externalTransactionID)
}

// GetByProviderIdempotencyKey lê por (providerId, idempotencyKey).
func (WagerTransactionRepository) GetByProviderIdempotencyKey(ctx context.Context, q Querier, providerID, idempotencyKey string) (*domain.WagerTransaction, error) {
	return getWager(ctx, q, "postgres: ler transação por chave de idempotência",
		`SELECT `+wagerColumns+` FROM wager_transactions WHERE provider_id = $1 AND idempotency_key = $2`,
		providerID, idempotencyKey)
}

// ListByProviderReference lista, em ordem de criação, as operações do
// provedor que citam referenceExternalTransactionId, em qualquer estado. É a
// lista de reversões existentes que DecideWager (T06) recebe.
func (WagerTransactionRepository) ListByProviderReference(ctx context.Context, q Querier, providerID, referenceExternalTransactionID string) ([]*domain.WagerTransaction, error) {
	return listWagers(ctx, q, "postgres: listar transações por referência",
		`SELECT `+wagerColumns+` FROM wager_transactions
WHERE provider_id = $1 AND reference_external_transaction_id = $2
ORDER BY created_at, id`, providerID, referenceExternalTransactionID)
}

// ClaimDuePendingReferences trava e devolve até limit transações em
// PENDING_REFERENCE com next_attempt_at <= now, as mais atrasadas primeiro.
// Linhas já travadas por outra instância são puladas (SKIP LOCKED). Exige
// pgx.Tx: o lock vale até o commit, e o worker deve reprocessar e gravar
// (Update) na mesma transação.
func (WagerTransactionRepository) ClaimDuePendingReferences(ctx context.Context, q Querier, now time.Time, limit int) ([]*domain.WagerTransaction, error) {
	const op = "postgres: reivindicar referências pendentes"
	if err := requireTx(op, q); err != nil {
		return nil, err
	}
	if limit < 1 || limit > MaxClaimBatch {
		return nil, fmt.Errorf("%s: %w: %d (1..%d)", op, ErrInvalidPageLimit, limit, MaxClaimBatch)
	}
	return listWagers(ctx, q, op, `SELECT `+wagerColumns+` FROM wager_transactions
WHERE status = 'PENDING_REFERENCE' AND next_attempt_at <= $1
ORDER BY next_attempt_at, id
LIMIT $2
FOR UPDATE SKIP LOCKED`, now, limit)
}

// balanceAfterColumn devolve balance_after (NULL quando ausente). Presente,
// precisa ser Money válido na moeda da operação.
func balanceAfterColumn(tx *domain.WagerTransaction) (pgtype.Int8, error) {
	b := tx.BalanceAfter()
	if !b.IsValid() {
		return pgtype.Int8{}, nil
	}
	if b.Currency() != tx.Money().Currency() {
		return pgtype.Int8{}, fmt.Errorf("%w: balance_after em %s, operação em %s", ErrInvalidAggregate, b.Currency(), tx.Money().Currency())
	}
	return pgtype.Int8{Int64: b.MinorUnits(), Valid: true}, nil
}

func getWager(ctx context.Context, q Querier, op, sql string, args ...any) (*domain.WagerTransaction, error) {
	tx, err := scanWager(q.QueryRow(ctx, sql, args...))
	if err != nil {
		return nil, mapReadError(op, err)
	}
	return tx, nil
}

func listWagers(ctx context.Context, q Querier, op, sql string, args ...any) ([]*domain.WagerTransaction, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, Translate(err))
	}
	defer rows.Close()
	var out []*domain.WagerTransaction
	for rows.Next() {
		tx, err := scanWager(rows)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, Translate(err))
		}
		out = append(out, tx)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, Translate(err))
	}
	return out, nil
}

func scanWager(row pgx.Row) (*domain.WagerTransaction, error) {
	var (
		id, wallet, player                                      uuid.UUID
		origin, kind, status, currency                          string
		amount                                                  int64
		provider, external, key, hash, round, game, refExternal pgtype.Text
		failure                                                 pgtype.Text
		refID                                                   pgtype.UUID
		balanceAfter                                            pgtype.Int8
		attempts                                                int32
		nextAttempt, completedAt                                pgtype.Timestamptz
		createdAt, updatedAt                                    time.Time
	)
	if err := row.Scan(&id, &origin, &kind, &status, &wallet, &player, &amount, &currency,
		&provider, &external, &key, &hash, &round, &game,
		&refExternal, &refID, &failure, &balanceAfter,
		&attempts, &nextAttempt, &createdAt, &updatedAt, &completedAt); err != nil {
		return nil, err
	}
	money, err := moneyFromColumns("amount", amount, currency)
	if err != nil {
		return nil, fmt.Errorf("reidratar transação %s: %w", id, err)
	}
	var balance domain.Money
	if balanceAfter.Valid {
		if balance, err = moneyFromColumns("balance_after", balanceAfter.Int64, currency); err != nil {
			return nil, fmt.Errorf("reidratar transação %s: %w", id, err)
		}
	}
	tx, err := domain.RehydrateWagerTransaction(domain.RehydrateWagerTransactionParams{
		ID:                             id,
		Origin:                         domain.WagerOrigin(origin),
		Kind:                           domain.WagerKind(kind),
		Status:                         domain.WagerStatus(status),
		WalletID:                       wallet,
		PlayerID:                       player,
		Money:                          money,
		ProviderID:                     textOrEmpty(provider),
		ExternalTransactionID:          textOrEmpty(external),
		IdempotencyKey:                 textOrEmpty(key),
		RequestHash:                    textOrEmpty(hash),
		RoundID:                        textOrEmpty(round),
		GameID:                         textOrEmpty(game),
		ReferenceExternalTransactionID: textOrEmpty(refExternal),
		ReferenceTransactionID:         uuidOrNil(refID),
		FailureCode:                    domain.FailureCode(textOrEmpty(failure)),
		BalanceAfter:                   balance,
		Attempts:                       attempts,
		NextAttemptAt:                  timeOrZero(nextAttempt),
		CreatedAt:                      createdAt,
		UpdatedAt:                      updatedAt,
		CompletedAt:                    timeOrZero(completedAt),
	})
	if err != nil {
		return nil, fmt.Errorf("reidratar transação %s: %w", id, err)
	}
	return tx, nil
}
