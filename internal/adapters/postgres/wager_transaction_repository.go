package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
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
//     ter lido a linha sob lock (GetByIDForUpdate, GetClaimedForUpdate ou o
//     lock da carteira, que serializa as operações da mesma carteira). Update
//     também solta o lease do worker (claimed_by/claimed_until = NULL) e zera
//     resume_failures, salvo ao gravar FAILED (o contador fica para
//     auditoria; T30, ADR-031).
//   - LockClaimedForFailure, RecordResumeFailure e FailClaimedUnreadable (T30,
//     ADR-031) registram falhas não transitórias da retomada sem reidratar a
//     linha (o item envenenado pode ser justamente uma linha que não
//     reidrata), sempre com o fencing do lease (claimed_by = token).
//   - ClaimDue (T21, ADR-023) grava um lease na linha (claimed_by =
//     token, claimed_until = now + lease) num único comando, escolhendo as
//     linhas com FOR NO KEY UPDATE SKIP LOCKED: várias instâncias pegam lotes
//     disjuntos sem esperar umas pelas outras, e um worker que morre não perde
//     a linha (o lease expira). GetClaimedForUpdate é o fencing: só devolve a
//     linha se ela ainda estiver reivindicada pelo mesmo token.
type WagerTransactionRepository struct{}

// NewWagerTransactionRepository cria o repositório (stateless).
func NewWagerTransactionRepository() *WagerTransactionRepository {
	return &WagerTransactionRepository{}
}

// MaxClaimBatch é o maior lote aceito por ClaimDue.
const MaxClaimBatch = 1000

// MaxClaimLease é o maior lease aceito por ClaimDue.
const MaxClaimLease = 24 * time.Hour

// ErrInvalidClaim indica parâmetros de ClaimDue inválidos.
var ErrInvalidClaim = errors.New("postgres: parâmetros de reivindicação inválidos")

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
    attempts = $6, next_attempt_at = $7, updated_at = $8, completed_at = $9,
    claimed_by = NULL, claimed_until = NULL,
    resume_failures = CASE WHEN $2 = 'FAILED' THEN resume_failures ELSE 0 END
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

// ClaimParams são os parâmetros de ClaimDue.
type ClaimParams struct {
	// Now é o instante de referência (relógio da aplicação, o mesmo que
	// grava next_attempt_at): vencimento e fim do lease contam a partir dele.
	Now time.Time
	// Token identifica o lote reivindicado (fencing). Não pode ser nulo.
	Token uuid.UUID
	// Lease é a duração da reivindicação (1ms..MaxClaimLease).
	Lease time.Duration
	// Limit é o tamanho máximo do lote (1..MaxClaimBatch).
	Limit int
	// OrphanGrace é a idade mínima de um PENDING sem next_attempt_at para ser
	// tratado como órfão (>= 0).
	OrphanGrace time.Duration
}

// ClaimedWager é uma linha reivindicada por ClaimDue.
type ClaimedWager struct {
	ID     uuid.UUID
	Status domain.WagerStatus
	// DueAt é o instante em que a linha venceu (ordem de processamento).
	DueAt time.Time
}

// ClaimDue reivindica até Limit transações vencidas e sem lease vigente,
// gravando claimed_by = Token e claimed_until = Now + Lease. São elegíveis,
// nesta ordem:
//   - PENDING_REFERENCE com next_attempt_at <= Now (as mais atrasadas
//     primeiro);
//   - PENDING órfão: COALESCE(next_attempt_at, created_at + OrphanGrace) <=
//     Now (as mais antigas primeiro). O fluxo síncrono não confirma PENDING
//     (ADR-017); uma linha PENDING confirmada é resto de um aceite
//     interrompido e precisa de retomada.
//
// Cada grupo é um UPDATE ... FROM (SELECT ... LIMIT ... FOR NO KEY UPDATE SKIP
// LOCKED): só as linhas efetivamente reivindicadas ficam travadas, instâncias
// concorrentes pegam conjuntos disjuntos sem esperar, e o modo NO KEY não
// conflita com o KEY SHARE da FK reference_transaction_id (uma operação nova
// que cita a linha não espera pelo claim). Aceita qualquer Querier: com o
// pool cada comando é autocommit; numa transação, o lease vale a partir do
// commit dela. Devolve só id, estado e vencimento (nada é reidratado aqui: uma
// linha corrompida falha sozinha no processamento, sem derrubar o lote), na
// ordem acima.
func (WagerTransactionRepository) ClaimDue(ctx context.Context, q Querier, p ClaimParams) ([]ClaimedWager, error) {
	const op = "postgres: reivindicar transações pendentes"
	switch {
	case p.Now.IsZero():
		return nil, fmt.Errorf("%s: %w: instante zero", op, ErrInvalidClaim)
	case p.Token == uuid.Nil:
		return nil, fmt.Errorf("%s: %w: token nulo", op, ErrInvalidClaim)
	case p.Lease < time.Millisecond || p.Lease > MaxClaimLease:
		return nil, fmt.Errorf("%s: %w: lease %v (1ms..%v)", op, ErrInvalidClaim, p.Lease, MaxClaimLease)
	case p.Limit < 1 || p.Limit > MaxClaimBatch:
		return nil, fmt.Errorf("%s: %w: lote %d (1..%d)", op, ErrInvalidClaim, p.Limit, MaxClaimBatch)
	case p.OrphanGrace < 0:
		return nil, fmt.Errorf("%s: %w: carência negativa %v", op, ErrInvalidClaim, p.OrphanGrace)
	}
	refs, err := claimGroup(ctx, q, `WITH due AS (
    SELECT id, next_attempt_at AS due_at FROM wager_transactions
    WHERE status = 'PENDING_REFERENCE' AND next_attempt_at <= $1
      AND (claimed_until IS NULL OR claimed_until <= $1)
    ORDER BY next_attempt_at, id
    LIMIT $3
    FOR NO KEY UPDATE SKIP LOCKED
)
UPDATE wager_transactions w
SET claimed_by = $2, claimed_until = $1 + $4 * interval '1 microsecond'
FROM due
WHERE w.id = due.id
RETURNING w.id, w.status, due.due_at`, p.Now, p.Token, p.Limit, p.Lease.Microseconds())
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if len(refs) == p.Limit {
		return refs, nil
	}
	orphans, err := claimGroup(ctx, q, `WITH due AS (
    SELECT id, COALESCE(next_attempt_at, created_at + $5 * interval '1 microsecond') AS due_at
    FROM wager_transactions
    WHERE status = 'PENDING'
      AND COALESCE(next_attempt_at, created_at + $5 * interval '1 microsecond') <= $1
      AND (claimed_until IS NULL OR claimed_until <= $1)
    ORDER BY created_at, id
    LIMIT $3
    FOR NO KEY UPDATE SKIP LOCKED
)
UPDATE wager_transactions w
SET claimed_by = $2, claimed_until = $1 + $4 * interval '1 microsecond'
FROM due
WHERE w.id = due.id
RETURNING w.id, w.status, due.due_at`, p.Now, p.Token, p.Limit-len(refs), p.Lease.Microseconds(), p.OrphanGrace.Microseconds())
	if err != nil {
		// O primeiro grupo pode já estar confirmado (autocommit): o lease dele
		// expira sozinho.
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return append(refs, orphans...), nil
}

// claimGroup executa um UPDATE ... RETURNING id, status, due_at e ordena o
// resultado (RETURNING não garante ordem).
func claimGroup(ctx context.Context, q Querier, sql string, args ...any) ([]ClaimedWager, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, Translate(err)
	}
	defer rows.Close()
	var out []ClaimedWager
	for rows.Next() {
		var (
			c      ClaimedWager
			status string
		)
		if err := rows.Scan(&c.ID, &status, &c.DueAt); err != nil {
			return nil, Translate(err)
		}
		c.Status = domain.WagerStatus(status)
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, Translate(err)
	}
	slices.SortFunc(out, func(a, b ClaimedWager) int {
		if c := a.DueAt.Compare(b.DueAt); c != 0 {
			return c
		}
		return bytes.Compare(a.ID[:], b.ID[:])
	})
	return out, nil
}

// GetClaimedForUpdate lê e trava (FOR NO KEY UPDATE) a transação só se ela
// ainda estiver reivindicada por token (fencing do lease, ADR-023). Outra
// instância que reassumiu a linha depois de um lease expirado tem outro token:
// o chamador recebe ErrNotFound e desiste sem efeito. A linha pode estar com o
// lease vencido e ainda ser devolvida, desde que ninguém a tenha reassumido:
// o lock garante a exclusão daqui até o commit. Exige pgx.Tx.
func (WagerTransactionRepository) GetClaimedForUpdate(ctx context.Context, q Querier, id, token uuid.UUID) (*domain.WagerTransaction, error) {
	const op = "postgres: travar transação reivindicada"
	if err := requireTx(op, q); err != nil {
		return nil, err
	}
	if token == uuid.Nil {
		return nil, fmt.Errorf("%s: %w: token nulo", op, ErrInvalidClaim)
	}
	return getWager(ctx, q, op, `SELECT `+wagerColumns+` FROM wager_transactions
WHERE id = $1 AND claimed_by = $2
FOR NO KEY UPDATE`, id, token)
}

// ResumeFailureState é o estado operacional de uma linha reivindicada, lido
// sem reidratar o agregado (T30, ADR-031).
type ResumeFailureState struct {
	Status    domain.WagerStatus
	CreatedAt time.Time
	// Failures é resume_failures: falhas consecutivas não transitórias da
	// retomada.
	Failures int32
}

// LockClaimedForFailure trava (FOR NO KEY UPDATE) a linha só se ela ainda
// estiver reivindicada por token (mesmo fencing de GetClaimedForUpdate) e
// devolve status, created_at e resume_failures SEM reidratar: precisa
// funcionar justamente na linha que não reidrata (item envenenado). Perdeu o
// lease ou inexistente: ErrNotFound. Exige pgx.Tx.
func (WagerTransactionRepository) LockClaimedForFailure(ctx context.Context, q Querier, id, token uuid.UUID) (ResumeFailureState, error) {
	const op = "postgres: travar transação reivindicada para registrar falha"
	if err := requireTx(op, q); err != nil {
		return ResumeFailureState{}, err
	}
	if token == uuid.Nil {
		return ResumeFailureState{}, fmt.Errorf("%s: %w: token nulo", op, ErrInvalidClaim)
	}
	var (
		st     ResumeFailureState
		status string
	)
	err := q.QueryRow(ctx, `SELECT status, created_at, resume_failures FROM wager_transactions
WHERE id = $1 AND claimed_by = $2
FOR NO KEY UPDATE`, id, token).Scan(&status, &st.CreatedAt, &st.Failures)
	if err != nil {
		return ResumeFailureState{}, mapReadError(op, err)
	}
	st.Status = domain.WagerStatus(status)
	return st, nil
}

// ResumeFailureParams são os parâmetros de RecordResumeFailure.
type ResumeFailureParams struct {
	ID    uuid.UUID
	Token uuid.UUID
	// Failures é o novo valor de resume_failures (>= 0).
	Failures int32
	// HoldUntil é o novo fim do lease: o backoff até a próxima retomada.
	HoldUntil time.Time
}

// RecordResumeFailure grava resume_failures = p.Failures e claimed_until =
// p.HoldUntil numa linha não terminal ainda reivindicada por p.Token. Não
// toca em status, attempts, next_attempt_at nem updated_at (colunas do
// agregado): o backoff da falha é o próprio lease, que mantém o fencing e
// impede ClaimDue de devolver a linha antes do prazo. Sem linha afetada
// (lease perdido ou terminal): ErrNotFound. Contrato: chamar depois de
// LockClaimedForFailure na mesma transação.
func (WagerTransactionRepository) RecordResumeFailure(ctx context.Context, q Querier, p ResumeFailureParams) error {
	const op = "postgres: registrar falha de retomada"
	if err := requireTx(op, q); err != nil {
		return err
	}
	switch {
	case p.Token == uuid.Nil:
		return fmt.Errorf("%s: %w: token nulo", op, ErrInvalidClaim)
	case p.Failures < 0:
		return fmt.Errorf("%s: %w: falhas negativas %d", op, ErrInvalidClaim, p.Failures)
	case p.HoldUntil.IsZero():
		return fmt.Errorf("%s: %w: fim do lease zero", op, ErrInvalidClaim)
	}
	tag, err := q.Exec(ctx, `UPDATE wager_transactions
SET resume_failures = $3, claimed_until = $4
WHERE id = $1 AND claimed_by = $2 AND status IN ('PENDING', 'PENDING_REFERENCE')`,
		p.ID, p.Token, p.Failures, p.HoldUntil)
	if err != nil {
		return mapWriteError(op, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%s: %w: transação %s não está reivindicada por este token", op, ErrNotFound, p.ID)
	}
	return nil
}

// FailClaimedParams são os parâmetros de FailClaimedUnreadable.
type FailClaimedParams struct {
	ID    uuid.UUID
	Token uuid.UUID
	// From é o estado lido por LockClaimedForFailure (condição do UPDATE).
	From domain.WagerStatus
	// Code é o código INFRASTRUCTURE gravado.
	Code domain.FailureCode
	Now  time.Time
}

// FailClaimedUnreadable grava FAILED numa linha reivindicada por p.Token que
// NÃO pode ser reidratada (domain.ErrCorruptedWagerTransaction), sem passar
// pelo agregado. A transição é validada pelo domínio
// (domain.ValidateFailTransition) aqui e no chamador; o UPDATE só muda as
// colunas de resultado com a forma exigida pelos CHECKs de FAILED
// (failure_code, balance_after NULL, next_attempt_at NULL, completed_at =
// updated_at = max(updated_at, Now)), solta o lease e mantém resume_failures
// para auditoria. Linhas que reidratam usam GetClaimedForUpdate + Fail +
// Update; este caminho também é o fallback quando essa gravação falha de forma
// não transitória (ADR-031). Sem linha afetada (lease perdido, estado mudou):
// ErrNotFound.
func (WagerTransactionRepository) FailClaimedUnreadable(ctx context.Context, q Querier, p FailClaimedParams) error {
	const op = "postgres: gravar FAILED em transação ilegível"
	if err := requireTx(op, q); err != nil {
		return err
	}
	if p.Token == uuid.Nil || p.Now.IsZero() {
		return fmt.Errorf("%s: %w: token nulo ou instante zero", op, ErrInvalidClaim)
	}
	if err := domain.ValidateFailTransition(p.From, p.Code); err != nil {
		return fmt.Errorf("%s: %w: %w", op, ErrInvalidAggregate, err)
	}
	tag, err := q.Exec(ctx, `UPDATE wager_transactions
SET status = 'FAILED', failure_code = $4, balance_after = NULL, next_attempt_at = NULL,
    updated_at = GREATEST(updated_at, $5), completed_at = GREATEST(updated_at, $5),
    claimed_by = NULL, claimed_until = NULL
WHERE id = $1 AND claimed_by = $2 AND status = $3`,
		p.ID, p.Token, p.From.String(), p.Code.String(), p.Now.UTC())
	if err != nil {
		return mapWriteError(op, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%s: %w: transação %s não está reivindicada por este token em %s", op, ErrNotFound, p.ID, p.From)
	}
	return nil
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
		return nil, corruptedWager(id, err)
	}
	var balance domain.Money
	if balanceAfter.Valid {
		if balance, err = moneyFromColumns("balance_after", balanceAfter.Int64, currency); err != nil {
			return nil, corruptedWager(id, err)
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
		return nil, corruptedWager(id, err)
	}
	return tx, nil
}

// corruptedWager marca QUALQUER falha de reidratação de uma linha lida
// (inclusive a do Money: moeda que passa no CHECK do banco mas o domínio não
// suporta) com domain.ErrCorruptedWagerTransaction. Assim quem lê distingue
// "a linha persistida é inválida" de erro de entrada (ValidationFailureCode
// não a confunde com INVALID_CURRENCY) e o worker encerra o item envenenado
// pelo caminho sem agregado (T30, ADR-031) em vez de repetir para sempre.
func corruptedWager(id uuid.UUID, err error) error {
	if errors.Is(err, domain.ErrCorruptedWagerTransaction) {
		return fmt.Errorf("reidratar transação %s: %w", id, err)
	}
	return fmt.Errorf("reidratar transação %s: %w: %w", id, domain.ErrCorruptedWagerTransaction, err)
}
