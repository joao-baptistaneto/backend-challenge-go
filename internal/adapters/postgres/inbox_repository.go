package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// InboxRepository persiste a inbox do consumidor SQS (migration 0004,
// ARCHITECTURE.md ADR-011 e ADR-015).
//
// Decisões:
//
//   - Register e Complete exigem pgx.Tx (ErrTxRequired): a inbox só protege
//     contra reprocessamento se o registro, a conclusão e as alterações de
//     domínio entrarem no MESMO commit (ADR-017/ADR-022). Fora de transação, um crash
//     entre o registro e o efeito deixaria a mensagem "vista" sem efeito.
//   - Register é INSERT ... ON CONFLICT DO NOTHING. Se outra transação
//     inseriu a mesma (consumer, messageId) e ainda não confirmou, o INSERT
//     espera por ela (índice único); quando ela confirma, a linha existente é
//     lida com FOR UPDATE, o que serializa duas entregas concorrentes da
//     mesma mensagem até o commit de quem a registrou.
//   - Comparação de hash: a mesma (consumer, messageId) com request_hash
//     diferente é ErrInboxHashMismatch (mensagem reentregue com conteúdo
//     alterado: nunca reaproveitar o resultado anterior). Hash igual devolve
//     o estado existente (InboxCompleted → o consumidor apenas confirma/remove
//     a mensagem; InboxInProgress → registrada e não concluída).
//   - completed_at = GREATEST(now, received_at): o CHECK
//     inbox_completed_after_received não pode falhar por relógio de outra
//     instância um pouco atrás.
type InboxRepository struct{}

// NewInboxRepository cria o repositório (stateless).
func NewInboxRepository() *InboxRepository { return &InboxRepository{} }

// Sentinelas da inbox.
var (
	// ErrInvalidInboxMessage indica identificadores ou hash inválidos
	// (validados antes do SQL, espelhando os CHECKs da 0004).
	ErrInvalidInboxMessage = errors.New("postgres: mensagem de inbox inválida")
	// ErrInboxHashMismatch indica mesma (consumer, messageId) com conteúdo
	// diferente do registrado.
	ErrInboxHashMismatch = errors.New("postgres: mensagem já registrada com outro conteúdo (hash divergente)")
	// ErrInboxAlreadyCompleted indica Complete numa mensagem já concluída.
	ErrInboxAlreadyCompleted = errors.New("postgres: mensagem da inbox já concluída")
)

// InboxStatus é o resultado de Register.
type InboxStatus int

const (
	// InboxNew indica que a mensagem foi registrada agora (processar).
	InboxNew InboxStatus = iota + 1
	// InboxInProgress indica registro anterior, com o mesmo hash, ainda sem
	// conclusão confirmada.
	InboxInProgress
	// InboxCompleted indica mensagem já processada (mesmo hash): não
	// reprocessar; o resultado está em TransactionID.
	InboxCompleted
)

// String devolve o nome do status.
func (s InboxStatus) String() string {
	switch s {
	case InboxNew:
		return "NEW"
	case InboxInProgress:
		return "IN_PROGRESS"
	case InboxCompleted:
		return "COMPLETED"
	}
	return fmt.Sprintf("InboxStatus(%d)", int(s))
}

// InboxMessage é uma linha da inbox.
type InboxMessage struct {
	ConsumerName  string
	MessageID     string
	RequestHash   string
	TransactionID uuid.UUID // uuid.Nil quando a mensagem não gerou transação
	ReceivedAt    time.Time
	CompletedAt   time.Time // zero enquanto não concluída
}

// Completed informa se a mensagem já foi concluída.
func (m InboxMessage) Completed() bool { return !m.CompletedAt.IsZero() }

// InboxRegistration é o resultado de Register.
type InboxRegistration struct {
	Status  InboxStatus
	Message InboxMessage
}

const inboxColumns = `consumer_name, message_id, request_hash, transaction_id, received_at, completed_at`

// maxInboxIdentifierLength espelha inbox_*_length (octet_length 1..255).
const maxInboxIdentifierLength = 255

func validateInboxKey(consumer, messageID string) error {
	if l := len(consumer); l < 1 || l > maxInboxIdentifierLength {
		return fmt.Errorf("%w: consumerName com %d bytes (1..%d)", ErrInvalidInboxMessage, l, maxInboxIdentifierLength)
	}
	if l := len(messageID); l < 1 || l > maxInboxIdentifierLength {
		return fmt.Errorf("%w: messageId com %d bytes (1..%d)", ErrInvalidInboxMessage, l, maxInboxIdentifierLength)
	}
	return nil
}

// validRequestHash espelha inbox_request_hash_format (^[0-9a-f]{64}$).
func validRequestHash(h string) bool {
	if len(h) != 64 {
		return false
	}
	for i := range len(h) {
		c := h[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func validateInboxMessage(consumer, messageID, hash string, now time.Time) error {
	if err := validateInboxKey(consumer, messageID); err != nil {
		return err
	}
	if !validRequestHash(hash) {
		return fmt.Errorf("%w: requestHash deve ser SHA-256 em hex minúsculo", ErrInvalidInboxMessage)
	}
	if now.IsZero() {
		return fmt.Errorf("%w: instante zero", ErrInvalidInboxMessage)
	}
	return nil
}

// Register registra a recepção da mensagem (consumer, messageID) com o hash
// canônico do conteúdo (ADR-010). Exige pgx.Tx: o registro deve ser confirmado
// junto com o efeito de domínio.
//
//   - Nova: grava e devolve InboxNew.
//   - Existente com o mesmo hash: devolve InboxCompleted ou InboxInProgress e
//     a linha (travada com FOR UPDATE até o fim da transação).
//   - Existente com outro hash: ErrInboxHashMismatch, junto com a linha
//     registrada (Message), que não é alterada.
func (InboxRepository) Register(ctx context.Context, q Querier, consumer, messageID, requestHash string, now time.Time) (InboxRegistration, error) {
	const op = "postgres: registrar mensagem na inbox"
	if err := requireTx(op, q); err != nil {
		return InboxRegistration{}, err
	}
	if err := validateInboxMessage(consumer, messageID, requestHash, now); err != nil {
		return InboxRegistration{}, fmt.Errorf("%s: %w", op, err)
	}
	inserted, err := scanInbox(q.QueryRow(ctx, `INSERT INTO inbox (consumer_name, message_id, request_hash, received_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (consumer_name, message_id) DO NOTHING
RETURNING `+inboxColumns, consumer, messageID, requestHash, now))
	if err == nil {
		return InboxRegistration{Status: InboxNew, Message: inserted}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return InboxRegistration{}, mapWriteError(op, err)
	}
	existing, err := scanInbox(q.QueryRow(ctx, `SELECT `+inboxColumns+` FROM inbox
WHERE consumer_name = $1 AND message_id = $2
FOR UPDATE`, consumer, messageID))
	if err != nil {
		// A linha conflitante sempre existe após o ON CONFLICT (não há DELETE
		// concorrente no fluxo normal); ErrNotFound aqui indica limpeza
		// concorrente, e o chamador pode repetir.
		return InboxRegistration{}, mapReadError(op, err)
	}
	if existing.RequestHash != requestHash {
		return InboxRegistration{Message: existing}, fmt.Errorf("%s: %w", op, ErrInboxHashMismatch)
	}
	status := InboxInProgress
	if existing.Completed() {
		status = InboxCompleted
	}
	return InboxRegistration{Status: status, Message: existing}, nil
}

// Complete marca a mensagem como concluída, associando a transação de
// domínio resultante (uuid.Nil quando não houve transação). Exige pgx.Tx e o
// mesmo hash do registro. Erros: ErrNotFound (não registrada),
// ErrInboxHashMismatch, ErrInboxAlreadyCompleted, ErrForeignKeyViolation
// (transação inexistente).
func (InboxRepository) Complete(ctx context.Context, q Querier, consumer, messageID, requestHash string, transactionID uuid.UUID, now time.Time) error {
	const op = "postgres: concluir mensagem da inbox"
	if err := requireTx(op, q); err != nil {
		return err
	}
	if err := validateInboxMessage(consumer, messageID, requestHash, now); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	tag, err := q.Exec(ctx, `UPDATE inbox
SET completed_at = GREATEST($5::timestamptz, received_at), transaction_id = $4
WHERE consumer_name = $1 AND message_id = $2 AND request_hash = $3 AND completed_at IS NULL`,
		consumer, messageID, requestHash, nullUUID(transactionID), now)
	if err != nil {
		return mapWriteError(op, err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	// Diagnóstico: por que nada foi atualizado?
	existing, err := scanInbox(q.QueryRow(ctx, `SELECT `+inboxColumns+` FROM inbox
WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID))
	if err != nil {
		return mapReadError(op, err)
	}
	if existing.RequestHash != requestHash {
		return fmt.Errorf("%s: %w", op, ErrInboxHashMismatch)
	}
	return fmt.Errorf("%s: %w", op, ErrInboxAlreadyCompleted)
}

// Get lê a mensagem (qualquer Querier). ErrNotFound se não registrada.
func (InboxRepository) Get(ctx context.Context, q Querier, consumer, messageID string) (InboxMessage, error) {
	const op = "postgres: ler mensagem da inbox"
	if err := validateInboxKey(consumer, messageID); err != nil {
		return InboxMessage{}, fmt.Errorf("%s: %w", op, err)
	}
	m, err := scanInbox(q.QueryRow(ctx, `SELECT `+inboxColumns+` FROM inbox
WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID))
	if err != nil {
		return InboxMessage{}, mapReadError(op, err)
	}
	return m, nil
}

func scanInbox(row pgx.Row) (InboxMessage, error) {
	var (
		m         InboxMessage
		txID      pgtype.UUID
		received  time.Time
		completed pgtype.Timestamptz
	)
	if err := row.Scan(&m.ConsumerName, &m.MessageID, &m.RequestHash, &txID, &received, &completed); err != nil {
		return InboxMessage{}, err
	}
	m.TransactionID = uuidOrNil(txID)
	m.ReceivedAt = received.UTC()
	m.CompletedAt = timeOrZero(completed)
	return m, nil
}
