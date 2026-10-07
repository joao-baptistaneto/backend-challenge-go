package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// OutboxRepository persiste a transactional outbox (migration 0005,
// ARCHITECTURE.md ADR-011 e ADR-015).
//
// Decisões:
//
//   - Enqueue grava os eventos com o Querier do caso de uso (o pgx.Tx de
//     WithinTx), no mesmo commit das alterações que os originaram. Grava
//     exatamente IntegrationEvent.Payload() (coluna JSON, byte a byte).
//   - Claim reivindica um lote com LEASE persistido, e não com lock de linha
//     até o commit: o publisher (T22) faz o claim numa transação curta (ou em
//     autocommit), publica no SQS FORA de transação e depois chama
//     MarkPublished/MarkFailed. Por isso Claim aceita qualquer Querier. O
//     lote é escolhido com FOR UPDATE SKIP LOCKED (claimers concorrentes não
//     esperam nem duplicam) entre os eventos não publicados, vencidos
//     (next_attempt_at) e sem lease vigente (locked_until NULL ou expirado);
//     lease expirado é reassumido.
//   - Ordem: ORDER BY seq (IDENTITY, ordem de inserção), nunca occurred_at,
//     que vem do relógio de cada instância. O resultado volta ordenado por seq.
//   - Relógio único: leases e agendamento usam statement_timestamp() do
//     PostgreSQL, não o relógio da instância. Instâncias com relógios
//     divergentes não roubam nem estendem leases umas das outras.
//   - Fencing: cada Claim incrementa attempts; o lease é identificado por
//     (seq, locked_by, attempts) e só vale enquanto locked_until > agora.
//     MarkPublished/MarkFailed só têm efeito para esse lease vigente. Um
//     claimer cujo lease expirou (mesmo que ninguém tenha reassumido ainda) ou
//     que foi substituído — inclusive por outro worker com o mesmo owner —
//     recebe ErrOutboxLeaseLost e não altera nada. Como attempts conta claims,
//     um crash entre claim e mark também conta tentativa e entra no backoff.
//   - MarkFailed agenda next_attempt_at = agora + OutboxBackoff.Delay(attempts)
//     (exponencial, com teto obrigatório e sem overflow) e solta o lease.
//   - Ordem por agregado (T22, ADR-024): o MessageGroupId é o aggregateId e o
//     claim só devolve o evento não publicado MAIS ANTIGO (menor seq) de cada
//     agregado (head-of-line). Enquanto ele estiver em lease, em backoff ou
//     ainda não marcado como publicado, os eventos seguintes do mesmo agregado
//     não são elegíveis; os outros agregados seguem normalmente. A condição só
//     depende de published_at, que só muda pelo MarkPublished do detentor do
//     lease. Por isso é segura entre claimers concorrentes sem serializar os
//     claims: um snapshot antigo só pode ser mais restritivo. Consequência: um
//     lote tem no máximo um evento por agregado e pode ser publicado em
//     paralelo sem quebrar a ordem.
type OutboxRepository struct{}

// NewOutboxRepository cria o repositório (stateless).
func NewOutboxRepository() *OutboxRepository { return &OutboxRepository{} }

// Sentinelas da outbox.
var (
	// ErrInvalidOutboxEvent indica evento não inicializado.
	ErrInvalidOutboxEvent = errors.New("postgres: evento inválido para a outbox")
	// ErrDuplicateEvent indica eventId já enfileirado.
	ErrDuplicateEvent = errors.New("postgres: evento já enfileirado na outbox")
	// ErrInvalidLease indica parâmetros de claim/lease inválidos (owner,
	// duração, lote).
	ErrInvalidLease = errors.New("postgres: lease da outbox inválido")
	// ErrOutboxLeaseLost indica que o chamador não detém mais o lease vigente
	// (expirou, foi reassumido ou o evento já foi publicado): nada foi
	// alterado.
	ErrOutboxLeaseLost = errors.New("postgres: lease da outbox perdido ou expirado")
	// ErrInvalidBackoff indica política de backoff inválida.
	ErrInvalidBackoff = errors.New("postgres: política de backoff inválida")
)

// Limites da outbox.
const (
	// MaxOutboxClaimBatch é o maior lote aceito por Claim.
	MaxOutboxClaimBatch = 1000
	// MaxOutboxLease é a maior duração de lease aceita.
	MaxOutboxLease = time.Hour
	// maxLeaseOwnerLength limita locked_by.
	maxLeaseOwnerLength = 255
	// maxOutboxErrorLength espelha outbox_last_error_length (bytes).
	maxOutboxErrorLength = 2048
)

// OutboxBackoff é a política de backoff exponencial de MarkFailed:
// Delay(n) = min(Base·2^(n-1), Max). Determinística (sem jitter): o atraso
// é por evento, não há manada a dispersar.
type OutboxBackoff struct {
	Base time.Duration
	Max  time.Duration
}

// DefaultOutboxBackoff: 1s, 2s, 4s, ... até 5min.
func DefaultOutboxBackoff() OutboxBackoff {
	return OutboxBackoff{Base: time.Second, Max: 5 * time.Minute}
}

// Validate exige 0 < Base <= Max. O teto é obrigatório: sem ele o atraso
// cresceria até anos.
func (b OutboxBackoff) Validate() error {
	if b.Base <= 0 || b.Max <= 0 || b.Base > b.Max {
		return fmt.Errorf("%w: base %v, max %v (exige 0 < base <= max)", ErrInvalidBackoff, b.Base, b.Max)
	}
	return nil
}

// Delay devolve o atraso após a tentativa attempt (1 = primeira falha).
// Nunca excede Max e nunca estoura, para qualquer attempt.
func (b OutboxBackoff) Delay(attempt int) time.Duration {
	return exponentialDelay(b.Base, b.Max, attempt)
}

// OutboxLease identifica um claim: só o detentor do lease vigente consegue
// marcar o evento.
type OutboxLease struct {
	Seq     int64
	EventID uuid.UUID
	Owner   string
	// Attempt é o valor de attempts gravado no claim (token de fencing).
	Attempt int
	// Until é o fim do lease (relógio do PostgreSQL).
	Until time.Time
}

// OutboxMessage é um evento reivindicado, pronto para publicação.
type OutboxMessage struct {
	Seq           int64
	EventID       uuid.UUID
	EventType     string
	EventVersion  int
	AggregateID   string
	CorrelationID string
	// Payload é o snapshot JSON gravado no Enqueue, byte a byte.
	Payload    []byte
	OccurredAt time.Time
	CreatedAt  time.Time
	// LastError é o erro da tentativa anterior ("" se nenhuma falhou).
	LastError string
	Lease     OutboxLease
}

// Enqueue grava os eventos na ordem recebida (seq crescente). Deve receber
// o pgx.Tx do caso de uso para entrar no mesmo commit das alterações.
func (OutboxRepository) Enqueue(ctx context.Context, q Querier, events ...domain.IntegrationEvent) error {
	const op = "postgres: enfileirar evento na outbox"
	for i, e := range events {
		if !e.IsInitialized() {
			return fmt.Errorf("%s: %w: evento %d não inicializado", op, ErrInvalidOutboxEvent, i)
		}
	}
	for _, e := range events {
		_, err := q.Exec(ctx, `INSERT INTO outbox
(event_id, event_type, event_version, aggregate_id, correlation_id, payload, occurred_at)
VALUES ($1, $2, $3, $4, $5, $6::json, $7)`,
			e.ID(), e.Type().String(), e.Version(), e.AggregateID().String(),
			nullText(e.CorrelationID()), string(e.Payload()), e.OccurredAt())
		if err != nil {
			return mapWriteError(op, err)
		}
	}
	return nil
}

// outboxReturning são as colunas devolvidas pelo claim (alias o: a CTE due
// também tem seq).
const outboxReturning = `o.seq, o.event_id, o.event_type, o.event_version, o.aggregate_id,
o.correlation_id, o.payload::text, o.occurred_at, o.created_at, o.attempts, o.locked_by,
o.locked_until, o.last_error`

// Claim reivindica até limit eventos para owner, com lease de duração lease,
// em ordem de seq, no máximo um por agregado (o mais antigo não publicado;
// ver decisões). Cada evento reivindicado tem attempts incrementado. Aceita
// qualquer Querier; o publisher usa o pool (autocommit), para que o lease
// conte do statement_timestamp() do próprio claim e valha já no retorno.
func (OutboxRepository) Claim(ctx context.Context, q Querier, owner string, lease time.Duration, limit int) ([]OutboxMessage, error) {
	const op = "postgres: reivindicar eventos da outbox"
	if l := len(owner); l < 1 || l > maxLeaseOwnerLength {
		return nil, fmt.Errorf("%s: %w: owner com %d bytes (1..%d)", op, ErrInvalidLease, l, maxLeaseOwnerLength)
	}
	if lease < time.Millisecond || lease > MaxOutboxLease {
		return nil, fmt.Errorf("%s: %w: duração %v (1ms..%v)", op, ErrInvalidLease, lease, MaxOutboxLease)
	}
	if limit < 1 || limit > MaxOutboxClaimBatch {
		return nil, fmt.Errorf("%s: %w: lote %d (1..%d)", op, ErrInvalidLease, limit, MaxOutboxClaimBatch)
	}
	rows, err := q.Query(ctx, `WITH due AS (
    SELECT c.seq FROM outbox c
    WHERE c.published_at IS NULL
      AND c.next_attempt_at <= statement_timestamp()
      AND (c.locked_until IS NULL OR c.locked_until <= statement_timestamp())
      -- Head-of-line por agregado (MessageGroupId): só o evento não
      -- publicado mais antigo de cada agregado é elegível.
      AND NOT EXISTS (
          SELECT 1 FROM outbox prev
          WHERE prev.aggregate_id = c.aggregate_id
            AND prev.seq < c.seq
            AND prev.published_at IS NULL)
    ORDER BY c.seq
    LIMIT $3
    FOR UPDATE OF c SKIP LOCKED
)
UPDATE outbox o
SET locked_by = $1,
    locked_until = statement_timestamp() + $2 * interval '1 microsecond',
    attempts = o.attempts + 1
FROM due
WHERE o.seq = due.seq
RETURNING `+outboxReturning, owner, lease.Microseconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, Translate(err))
	}
	defer rows.Close()
	var out []OutboxMessage
	for rows.Next() {
		m, err := scanOutbox(rows)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, Translate(err))
	}
	// UPDATE ... RETURNING não garante ordem.
	slices.SortFunc(out, func(a, b OutboxMessage) int {
		switch {
		case a.Seq < b.Seq:
			return -1
		case a.Seq > b.Seq:
			return 1
		}
		return 0
	})
	return out, nil
}

// leaseCondition restringe a escrita ao detentor do lease vigente.
const leaseCondition = `seq = $1 AND event_id = $2 AND locked_by = $3 AND attempts = $4
  AND locked_until > statement_timestamp() AND published_at IS NULL`

// MarkPublished marca o evento como publicado e solta o lease. Só tem efeito
// para o lease vigente; caso contrário devolve ErrOutboxLeaseLost.
func (OutboxRepository) MarkPublished(ctx context.Context, q Querier, lease OutboxLease) error {
	const op = "postgres: marcar evento como publicado"
	if err := validateLease(lease); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	tag, err := q.Exec(ctx, `UPDATE outbox
SET published_at = statement_timestamp(), locked_by = NULL, locked_until = NULL
WHERE `+leaseCondition, lease.Seq, lease.EventID, lease.Owner, lease.Attempt)
	if err != nil {
		return mapWriteError(op, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%s: evento %s: %w", op, lease.EventID, ErrOutboxLeaseLost)
	}
	return nil
}

// MarkFailed registra a falha de publicação (cause, truncada a 2048 bytes),
// reagenda o evento para agora + backoff.Delay(lease.Attempt) e solta o
// lease. Só tem efeito para o lease vigente (ErrOutboxLeaseLost).
func (OutboxRepository) MarkFailed(ctx context.Context, q Querier, lease OutboxLease, cause string, backoff OutboxBackoff) error {
	const op = "postgres: marcar falha de publicação"
	if err := validateLease(lease); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if err := backoff.Validate(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	delay := backoff.Delay(lease.Attempt)
	tag, err := q.Exec(ctx, `UPDATE outbox
SET locked_by = NULL, locked_until = NULL, last_error = $5,
    next_attempt_at = statement_timestamp() + $6 * interval '1 microsecond'
WHERE `+leaseCondition, lease.Seq, lease.EventID, lease.Owner, lease.Attempt,
		nullText(truncateUTF8(cause, maxOutboxErrorLength)), delay.Microseconds())
	if err != nil {
		return mapWriteError(op, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%s: evento %s: %w", op, lease.EventID, ErrOutboxLeaseLost)
	}
	return nil
}

// Release solta o lease sem registrar falha nem reagendar: o evento volta a
// ser elegível imediatamente (next_attempt_at intocado). O publisher usa para
// devolver itens reivindicados que não chegou a publicar (shutdown, lease
// perto do fim). attempts não diminui, porque o trigger exige
// monotonicidade. Só tem efeito para o lease vigente (ErrOutboxLeaseLost).
func (OutboxRepository) Release(ctx context.Context, q Querier, lease OutboxLease) error {
	const op = "postgres: soltar lease da outbox"
	if err := validateLease(lease); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	tag, err := q.Exec(ctx, `UPDATE outbox SET locked_by = NULL, locked_until = NULL
WHERE `+leaseCondition, lease.Seq, lease.EventID, lease.Owner, lease.Attempt)
	if err != nil {
		return mapWriteError(op, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%s: evento %s: %w", op, lease.EventID, ErrOutboxLeaseLost)
	}
	return nil
}

// OutboxBacklog resume os eventos ainda não publicados.
type OutboxBacklog struct {
	// Pending é o número de eventos não publicados.
	Pending int64
	// OldestOccurredAt é o occurred_at mais antigo entre eles (zero se
	// Pending = 0).
	OldestOccurredAt time.Time
	// Now é o relógio do PostgreSQL no momento da consulta.
	Now time.Time
}

// OldestAge devolve Now − OldestOccurredAt. Devolve 0 sem pendências ou
// quando o relógio da instância que gravou o evento estava adiantado.
func (b OutboxBacklog) OldestAge() time.Duration {
	if b.Pending <= 0 || b.OldestOccurredAt.IsZero() {
		return 0
	}
	return max(b.Now.Sub(b.OldestOccurredAt), 0)
}

// Backlog conta os eventos não publicados e devolve o occurred_at mais
// antigo, para as métricas de pendentes e de atraso da outbox.
func (OutboxRepository) Backlog(ctx context.Context, q Querier) (OutboxBacklog, error) {
	const op = "postgres: medir pendências da outbox"
	var (
		b      OutboxBacklog
		oldest pgtype.Timestamptz
	)
	err := q.QueryRow(ctx, `SELECT count(*), min(occurred_at), statement_timestamp()
FROM outbox WHERE published_at IS NULL`).Scan(&b.Pending, &oldest, &b.Now)
	if err != nil {
		return OutboxBacklog{}, fmt.Errorf("%s: %w", op, Translate(err))
	}
	b.OldestOccurredAt = timeOrZero(oldest)
	b.Now = b.Now.UTC()
	return b, nil
}

func validateLease(l OutboxLease) error {
	if l.Seq < 1 || l.EventID == uuid.Nil || l.Owner == "" || l.Attempt < 1 {
		return fmt.Errorf("%w: lease não inicializado", ErrInvalidLease)
	}
	return nil
}

// truncateUTF8 corta s em no máximo n bytes sem partir um caractere UTF-8.
// Antes, bytes inválidos viram U+FFFD e NUL é removido (o TEXT do PostgreSQL
// rejeita ambos).
func truncateUTF8(s string, n int) string {
	s = strings.ReplaceAll(strings.ToValidUTF8(s, "�"), "\x00", "")
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

type outboxScanner interface {
	Scan(dest ...any) error
}

func scanOutbox(row outboxScanner) (OutboxMessage, error) {
	var (
		m           OutboxMessage
		correlation pgtype.Text
		payload     string
		lockedBy    pgtype.Text
		lockedUntil pgtype.Timestamptz
		lastError   pgtype.Text
		attempts    int32
		version     int32
	)
	if err := row.Scan(&m.Seq, &m.EventID, &m.EventType, &version, &m.AggregateID, &correlation,
		&payload, &m.OccurredAt, &m.CreatedAt, &attempts, &lockedBy, &lockedUntil, &lastError); err != nil {
		return OutboxMessage{}, err
	}
	m.EventVersion = int(version)
	m.CorrelationID = textOrEmpty(correlation)
	m.Payload = []byte(payload)
	m.OccurredAt = m.OccurredAt.UTC()
	m.CreatedAt = m.CreatedAt.UTC()
	m.LastError = textOrEmpty(lastError)
	m.Lease = OutboxLease{
		Seq: m.Seq, EventID: m.EventID, Owner: textOrEmpty(lockedBy),
		Attempt: int(attempts), Until: timeOrZero(lockedUntil),
	}
	return m, nil
}
