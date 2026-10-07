package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// Classificação de erros do PostgreSQL/pgx (ARCHITECTURE.md, ADR-013).
//
// Decisões:
//
//   - O adapter NÃO importa internal/domain. Ele classifica em dois eixos
//     neutros: (1) transitório x permanente e (2) violação de constraint com
//     tipo (UNIQUE/CHECK/FK/RESTRICT/...) e NOME da constraint. Os
//     repositórios (T12/T13) fazem o mapeamento final para erros de domínio
//     com base no nome (ex.: wallets_player_currency_key → conflito de
//     carteira). O nome é estável porque todas as constraints e todas as
//     guardas de trigger (RAISE ... USING ERRCODE = 'restrict_violation',
//     CONSTRAINT = '...') têm nome explícito nas migrations (ADR-011).
//   - As funções Is* e AsConstraintViolation inspecionam a cadeia de erros
//     (errors.As em *pgconn.PgError), então funcionam tanto no erro cru do pgx
//     quanto em erros já embrulhados com %w (inclusive por Translate ou por um
//     erro de domínio que preserve a causa).
//   - "Transitório" (IsTransient) significa: a mesma requisição pode dar certo
//     se repetida mais tarde. "Retentável pelo TxManager" (IsRetryable) é mais
//     estrito: além de transitório, é garantido que a transação NÃO foi
//     confirmada, então reexecutar a função inteira é seguro mesmo sem
//     idempotência. A diferença é o commit ambíguo (ErrCommitOutcomeUnknown):
//     ele é transitório para quem chama (que pode repetir via chave de
//     idempotência), mas nunca é reexecutado automaticamente aqui.
//   - context.Canceled nunca é transitório: indica que quem chamou desistiu
//     (shutdown, cliente desconectou). context.DeadlineExceeded é tratado como
//     timeout (transitório), mas o loop de retry também para quando o ctx da
//     chamada está encerrado, então um prazo esgotado não gera novas tentativas.

// SQLSTATEs relevantes (https://www.postgresql.org/docs/current/errcodes-appendix.html).
const (
	CodeSerializationFailure  = "40001"
	CodeDeadlockDetected      = "40P01"
	CodeLockNotAvailable      = "55P03"
	CodeQueryCanceled         = "57014"
	CodeAdminShutdown         = "57P01"
	CodeCrashShutdown         = "57P02"
	CodeCannotConnectNow      = "57P03"
	CodeTooManyConnections    = "53300"
	CodeInsufficientResources = "53000"
	CodeDiskFull              = "53100"
	CodeOutOfMemory           = "53200"
	CodeReadOnlyTransaction   = "25006"
	CodeUniqueViolation       = "23505"
	CodeCheckViolation        = "23514"
	CodeForeignKeyViolation   = "23503"
	CodeRestrictViolation     = "23001"
	CodeNotNullViolation      = "23502"
	CodeExclusionViolation    = "23P01"
	classConnectionException  = "08"
	classIntegrityConstraints = "23"
)

// Sentinelas. *ConstraintViolation satisfaz errors.Is com
// ErrConstraintViolation e com a sentinela do seu tipo; *TransientError
// satisfaz errors.Is com ErrTransient. Ambos só aparecem na cadeia depois de
// Translate (chamado pelos repositórios ao mapear erros de escrita/leitura).
// WithinTx NÃO traduz: devolve o erro de fn intacto e embrulha os de
// BEGIN/COMMIT apenas com %w (e ErrCommitOutcomeUnknown no commit ambíguo).
// Os predicados (IsTransient, IsRetryable, Is*Violation) funcionam no erro
// cru, então quem precisa da forma tipada chama Translate no resultado.
var (
	ErrConstraintViolation = errors.New("postgres: violação de constraint")
	ErrUniqueViolation     = errors.New("postgres: violação de UNIQUE")
	ErrCheckViolation      = errors.New("postgres: violação de CHECK")
	ErrForeignKeyViolation = errors.New("postgres: violação de FOREIGN KEY")
	ErrRestrictViolation   = errors.New("postgres: violação de guarda (restrict_violation)")
	ErrNotNullViolation    = errors.New("postgres: violação de NOT NULL")
	ErrExclusionViolation  = errors.New("postgres: violação de EXCLUDE")

	// ErrTransient marca falhas que podem ter sucesso se repetidas.
	ErrTransient = errors.New("postgres: falha transitória")

	// ErrCommitOutcomeUnknown indica que o COMMIT foi enviado mas a resposta
	// não chegou (conexão caiu, contexto cancelado durante o commit): a
	// transação pode ou não ter sido confirmada. O TxManager nunca reexecuta
	// a função nesse caso; quem chama deve reconsultar o estado pela chave de
	// idempotência (replay) antes de repetir a operação.
	ErrCommitOutcomeUnknown = errors.New("postgres: resultado do commit desconhecido")
)

// ConstraintKind é o tipo de constraint violada.
type ConstraintKind string

// Tipos de constraint.
const (
	ConstraintUnique     ConstraintKind = "unique"
	ConstraintCheck      ConstraintKind = "check"
	ConstraintForeignKey ConstraintKind = "foreign_key"
	ConstraintRestrict   ConstraintKind = "restrict"
	ConstraintNotNull    ConstraintKind = "not_null"
	ConstraintExclusion  ConstraintKind = "exclusion"
)

// constraintKinds mapeia SQLSTATE → tipo de constraint.
var constraintKinds = map[string]ConstraintKind{
	CodeUniqueViolation:     ConstraintUnique,
	CodeCheckViolation:      ConstraintCheck,
	CodeForeignKeyViolation: ConstraintForeignKey,
	CodeRestrictViolation:   ConstraintRestrict,
	CodeNotNullViolation:    ConstraintNotNull,
	CodeExclusionViolation:  ConstraintExclusion,
}

func (k ConstraintKind) sentinel() error {
	switch k {
	case ConstraintUnique:
		return ErrUniqueViolation
	case ConstraintCheck:
		return ErrCheckViolation
	case ConstraintForeignKey:
		return ErrForeignKeyViolation
	case ConstraintRestrict:
		return ErrRestrictViolation
	case ConstraintNotNull:
		return ErrNotNullViolation
	case ConstraintExclusion:
		return ErrExclusionViolation
	}
	return nil
}

// ConstraintViolation é a forma tipada de uma violação de integridade
// (classe SQLSTATE 23). Constraint é o nome declarado na migration (para
// NOT NULL o PostgreSQL não informa nome; use Column).
type ConstraintViolation struct {
	Kind       ConstraintKind
	Constraint string
	Table      string
	Column     string
	SQLState   string
	pgErr      *pgconn.PgError
}

// Error não inclui o Detail do PostgreSQL ("Key (...)=(...)"), que pode
// carregar valores da linha; inclui só tipo, constraint e tabela.
func (e *ConstraintViolation) Error() string {
	var b strings.Builder
	b.WriteString("postgres: violação de ")
	b.WriteString(string(e.Kind))
	if e.Constraint != "" {
		fmt.Fprintf(&b, " (constraint %s", e.Constraint)
	} else {
		b.WriteString(" (constraint sem nome")
	}
	if e.Table != "" {
		fmt.Fprintf(&b, ", tabela %s", e.Table)
	}
	if e.Column != "" {
		fmt.Fprintf(&b, ", coluna %s", e.Column)
	}
	fmt.Fprintf(&b, ", SQLSTATE %s)", e.SQLState)
	return b.String()
}

// Unwrap devolve o *pgconn.PgError original.
func (e *ConstraintViolation) Unwrap() error { return e.pgErr }

// Is casa com ErrConstraintViolation e com a sentinela do tipo.
func (e *ConstraintViolation) Is(target error) bool {
	return target == ErrConstraintViolation || target == e.Kind.sentinel()
}

// TransientReason descreve a causa de uma falha transitória. Os valores com
// equivalente em metrics.Conflict* usam o mesmo texto.
type TransientReason string

// Causas de falha transitória.
const (
	ReasonSerialization TransientReason = "serialization_failure"
	ReasonDeadlock      TransientReason = "deadlock"
	ReasonLockTimeout   TransientReason = "lock_timeout"
	ReasonQueryCanceled TransientReason = "query_canceled"
	ReasonConnection    TransientReason = "connection"
	ReasonTimeout       TransientReason = "timeout"
	ReasonCommitUnknown TransientReason = "commit_unknown"
	// ReasonResources: o servidor ficou sem disco ou memória (53000, 53100,
	// 53200). Nada foi gravado e a condição é do ambiente, não do item.
	ReasonResources TransientReason = "insufficient_resources"
	// ReasonReadOnly: escrita recusada por um servidor somente leitura
	// (25006), típico de failover em andamento (conexão ainda no antigo
	// primário rebaixado ou numa réplica).
	ReasonReadOnly TransientReason = "read_only"
)

// TransientError embrulha uma falha transitória com sua causa.
type TransientError struct {
	Reason TransientReason
	Err    error
}

func (e *TransientError) Error() string {
	return fmt.Sprintf("postgres: falha transitória (%s): %v", e.Reason, e.Err)
}

// Unwrap devolve a causa original.
func (e *TransientError) Unwrap() error { return e.Err }

// Is casa com ErrTransient.
func (e *TransientError) Is(target error) bool { return target == ErrTransient }

// pgErrorOf extrai o *pgconn.PgError da cadeia.
func pgErrorOf(err error) (*pgconn.PgError, bool) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr, true
	}
	return nil, false
}

// SQLState devolve o SQLSTATE do *pgconn.PgError na cadeia, ou "".
func SQLState(err error) string {
	if pgErr, ok := pgErrorOf(err); ok {
		return pgErr.Code
	}
	return ""
}

// AsConstraintViolation devolve a violação tipada se a cadeia contém um erro
// da classe 23 (ou já um *ConstraintViolation).
func AsConstraintViolation(err error) (*ConstraintViolation, bool) {
	var cv *ConstraintViolation
	if errors.As(err, &cv) {
		return cv, true
	}
	pgErr, ok := pgErrorOf(err)
	if !ok {
		return nil, false
	}
	kind, ok := constraintKinds[pgErr.Code]
	if !ok {
		return nil, false
	}
	return &ConstraintViolation{
		Kind:       kind,
		Constraint: pgErr.ConstraintName,
		Table:      pgErr.TableName,
		Column:     pgErr.ColumnName,
		SQLState:   pgErr.Code,
		pgErr:      pgErr,
	}, true
}

// isViolation testa tipo e, se constraint != "", o nome exato.
func isViolation(err error, kind ConstraintKind, constraint string) bool {
	cv, ok := AsConstraintViolation(err)
	if !ok || cv.Kind != kind {
		return false
	}
	return constraint == "" || cv.Constraint == constraint
}

// IsUniqueViolation informa se err é 23505 na constraint dada ("" = qualquer).
func IsUniqueViolation(err error, constraint string) bool {
	return isViolation(err, ConstraintUnique, constraint)
}

// IsCheckViolation informa se err é 23514 na constraint dada ("" = qualquer).
func IsCheckViolation(err error, constraint string) bool {
	return isViolation(err, ConstraintCheck, constraint)
}

// IsForeignKeyViolation informa se err é 23503 na constraint dada ("" = qualquer).
func IsForeignKeyViolation(err error, constraint string) bool {
	return isViolation(err, ConstraintForeignKey, constraint)
}

// IsRestrictViolation informa se err é 23001 (guardas de trigger) com o nome
// de constraint dado ("" = qualquer).
func IsRestrictViolation(err error, constraint string) bool {
	return isViolation(err, ConstraintRestrict, constraint)
}

// transientReasonForCode classifica um SQLSTATE devolvido pelo servidor.
func transientReasonForCode(code string) (TransientReason, bool) {
	switch code {
	case CodeSerializationFailure:
		return ReasonSerialization, true
	case CodeDeadlockDetected:
		return ReasonDeadlock, true
	case CodeLockNotAvailable:
		return ReasonLockTimeout, true
	case CodeQueryCanceled:
		// statement_timeout/lock_timeout do servidor ou pg_cancel_backend.
		// Cancelamento pelo ctx do cliente não chega aqui como PgError: o
		// pgx devolve o erro de contexto, tratado antes.
		return ReasonQueryCanceled, true
	case CodeInsufficientResources, CodeDiskFull, CodeOutOfMemory:
		// Ambiente sem recurso: transitório (ADR-031). Contá-lo como falha
		// permanente levaria itens bons a FAILED durante um disco cheio.
		// 53400 (configuration_limit_exceeded) continua permanente.
		return ReasonResources, true
	case CodeReadOnlyTransaction:
		return ReasonReadOnly, true
	case CodeAdminShutdown, CodeCrashShutdown, CodeCannotConnectNow, CodeTooManyConnections:
		return ReasonConnection, true
	}
	if strings.HasPrefix(code, classConnectionException) {
		return ReasonConnection, true
	}
	return "", false
}

// TransientReasonOf devolve a causa se err é transitório.
func TransientReasonOf(err error) (TransientReason, bool) {
	if err == nil {
		return "", false
	}
	// Cancelamento explícito por quem chamou tem precedência: o pgx embrulha
	// ctx.Err() em erros que também respondem SafeToRetry/Timeout.
	if errors.Is(err, context.Canceled) {
		return "", false
	}
	var te *TransientError
	if errors.As(err, &te) {
		return te.Reason, true
	}
	if errors.Is(err, ErrCommitOutcomeUnknown) {
		return ReasonCommitUnknown, true
	}
	// Um erro do servidor decide pelo SQLSTATE; um PgError permanente (ex.:
	// 28P01 senha errada, 3D000 banco inexistente, classe 23) não é
	// transitório mesmo que venha dentro de um *pgconn.ConnectError.
	if pgErr, ok := pgErrorOf(err); ok {
		return transientReasonForCode(pgErr.Code)
	}
	if errors.Is(err, context.DeadlineExceeded) || pgconn.Timeout(err) {
		return ReasonTimeout, true
	}
	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return ReasonConnection, true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return ReasonTimeout, true
		}
		return ReasonConnection, true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return ReasonConnection, true
	}
	// Erros do pgconn que garantem que nada foi enviado ao servidor.
	if pgconn.SafeToRetry(err) {
		return ReasonConnection, true
	}
	return "", false
}

// IsTransient informa se a mesma requisição pode ter sucesso se repetida mais
// tarde (serialização, deadlock, lock_timeout, conexão, timeout, commit
// ambíguo). Use para decidir 503/Retry-After no HTTP e não remover a mensagem
// no SQS.
func IsTransient(err error) bool {
	_, ok := TransientReasonOf(err)
	return ok
}

// IsRetryable informa se o TxManager pode reexecutar a transação inteira com
// segurança: é transitório E não há dúvida de que nada foi confirmado (exclui
// ErrCommitOutcomeUnknown).
func IsRetryable(err error) bool {
	if errors.Is(err, ErrCommitOutcomeUnknown) {
		return false
	}
	return IsTransient(err)
}

// Translate envolve err na forma tipada correspondente, preservando a cadeia
// (errors.Is/As continuam encontrando o *pgconn.PgError e erros de contexto):
//
//   - classe 23 → *ConstraintViolation (errors.Is com ErrUniqueViolation...);
//   - transitório → *TransientError (errors.Is com ErrTransient);
//   - demais → err inalterado.
//
// É idempotente: um erro já traduzido é devolvido como está.
func Translate(err error) error {
	if err == nil {
		return nil
	}
	var cv *ConstraintViolation
	var te *TransientError
	if errors.As(err, &cv) || errors.As(err, &te) {
		return err
	}
	if v, ok := AsConstraintViolation(err); ok {
		if v.pgErr == err {
			return v
		}
		// Mantém o contexto de embrulho existente na mensagem e na cadeia.
		return &wrappedViolation{ConstraintViolation: v, outer: err}
	}
	if reason, ok := TransientReasonOf(err); ok {
		return &TransientError{Reason: reason, Err: err}
	}
	return err
}

// wrappedViolation preserva o erro externo (com seu contexto) na cadeia,
// enquanto expõe a violação tipada via errors.As(*ConstraintViolation).
type wrappedViolation struct {
	*ConstraintViolation
	outer error
}

func (w *wrappedViolation) Error() string {
	return w.ConstraintViolation.Error() + ": " + w.outer.Error()
}

func (w *wrappedViolation) Unwrap() []error { return []error{w.ConstraintViolation, w.outer} }

// Is delega ao ConstraintViolation (sentinelas).
func (w *wrappedViolation) Is(target error) bool { return w.ConstraintViolation.Is(target) }
