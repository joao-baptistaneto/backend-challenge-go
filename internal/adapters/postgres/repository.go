package postgres

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// Infraestrutura comum dos repositórios (ARCHITECTURE.md, ADR-014).
//
// Decisões:
//
//   - SQL explícito com pgx. Cada método recebe um Querier: dentro de
//     WithinTx é o pgx.Tx do caso de uso, e assim wallet, transação, ledger e
//     outbox entram no mesmo commit. Os métodos que travam linhas
//     (GetForUpdate, ClaimDuePendingReferences) exigem um pgx.Tx
//     (ErrTxRequired): FOR UPDATE fora de transação solta o lock na hora e
//     daria uma falsa sensação de exclusão.
//   - Money ↔ BIGINT + CHAR(3): MinorUnits() e Currency() na escrita e
//     domain.FromMinorUnits na leitura. Antes de persistir, todo Money passa por
//     IsValid(), porque MinorUnits()/Currency() do zero-value não devolvem erro
//     e gravariam 0 com moeda vazia (barrada pelo CHECK, mas com um erro opaco).
//   - Os repositórios são stateless. A leitura sempre reidrata pelo domínio
//     (RehydrateWallet, RehydrateWalletLedgerEntry, RehydrateWagerTransaction),
//     que revalida as invariantes: dado persistido inconsistente falha alto.
//   - Violações de constraint viram sentinelas deste pacote pelo NOME da
//     constraint (ADR-011/ADR-013). O erro original segue na cadeia
//     (errors.As em *ConstraintViolation e *pgconn.PgError continua valendo).

// Sentinelas dos repositórios. Todos funcionam com errors.Is.
var (
	// ErrNotFound indica que a linha procurada não existe.
	ErrNotFound = errors.New("postgres: registro não encontrado")
	// ErrTxRequired indica chamada que trava linhas fora de um pgx.Tx.
	ErrTxRequired = errors.New("postgres: operação exige transação (pgx.Tx)")
	// ErrInvalidAggregate indica agregado não inicializado ou em estado que o
	// repositório não pode gravar (Money zero-value, carteira com mais de um
	// movimento pendente, carteira nova passada a Update etc.).
	ErrInvalidAggregate = errors.New("postgres: agregado inválido para persistência")
	// ErrStaleWallet indica lost update detectado: o UPDATE ... WHERE version
	// = $lida não afetou linha nenhuma, porque outro escritor confirmou uma
	// versão mais nova (ou a carteira não existe mais). O caso de uso deve
	// reler sob lock e decidir de novo; nada foi gravado.
	ErrStaleWallet = errors.New("postgres: carteira alterada concorrentemente (versão divergente)")
	// ErrWalletAlreadyExists indica (playerId, currency) já com carteira.
	ErrWalletAlreadyExists = errors.New("postgres: já existe carteira para o jogador e a moeda")
	// ErrDuplicateKey indica chave primária repetida (id interno reutilizado).
	ErrDuplicateKey = errors.New("postgres: id já existente")
	// ErrDuplicateExternalTransaction indica (providerId,
	// externalTransactionId) já registrado.
	ErrDuplicateExternalTransaction = errors.New("postgres: transação externa já registrada para o provedor")
	// ErrDuplicateIdempotencyKey indica (providerId, idempotencyKey) já usado.
	ErrDuplicateIdempotencyKey = errors.New("postgres: chave de idempotência já usada pelo provedor")
	// ErrOpeningAlreadyExists indica segundo OPENING para a mesma carteira.
	ErrOpeningAlreadyExists = errors.New("postgres: carteira já tem crédito de abertura")
	// ErrReferenceAlreadyReversed indica segunda reversão PROCESSED da mesma
	// referência (índice wager_transactions_reversal_once_key).
	ErrReferenceAlreadyReversed = errors.New("postgres: referência já revertida")
	// ErrTransactionFinalized indica tentativa de alterar transação terminal.
	ErrTransactionFinalized = errors.New("postgres: transação já está em estado terminal")
	// ErrLedgerEntryExists indica segundo lançamento para (wallet, transação).
	ErrLedgerEntryExists = errors.New("postgres: lançamento já registrado para a transação")
	// ErrInvalidCursor indica cursor de paginação malformado, adulterado ou de
	// outra carteira.
	ErrInvalidCursor = errors.New("postgres: cursor inválido")
	// ErrInvalidPageLimit indica limite de página fora de [1, max].
	ErrInvalidPageLimit = errors.New("postgres: limite de página inválido")
)

// constraintErrors mapeia o nome da constraint (migrations) para a sentinela.
var constraintErrors = map[string]error{
	"wallets_pkey":                                 ErrDuplicateKey,
	"wallets_player_currency_key":                  ErrWalletAlreadyExists,
	"wager_transactions_pkey":                      ErrDuplicateKey,
	"wager_transactions_provider_external_key":     ErrDuplicateExternalTransaction,
	"wager_transactions_provider_idempotency_key":  ErrDuplicateIdempotencyKey,
	"wager_transactions_opening_per_wallet_key":    ErrOpeningAlreadyExists,
	"wager_transactions_reversal_once_key":         ErrReferenceAlreadyReversed,
	"wager_transactions_terminal_immutable":        ErrTransactionFinalized,
	"wallet_ledger_entries_pkey":                   ErrDuplicateKey,
	"wallet_ledger_entries_wallet_transaction_key": ErrLedgerEntryExists,
	"outbox_pkey":                                  ErrDuplicateKey,
	"outbox_event_id_key":                          ErrDuplicateEvent,
}

// mapWriteError traduz o erro de uma escrita: constraint conhecida vira a
// sentinela correspondente (com a causa preservada); o resto passa por
// Translate.
func mapWriteError(op string, err error) error {
	if err == nil {
		return nil
	}
	if cv, ok := AsConstraintViolation(err); ok {
		if sentinel, known := constraintErrors[cv.Constraint]; known {
			return fmt.Errorf("%s: %w: %w", op, sentinel, Translate(err))
		}
	}
	return fmt.Errorf("%s: %w", op, Translate(err))
}

// mapReadError traduz o erro de uma leitura de linha única.
func mapReadError(op string, err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%s: %w", op, ErrNotFound)
	}
	return fmt.Errorf("%s: %w", op, Translate(err))
}

func requireTx(op string, q Querier) error {
	if _, ok := q.(pgx.Tx); !ok {
		return fmt.Errorf("%s: %w", op, ErrTxRequired)
	}
	return nil
}

// moneyColumns devolve (BIGINT, CHAR(3)) de um Money válido.
func moneyColumns(field string, m domain.Money) (int64, string, error) {
	if !m.IsValid() {
		return 0, "", fmt.Errorf("%w: %s: %w", ErrInvalidAggregate, field, domain.ErrUninitializedMoney)
	}
	return m.MinorUnits(), m.Currency().String(), nil
}

// moneyFromColumns reconstrói Money a partir das colunas.
func moneyFromColumns(field string, minor int64, currency string) (domain.Money, error) {
	m, err := domain.FromMinorUnits(minor, currency)
	if err != nil {
		return domain.Money{}, fmt.Errorf("%s: %w", field, err)
	}
	return m, nil
}

// Conversões para colunas anuláveis: valor "vazio" do domínio ↔ NULL.

func nullText(s string) pgtype.Text { return pgtype.Text{String: s, Valid: s != ""} }

func nullUUID(id uuid.UUID) pgtype.UUID { return pgtype.UUID{Bytes: id, Valid: id != uuid.Nil} }

func nullTime(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: t, Valid: !t.IsZero()}
}

func textOrEmpty(t pgtype.Text) string {
	if !t.Valid {
		return ""
	}
	return t.String
}

func uuidOrNil(u pgtype.UUID) uuid.UUID {
	if !u.Valid {
		return uuid.Nil
	}
	return u.Bytes
}

func timeOrZero(t pgtype.Timestamptz) time.Time {
	if !t.Valid {
		return time.Time{}
	}
	return t.Time.UTC()
}
