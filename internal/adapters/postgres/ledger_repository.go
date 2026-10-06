package postgres

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// LedgerRepository grava e pagina o ledger append-only (ARCHITECTURE.md,
// ADR-014).
//
//   - Append só insere. UPDATE/DELETE/TRUNCATE são barrados pelo schema
//     (0003) e não existem aqui. (walletId, transactionId) repetido devolve
//     ErrLedgerEntryExists.
//   - Paginação keyset pela coluna seq (0006): dentro de uma carteira, seq
//     cresce na ordem de commit, porque todo lançamento é inserido com a linha
//     da carteira travada. A página é WHERE wallet_id = $1 AND seq > $cursor
//     ORDER BY seq LIMIT n: sem OFFSET, então lançamentos novos nunca deslocam
//     nem duplicam páginas já lidas e sempre aparecem depois do cursor.
//   - Cursor opaco: base64url de [versão 1 byte][walletId 16 bytes][seq 8
//     bytes]. Ele carrega a carteira, então um cursor usado em outra carteira é
//     recusado (ErrInvalidCursor). Não é segredo nem assinatura: adulterar o
//     seq só desloca a página dentro da própria carteira, que o chamador já
//     está autorizado a ler.
type LedgerRepository struct{}

// NewLedgerRepository cria o repositório (stateless).
func NewLedgerRepository() *LedgerRepository { return &LedgerRepository{} }

// Limites de página do extrato.
const (
	// DefaultLedgerPageSize é o tamanho sugerido quando o cliente não informa.
	DefaultLedgerPageSize = 50
	// MaxLedgerPageSize é o maior limite aceito.
	MaxLedgerPageSize = 200
)

const (
	ledgerCursorVersion byte = 1
	ledgerCursorLen          = 1 + 16 + 8
)

// LedgerPage é uma página do extrato em ordem de lançamento.
type LedgerPage struct {
	Entries []domain.WalletLedgerEntry
	// NextCursor é vazio quando não há mais lançamentos no momento da leitura.
	NextCursor string
}

// Append grava um lançamento novo.
func (LedgerRepository) Append(ctx context.Context, q Querier, e domain.WalletLedgerEntry) error {
	const op = "postgres: gravar lançamento"
	if !e.IsInitialized() {
		return fmt.Errorf("%s: %w: lançamento não inicializado", op, ErrInvalidAggregate)
	}
	amount, currency, err := moneyColumns("amount", e.Amount())
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	before, _, err := moneyColumns("balance_before", e.BalanceBefore())
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	after, _, err := moneyColumns("balance_after", e.BalanceAfter())
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	_, err = q.Exec(ctx, `INSERT INTO wallet_ledger_entries
(id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		e.ID(), e.WalletID(), e.TransactionID(), e.Direction().String(), amount, currency, before, after, e.CreatedAt())
	return mapWriteError(op, err)
}

// ListByWallet devolve até limit lançamentos da carteira depois do cursor
// (vazio = início), em ordem de lançamento. limit deve estar em
// [1, MaxLedgerPageSize]. Carteira sem lançamentos (ou inexistente) devolve
// página vazia; a existência da carteira é verificada pelo caso de uso.
func (LedgerRepository) ListByWallet(ctx context.Context, q Querier, walletID uuid.UUID, cursor string, limit int) (LedgerPage, error) {
	const op = "postgres: listar lançamentos"
	if limit < 1 || limit > MaxLedgerPageSize {
		return LedgerPage{}, fmt.Errorf("%s: %w: %d (1..%d)", op, ErrInvalidPageLimit, limit, MaxLedgerPageSize)
	}
	var after int64
	if cursor != "" {
		var err error
		if after, err = decodeLedgerCursor(cursor, walletID); err != nil {
			return LedgerPage{}, fmt.Errorf("%s: %w", op, err)
		}
	}
	rows, err := q.Query(ctx, `SELECT seq, id, wallet_id, transaction_id, direction, amount, currency,
       balance_before, balance_after, created_at
FROM wallet_ledger_entries
WHERE wallet_id = $1 AND seq > $2
ORDER BY seq
LIMIT $3`, walletID, after, limit+1)
	if err != nil {
		return LedgerPage{}, fmt.Errorf("%s: %w", op, Translate(err))
	}
	defer rows.Close()

	entries := make([]domain.WalletLedgerEntry, 0, limit)
	var lastSeq int64
	more := false
	for rows.Next() {
		if len(entries) == limit {
			more = true
			break
		}
		var (
			seq, amount, before, afterBal int64
			id, wallet, tx                uuid.UUID
			direction, currency           string
			createdAt                     time.Time
		)
		if err := rows.Scan(&seq, &id, &wallet, &tx, &direction, &amount, &currency, &before, &afterBal, &createdAt); err != nil {
			return LedgerPage{}, fmt.Errorf("%s: %w", op, Translate(err))
		}
		e, err := rehydrateLedgerEntry(id, wallet, tx, direction, amount, currency, before, afterBal, createdAt)
		if err != nil {
			return LedgerPage{}, fmt.Errorf("%s: %w", op, err)
		}
		entries = append(entries, e)
		lastSeq = seq
	}
	if err := rows.Err(); err != nil {
		return LedgerPage{}, fmt.Errorf("%s: %w", op, Translate(err))
	}
	page := LedgerPage{Entries: entries}
	if more {
		page.NextCursor = encodeLedgerCursor(walletID, lastSeq)
	}
	return page, nil
}

// GetByTransaction devolve o lançamento de (carteira, transação), se houver.
func (LedgerRepository) GetByTransaction(ctx context.Context, q Querier, walletID, transactionID uuid.UUID) (domain.WalletLedgerEntry, error) {
	const op = "postgres: ler lançamento da transação"
	var (
		amount, before, after int64
		id, wallet, tx        uuid.UUID
		direction, currency   string
		createdAt             time.Time
	)
	err := q.QueryRow(ctx, `SELECT id, wallet_id, transaction_id, direction, amount, currency,
       balance_before, balance_after, created_at
FROM wallet_ledger_entries WHERE wallet_id = $1 AND transaction_id = $2`, walletID, transactionID).
		Scan(&id, &wallet, &tx, &direction, &amount, &currency, &before, &after, &createdAt)
	if err != nil {
		return domain.WalletLedgerEntry{}, mapReadError(op, err)
	}
	e, err := rehydrateLedgerEntry(id, wallet, tx, direction, amount, currency, before, after, createdAt)
	if err != nil {
		return domain.WalletLedgerEntry{}, fmt.Errorf("%s: %w", op, err)
	}
	return e, nil
}

func rehydrateLedgerEntry(id, wallet, tx uuid.UUID, direction string, amount int64, currency string, before, after int64, createdAt time.Time) (domain.WalletLedgerEntry, error) {
	am, err := moneyFromColumns("amount", amount, currency)
	if err != nil {
		return domain.WalletLedgerEntry{}, fmt.Errorf("reidratar lançamento %s: %w", id, err)
	}
	bb, err := moneyFromColumns("balance_before", before, currency)
	if err != nil {
		return domain.WalletLedgerEntry{}, fmt.Errorf("reidratar lançamento %s: %w", id, err)
	}
	ba, err := moneyFromColumns("balance_after", after, currency)
	if err != nil {
		return domain.WalletLedgerEntry{}, fmt.Errorf("reidratar lançamento %s: %w", id, err)
	}
	return domain.RehydrateWalletLedgerEntry(domain.RehydrateWalletLedgerEntryParams{
		ID: id, WalletID: wallet, TransactionID: tx, Direction: domain.LedgerDirection(direction),
		Amount: am, BalanceBefore: bb, BalanceAfter: ba, CreatedAt: createdAt,
	})
}

func encodeLedgerCursor(walletID uuid.UUID, seq int64) string {
	var b [ledgerCursorLen]byte
	b[0] = ledgerCursorVersion
	copy(b[1:17], walletID[:])
	binary.BigEndian.PutUint64(b[17:], uint64(seq))
	return base64.RawURLEncoding.EncodeToString(b[:])
}

func decodeLedgerCursor(cursor string, walletID uuid.UUID) (int64, error) {
	b, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil || len(b) != ledgerCursorLen || b[0] != ledgerCursorVersion {
		return 0, fmt.Errorf("%w: formato", ErrInvalidCursor)
	}
	if uuid.UUID(b[1:17]) != walletID {
		return 0, fmt.Errorf("%w: cursor de outra carteira", ErrInvalidCursor)
	}
	seq := binary.BigEndian.Uint64(b[17:])
	if seq == 0 || seq > 1<<63-1 {
		return 0, fmt.Errorf("%w: posição", ErrInvalidCursor)
	}
	return int64(seq), nil
}
