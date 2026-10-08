package domain

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Decisão arquitetural (ADR-006 em ARCHITECTURE.md): WalletLedgerEntry é o
// registro auditável e append-only de cada movimento de saldo.
//
//   - Imutabilidade: todos os campos são privados e só há getters por valor
//     (Money e uuid.UUID são tipos-valor; time.Time é copiado). Não existe
//     setter nem método que altere o lançamento. Correções financeiras exigem
//     novos lançamentos (README, regra 5); a proteção contra UPDATE/DELETE no
//     banco é responsabilidade do schema (ADR-011).
//   - Invariantes (verificadas em TODA construção, nova ou reidratada):
//     ids não nulos; direção DEBIT|CREDIT; amount, balanceBefore e balanceAfter
//     inicializados e na mesma moeda; amount > 0 (LOSS/rejeições não geram
//     lançamento); saldos >= 0; balanceAfter = balanceBefore + amount (CREDIT)
//     ou balanceBefore - amount (DEBIT), sem overflow; createdAt não zero
//     (normalizado para UTC).
//   - Construção separada: NewWalletLedgerEntry é usado pela Wallet ao
//     movimentar saldo (Debit/Credit/abertura). RehydrateWalletLedgerEntry é
//     usado pelos repositórios para reconstruir lançamentos lidos do banco. A
//     reidratação NÃO confia no banco: revalida as mesmas invariantes e marca o
//     erro como dado persistido corrompido, para que leituras de ledger
//     inconsistentes (reconciliação, paginação) falhem alto em vez de propagar
//     dados errados.
//   - O zero-value WalletLedgerEntry{} não é um lançamento válido
//     (IsInitialized() == false); ele só aparece como retorno acompanhado de erro.

// ErrInvalidLedgerEntry indica lançamento inconsistente (ids nulos, direção
// inválida, valor não positivo, moeda divergente, saldo negativo, aritmética
// balanceAfter != balanceBefore ± amount, instante zero).
var ErrInvalidLedgerEntry = errors.New("ledger: invalid entry")

// LedgerDirection é a direção do lançamento.
type LedgerDirection string

const (
	// LedgerDirectionDebit reduz o saldo.
	LedgerDirectionDebit LedgerDirection = "DEBIT"
	// LedgerDirectionCredit aumenta o saldo.
	LedgerDirectionCredit LedgerDirection = "CREDIT"
)

// IsValid indica se a direção é DEBIT ou CREDIT.
func (d LedgerDirection) IsValid() bool {
	return d == LedgerDirectionDebit || d == LedgerDirectionCredit
}

// String devolve o nome da direção.
func (d LedgerDirection) String() string { return string(d) }

// WalletLedgerEntry é um lançamento imutável do ledger (append-only).
type WalletLedgerEntry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     LedgerDirection
	amount        Money
	balanceBefore Money
	balanceAfter  Money
	createdAt     time.Time
}

// NewWalletLedgerEntryParams agrupa os dados de um novo lançamento.
type NewWalletLedgerEntryParams struct {
	// ID do lançamento (UUIDv7 recomendado: ordenável no tempo, favorece keyset).
	ID uuid.UUID
	// WalletID da carteira movimentada.
	WalletID uuid.UUID
	// TransactionID da transação interna que originou o movimento.
	// (WalletID, TransactionID) é único no banco (ADR-011).
	TransactionID uuid.UUID
	// Direction é DEBIT ou CREDIT.
	Direction LedgerDirection
	// Amount é o valor movimentado (> 0).
	Amount Money
	// BalanceBefore é o saldo da carteira antes do movimento (>= 0).
	BalanceBefore Money
	// BalanceAfter é o saldo após o movimento (>= 0, = BalanceBefore ± Amount).
	BalanceAfter Money
	// CreatedAt é o instante do movimento (normalizado para UTC).
	CreatedAt time.Time
}

// RehydrateWalletLedgerEntryParams agrupa o estado persistido de um lançamento.
// Os campos têm a mesma semântica de NewWalletLedgerEntryParams.
type RehydrateWalletLedgerEntryParams struct {
	ID            uuid.UUID
	WalletID      uuid.UUID
	TransactionID uuid.UUID
	Direction     LedgerDirection
	Amount        Money
	BalanceBefore Money
	BalanceAfter  Money
	CreatedAt     time.Time
}

// ledgerEntryFields é a forma interna comum a criação e reidratação.
type ledgerEntryFields struct {
	id, walletID, transactionID uuid.UUID
	direction                   LedgerDirection
	amount, before, after       Money
	createdAt                   time.Time
}

// NewWalletLedgerEntry valida e constrói um novo lançamento. Erros satisfazem
// errors.Is(err, ErrInvalidLedgerEntry) e, quando aplicável, também o
// sentinela de Money correspondente (ErrCurrencyMismatch, ErrInvalidAmount,
// ErrNegativeAmount, ErrUninitializedMoney, ErrMoneyOverflow).
func NewWalletLedgerEntry(p NewWalletLedgerEntryParams) (WalletLedgerEntry, error) {
	return buildLedgerEntry(ledgerEntryFields{
		id:            p.ID,
		walletID:      p.WalletID,
		transactionID: p.TransactionID,
		direction:     p.Direction,
		amount:        p.Amount,
		before:        p.BalanceBefore,
		after:         p.BalanceAfter,
		createdAt:     p.CreatedAt,
	})
}

// RehydrateWalletLedgerEntry reconstrói um lançamento lido da persistência,
// revalidando todas as invariantes. Dados inválidos são rejeitados com
// ErrInvalidLedgerEntry (mais o sentinela de Money aplicável) e a mensagem
// identifica a origem ("rehydrate"), indicando dado persistido corrompido.
func RehydrateWalletLedgerEntry(p RehydrateWalletLedgerEntryParams) (WalletLedgerEntry, error) {
	e, err := buildLedgerEntry(ledgerEntryFields{
		id:            p.ID,
		walletID:      p.WalletID,
		transactionID: p.TransactionID,
		direction:     p.Direction,
		amount:        p.Amount,
		before:        p.BalanceBefore,
		after:         p.BalanceAfter,
		createdAt:     p.CreatedAt,
	})
	if err != nil {
		return WalletLedgerEntry{}, fmt.Errorf("rehydrate ledger entry %s: %w", p.ID, err)
	}
	return e, nil
}

// buildLedgerEntry concentra as invariantes para que criação e reidratação
// nunca divirjam.
func buildLedgerEntry(f ledgerEntryFields) (WalletLedgerEntry, error) {
	if f.id == uuid.Nil {
		return WalletLedgerEntry{}, fmt.Errorf("%w: nil entry id", ErrInvalidLedgerEntry)
	}
	if f.walletID == uuid.Nil {
		return WalletLedgerEntry{}, fmt.Errorf("%w: nil wallet id", ErrInvalidLedgerEntry)
	}
	if f.transactionID == uuid.Nil {
		return WalletLedgerEntry{}, fmt.Errorf("%w: nil transaction id", ErrInvalidLedgerEntry)
	}
	if !f.direction.IsValid() {
		return WalletLedgerEntry{}, fmt.Errorf("%w: direction %q", ErrInvalidLedgerEntry, f.direction)
	}
	if !f.amount.IsValid() || !f.before.IsValid() || !f.after.IsValid() {
		return WalletLedgerEntry{}, fmt.Errorf("%w: %w", ErrInvalidLedgerEntry, ErrUninitializedMoney)
	}
	if f.amount.Currency() != f.before.Currency() || f.amount.Currency() != f.after.Currency() {
		return WalletLedgerEntry{}, fmt.Errorf("%w: %w: amount %s, before %s, after %s",
			ErrInvalidLedgerEntry, ErrCurrencyMismatch, f.amount.Currency(), f.before.Currency(), f.after.Currency())
	}
	if !f.amount.IsPositive() {
		if f.amount.IsNegative() {
			return WalletLedgerEntry{}, fmt.Errorf("%w: %w: %w: %s", ErrInvalidLedgerEntry, ErrInvalidAmount, ErrNegativeAmount, f.amount)
		}
		return WalletLedgerEntry{}, fmt.Errorf("%w: %w: amount must be positive", ErrInvalidLedgerEntry, ErrInvalidAmount)
	}
	if f.before.IsNegative() || f.after.IsNegative() {
		return WalletLedgerEntry{}, fmt.Errorf("%w: %w: negative balance (before %s, after %s)",
			ErrInvalidLedgerEntry, ErrNegativeAmount, f.before, f.after)
	}
	var expected Money
	var err error
	if f.direction == LedgerDirectionCredit {
		expected, err = f.before.Add(f.amount)
	} else {
		expected, err = f.before.Sub(f.amount)
	}
	if err != nil {
		return WalletLedgerEntry{}, fmt.Errorf("%w: %w", ErrInvalidLedgerEntry, err)
	}
	if expected != f.after {
		return WalletLedgerEntry{}, fmt.Errorf("%w: inconsistent arithmetic: %s %s %s = %s, got %s",
			ErrInvalidLedgerEntry, f.before, f.direction, f.amount, expected, f.after)
	}
	if f.createdAt.IsZero() {
		return WalletLedgerEntry{}, fmt.Errorf("%w: zero time", ErrInvalidLedgerEntry)
	}
	return WalletLedgerEntry{
		id:            f.id,
		walletID:      f.walletID,
		transactionID: f.transactionID,
		direction:     f.direction,
		amount:        f.amount,
		balanceBefore: f.before,
		balanceAfter:  f.after,
		createdAt:     f.createdAt.UTC(),
	}, nil
}

// ID devolve a identidade do lançamento.
func (e WalletLedgerEntry) ID() uuid.UUID { return e.id }

// WalletID devolve a carteira do lançamento.
func (e WalletLedgerEntry) WalletID() uuid.UUID { return e.walletID }

// TransactionID devolve a transação interna que originou o lançamento.
// (WalletID, TransactionID) é único no banco (ADR-011).
func (e WalletLedgerEntry) TransactionID() uuid.UUID { return e.transactionID }

// Direction devolve DEBIT ou CREDIT.
func (e WalletLedgerEntry) Direction() LedgerDirection { return e.direction }

// Amount devolve o valor movimentado (> 0).
func (e WalletLedgerEntry) Amount() Money { return e.amount }

// BalanceBefore devolve o saldo antes do lançamento.
func (e WalletLedgerEntry) BalanceBefore() Money { return e.balanceBefore }

// BalanceAfter devolve o saldo após o lançamento.
func (e WalletLedgerEntry) BalanceAfter() Money { return e.balanceAfter }

// CreatedAt devolve o instante do lançamento (UTC).
func (e WalletLedgerEntry) CreatedAt() time.Time { return e.createdAt }

// IsInitialized indica se o lançamento foi construído por
// NewWalletLedgerEntry/RehydrateWalletLedgerEntry (o zero-value não é).
func (e WalletLedgerEntry) IsInitialized() bool { return e.id != uuid.Nil }
