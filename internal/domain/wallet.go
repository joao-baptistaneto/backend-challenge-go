package domain

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
)

// Decisão arquitetural (ADR-005 em ARCHITECTURE.md): Wallet é a raiz do agregado
// financeiro e o único ponto do código que altera saldo.
//
//   - Construção: NewWallet (abertura, gera o lançamento de crédito inicial
//     quando o saldo inicial é positivo) e RehydrateWallet (reconstrução a partir da
//     persistência, sem efeitos colaterais) são separados. O zero-value
//     Wallet{} é "não inicializado" e é rejeitado por Debit/Credit com
//     ErrUninitializedWallet.
//   - Toda mudança de saldo devolve o WalletLedgerEntry correspondente; o caso
//     de uso deve persistir saldo, versão e lançamento na mesma transação SQL.
//     Não existe setter de saldo.
//   - Versionamento: versão inicial 1 (inclusive quando a abertura credita saldo
//     inicial positivo — o lançamento de abertura não incrementa a versão). Cada
//     Debit/Credit bem-sucedido incrementa a versão em exatamente 1. Operações
//     sem movimento (LOSS, rejeições) não chamam Debit/Credit e portanto não
//     alteram a versão.
//   - Concorrência: o agregado NÃO é seguro para uso concorrente em memória; ele
//     vive dentro de uma única transação SQL. A coordenação entre escritores é
//     feita no banco, por carteira (T12): SELECT ... FOR UPDATE da linha da
//     carteira (pessimista) + UPDATE ... WHERE id = $1 AND version =
//     PersistedVersion() (otimista, defesa em profundidade). Se o UPDATE afetar
//     0 linhas, houve escrita concorrente e a operação é refeita — uma
//     atualização confirmada nunca é descartada (lost update).
//   - Atomicidade de falha: se Debit/Credit retornam erro, o agregado permanece
//     exatamente no estado anterior.
//   - A instância lembra, só em memória, o id do último lançamento que ela
//     produziu. NewWalletBalanceChangedEvent usa isso para garantir que
//     walletVersion = Version() é a versão resultante daquele lançamento.
//   - Tempo: o relógio é injetado (parâmetro now), normalizado para UTC.
//     UpdatedAt é monotônico (max(updatedAt, now)) para tolerar pequenas
//     diferenças de relógio entre instâncias sem rejeitar a operação.

// Erros sentinela da Wallet (errors.Is). Os erros de valor reutilizam os
// sentinelas de Money (ErrInvalidAmount, ErrCurrencyMismatch, ErrMoneyOverflow,
// ErrNegativeAmount, ErrUninitializedMoney).
var (
	// ErrInsufficientFunds indica débito que deixaria o saldo negativo. A
	// distinção entre aposta (INSUFFICIENT_FUNDS) e reversão
	// (REVERSAL_INSUFFICIENT_FUNDS) é feita pelas regras de operação (T06).
	ErrInsufficientFunds = errors.New("wallet: insufficient funds")
	// ErrUninitializedWallet indica uso do zero-value (ou nil) de Wallet.
	ErrUninitializedWallet = errors.New("wallet: uninitialized wallet")
	// ErrInvalidWallet indica parâmetros inválidos em NewWallet/RehydrateWallet (ids nulos,
	// saldo negativo, versão < 1, timestamps inconsistentes) ou estado que não
	// pode avançar (overflow de versão).
	ErrInvalidWallet = errors.New("wallet: invalid wallet")
	// ErrInvalidMovement indica argumentos de movimento inválidos que não são
	// relativos ao valor (transactionId nulo, instante zero).
	ErrInvalidMovement = errors.New("wallet: invalid movement")
)

// InitialWalletVersion é a versão de uma carteira recém-criada.
const InitialWalletVersion int64 = 1

// newLedgerEntryID gera o id do lançamento. UUIDv7 é ordenável por tempo, o que
// favorece índices e a paginação keyset do ledger (T12). É variável de pacote
// apenas para permitir testar a falha do gerador.
var newLedgerEntryID = uuid.NewV7

// Wallet é a raiz do agregado financeiro. Campos privados: o saldo só muda via
// Debit/Credit.
type Wallet struct {
	id               uuid.UUID
	playerID         uuid.UUID
	balance          Money
	version          int64
	persistedVersion int64
	createdAt        time.Time
	updatedAt        time.Time
	// lastEntryID é o id do último lançamento produzido por ESTA instância
	// (NewWallet ou Debit/Credit) e uuid.Nil após RehydrateWallet. Existe só em
	// memória. NewWalletBalanceChangedEvent o usa para provar que Version() é a
	// versão resultante daquele lançamento, o que detecta o caso A-B-A (a
	// carteira é movimentada de novo e volta ao mesmo saldo).
	lastEntryID uuid.UUID
}

// NewWalletParams agrupa os dados de abertura de carteira.
type NewWalletParams struct {
	// ID da carteira (gerado pelo caso de uso, ex.: UUIDv7).
	ID uuid.UUID
	// PlayerID do jogador. (PlayerID, moeda) é único — garantido pelo schema (T10).
	PlayerID uuid.UUID
	// InitialBalance define moeda e saldo inicial; deve ser >= 0.
	InitialBalance Money
	// OpeningTransactionID é o id interno da transação OPENING. Obrigatório
	// quando InitialBalance > 0; ignorado quando o saldo inicial é zero (não há
	// OPENING nem lançamento nesse caso).
	OpeningTransactionID uuid.UUID
	// Now é o instante da abertura.
	Now time.Time
}

// NewWallet abre uma carteira na versão 1.
//
// Se InitialBalance > 0, devolve também o lançamento CREDIT de abertura
// (balanceBefore 0, balanceAfter InitialBalance), sem incrementar a versão. Se
// InitialBalance == 0, o lançamento é nil: abertura com saldo zero não gera
// OPENING, ledger nem eventos financeiros (T14).
func NewWallet(p NewWalletParams) (*Wallet, *WalletLedgerEntry, error) {
	if p.ID == uuid.Nil {
		return nil, nil, fmt.Errorf("%w: nil wallet id", ErrInvalidWallet)
	}
	if p.PlayerID == uuid.Nil {
		return nil, nil, fmt.Errorf("%w: nil player id", ErrInvalidWallet)
	}
	if !p.InitialBalance.IsValid() {
		return nil, nil, fmt.Errorf("%w: initial balance: %w", ErrInvalidWallet, ErrUninitializedMoney)
	}
	if p.InitialBalance.IsNegative() {
		return nil, nil, fmt.Errorf("%w: initial balance: %w: %s", ErrInvalidWallet, ErrNegativeAmount, p.InitialBalance)
	}
	if p.Now.IsZero() {
		return nil, nil, fmt.Errorf("%w: zero creation time", ErrInvalidWallet)
	}
	if p.InitialBalance.IsPositive() && p.OpeningTransactionID == uuid.Nil {
		return nil, nil, fmt.Errorf("%w: positive initial balance requires opening transaction id", ErrInvalidWallet)
	}

	now := p.Now.UTC()
	w := &Wallet{
		id:               p.ID,
		playerID:         p.PlayerID,
		balance:          p.InitialBalance,
		version:          InitialWalletVersion,
		persistedVersion: 0, // ainda não persistida: o repositório faz INSERT.
		createdAt:        now,
		updatedAt:        now,
	}
	if !p.InitialBalance.IsPositive() {
		return w, nil, nil
	}

	zero := Money{minor: 0, currency: p.InitialBalance.Currency()}
	entry, err := w.buildEntry(LedgerDirectionCredit, p.InitialBalance, p.OpeningTransactionID, zero, p.InitialBalance, now)
	if err != nil {
		return nil, nil, err
	}
	w.lastEntryID = entry.ID()
	return w, &entry, nil
}

// RehydrateWalletParams agrupa o estado persistido da carteira.
type RehydrateWalletParams struct {
	ID        uuid.UUID
	PlayerID  uuid.UUID
	Balance   Money
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// RehydrateWallet reconstrói uma carteira a partir da persistência. Não gera
// lançamentos, não altera versão nem timestamps: apenas valida as invariantes
// (ids não nulos, saldo inicializado e >= 0, versão >= 1, createdAt <=
// updatedAt). PersistedVersion() passa a ser Version, base do UPDATE otimista.
func RehydrateWallet(p RehydrateWalletParams) (*Wallet, error) {
	if p.ID == uuid.Nil {
		return nil, fmt.Errorf("%w: nil wallet id", ErrInvalidWallet)
	}
	if p.PlayerID == uuid.Nil {
		return nil, fmt.Errorf("%w: nil player id", ErrInvalidWallet)
	}
	if !p.Balance.IsValid() {
		return nil, fmt.Errorf("%w: balance: %w", ErrInvalidWallet, ErrUninitializedMoney)
	}
	if p.Balance.IsNegative() {
		return nil, fmt.Errorf("%w: balance: %w: %s", ErrInvalidWallet, ErrNegativeAmount, p.Balance)
	}
	if p.Version < InitialWalletVersion {
		return nil, fmt.Errorf("%w: version %d < %d", ErrInvalidWallet, p.Version, InitialWalletVersion)
	}
	if p.CreatedAt.IsZero() || p.UpdatedAt.IsZero() {
		return nil, fmt.Errorf("%w: zero timestamp", ErrInvalidWallet)
	}
	if p.UpdatedAt.Before(p.CreatedAt) {
		return nil, fmt.Errorf("%w: updatedAt before createdAt", ErrInvalidWallet)
	}
	return &Wallet{
		id:               p.ID,
		playerID:         p.PlayerID,
		balance:          p.Balance,
		version:          p.Version,
		persistedVersion: p.Version,
		createdAt:        p.CreatedAt.UTC(),
		updatedAt:        p.UpdatedAt.UTC(),
	}, nil
}

// ID devolve a identidade da carteira.
func (w *Wallet) ID() uuid.UUID { return w.id }

// PlayerID devolve o jogador dono da carteira.
func (w *Wallet) PlayerID() uuid.UUID { return w.playerID }

// Currency devolve a moeda da carteira (imutável).
func (w *Wallet) Currency() Currency { return w.balance.Currency() }

// Balance devolve o saldo atual (sempre >= 0).
func (w *Wallet) Balance() Money { return w.balance }

// Version devolve a versão atual (>= 1).
func (w *Wallet) Version() int64 { return w.version }

// PersistedVersion devolve a versão lida da persistência (RehydrateWallet) ou 0 para
// carteira nova ainda não inserida (NewWallet). O repositório usa esse valor no
// UPDATE ... WHERE version = $persisted (controle otimista).
func (w *Wallet) PersistedVersion() int64 { return w.persistedVersion }

// CreatedAt devolve o instante de criação (UTC).
func (w *Wallet) CreatedAt() time.Time { return w.createdAt }

// UpdatedAt devolve o instante da última alteração (UTC).
func (w *Wallet) UpdatedAt() time.Time { return w.updatedAt }

// IsInitialized indica se a carteira foi construída por NewWallet/RehydrateWallet.
func (w *Wallet) IsInitialized() bool {
	return w != nil && w.id != uuid.Nil && w.version >= InitialWalletVersion
}

// Debit retira amount do saldo. Falha com ErrInsufficientFunds se o saldo
// ficaria negativo. Em sucesso, incrementa a versão e devolve o lançamento DEBIT.
func (w *Wallet) Debit(amount Money, transactionID uuid.UUID, now time.Time) (WalletLedgerEntry, error) {
	return w.apply(LedgerDirectionDebit, amount, transactionID, now)
}

// Credit soma amount ao saldo. Falha com ErrMoneyOverflow se o saldo excederia
// a faixa de int64. Em sucesso, incrementa a versão e devolve o lançamento CREDIT.
func (w *Wallet) Credit(amount Money, transactionID uuid.UUID, now time.Time) (WalletLedgerEntry, error) {
	return w.apply(LedgerDirectionCredit, amount, transactionID, now)
}

// apply concentra validação e mutação. Todo o cálculo é feito antes de qualquer
// atribuição, garantindo que um erro deixe o agregado intacto.
func (w *Wallet) apply(dir LedgerDirection, amount Money, transactionID uuid.UUID, now time.Time) (WalletLedgerEntry, error) {
	if !w.IsInitialized() {
		return WalletLedgerEntry{}, ErrUninitializedWallet
	}
	if !amount.IsValid() {
		return WalletLedgerEntry{}, fmt.Errorf("%w: %w", ErrInvalidAmount, ErrUninitializedMoney)
	}
	if amount.Currency() != w.Currency() {
		return WalletLedgerEntry{}, fmt.Errorf("%w: wallet %s, movement %s", ErrCurrencyMismatch, w.Currency(), amount.Currency())
	}
	if amount.IsNegative() {
		return WalletLedgerEntry{}, fmt.Errorf("%w: %w: %s", ErrInvalidAmount, ErrNegativeAmount, amount)
	}
	if amount.IsZero() {
		// Movimentos de valor zero não existem: LOSS (0.00) não chama o agregado.
		return WalletLedgerEntry{}, fmt.Errorf("%w: movement amount must be positive", ErrInvalidAmount)
	}
	if transactionID == uuid.Nil {
		return WalletLedgerEntry{}, fmt.Errorf("%w: nil transaction id", ErrInvalidMovement)
	}
	if now.IsZero() {
		return WalletLedgerEntry{}, fmt.Errorf("%w: zero time", ErrInvalidMovement)
	}
	if w.version == math.MaxInt64 {
		return WalletLedgerEntry{}, fmt.Errorf("%w: version overflow", ErrInvalidWallet)
	}

	before := w.balance
	var after Money
	var err error
	switch dir {
	case LedgerDirectionDebit:
		after, err = before.Sub(amount)
		if err != nil {
			return WalletLedgerEntry{}, err
		}
		if after.IsNegative() {
			return WalletLedgerEntry{}, fmt.Errorf("%w: balance %s, debit %s", ErrInsufficientFunds, before, amount)
		}
	case LedgerDirectionCredit:
		after, err = before.Add(amount)
		if err != nil {
			return WalletLedgerEntry{}, err
		}
	default:
		return WalletLedgerEntry{}, fmt.Errorf("%w: direction %q", ErrInvalidLedgerEntry, dir)
	}

	now = now.UTC()
	entry, err := w.buildEntry(dir, amount, transactionID, before, after, now)
	if err != nil {
		return WalletLedgerEntry{}, err
	}

	// Mutação (só após todas as validações).
	w.balance = after
	w.version++
	w.lastEntryID = entry.ID()
	if now.After(w.updatedAt) {
		w.updatedAt = now
	}
	return entry, nil
}

func (w *Wallet) buildEntry(dir LedgerDirection, amount Money, txID uuid.UUID, before, after Money, now time.Time) (WalletLedgerEntry, error) {
	id, err := newLedgerEntryID()
	if err != nil {
		return WalletLedgerEntry{}, fmt.Errorf("%w: generate id: %v", ErrInvalidLedgerEntry, err)
	}
	return NewWalletLedgerEntry(NewWalletLedgerEntryParams{
		ID:            id,
		WalletID:      w.id,
		TransactionID: txID,
		Direction:     dir,
		Amount:        amount,
		BalanceBefore: before,
		BalanceAfter:  after,
		CreatedAt:     now,
	})
}
