package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// WalletRepository persiste o agregado Wallet (ARCHITECTURE.md, ADR-014).
//
// Concorrência (ADR-005): o caso de uso trava a linha com GetForUpdate
// (SELECT ... FOR UPDATE, pessimista e por carteira, sem lock global) e grava
// com Update, que é condicional à versão lida (UPDATE ... WHERE version =
// $PersistedVersion). Se outro escritor confirmou antes, o UPDATE afeta 0
// linhas e devolve ErrStaleWallet: a atualização concorrente nunca é
// sobrescrita (lost update). Com o lock, isso só acontece se alguém ler sem
// lock (Get) e tentar gravar, ou seja, é defesa em profundidade.
type WalletRepository struct{}

// NewWalletRepository cria o repositório (stateless).
func NewWalletRepository() *WalletRepository { return &WalletRepository{} }

const walletColumns = `id, player_id, currency, balance, version, created_at, updated_at`

// Insert grava uma carteira nova (NewWallet, versão 1, PersistedVersion 0).
// (playerId, currency) repetido devolve ErrWalletAlreadyExists.
func (WalletRepository) Insert(ctx context.Context, q Querier, w *domain.Wallet) error {
	const op = "postgres: inserir carteira"
	if w == nil || !w.IsInitialized() {
		return fmt.Errorf("%s: %w: %w", op, ErrInvalidAggregate, domain.ErrUninitializedWallet)
	}
	if w.PersistedVersion() != 0 || w.Version() != domain.InitialWalletVersion {
		return fmt.Errorf("%s: %w: carteira %s não é nova (versão %d, persistida %d)",
			op, ErrInvalidAggregate, w.ID(), w.Version(), w.PersistedVersion())
	}
	balance, currency, err := moneyColumns("balance", w.Balance())
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	_, err = q.Exec(ctx, `INSERT INTO wallets (`+walletColumns+`)
VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		w.ID(), w.PlayerID(), currency, balance, w.Version(), w.CreatedAt(), w.UpdatedAt())
	return mapWriteError(op, err)
}

// Get lê a carteira sem lock (consultas). Inexistente: ErrNotFound.
func (WalletRepository) Get(ctx context.Context, q Querier, id uuid.UUID) (*domain.Wallet, error) {
	return getWallet(ctx, q, "postgres: ler carteira", `SELECT `+walletColumns+` FROM wallets WHERE id = $1`, id)
}

// GetForUpdate lê e trava a linha da carteira até o fim da transação
// (SELECT ... FOR UPDATE). Exige pgx.Tx. Escritores da mesma carteira esperam
// o commit; carteiras diferentes seguem em paralelo.
func (WalletRepository) GetForUpdate(ctx context.Context, q Querier, id uuid.UUID) (*domain.Wallet, error) {
	const op = "postgres: travar carteira"
	if err := requireTx(op, q); err != nil {
		return nil, err
	}
	return getWallet(ctx, q, op, `SELECT `+walletColumns+` FROM wallets WHERE id = $1 FOR UPDATE`, id)
}

// GetByPlayerCurrency lê a carteira de (jogador, moeda), sem lock.
func (WalletRepository) GetByPlayerCurrency(ctx context.Context, q Querier, playerID uuid.UUID, currency domain.Currency) (*domain.Wallet, error) {
	return getWallet(ctx, q, "postgres: ler carteira por jogador",
		`SELECT `+walletColumns+` FROM wallets WHERE player_id = $1 AND currency = $2`, playerID, currency.String())
}

// Update grava saldo, versão e updatedAt de uma carteira reidratada que
// sofreu exatamente um movimento (Version() = PersistedVersion() + 1, como
// exige o trigger wallets_guard_update). Sem movimento (LOSS), não faz nada.
// A escrita é condicional à versão lida; 0 linhas afetadas devolve
// ErrStaleWallet. Depois de um Update, a instância não deve ser gravada de
// novo: releia a carteira.
func (WalletRepository) Update(ctx context.Context, q Querier, w *domain.Wallet) error {
	const op = "postgres: atualizar carteira"
	if w == nil || !w.IsInitialized() {
		return fmt.Errorf("%s: %w: %w", op, ErrInvalidAggregate, domain.ErrUninitializedWallet)
	}
	if w.PersistedVersion() == 0 {
		return fmt.Errorf("%s: %w: carteira %s ainda não foi inserida", op, ErrInvalidAggregate, w.ID())
	}
	if w.Version() == w.PersistedVersion() {
		return nil
	}
	if w.Version() != w.PersistedVersion()+1 {
		return fmt.Errorf("%s: %w: carteira %s com %d movimentos pendentes (esperado 1)",
			op, ErrInvalidAggregate, w.ID(), w.Version()-w.PersistedVersion())
	}
	balance, _, err := moneyColumns("balance", w.Balance())
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	tag, err := q.Exec(ctx, `UPDATE wallets SET balance = $2, version = $3, updated_at = $4
WHERE id = $1 AND version = $5`,
		w.ID(), balance, w.Version(), w.UpdatedAt(), w.PersistedVersion())
	if err != nil {
		return mapWriteError(op, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%s: %w: carteira %s não está mais na versão %d", op, ErrStaleWallet, w.ID(), w.PersistedVersion())
	}
	return nil
}

func getWallet(ctx context.Context, q Querier, op, sql string, args ...any) (*domain.Wallet, error) {
	var (
		id, player           uuid.UUID
		currency             string
		balance, version     int64
		createdAt, updatedAt time.Time
	)
	err := q.QueryRow(ctx, sql, args...).Scan(&id, &player, &currency, &balance, &version, &createdAt, &updatedAt)
	if err != nil {
		return nil, mapReadError(op, err)
	}
	money, err := moneyFromColumns("balance", balance, currency)
	if err != nil {
		return nil, fmt.Errorf("%s: reidratar carteira %s: %w", op, id, err)
	}
	w, err := domain.RehydrateWallet(domain.RehydrateWalletParams{
		ID: id, PlayerID: player, Balance: money, Version: version,
		CreatedAt: createdAt, UpdatedAt: updatedAt,
	})
	if err != nil {
		return nil, fmt.Errorf("%s: reidratar carteira %s: %w", op, id, err)
	}
	return w, nil
}
