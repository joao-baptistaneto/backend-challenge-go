package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// UnitOfWork implementa app.UnitOfWork sobre o TxManager e os repositórios
// (ARCHITECTURE.md, ADR-016).
//
//   - WithinTx abre UMA transação (Transactor.WithinTx) e entrega ao caso de
//     uso um app.Tx cujos stores usam todos o mesmo pgx.Tx: a fronteira
//     transacional continua explícita e auditável (ADR-013), sem pgx nas
//     portas da aplicação.
//   - TxOptions.Retry liga WithRetry(DefaultRetryPolicy(Operation)).
//   - Tradução de erros para as sentinelas da aplicação, preservando a causa:
//     ErrWalletAlreadyExists → app.ErrWalletAlreadyExists; falha transitória
//     (IsTransient, inclusive ErrCommitOutcomeUnknown) → app.ErrUnavailable.
//     Os demais erros (inclusive os do próprio caso de uso) voltam intactos.
type UnitOfWork struct {
	tx      Transactor
	wallets *WalletRepository
	ledger  *LedgerRepository
	wagers  *WagerTransactionRepository
	outbox  *OutboxRepository
}

var _ app.UnitOfWork = (*UnitOfWork)(nil)

// NewUnitOfWork cria a unidade de trabalho.
func NewUnitOfWork(t Transactor, w *WalletRepository, l *LedgerRepository, x *WagerTransactionRepository, o *OutboxRepository) *UnitOfWork {
	return &UnitOfWork{tx: t, wallets: w, ledger: l, wagers: x, outbox: o}
}

// WithinTx executa fn numa transação (ver app.UnitOfWork).
func (u *UnitOfWork) WithinTx(ctx context.Context, opts app.TxOptions, fn app.TxFunc) error {
	var txOpts []TxOption
	if opts.Retry {
		txOpts = append(txOpts, WithRetry(DefaultRetryPolicy(opts.Operation)))
	}
	err := u.tx.WithinTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return fn(ctx, txStores{u: u, q: tx})
	}, txOpts...)
	return portError(err)
}

// portError marca falhas transitórias com app.ErrUnavailable.
func portError(err error) error {
	if err == nil || errors.Is(err, app.ErrUnavailable) || !IsTransient(err) {
		return err
	}
	return fmt.Errorf("%w: %w", app.ErrUnavailable, err)
}

// txStores liga os repositórios ao pgx.Tx de uma chamada.
type txStores struct {
	u *UnitOfWork
	q Querier
}

func (s txStores) Wallets() app.WalletStore                { return walletStore(s) }
func (s txStores) Transactions() app.WagerTransactionStore { return wagerStore(s) }
func (s txStores) Ledger() app.LedgerStore                 { return ledgerStore(s) }
func (s txStores) Outbox() app.OutboxStore                 { return outboxStore(s) }

type walletStore txStores

func (s walletStore) Insert(ctx context.Context, w *domain.Wallet) error {
	err := s.u.wallets.Insert(ctx, s.q, w)
	if errors.Is(err, ErrWalletAlreadyExists) {
		return fmt.Errorf("%w: %w", app.ErrWalletAlreadyExists, err)
	}
	return err
}

type wagerStore txStores

func (s wagerStore) Insert(ctx context.Context, t *domain.WagerTransaction) error {
	return s.u.wagers.Insert(ctx, s.q, t)
}

type ledgerStore txStores

func (s ledgerStore) Append(ctx context.Context, e domain.WalletLedgerEntry) error {
	return s.u.ledger.Append(ctx, s.q, e)
}

type outboxStore txStores

func (s outboxStore) Enqueue(ctx context.Context, events ...domain.IntegrationEvent) error {
	return s.u.outbox.Enqueue(ctx, s.q, events...)
}
