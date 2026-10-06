package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// UnitOfWork implementa app.UnitOfWork sobre o TxManager e os repositórios
// (ARCHITECTURE.md, ADR-016 e ADR-017).
//
//   - WithinTx abre UMA transação (Transactor.WithinTx) e entrega ao caso de
//     uso um app.Tx cujos stores usam todos o mesmo pgx.Tx: a fronteira
//     transacional continua explícita e auditável (ADR-013), sem pgx nas
//     portas da aplicação.
//   - TxOptions.Retry liga WithRetry(DefaultRetryPolicy(Operation)), com
//     TxOptions.RetryIf repassado à política.
//   - Tradução de erros para as sentinelas da aplicação, preservando a causa:
//     ErrWalletAlreadyExists → app.ErrWalletAlreadyExists; ErrNotFound →
//     app.ErrRecordNotFound; ErrDuplicateExternalTransaction,
//     ErrDuplicateIdempotencyKey e ErrReferenceAlreadyReversed →
//     app.ErrDuplicateWagerTransaction; ErrInboxHashMismatch →
//     app.ErrInboxHashMismatch; falha transitória (IsTransient, inclusive
//     ErrCommitOutcomeUnknown) → app.ErrUnavailable. Os demais erros
//     (inclusive os do próprio caso de uso) voltam intactos.
type UnitOfWork struct {
	tx      Transactor
	wallets *WalletRepository
	ledger  *LedgerRepository
	wagers  *WagerTransactionRepository
	outbox  *OutboxRepository
	inbox   *InboxRepository
}

var _ app.UnitOfWork = (*UnitOfWork)(nil)

// NewUnitOfWork cria a unidade de trabalho.
func NewUnitOfWork(t Transactor, w *WalletRepository, l *LedgerRepository, x *WagerTransactionRepository, o *OutboxRepository, i *InboxRepository) *UnitOfWork {
	return &UnitOfWork{tx: t, wallets: w, ledger: l, wagers: x, outbox: o, inbox: i}
}

// WithinTx executa fn numa transação (ver app.UnitOfWork).
func (u *UnitOfWork) WithinTx(ctx context.Context, opts app.TxOptions, fn app.TxFunc) error {
	var txOpts []TxOption
	if opts.Retry {
		p := DefaultRetryPolicy(opts.Operation)
		p.RetryIf = opts.RetryIf
		txOpts = append(txOpts, WithRetry(p))
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

// notFound marca ErrNotFound com app.ErrRecordNotFound.
func notFound(err error) error {
	if errors.Is(err, ErrNotFound) {
		return fmt.Errorf("%w: %w", app.ErrRecordNotFound, err)
	}
	return err
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
func (s txStores) Inbox() app.InboxStore                   { return inboxStore(s) }

type walletStore txStores

func (s walletStore) Insert(ctx context.Context, w *domain.Wallet) error {
	err := s.u.wallets.Insert(ctx, s.q, w)
	if errors.Is(err, ErrWalletAlreadyExists) {
		return fmt.Errorf("%w: %w", app.ErrWalletAlreadyExists, err)
	}
	return err
}

func (s walletStore) GetForUpdate(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	w, err := s.u.wallets.GetForUpdate(ctx, s.q, id)
	return w, notFound(err)
}

func (s walletStore) Update(ctx context.Context, w *domain.Wallet) error {
	return s.u.wallets.Update(ctx, s.q, w)
}

type wagerStore txStores

func (s wagerStore) Insert(ctx context.Context, t *domain.WagerTransaction) error {
	err := s.u.wagers.Insert(ctx, s.q, t)
	if errors.Is(err, ErrDuplicateExternalTransaction) || errors.Is(err, ErrDuplicateIdempotencyKey) ||
		errors.Is(err, ErrReferenceAlreadyReversed) {
		return fmt.Errorf("%w: %w", app.ErrDuplicateWagerTransaction, err)
	}
	return err
}

func (s wagerStore) Update(ctx context.Context, t *domain.WagerTransaction) error {
	err := s.u.wagers.Update(ctx, s.q, t)
	if errors.Is(err, ErrReferenceAlreadyReversed) {
		return fmt.Errorf("%w: %w", app.ErrDuplicateWagerTransaction, err)
	}
	return notFound(err)
}

func (s wagerStore) GetByID(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	t, err := s.u.wagers.GetByID(ctx, s.q, id)
	return t, notFound(err)
}

func (s wagerStore) GetByProviderIdempotencyKey(ctx context.Context, providerID, key string) (*domain.WagerTransaction, error) {
	t, err := s.u.wagers.GetByProviderIdempotencyKey(ctx, s.q, providerID, key)
	return t, notFound(err)
}

func (s wagerStore) GetByProviderExternalID(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	t, err := s.u.wagers.GetByProviderExternalID(ctx, s.q, providerID, externalID)
	return t, notFound(err)
}

func (s wagerStore) ListByProviderReference(ctx context.Context, providerID, referenceExternalID string) ([]*domain.WagerTransaction, error) {
	return s.u.wagers.ListByProviderReference(ctx, s.q, providerID, referenceExternalID)
}

type ledgerStore txStores

func (s ledgerStore) Append(ctx context.Context, e domain.WalletLedgerEntry) error {
	return s.u.ledger.Append(ctx, s.q, e)
}

type outboxStore txStores

func (s outboxStore) Enqueue(ctx context.Context, events ...domain.IntegrationEvent) error {
	return s.u.outbox.Enqueue(ctx, s.q, events...)
}

type inboxStore txStores

func inboxError(err error) error {
	if errors.Is(err, ErrInboxHashMismatch) {
		return fmt.Errorf("%w: %w", app.ErrInboxHashMismatch, err)
	}
	return notFound(err)
}

func (s inboxStore) Register(ctx context.Context, consumer, messageID, requestHash string, now time.Time) (app.InboxRecord, error) {
	reg, err := s.u.inbox.Register(ctx, s.q, consumer, messageID, requestHash, now)
	if err != nil {
		return app.InboxRecord{}, inboxError(err)
	}
	rec := app.InboxRecord{TransactionID: reg.Message.TransactionID}
	switch reg.Status {
	case InboxNew:
		rec.Status = app.InboxNew
	case InboxInProgress:
		rec.Status = app.InboxInProgress
	case InboxCompleted:
		rec.Status = app.InboxCompleted
	default:
		return app.InboxRecord{}, fmt.Errorf("postgres: status de inbox desconhecido: %s", reg.Status)
	}
	return rec, nil
}

func (s inboxStore) Complete(ctx context.Context, consumer, messageID, requestHash string, transactionID uuid.UUID, now time.Time) error {
	return inboxError(s.u.inbox.Complete(ctx, s.q, consumer, messageID, requestHash, transactionID, now))
}
