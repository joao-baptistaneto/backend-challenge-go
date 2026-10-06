package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// Caso de uso OpenWallet (ARCHITECTURE.md, ADR-016).
//
// Decisões:
//
//   - Tudo o que é puro (ids, carteira, OPENING, lançamento e eventos) é
//     montado ANTES da transação. O corpo da transação só grava objetos já
//     validados, nesta ordem: carteira, OPENING, lançamento, outbox. Assim a
//     carteira está inserida (e, portanto, exclusiva desta transação) antes do
//     Ledger.Append, como exige o contrato do ledger, e o corpo é
//     reexecutável sem efeitos externos, o que permite o retry limitado de
//     falhas transitórias.
//   - Saldo inicial > 0: carteira (versão 1) + OPENING PROCESSED + lançamento
//     CREDIT (0 → saldo) + WagerTransactionProcessed e WalletBalanceChanged
//     (walletVersion 1) na outbox, no MESMO commit.
//   - Saldo inicial zero: só a carteira. Sem OPENING, ledger nem eventos.
//   - (playerId, currency) repetido: o UNIQUE do banco decide (inclusive em
//     corrida entre instâncias) e o caso de uso devolve *Error KindConflict,
//     código WALLET_ALREADY_EXISTS. Nada é gravado (rollback).
//   - A abertura não tem chave de idempotência no contrato: repetir a chamada
//     depois de um commit de resultado desconhecido devolve conflito, e o
//     chamador consulta a carteira existente.

// OpenWalletCommand é a entrada do caso de uso. O InitialBalance já vem
// parseado (domain.ParseNonNegative) pela borda.
type OpenWalletCommand struct {
	PlayerID       uuid.UUID
	InitialBalance domain.Money
	// CorrelationID é obrigatório (vai para os eventos e logs).
	CorrelationID string
	// CausationID é opcional.
	CausationID string
}

// WalletSnapshot é uma cópia imutável do estado da carteira.
type WalletSnapshot struct {
	ID        uuid.UUID
	PlayerID  uuid.UUID
	Balance   domain.Money
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// SnapshotOf copia o estado de uma carteira.
func SnapshotOf(w *domain.Wallet) WalletSnapshot {
	return WalletSnapshot{
		ID: w.ID(), PlayerID: w.PlayerID(), Balance: w.Balance(), Version: w.Version(),
		CreatedAt: w.CreatedAt(), UpdatedAt: w.UpdatedAt(),
	}
}

// OpenWalletResult é o resultado da abertura.
type OpenWalletResult struct {
	Wallet WalletSnapshot
	// OpeningTransactionID é uuid.Nil quando o saldo inicial é zero.
	OpeningTransactionID uuid.UUID
}

// OpenWallet abre carteiras.
type OpenWallet struct {
	uow    UnitOfWork
	logger *slog.Logger
	now    func() time.Time
	newID  func() (uuid.UUID, error)
}

// Option configura um caso de uso.
type Option func(*options)

type options struct {
	logger *slog.Logger
	now    func() time.Time
	newID  func() (uuid.UUID, error)
}

// WithLogger define o logger (nil = descarta).
func WithLogger(l *slog.Logger) Option { return func(o *options) { o.logger = l } }

// WithClock substitui o relógio (testes).
func WithClock(now func() time.Time) Option { return func(o *options) { o.now = now } }

// WithIDGenerator substitui o gerador de ids internos (padrão UUIDv7).
func WithIDGenerator(f func() (uuid.UUID, error)) Option {
	return func(o *options) { o.newID = f }
}

func buildOptions(opts []Option) options {
	o := options{now: time.Now, newID: uuid.NewV7}
	for _, f := range opts {
		f(&o)
	}
	if o.logger == nil {
		o.logger = slog.New(slog.DiscardHandler)
	}
	return o
}

// NewOpenWallet cria o caso de uso.
func NewOpenWallet(uow UnitOfWork, opts ...Option) (*OpenWallet, error) {
	if uow == nil {
		return nil, errors.New("app: open wallet: nil UnitOfWork")
	}
	o := buildOptions(opts)
	if o.now == nil || o.newID == nil {
		return nil, errors.New("app: open wallet: nil clock or id generator")
	}
	return &OpenWallet{
		uow: uow, now: o.now, newID: o.newID,
		logger: o.logger.With(slog.String("component", "app.open_wallet")),
	}, nil
}

const opOpenWallet = "open wallet"

// openingPlan é tudo o que a transação grava, montado antes dela.
type openingPlan struct {
	wallet  *domain.Wallet
	opening *domain.WagerTransaction // nil com saldo zero
	entry   *domain.WalletLedgerEntry
	events  []domain.IntegrationEvent
}

// Execute abre a carteira.
func (uc *OpenWallet) Execute(ctx context.Context, cmd OpenWalletCommand) (OpenWalletResult, error) {
	if err := ctx.Err(); err != nil {
		return OpenWalletResult{}, err
	}
	plan, err := uc.plan(cmd)
	if err != nil {
		return OpenWalletResult{}, err
	}

	err = uc.uow.WithinTx(ctx, TxOptions{Operation: OperationOpenWallet, Retry: true},
		func(ctx context.Context, tx Tx) error {
			if err := tx.Wallets().Insert(ctx, plan.wallet); err != nil {
				return err
			}
			if plan.opening == nil {
				return nil
			}
			if err := tx.Transactions().Insert(ctx, plan.opening); err != nil {
				return err
			}
			// A carteira foi inserida acima nesta transação: está travada
			// (contrato de LedgerStore.Append).
			if err := tx.Ledger().Append(ctx, *plan.entry); err != nil {
				return err
			}
			return tx.Outbox().Enqueue(ctx, plan.events...)
		})
	if err != nil {
		return OpenWalletResult{}, uc.classify(ctx, cmd, err)
	}

	res := OpenWalletResult{Wallet: SnapshotOf(plan.wallet)}
	attrs := []any{
		slog.String("correlationId", cmd.CorrelationID),
		slog.String("walletId", plan.wallet.ID().String()),
		slog.Bool("openingCredit", plan.opening != nil),
	}
	if plan.opening != nil {
		res.OpeningTransactionID = plan.opening.ID()
		attrs = append(attrs, slog.String("transactionId", plan.opening.ID().String()))
	}
	uc.logger.InfoContext(ctx, "carteira aberta", attrs...)
	return res, nil
}

func (uc *OpenWallet) plan(cmd OpenWalletCommand) (openingPlan, error) {
	invalid := func(code string, err error) error {
		return &Error{Kind: KindInvalidInput, Code: code, Op: opOpenWallet, Err: err}
	}
	if cmd.PlayerID == uuid.Nil {
		return openingPlan{}, invalid(CodeInvalidPlayerID, errors.New("playerId is required"))
	}
	if !cmd.InitialBalance.IsValid() {
		return openingPlan{}, invalid(CodeInvalidInitialBalance, domain.ErrUninitializedMoney)
	}
	if cmd.InitialBalance.IsNegative() {
		return openingPlan{}, invalid(CodeInvalidInitialBalance, domain.ErrNegativeAmount)
	}
	if cmd.CorrelationID == "" {
		return openingPlan{}, invalid(CodeInvalidCorrelationID, errors.New("correlationId is required"))
	}

	walletID, err := uc.newID()
	if err != nil {
		return openingPlan{}, fmt.Errorf("app: %s: generate wallet id: %w", opOpenWallet, err)
	}
	var openingID uuid.UUID
	if cmd.InitialBalance.IsPositive() {
		if openingID, err = uc.newID(); err != nil {
			return openingPlan{}, fmt.Errorf("app: %s: generate opening id: %w", opOpenWallet, err)
		}
	}
	now := uc.now().UTC()

	w, entry, err := domain.NewWallet(domain.NewWalletParams{
		ID: walletID, PlayerID: cmd.PlayerID, InitialBalance: cmd.InitialBalance,
		OpeningTransactionID: openingID, Now: now,
	})
	if err != nil {
		return openingPlan{}, invalid(CodeInvalidInitialBalance, err)
	}
	p := openingPlan{wallet: w}
	if entry == nil {
		return p, nil
	}

	opening, err := domain.NewOpeningWagerTransaction(domain.NewOpeningWagerTransactionParams{
		ID: openingID, WalletID: w.ID(), PlayerID: w.PlayerID(), Amount: w.Balance(), Now: now,
	})
	if err != nil {
		return openingPlan{}, fmt.Errorf("app: %s: build opening: %w", opOpenWallet, err)
	}
	meta := domain.EventMetadata{CorrelationID: cmd.CorrelationID, CausationID: cmd.CausationID}
	processed, err := domain.NewWagerTransactionProcessedEvent(opening, meta)
	if err != nil {
		return openingPlan{}, invalid(CodeInvalidCorrelationID, err)
	}
	// Mesma instância de carteira que produziu o lançamento (walletVersion 1).
	changed, err := domain.NewWalletBalanceChangedEvent(*entry, w, meta)
	if err != nil {
		return openingPlan{}, fmt.Errorf("app: %s: build balance changed event: %w", opOpenWallet, err)
	}
	p.opening, p.entry = opening, entry
	p.events = []domain.IntegrationEvent{processed, changed}
	return p, nil
}

// classify converte o erro da transação no erro tipado do caso de uso.
func (uc *OpenWallet) classify(ctx context.Context, cmd OpenWalletCommand, err error) error {
	switch {
	case errors.Is(err, ErrWalletAlreadyExists):
		uc.logger.InfoContext(ctx, "abertura recusada: carteira já existe",
			slog.String("correlationId", cmd.CorrelationID),
			slog.String("currency", cmd.InitialBalance.Currency().String()))
		return &Error{Kind: KindConflict, Code: CodeWalletAlreadyExists, Op: opOpenWallet, Err: err}
	case errors.Is(err, ErrUnavailable):
		uc.logger.WarnContext(ctx, "abertura falhou por indisponibilidade transitória",
			slog.String("correlationId", cmd.CorrelationID), slog.String("error", err.Error()))
		return &Error{Kind: KindUnavailable, Code: CodeTemporarilyUnavailable, Op: opOpenWallet, Err: err}
	default:
		return fmt.Errorf("app: %s: %w", opOpenWallet, err)
	}
}
