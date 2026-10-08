package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// Consultas (ARCHITECTURE.md, ADR-018): carteira, extrato paginado e
// transação por id ou por (providerId, externalTransactionId).
//
// Decisões:
//
//   - Toda consulta roda numa transação READ ONLY (o banco recusa escrita) e
//     sem lock: não bloqueia nem é bloqueada pelos escritores. O corpo só lê,
//     então é reexecutável (Retry).
//   - ListLedger verifica a existência da carteira e lê a página na mesma
//     transação. Carteira inexistente → WALLET_NOT_FOUND (não uma página
//     vazia). A paginação é a keyset por seq com cursor opaco (ADR-014).
//   - Autorização por provedor: GetTransaction e GetTransactionByProvider
//     recebem o providerId EXPLÍCITO, que a borda (ADR-020) preenche com a
//     identidade autenticada, nunca com um valor do path ou do corpo sem
//     conferência. Transação de outro provedor (ou interna, como OPENING) é
//     respondida exatamente como inexistente (TRANSACTION_NOT_FOUND), sem
//     revelar que existe. Em GetTransactionByProvider o filtro está no SQL
//     (a chave é (provider, externalId)); em GetTransaction a linha é lida
//     pelo id e descartada se o provedor não casar.
//   - A visão da transação aplica as mesmas regras de exposição do resultado
//     do processamento (ADR-017): referenceTransactionId só em PROCESSED e
//     saldo omitido em WALLET_NOT_FOUND/WALLET_PLAYER_MISMATCH/
//     CURRENCY_MISMATCH. A chave de idempotência e o hash não são expostos.
//   - Autorização de carteira (só serviço interno) é da borda (ADR-019/ADR-020):
//     GetWallet/ListLedger não recebem identidade.

// DefaultLedgerPageLimit é o tamanho de página usado quando o chamador não
// informa (Limit = 0). MaxLedgerPageLimit é o maior aceito (mesmos limites
// do adapter, ADR-014).
const (
	DefaultLedgerPageLimit = 50
	MaxLedgerPageLimit     = 200
)

// Queries agrupa as consultas de leitura.
type Queries struct {
	uow    UnitOfWork
	logger *slog.Logger
}

// NewQueries cria as consultas.
func NewQueries(uow UnitOfWork, opts ...Option) (*Queries, error) {
	if uow == nil {
		return nil, errors.New("app: queries: nil UnitOfWork")
	}
	o := buildOptions(opts)
	return &Queries{uow: uow, logger: o.logger.With(slog.String("component", "app.queries"))}, nil
}

const (
	opGetWallet      = "get wallet"
	opListLedger     = "list ledger"
	opGetTransaction = "get transaction"
)

// queryTxOptions: somente leitura, READ COMMITTED, reexecutável.
var queryTxOptions = TxOptions{Operation: OperationQuery, Retry: true, ReadOnly: true}

// GetWallet devolve o estado atual da carteira.
func (q *Queries) GetWallet(ctx context.Context, walletID uuid.UUID) (WalletSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return WalletSnapshot{}, err
	}
	if walletID == uuid.Nil {
		return WalletSnapshot{}, invalidQuery(opGetWallet, CodeInvalidWalletID, "walletId is required")
	}
	var snap WalletSnapshot
	err := q.uow.WithinTx(ctx, queryTxOptions, func(ctx context.Context, tx Tx) error {
		w, err := tx.Wallets().Get(ctx, walletID)
		if err != nil {
			return err
		}
		snap = SnapshotOf(w)
		return nil
	})
	if err != nil {
		return WalletSnapshot{}, classifyQuery(opGetWallet, CodeWalletNotFound, err)
	}
	return snap, nil
}

// ListLedgerQuery é a entrada de ListLedger.
type ListLedgerQuery struct {
	WalletID uuid.UUID
	// Cursor é o NextCursor de uma página anterior (vazio = início).
	Cursor string
	// Limit em [1, MaxLedgerPageLimit]; 0 usa DefaultLedgerPageLimit.
	Limit int
}

// LedgerEntryView é um lançamento do extrato.
type LedgerEntryView struct {
	ID            uuid.UUID
	WalletID      uuid.UUID
	TransactionID uuid.UUID
	Direction     domain.LedgerDirection
	Amount        domain.Money
	BalanceBefore domain.Money
	BalanceAfter  domain.Money
	CreatedAt     time.Time
}

// LedgerPageView é uma página do extrato.
type LedgerPageView struct {
	Entries []LedgerEntryView
	// NextCursor é vazio na última página.
	NextCursor string
}

// ListLedger devolve uma página do extrato em ordem de lançamento.
func (q *Queries) ListLedger(ctx context.Context, in ListLedgerQuery) (LedgerPageView, error) {
	if err := ctx.Err(); err != nil {
		return LedgerPageView{}, err
	}
	if in.WalletID == uuid.Nil {
		return LedgerPageView{}, invalidQuery(opListLedger, CodeInvalidWalletID, "walletId is required")
	}
	limit := in.Limit
	if limit == 0 {
		limit = DefaultLedgerPageLimit
	}
	if limit < 1 || limit > MaxLedgerPageLimit {
		return LedgerPageView{}, invalidQuery(opListLedger, CodeInvalidPageLimit,
			fmt.Sprintf("limit must be between 1 and %d", MaxLedgerPageLimit))
	}
	var page LedgerPage
	err := q.uow.WithinTx(ctx, queryTxOptions, func(ctx context.Context, tx Tx) error {
		if _, err := tx.Wallets().Get(ctx, in.WalletID); err != nil {
			return err
		}
		p, err := tx.Ledger().List(ctx, in.WalletID, in.Cursor, limit)
		if err != nil {
			return err
		}
		page = p
		return nil
	})
	if err != nil {
		return LedgerPageView{}, classifyQuery(opListLedger, CodeWalletNotFound, err)
	}
	out := LedgerPageView{Entries: make([]LedgerEntryView, 0, len(page.Entries)), NextCursor: page.NextCursor}
	for _, e := range page.Entries {
		out.Entries = append(out.Entries, LedgerEntryView{
			ID: e.ID(), WalletID: e.WalletID(), TransactionID: e.TransactionID(), Direction: e.Direction(),
			Amount: e.Amount(), BalanceBefore: e.BalanceBefore(), BalanceAfter: e.BalanceAfter(),
			CreatedAt: e.CreatedAt(),
		})
	}
	return out, nil
}

// TransactionView é a visão de uma transação devolvida ao provedor.
type TransactionView struct {
	TransactionID                  uuid.UUID
	ProviderID                     string
	ExternalTransactionID          string
	WalletID                       uuid.UUID
	PlayerID                       uuid.UUID
	RoundID                        string
	GameID                         string
	Kind                           domain.WagerKind
	Status                         domain.WagerStatus
	Money                          domain.Money
	ReferenceExternalTransactionID string
	// ReferenceTransactionID só em PROCESSED (ver resultOf, ADR-017).
	ReferenceTransactionID uuid.UUID
	// Balance é o saldo observado no processamento (balance_after); Money{}
	// quando não se aplica ou não pode ser exposto.
	Balance     domain.Money
	FailureCode domain.FailureCode
	// Attempts e NextAttemptAt permitem acompanhar PENDING_REFERENCE.
	Attempts      int32
	NextAttemptAt time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
	CompletedAt   time.Time
}

// GetTransactionQuery busca pelo id interno, restrita ao provedor.
type GetTransactionQuery struct {
	TransactionID uuid.UUID
	// ProviderID é a identidade autenticada do chamador.
	ProviderID string
}

// GetTransaction devolve a transação do provedor pelo id interno.
// Inexistente ou de outro provedor: TRANSACTION_NOT_FOUND.
func (q *Queries) GetTransaction(ctx context.Context, in GetTransactionQuery) (TransactionView, error) {
	if err := ctx.Err(); err != nil {
		return TransactionView{}, err
	}
	if in.TransactionID == uuid.Nil {
		return TransactionView{}, invalidQuery(opGetTransaction, CodeInvalidTransactionID, "transactionId is required")
	}
	if !validQueryIdentifier(in.ProviderID) {
		return TransactionView{}, invalidQuery(opGetTransaction, CodeInvalidProviderID, "invalid providerId")
	}
	return q.getTransaction(ctx, in.ProviderID, func(ctx context.Context, tx Tx) (*domain.WagerTransaction, error) {
		return tx.Transactions().GetByID(ctx, in.TransactionID)
	})
}

// GetTransactionByProviderQuery busca por (providerId, externalTransactionId).
type GetTransactionByProviderQuery struct {
	// ProviderID é a identidade autenticada do chamador.
	ProviderID            string
	ExternalTransactionID string
}

// GetTransactionByProvider devolve a transação do provedor pelo id externo.
// Inexistente (para este provedor): TRANSACTION_NOT_FOUND.
func (q *Queries) GetTransactionByProvider(ctx context.Context, in GetTransactionByProviderQuery) (TransactionView, error) {
	if err := ctx.Err(); err != nil {
		return TransactionView{}, err
	}
	if !validQueryIdentifier(in.ProviderID) {
		return TransactionView{}, invalidQuery(opGetTransaction, CodeInvalidProviderID, "invalid providerId")
	}
	if !validQueryIdentifier(in.ExternalTransactionID) {
		return TransactionView{}, invalidQuery(opGetTransaction, CodeInvalidExternalTransactionID, "invalid externalTransactionId")
	}
	return q.getTransaction(ctx, in.ProviderID, func(ctx context.Context, tx Tx) (*domain.WagerTransaction, error) {
		return tx.Transactions().GetByProviderExternalID(ctx, in.ProviderID, in.ExternalTransactionID)
	})
}

func (q *Queries) getTransaction(ctx context.Context, providerID string,
	read func(context.Context, Tx) (*domain.WagerTransaction, error),
) (TransactionView, error) {
	var view TransactionView
	err := q.uow.WithinTx(ctx, queryTxOptions, func(ctx context.Context, tx Tx) error {
		t, err := read(ctx, tx)
		if err != nil {
			return err
		}
		// Isolamento entre provedores: a transação de outro provedor (ou
		// interna, sem provedor) é tratada como inexistente.
		if t.ProviderID() != providerID {
			return fmt.Errorf("%w: transaction of another provider", ErrRecordNotFound)
		}
		view = transactionViewOf(t)
		return nil
	})
	if err != nil {
		return TransactionView{}, classifyQuery(opGetTransaction, CodeTransactionNotFound, err)
	}
	return view, nil
}

func transactionViewOf(t *domain.WagerTransaction) TransactionView {
	v := TransactionView{
		TransactionID: t.ID(), ProviderID: t.ProviderID(), ExternalTransactionID: t.ExternalTransactionID(),
		WalletID: t.WalletID(), PlayerID: t.PlayerID(), RoundID: t.RoundID(), GameID: t.GameID(),
		Kind: t.Kind(), Status: t.Status(), Money: t.Money(),
		ReferenceExternalTransactionID: t.ReferenceExternalTransactionID(),
		FailureCode:                    t.FailureCode(),
		Attempts:                       t.Attempts(), NextAttemptAt: t.NextAttemptAt(),
		CreatedAt: t.CreatedAt(), UpdatedAt: t.UpdatedAt(), CompletedAt: t.CompletedAt(),
	}
	if b := t.BalanceAfter(); b.IsValid() && !slices.Contains(hiddenBalanceCodes, t.FailureCode()) {
		v.Balance = b
	}
	if t.Status() == domain.WagerStatusProcessed {
		v.ReferenceTransactionID = t.ReferenceTransactionID()
	}
	return v
}

func invalidQuery(op, code, msg string) error {
	return &Error{Kind: KindInvalidInput, Code: code, Op: op, Err: errors.New(msg)}
}

// validQueryIdentifier: não vazio, até domain.MaxWagerIdentifierLength bytes,
// UTF-8 válido e sem caracteres de controle (os mesmos limites com que os
// identificadores são gravados; um valor fora deles não existe no banco).
func validQueryIdentifier(v string) bool {
	if v == "" || len(v) > domain.MaxWagerIdentifierLength || !utf8.ValidString(v) {
		return false
	}
	for _, r := range v {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}
