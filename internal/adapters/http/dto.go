package httpadapter

import (
	"time"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// Corpos de resposta. Valores monetários são sempre domain.Money
// ({"amount":"25.00","currency":"BRL"}, strings); timestamps em RFC 3339 UTC.
// Campos opcionais usam omitzero/omitempty: um Money ausente é omitido, nunca
// serializado como zero.

type walletResponse struct {
	ID        uuid.UUID    `json:"id"`
	PlayerID  uuid.UUID    `json:"playerId"`
	Balance   domain.Money `json:"balance"`
	Version   int64        `json:"version"`
	CreatedAt time.Time    `json:"createdAt,omitzero"`
	UpdatedAt time.Time    `json:"updatedAt,omitzero"`
}

func walletResponseOf(s app.WalletSnapshot) walletResponse {
	return walletResponse{
		ID: s.ID, PlayerID: s.PlayerID, Balance: s.Balance, Version: s.Version,
		CreatedAt: s.CreatedAt.UTC(), UpdatedAt: s.UpdatedAt.UTC(),
	}
}

type ledgerEntryResponse struct {
	ID            uuid.UUID              `json:"id"`
	TransactionID uuid.UUID              `json:"transactionId"`
	Direction     domain.LedgerDirection `json:"direction"`
	Amount        domain.Money           `json:"amount"`
	BalanceBefore domain.Money           `json:"balanceBefore"`
	BalanceAfter  domain.Money           `json:"balanceAfter"`
	CreatedAt     time.Time              `json:"createdAt"`
}

type ledgerPageResponse struct {
	WalletID   uuid.UUID             `json:"walletId"`
	Entries    []ledgerEntryResponse `json:"entries"`
	NextCursor string                `json:"nextCursor,omitempty"`
}

func ledgerPageResponseOf(walletID uuid.UUID, p app.LedgerPageView) ledgerPageResponse {
	out := ledgerPageResponse{
		WalletID: walletID, NextCursor: p.NextCursor,
		Entries: make([]ledgerEntryResponse, 0, len(p.Entries)),
	}
	for _, e := range p.Entries {
		out.Entries = append(out.Entries, ledgerEntryResponse{
			ID: e.ID, TransactionID: e.TransactionID, Direction: e.Direction, Amount: e.Amount,
			BalanceBefore: e.BalanceBefore, BalanceAfter: e.BalanceAfter, CreatedAt: e.CreatedAt.UTC(),
		})
	}
	return out
}

type reconciliationResponse struct {
	WalletID          uuid.UUID    `json:"walletId"`
	StoredBalance     domain.Money `json:"storedBalance"`
	CalculatedBalance domain.Money `json:"calculatedBalance"`
	Difference        domain.Money `json:"difference"`
	Consistent        bool         `json:"consistent"`
	CheckedEntries    int64        `json:"checkedEntries"`
	WalletVersion     int64        `json:"walletVersion"`
}

func reconciliationResponseOf(r app.ReconciliationReport) reconciliationResponse {
	return reconciliationResponse{
		WalletID: r.WalletID, StoredBalance: r.StoredBalance, CalculatedBalance: r.CalculatedBalance,
		Difference: r.Difference, Consistent: r.Consistent, CheckedEntries: r.CheckedEntries,
		WalletVersion: r.WalletVersion,
	}
}

// wagerResultResponse é a resposta de POST /wagering/transactions (novo
// processamento ou replay).
type wagerResultResponse struct {
	TransactionID uuid.UUID          `json:"transactionId"`
	Status        domain.WagerStatus `json:"status"`
	// Balance é o saldo observado no processamento original (omitido quando
	// não se aplica ou não pode ser exposto).
	Balance                *domain.Money          `json:"balance,omitempty"`
	FailureCode            domain.FailureCode     `json:"failureCode,omitempty"`
	FailureCategory        domain.FailureCategory `json:"failureCategory,omitempty"`
	ReferenceTransactionID uuid.UUID              `json:"referenceTransactionId,omitzero"`
	IdempotentReplay       bool                   `json:"idempotentReplay"`
}

func wagerResultResponseOf(r app.WagerResult) wagerResultResponse {
	out := wagerResultResponse{
		TransactionID: r.TransactionID, Status: r.Status, FailureCode: r.FailureCode,
		ReferenceTransactionID: r.ReferenceTransactionID, IdempotentReplay: r.IdempotentReplay,
	}
	if r.FailureCode != "" {
		out.FailureCategory = r.FailureCode.Category()
	}
	if r.Balance.IsValid() {
		b := r.Balance
		out.Balance = &b
	}
	return out
}

// transactionResponse é a visão de GET /wagering/transactions/{id} e
// GET /providers/{providerId}/wagering/transactions/{externalId}. Não expõe
// a chave de idempotência nem o hash.
type transactionResponse struct {
	TransactionID                  uuid.UUID              `json:"transactionId"`
	ProviderID                     string                 `json:"providerId"`
	ExternalTransactionID          string                 `json:"externalTransactionId"`
	WalletID                       uuid.UUID              `json:"walletId"`
	PlayerID                       uuid.UUID              `json:"playerId"`
	RoundID                        string                 `json:"roundId"`
	GameID                         string                 `json:"gameId"`
	Kind                           domain.WagerKind       `json:"kind"`
	Status                         domain.WagerStatus     `json:"status"`
	Money                          domain.Money           `json:"money"`
	ReferenceExternalTransactionID string                 `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         uuid.UUID              `json:"referenceTransactionId,omitzero"`
	Balance                        *domain.Money          `json:"balance,omitempty"`
	FailureCode                    domain.FailureCode     `json:"failureCode,omitempty"`
	FailureCategory                domain.FailureCategory `json:"failureCategory,omitempty"`
	Attempts                       int32                  `json:"attempts"`
	NextAttemptAt                  time.Time              `json:"nextAttemptAt,omitzero"`
	CreatedAt                      time.Time              `json:"createdAt"`
	UpdatedAt                      time.Time              `json:"updatedAt"`
	CompletedAt                    time.Time              `json:"completedAt,omitzero"`
}

func transactionResponseOf(v app.TransactionView) transactionResponse {
	out := transactionResponse{
		TransactionID: v.TransactionID, ProviderID: v.ProviderID, ExternalTransactionID: v.ExternalTransactionID,
		WalletID: v.WalletID, PlayerID: v.PlayerID, RoundID: v.RoundID, GameID: v.GameID,
		Kind: v.Kind, Status: v.Status, Money: v.Money,
		ReferenceExternalTransactionID: v.ReferenceExternalTransactionID,
		ReferenceTransactionID:         v.ReferenceTransactionID,
		FailureCode:                    v.FailureCode,
		Attempts:                       v.Attempts,
		NextAttemptAt:                  utcOrZero(v.NextAttemptAt),
		CreatedAt:                      v.CreatedAt.UTC(),
		UpdatedAt:                      v.UpdatedAt.UTC(),
		CompletedAt:                    utcOrZero(v.CompletedAt),
	}
	if v.FailureCode != "" {
		out.FailureCategory = v.FailureCode.Category()
	}
	if v.Balance.IsValid() {
		b := v.Balance
		out.Balance = &b
	}
	return out
}

func utcOrZero(t time.Time) time.Time {
	if t.IsZero() {
		return time.Time{}
	}
	return t.UTC()
}
