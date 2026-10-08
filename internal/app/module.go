package app

import (
	"log/slog"
	"time"

	"go.uber.org/fx"
)

// ReferenceRetryDelay é o atraso da primeira nova busca de referência de uma
// operação aceita como PENDING_REFERENCE (WithReferenceRetryDelay). A
// composição (cmd/server, ADR-025) o provê a partir de
// PENDING_REFERENCE_RETRY_BASE_DELAY, a mesma base do backoff do worker de
// pendências (ADR-023). Ausente ou zero → DefaultReferenceRetryDelay.
type ReferenceRetryDelay time.Duration

// UseCaseParams são as dependências dos casos de uso. UnitOfWork é provida
// pelo adapter de persistência (postgres.Module).
type UseCaseParams struct {
	fx.In

	UoW    UnitOfWork
	Logger *slog.Logger `optional:"true"`
	// Reconciliations recebe a métrica de reconciliação. app não importa
	// platform: a composição (ADR-025) provê *metrics.Metrics como
	// ReconciliationRecorder. Ausente, a reconciliação só loga.
	Reconciliations ReconciliationRecorder `optional:"true"`
	// ReferenceRetryDelay é opcional (ver o tipo).
	ReferenceRetryDelay ReferenceRetryDelay `optional:"true"`
}

func newOpenWalletFx(p UseCaseParams) (*OpenWallet, error) {
	return NewOpenWallet(p.UoW, WithLogger(p.Logger))
}

func newProcessWagerFx(p UseCaseParams) (*ProcessWagerTransaction, error) {
	opts := []Option{WithLogger(p.Logger)}
	if p.ReferenceRetryDelay != 0 {
		opts = append(opts, WithReferenceRetryDelay(time.Duration(p.ReferenceRetryDelay)))
	}
	return NewProcessWagerTransaction(p.UoW, opts...)
}

func newReconcileWalletFx(p UseCaseParams) (*ReconcileWallet, error) {
	return NewReconcileWallet(p.UoW, WithLogger(p.Logger), WithReconciliationRecorder(p.Reconciliations))
}

func newQueriesFx(p UseCaseParams) (*Queries, error) {
	return NewQueries(p.UoW, WithLogger(p.Logger))
}

// Module provê os casos de uso (*OpenWallet, *ProcessWagerTransaction,
// *ReconcileWallet, *Queries). Depende de UnitOfWork; *slog.Logger,
// ReconciliationRecorder e ReferenceRetryDelay são opcionais.
var Module = fx.Module("app",
	fx.Provide(newOpenWalletFx, newProcessWagerFx, newReconcileWalletFx, newQueriesFx),
)
