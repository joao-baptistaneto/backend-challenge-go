package app

import (
	"log/slog"

	"go.uber.org/fx"
)

// UseCaseParams são as dependências dos casos de uso. UnitOfWork é provida
// pelo adapter de persistência (postgres.Module).
type UseCaseParams struct {
	fx.In

	UoW    UnitOfWork
	Logger *slog.Logger `optional:"true"`
	// Reconciliations recebe a métrica de reconciliação. app não importa
	// platform: a composição (T23) provê *metrics.Metrics como
	// ReconciliationRecorder. Ausente, a reconciliação só loga.
	Reconciliations ReconciliationRecorder `optional:"true"`
}

func newOpenWalletFx(p UseCaseParams) (*OpenWallet, error) {
	return NewOpenWallet(p.UoW, WithLogger(p.Logger))
}

func newProcessWagerFx(p UseCaseParams) (*ProcessWagerTransaction, error) {
	return NewProcessWagerTransaction(p.UoW, WithLogger(p.Logger))
}

func newReconcileWalletFx(p UseCaseParams) (*ReconcileWallet, error) {
	return NewReconcileWallet(p.UoW, WithLogger(p.Logger), WithReconciliationRecorder(p.Reconciliations))
}

func newQueriesFx(p UseCaseParams) (*Queries, error) {
	return NewQueries(p.UoW, WithLogger(p.Logger))
}

// Module provê os casos de uso (*OpenWallet, *ProcessWagerTransaction,
// *ReconcileWallet, *Queries). Depende de UnitOfWork; *slog.Logger e
// ReconciliationRecorder são opcionais.
var Module = fx.Module("app",
	fx.Provide(newOpenWalletFx, newProcessWagerFx, newReconcileWalletFx, newQueriesFx),
)
