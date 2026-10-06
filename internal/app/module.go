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
}

func newOpenWalletFx(p UseCaseParams) (*OpenWallet, error) {
	return NewOpenWallet(p.UoW, WithLogger(p.Logger))
}

// Module provê os casos de uso (*OpenWallet). Depende de UnitOfWork;
// *slog.Logger é opcional.
var Module = fx.Module("app",
	fx.Provide(newOpenWalletFx),
)
