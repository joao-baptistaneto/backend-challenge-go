package httpadapter

import (
	"log/slog"
	"net/http"

	"go.uber.org/fx"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/auth"
	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

// HandlerName é o nome Fx do http.Handler da API (o servidor da T19 o
// recebe por este nome).
const HandlerName = "apiHandler"

// ReadinessGroup é o grupo Fx das verificações de /health/ready. A
// composição contribui, por exemplo:
//
//	fx.Provide(fx.Annotate(
//		func(db *postgres.DB) httpadapter.ReadinessCheck {
//			return httpadapter.ReadinessCheck{Name: "postgres", Check: db.Ping}
//		},
//		fx.ResultTags(`group:"readinessChecks"`),
//	))
const ReadinessGroup = "readinessChecks"

// Params são as dependências Fx da API.
type Params struct {
	fx.In

	OpenWallet *app.OpenWallet
	Process    *app.ProcessWagerTransaction
	Reconcile  *app.ReconcileWallet
	Queries    *app.Queries

	Authenticator *auth.Authenticator
	Authorizer    *auth.Authorizer

	Metrics        *metrics.Metrics `optional:"true"`
	MetricsHandler http.Handler     `name:"metricsHandler" optional:"true"`
	// MetricsAccess é opcional; ausente = MetricsInternal.
	MetricsAccess MetricsAccess    `optional:"true"`
	Readiness     []ReadinessCheck `group:"readinessChecks"`
	Logger        *slog.Logger     `optional:"true"`
}

// NewHandlerFx monta a API a partir do grafo Fx.
func NewHandlerFx(p Params) (http.Handler, error) {
	return NewHandler(Deps{
		OpenWallet: p.OpenWallet, Process: p.Process, Reconcile: p.Reconcile, Queries: p.Queries,
		Authenticator: p.Authenticator, Authorizer: p.Authorizer,
		Metrics: p.Metrics, MetricsHandler: p.MetricsHandler, MetricsAccess: p.MetricsAccess,
		Readiness: p.Readiness, Logger: p.Logger,
	})
}

// Module provê o http.Handler nomeado "apiHandler". Depende dos casos de
// uso (app.Module), de *auth.Authenticator e *auth.Authorizer (auth.Module);
// métricas, logger, MetricsAccess e verificações de prontidão são opcionais.
// O servidor e seu ciclo de vida são da T19.
var Module = fx.Module("http",
	fx.Provide(fx.Annotate(NewHandlerFx, fx.ResultTags(`name:"`+HandlerName+`"`))),
)
