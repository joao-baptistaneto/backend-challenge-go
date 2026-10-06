package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"
)

// HandlerName é o nome Fx do http.Handler de /metrics, para que o adapter
// HTTP o receba sem ambiguidade com outros handlers.
const HandlerName = "metricsHandler"

// Module provê:
//   - *prometheus.Registry (registry isolado), também como
//     prometheus.Registerer e prometheus.Gatherer, para que adapters
//     registrem coletores próprios (ex.: estatísticas do pgxpool) no mesmo
//     registry;
//   - *Metrics com todas as métricas da aplicação;
//   - http.Handler nomeado "metricsHandler" para /metrics.
//
// Não depende de config: métricas não têm parâmetros de ambiente.
var Module = fx.Module("metrics",
	fx.Provide(
		fx.Annotate(NewRegistry,
			fx.As(fx.Self()),
			fx.As(new(prometheus.Registerer)),
			fx.As(new(prometheus.Gatherer)),
		),
		New,
		fx.Annotate(
			func(g prometheus.Gatherer) http.Handler { return Handler(g) },
			fx.ResultTags(`name:"`+HandlerName+`"`),
		),
	),
)
