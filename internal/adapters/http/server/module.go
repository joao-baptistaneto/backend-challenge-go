package httpserver

import (
	"log/slog"
	"net/http"

	"go.uber.org/fx"

	httpadapter "github.com/junglegaming/backend-challenge-go/internal/adapters/http"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
)

// ConfigFrom traduz a configuração validada da aplicação.
func ConfigFrom(c config.Config) Config {
	return Config{
		Addr:              c.App.HTTPAddr,
		ReadHeaderTimeout: c.HTTP.ReadHeaderTimeout,
		ReadTimeout:       c.HTTP.ReadTimeout,
		WriteTimeout:      c.HTTP.WriteTimeout,
		IdleTimeout:       c.HTTP.IdleTimeout,
		DrainDelay:        c.HTTP.DrainDelay,
		ShutdownTimeout:   c.App.ShutdownTimeout,
	}
}

// Params são as dependências Fx do servidor.
type Params struct {
	fx.In

	Lifecycle  fx.Lifecycle
	Shutdowner fx.Shutdowner
	Config     config.Config
	Handler    http.Handler `name:"apiHandler"`
	Drain      *Drain
	Logger     *slog.Logger `optional:"true"`
}

// NewFx cria o servidor e registra Start/Stop no ciclo de vida. Uma falha
// inesperada do Serve derruba a aplicação (exit code 1) via fx.Shutdowner.
func NewFx(p Params) *Server {
	s := New(ConfigFrom(p.Config), p.Handler, p.Drain, p.Logger)
	s.onServeError = func(error) {
		_ = p.Shutdowner.Shutdown(fx.ExitCode(1))
	}
	p.Lifecycle.Append(fx.Hook{OnStart: s.Start, OnStop: s.Stop})
	return s
}

// Module provê *Drain, contribui a verificação de prontidão "http-server"
// para /health/ready (grupo httpadapter.ReadinessGroup) e instancia o
// *Server com seu ciclo de vida. Depende de config.Config e do http.Handler
// nomeado httpadapter.HandlerName (httpadapter.Module).
//
// Ordem de parada: o Fx executa os OnStop na ordem inversa dos OnStart. Como
// o servidor depende (via handler) dos casos de uso e estes do pool/clients,
// o servidor é iniciado depois e parado antes deles: as requisições em voo
// drenam com as dependências ainda abertas.
var Module = fx.Module("http-server",
	fx.Provide(
		NewDrain,
		fx.Annotate(
			func(d *Drain) httpadapter.ReadinessCheck { return d.ReadinessCheck() },
			fx.ResultTags(`group:"`+httpadapter.ReadinessGroup+`"`),
		),
		NewFx,
	),
	fx.Invoke(func(*Server) {}),
)
