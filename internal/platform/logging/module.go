package logging

import (
	"io"
	"log/slog"
	"os"

	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
)

// ServiceName identifica o serviço em todos os logs.
const ServiceName = "wallet"

// OutputName é o nome Fx de um io.Writer opcional que substitui os.Stdout
// como destino dos logs (usado em testes).
const OutputName = "logOutput"

// Params são as dependências de NewFromConfig.
type Params struct {
	fx.In

	Config config.Config
	Output io.Writer `name:"logOutput" optional:"true"`
}

// NewFromConfig cria o logger da aplicação a partir da configuração validada:
// nível de LOG_LEVEL, saída em stdout (padrão para containers) e atributos
// fixos service/env.
func NewFromConfig(p Params) *slog.Logger {
	out := p.Output
	if out == nil {
		out = os.Stdout
	}
	return New(out, Options{Level: p.Config.App.LogLevel}).With(
		slog.String("service", ServiceName),
		slog.String("env", p.Config.App.Env),
	)
}

// Module provê *slog.Logger. Depende de config.Config (config.Module).
var Module = fx.Module("logging",
	fx.Provide(NewFromConfig),
)

// WithOutput provê w como destino dos logs no lugar de os.Stdout. O writer
// precisa ser seguro para uso concorrente (o JSONHandler serializa cada
// registro em uma única chamada Write, mas não sincroniza writers distintos).
func WithOutput(w io.Writer) fx.Option {
	return fx.Provide(fx.Annotate(
		func() io.Writer { return w },
		fx.ResultTags(`name:"`+OutputName+`"`),
	))
}

// FxEventLogger direciona os eventos internos do Fx (provide/invoke/start/
// stop) para o mesmo logger JSON. Deve ser usado no nível raiz do app
// (cmd/server), pois fx.WithLogger dentro de um fx.Module só afeta o módulo.
func FxEventLogger() fx.Option {
	return fx.WithLogger(func(l *slog.Logger) fxevent.Logger {
		return &fxevent.SlogLogger{Logger: l.With(slog.String("component", "fx"))}
	})
}
