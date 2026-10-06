package sqs

import (
	"context"
	"log/slog"

	"go.uber.org/fx"

	httpadapter "github.com/junglegaming/backend-challenge-go/internal/adapters/http"
	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

// ReadinessCheckName é o nome da verificação de prontidão do consumidor.
const ReadinessCheckName = "sqs"

// ClientParams são as dependências Fx do cliente.
type ClientParams struct {
	fx.In

	Lifecycle fx.Lifecycle
	Config    config.Config
}

// NewClientFx cria o cliente e registra Close (conexões ociosas) no OnStop.
// Como o cliente é construído antes de quem o usa, o seu OnStop roda depois
// dos OnStop do consumidor e do publisher.
func NewClientFx(p ClientParams) (*Client, error) {
	c, err := NewClient(context.Background(), p.Config.AWS)
	if err != nil {
		return nil, err
	}
	p.Lifecycle.Append(fx.Hook{OnStop: c.Close})
	return c, nil
}

// ConsumerParams são as dependências Fx do consumidor.
type ConsumerParams struct {
	fx.In

	Lifecycle fx.Lifecycle
	Config    config.Config
	API       API
	Processor WagerProcessor
	Metrics   *metrics.Metrics `optional:"true"`
	Logger    *slog.Logger     `optional:"true"`
}

// NewConsumerFx cria o consumidor e registra Start/Stop no ciclo de vida.
func NewConsumerFx(p ConsumerParams) (*Consumer, error) {
	c, err := NewConsumer(ConsumerConfigFrom(p.Config), p.API, p.Processor,
		WithLogger(p.Logger), WithMetrics(p.Metrics))
	if err != nil {
		return nil, err
	}
	p.Lifecycle.Append(fx.Hook{OnStart: c.Start, OnStop: c.Stop})
	return c, nil
}

// ClientModule provê *Client (com Close no OnStop) e API. Será reaproveitado
// pelo publisher da outbox (T22). Depende de config.Config.
var ClientModule = fx.Module("sqs-client",
	fx.Provide(
		NewClientFx,
		func(c *Client) API { return c.SQS() },
	),
)

// ConsumerModule instancia o *Consumer com seu ciclo de vida e contribui a
// verificação "sqs" ao grupo readinessChecks. Depende de config.Config, API
// e WagerProcessor; métricas e logger são opcionais.
var ConsumerModule = fx.Module("sqs-consumer",
	fx.Provide(
		NewConsumerFx,
		fx.Annotate(
			func(c *Consumer) httpadapter.ReadinessCheck {
				return httpadapter.ReadinessCheck{Name: ReadinessCheckName, Check: c.Ready}
			},
			fx.ResultTags(`group:"`+httpadapter.ReadinessGroup+`"`),
		),
	),
	fx.Invoke(func(*Consumer) {}),
)

// Module compõe cliente e consumidor e liga o caso de uso
// (*app.ProcessWagerTransaction, de app.Module) como WagerProcessor.
var Module = fx.Options(
	ClientModule,
	ConsumerModule,
	fx.Provide(func(p *app.ProcessWagerTransaction) WagerProcessor { return p }),
)
