package sqs

import (
	"context"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	httpadapter "github.com/junglegaming/backend-challenge-go/internal/adapters/http"
	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
)

func fxConfig(f *fakeSQS) config.Config {
	var c config.Config
	c.AWS = config.AWSConfig{Region: "us-east-1", AccessKeyID: "test", SecretAccessKey: "test", EndpointURL: "http://127.0.0.1:1"}
	c.SQS = config.SQSConfig{
		InboundQueueURL: f.queueURL, InboundDLQURL: f.dlqURL,
		Consumer: config.SQSConsumerConfig{
			Name: "fx-consumer", Concurrency: 2, WaitTime: time.Second, VisibilityTimeout: 30 * time.Second,
			ProcessTimeout: 5 * time.Second, RetryBaseDelay: time.Second, RetryMaxDelay: 4 * time.Second,
			DrainTimeout: 2 * time.Second, MaxAttempts: 2,
		},
	}
	return c
}

func TestModuleValidateApp(t *testing.T) {
	f := newFakeSQS(3)
	if err := fx.ValidateApp(
		fx.Supply(fxConfig(f)),
		postgres.Module, app.Module, Module,
	); err != nil {
		t.Fatal(err)
	}
}

// Composição Fx real do consumidor (fila falsa): o OnStart inicia o laço, a
// mensagem é processada, a readiness "sqs" está no grupo e o OnStop encerra
// todas as goroutines (goleak no TestMain).
func TestConsumerModuleLifecycle(t *testing.T) {
	f := newFakeSQS(3)
	f.send("g", envelopeJSON(t, "msg-1", "tx-1", nil))
	p := &fakeProcessor{}
	var checks []httpadapter.ReadinessCheck
	fxApp := fxtest.New(t,
		fx.Supply(fxConfig(f)),
		fx.Provide(
			func() API { return f },
			func() WagerProcessor { return p },
		),
		ConsumerModule,
		fx.Invoke(fx.Annotate(func(cs []httpadapter.ReadinessCheck) { checks = cs },
			fx.ParamTags(`group:"`+httpadapter.ReadinessGroup+`"`))),
	)
	fxApp.RequireStart()
	waitFor(t, "delete", func() bool { return len(f.state().deleted) == 1 })
	if len(checks) != 1 || checks[0].Name != ReadinessCheckName {
		t.Fatalf("readiness: %+v", checks)
	}
	if err := checks[0].Check(context.Background()); err != nil {
		t.Fatalf("readiness falhou: %v", err)
	}
	fxApp.RequireStop()
	if p.snapshot()[0].Inbox.ConsumerName != "fx-consumer" {
		t.Fatal("o nome do consumidor deve vir da configuração")
	}
}

// O cliente real é criado sem rede (credenciais estáticas) e fechado no
// OnStop.
func TestClientModuleLifecycle(t *testing.T) {
	f := newFakeSQS(3)
	var client *Client
	fxApp := fxtest.New(t, fx.Supply(fxConfig(f)), ClientModule, fx.Populate(&client))
	fxApp.RequireStart()
	if client == nil || client.SQS() == nil {
		t.Fatal("cliente não provido")
	}
	fxApp.RequireStop()
}
