package pendingworker

import (
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
)

func fxConfig() config.Config {
	var c config.Config
	c.Database = config.DatabaseConfig{URL: "postgres://u:p@127.0.0.1:1/db", MaxConns: 2, ConnectTimeout: time.Second}
	c.Workers.PendingReferenceMaxAttempts = 5
	c.Workers.PendingReferenceTTL = time.Minute
	c.Workers.PendingReferenceRetryBaseDelay = time.Second
	c.Workers.PendingReferenceRetryMaxDelay = 10 * time.Second
	c.Workers.PendingWorker = config.PendingWorkerConfig{
		PollInterval: 10 * time.Millisecond, BatchSize: 5, Concurrency: 2, Lease: time.Minute,
		ProcessTimeout: time.Second, DrainTimeout: time.Second, OrphanGrace: time.Second,
	}
	return c
}

func TestModuleValidateApp(t *testing.T) {
	if err := fx.ValidateApp(fx.Supply(fxConfig()), postgres.Module, app.Module, Module); err != nil {
		t.Fatal(err)
	}
}

func TestPolicyAndConfigFrom(t *testing.T) {
	c := fxConfig()
	p := PolicyFrom(c)
	if p.MaxAttempts != 5 || p.TTL != time.Minute || p.RetryBaseDelay != time.Second || p.RetryMaxDelay != 10*time.Second {
		t.Fatalf("policy = %+v", p)
	}
	wc := ConfigFrom(c)
	if wc.BatchSize != 5 || wc.Concurrency != 2 || wc.Lease != time.Minute || wc.OrphanGrace != time.Second {
		t.Fatalf("config = %+v", wc)
	}
}

// Ciclo Fx do worker com um Resumer falso: OnStart inicia o laço, os itens
// são processados e o OnStop encerra as goroutines (goleak no TestMain).
func TestWorkerModuleLifecycle(t *testing.T) {
	f := &fakeResumer{}
	f.push(4)
	var w *Worker
	fxApp := fxtest.New(t,
		fx.Supply(fxConfig()),
		fx.Provide(func() Resumer { return f }, NewWorkerFx),
		fx.Populate(&w),
	)
	fxApp.RequireStart()
	waitFor(t, func() bool { return f.resumedCount() == 4 })
	fxApp.RequireStop()
	if w == nil {
		t.Fatal("worker not built")
	}
}
