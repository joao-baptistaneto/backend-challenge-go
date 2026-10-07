package main

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"go.uber.org/fx"

	httpadapter "github.com/junglegaming/backend-challenge-go/internal/adapters/http"
	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
	"github.com/junglegaming/backend-challenge-go/internal/platform/logging"
	"github.com/junglegaming/backend-challenge-go/internal/platform/startbudget"
)

// testEnv é uma configuração completa e válida (sem segredos reais). Nada é
// contatado: os testes daqui só montam o grafo.
func testEnv() map[string]string {
	return map[string]string{
		config.EnvHTTPAddr:           "127.0.0.1:0",
		config.EnvDatabaseURL:        "postgres://wallet:x@127.0.0.1:1/wallet?sslmode=disable",
		config.EnvAWSRegion:          "us-east-1",
		config.EnvAWSAccessKeyID:     "test",
		config.EnvAWSSecretAccessKey: "test",
		config.EnvAWSEndpointURL:     "http://127.0.0.1:1",
		config.EnvSQSInboundQueueURL: "http://127.0.0.1:1/000000000000/wager-transactions.fifo",
		config.EnvSQSEventsQueueURL:  "http://127.0.0.1:1/000000000000/wager-events.fifo",
		config.EnvOIDCIssuerURL:      "http://127.0.0.1:1/realms/test",
		config.EnvOIDCAudience:       "wallet-api",
	}
}

func loadConfig(t *testing.T, env map[string]string) config.Config {
	t.Helper()
	cfg, err := config.Load(func(k string) (string, bool) { v, ok := env[k]; return v, ok })
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	return cfg
}

func TestAppGraphIsValid(t *testing.T) {
	if err := fx.ValidateApp(Options(loadConfig(t, testEnv()))...); err != nil {
		t.Fatalf("fx graph inválido: %v", err)
	}
}

func TestStopTimeoutIsTheSumOfStopBudgetsPlusSlack(t *testing.T) {
	cfg := loadConfig(t, testEnv())
	// Defaults: 15s + max(SQS_DRAIN_TIMEOUT 10s, SQS_WAIT_TIME 20s + 5s) +
	// 10s + 10s + folga. O consumidor espera o último long polling terminar
	// antes de liberar mensagens (ADR-022), em paralelo com a drenagem.
	if got, want := StopTimeout(cfg), 60*time.Second+stopSlack; got != want {
		t.Fatalf("StopTimeout = %s, quer %s", got, want)
	}

	env := testEnv()
	env[config.EnvShutdownTimeout] = "20s"
	env[config.EnvSQSDrainTimeout] = "7s"
	env[config.EnvSQSWaitTime] = "1s"
	env[config.EnvPendingWorkerDrainTimeout] = "3s"
	env[config.EnvOutboxDrainTimeout] = "11s"
	cfg = loadConfig(t, env)
	got := StopTimeout(cfg)
	// Drenagem SQS (7s) maior que WaitTime + 5s (6s): vale a drenagem.
	sum := cfg.App.ShutdownTimeout + cfg.SQS.Consumer.DrainTimeout +
		cfg.Workers.PendingWorker.DrainTimeout + cfg.Workers.OutboxDrainTimeout
	if got != sum+stopSlack || got <= sum {
		t.Fatalf("StopTimeout = %s, quer soma %s + folga %s", got, sum, stopSlack)
	}
	// Long polling (15s + 5s) maior que a drenagem (7s): vale a espera dele.
	env[config.EnvSQSWaitTime] = "15s"
	cfg = loadConfig(t, env)
	if got, want := StopTimeout(cfg), 20*time.Second+20*time.Second+3*time.Second+11*time.Second+stopSlack; got != want {
		t.Fatalf("StopTimeout = %s, quer %s (espera do último long polling)", got, want)
	}
	// A folga cobre a liberação das mensagens SQS depois do estouro da
	// drenagem (5s por chamada) e a última marcação da outbox.
	if stopSlack < 5*time.Second+cfg.Workers.OutboxPublishTimeout {
		t.Fatalf("folga %s pequena demais", stopSlack)
	}
	// Start: orçamento dos OnStart que repetem + reserva de rollback igual
	// ao prazo de um stop completo.
	if StartBudget(cfg) != cfg.Database.ConnectTimeout+startSlack {
		t.Fatalf("StartBudget = %s", StartBudget(cfg))
	}
	if time.Duration(StartReserve(cfg)) != StopTimeout(cfg) {
		t.Fatalf("StartReserve = %s, quer StopTimeout %s", time.Duration(StartReserve(cfg)), StopTimeout(cfg))
	}
	if StartTimeout(cfg) != StartBudget(cfg)+StopTimeout(cfg) {
		t.Fatalf("StartTimeout = %s", StartTimeout(cfg))
	}
}

// TestStartBudgetLeavesTimeForRollback reproduz, sem dependências externas,
// um OnStart que repete até o prazo do ctx (dependência indisponível) num
// app com os prazos e a reserva de cmd/server: o hook recebe a reserva, o
// start falha antes do prazo e o Fx ainda executa o OnStop já iniciado.
func TestStartBudgetLeavesTimeForRollback(t *testing.T) {
	const (
		budget  = 200 * time.Millisecond
		reserve = startbudget.Reserve(2 * time.Second)
	)
	var (
		rolledBack bool
		hookErr    error
	)
	a := fx.New(fx.NopLogger,
		fx.StartTimeout(budget+time.Duration(reserve)),
		fx.Supply(reserve),
		fx.Invoke(func(lc fx.Lifecycle, r startbudget.Reserve) {
			lc.Append(fx.Hook{OnStop: func(ctx context.Context) error {
				rolledBack = ctx.Err() == nil
				return nil
			}})
			lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
				// Como postgres.DB.Start e sqs.Consumer.Start: repete até o
				// orçamento acabar.
				ctx, cancel := startbudget.Context(ctx, r)
				defer cancel()
				<-ctx.Done()
				hookErr = ctx.Err()
				return hookErr
			}})
		}),
	)
	begin := time.Now()
	if code := runApp(a); code != 1 {
		t.Fatalf("exit code = %d, quer 1", code)
	}
	if !errors.Is(hookErr, context.DeadlineExceeded) {
		t.Fatalf("hook: %v", hookErr)
	}
	if !rolledBack {
		t.Fatal("o OnStop já iniciado deveria ter rodado no rollback, com ctx válido")
	}
	if elapsed := time.Since(begin); elapsed >= budget+time.Duration(reserve) {
		t.Fatalf("start levou %s: consumiu a reserva de rollback", elapsed)
	}
}

func TestAppUsesConfiguredTimeouts(t *testing.T) {
	cfg := loadConfig(t, testEnv())
	a := newApp(cfg, logging.WithOutput(io.Discard), fx.Invoke(func(db *postgres.DB) {
		// O pool não abre conexões no fx.New, mas tem goroutine de health
		// check: fecha-o, pois o app não é iniciado aqui.
		t.Cleanup(func() { _ = db.Stop(context.Background()) })
	}))
	if err := a.Err(); err != nil {
		t.Fatalf("fx.New: %v", err)
	}
	if a.StopTimeout() != StopTimeout(cfg) || a.StartTimeout() != StartTimeout(cfg) {
		t.Fatalf("timeouts do Fx: start %s stop %s", a.StartTimeout(), a.StopTimeout())
	}
}

func TestMetricsAccessFromConfig(t *testing.T) {
	cfg := loadConfig(t, testEnv())
	if metricsAccess(cfg) != httpadapter.MetricsInternal {
		t.Fatal("padrão deve ser MetricsInternal")
	}
	env := testEnv()
	env[config.EnvMetricsAccess] = config.MetricsAccessPublic
	if metricsAccess(loadConfig(t, env)) != httpadapter.MetricsPublic {
		t.Fatal("METRICS_ACCESS=public deve dar MetricsPublic")
	}
}

// TestWiring confere as ligações feitas por cmd/server: verificações de
// prontidão, atraso de referência pendente da config e o recorder de
// reconciliação.
func TestWiring(t *testing.T) {
	env := testEnv()
	env[config.EnvPendingReferenceRetryBaseDelay] = "3s"
	cfg := loadConfig(t, env)

	var (
		process  *app.ProcessWagerTransaction
		recorder app.ReconciliationRecorder
		checks   []string
	)
	a := newApp(cfg, logging.WithOutput(io.Discard),
		fx.Populate(&process, &recorder),
		fx.Invoke(fx.Annotate(func(cs []httpadapter.ReadinessCheck) {
			for _, c := range cs {
				checks = append(checks, c.Name)
			}
		}, fx.ParamTags(`group:"`+httpadapter.ReadinessGroup+`"`))),
		fx.Invoke(func(db *postgres.DB) {
			t.Cleanup(func() { _ = db.Stop(context.Background()) })
		}),
	)
	if err := a.Err(); err != nil {
		t.Fatalf("fx.New: %v", err)
	}
	if process.ReferenceRetryDelay() != 3*time.Second {
		t.Fatalf("ReferenceRetryDelay = %s, quer 3s (PENDING_REFERENCE_RETRY_BASE_DELAY)", process.ReferenceRetryDelay())
	}
	if recorder == nil {
		t.Fatal("ReconciliationRecorder não provido")
	}
	slices.Sort(checks)
	if want := []string{"http-server", "postgres", "sqs"}; !slices.Equal(checks, want) {
		t.Fatalf("readiness checks = %v, quer %v", checks, want)
	}
}

// --- runApp: exit codes --------------------------------------------------

func TestRunAppExitCodeFromShutdowner(t *testing.T) {
	a := fx.New(fx.NopLogger, fx.Invoke(func(lc fx.Lifecycle, s fx.Shutdowner) {
		lc.Append(fx.Hook{OnStart: func(context.Context) error {
			return s.Shutdown(fx.ExitCode(3))
		}})
	}))
	if code := runApp(a); code != 3 {
		t.Fatalf("exit code = %d, quer 3", code)
	}
}

func TestRunAppCleanShutdownIsZero(t *testing.T) {
	stopped := false
	a := fx.New(fx.NopLogger, fx.Invoke(func(lc fx.Lifecycle, s fx.Shutdowner) {
		lc.Append(fx.Hook{
			OnStart: func(context.Context) error { return s.Shutdown() },
			OnStop:  func(context.Context) error { stopped = true; return nil },
		})
	}))
	if code := runApp(a); code != 0 || !stopped {
		t.Fatalf("exit code = %d, stopped = %v", code, stopped)
	}
}

func TestRunAppStartFailureIsOne(t *testing.T) {
	rolledBack := false
	a := fx.New(fx.NopLogger, fx.Invoke(func(lc fx.Lifecycle) {
		lc.Append(fx.Hook{OnStop: func(context.Context) error { rolledBack = true; return nil }})
		lc.Append(fx.Hook{OnStart: func(context.Context) error { return errors.New("boom") }})
	}))
	if code := runApp(a); code != 1 {
		t.Fatalf("exit code = %d, quer 1", code)
	}
	if !rolledBack {
		t.Fatal("hooks já iniciados deveriam ter sido parados (rollback)")
	}
}

func TestRunAppStopFailureIsOne(t *testing.T) {
	a := fx.New(fx.NopLogger, fx.Invoke(func(lc fx.Lifecycle, s fx.Shutdowner) {
		lc.Append(fx.Hook{
			OnStart: func(context.Context) error { return s.Shutdown() },
			OnStop:  func(context.Context) error { return errors.New("boom") },
		})
	}))
	if code := runApp(a); code != 1 {
		t.Fatalf("exit code = %d, quer 1", code)
	}
}
