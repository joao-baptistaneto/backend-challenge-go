// Command server é o ponto de entrada da aplicação: compõe os módulos Fx
// (configuração, conexões, repositórios, casos de uso, handlers e workers)
// e trata o ciclo de vida do processo (ARCHITECTURE.md, ADR-025).
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"go.uber.org/fx"

	httpadapter "github.com/junglegaming/backend-challenge-go/internal/adapters/http"
	httpserver "github.com/junglegaming/backend-challenge-go/internal/adapters/http/server"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/auth"
	"github.com/junglegaming/backend-challenge-go/internal/adapters/outboxpublisher"
	"github.com/junglegaming/backend-challenge-go/internal/adapters/pendingworker"
	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/adapters/sqs"
	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
	"github.com/junglegaming/backend-challenge-go/internal/platform/logging"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
	"github.com/junglegaming/backend-challenge-go/internal/platform/startbudget"
)

// Folgas dos prazos de start e stop (ADR-025).
const (
	// startSlack soma-se a DATABASE_CONNECT_TIMEOUT no orçamento do start
	// (StartBudget): o ping do pool e as chamadas iniciais ao SQS repetem
	// com backoff até esse orçamento.
	startSlack = 15 * time.Second
	// stopSlack cobre o que roda além dos prazos configurados no stop: a
	// liberação das mensagens SQS depois do estouro da drenagem (5s por
	// chamada, em paralelo), a última marcação da outbox
	// (OUTBOX_PUBLISH_TIMEOUT, que é < OUTBOX_LEASE/2), o fechamento do pool
	// e dos clients.
	stopSlack = 15 * time.Second
)

// StartBudget é o orçamento dos OnStart que repetem com backoff (ping do
// pool, chamadas iniciais ao SQS).
func StartBudget(c config.Config) time.Duration {
	return c.Database.ConnectTimeout + startSlack
}

// StartTimeout é o prazo do fx.App.Start: o orçamento mais a reserva de
// rollback (StartReserve). O Fx executa o rollback de um start que falhou
// (OnStop dos hooks já iniciados, em sequência) com o MESMO ctx do start e
// não executa nenhum OnStop se esse ctx já venceu; por isso os OnStart que
// repetem (postgres.DB, sqs.Consumer) desistem StartReserve antes do prazo
// (startbudget.Context), deixando ao rollback o mesmo tempo de um stop.
func StartTimeout(c config.Config) time.Duration {
	return StartBudget(c) + time.Duration(StartReserve(c))
}

// StartReserve é a parte final do prazo do start reservada ao rollback: o
// pior caso de parar os componentes já iniciados, igual ao StopTimeout.
func StartReserve(c config.Config) startbudget.Reserve {
	return startbudget.Reserve(StopTimeout(c))
}

// StopTimeout é o prazo do fx.App.Stop. O Fx executa os OnStop em sequência
// (ordem inversa dos OnStart), todos com o mesmo ctx; no pior caso cada
// componente usa o seu prazo inteiro, então o total é a SOMA: servidor HTTP
// (SHUTDOWN_TIMEOUT, que já inclui HTTP_DRAIN_DELAY) + consumidor SQS
// (SQS_DRAIN_TIMEOUT) + worker de pendências (PENDING_WORKER_DRAIN_TIMEOUT)
// + publisher da outbox (OUTBOX_DRAIN_TIMEOUT) + folga. Se o ctx do OnStop
// vencesse antes, o Fx abandonaria os hooks restantes e o pool/clients não
// seriam fechados de forma ordenada.
func StopTimeout(c config.Config) time.Duration {
	return c.App.ShutdownTimeout +
		c.SQS.Consumer.DrainTimeout +
		c.Workers.PendingWorker.DrainTimeout +
		c.Workers.OutboxDrainTimeout +
		stopSlack
}

// metricsAccess traduz METRICS_ACCESS para a API HTTP.
func metricsAccess(c config.Config) httpadapter.MetricsAccess {
	if c.App.MetricsAccess == config.MetricsAccessPublic {
		return httpadapter.MetricsPublic
	}
	return httpadapter.MetricsInternal
}

// postgresReadiness é a verificação "postgres" de /health/ready.
func postgresReadiness(db *postgres.DB) httpadapter.ReadinessCheck {
	return httpadapter.ReadinessCheck{Name: "postgres", Check: db.Ping}
}

// wiring liga as peças que nenhum módulo conhece sozinho.
var wiring = fx.Options(
	fx.Provide(
		// Métrica de divergência da reconciliação (ADR-018).
		func(m *metrics.Metrics) app.ReconciliationRecorder { return m },
		// Mesma base de backoff do worker de pendências (ADR-023).
		func(c config.Config) app.ReferenceRetryDelay {
			return app.ReferenceRetryDelay(c.Workers.PendingReferenceRetryBaseDelay)
		},
		metricsAccess,
		// Reserva de rollback do prazo do start (ADR-025).
		StartReserve,
		fx.Annotate(postgresReadiness, fx.ResultTags(`group:"`+httpadapter.ReadinessGroup+`"`)),
	),
)

// Options compõe a aplicação a partir da configuração já carregada. A config
// é carregada antes do fx.New (e não por config.Module) porque os prazos de
// start/stop do Fx dependem dela.
//
// A ordem dos módulos com ciclo de vida importa só entre componentes
// independentes: o Fx registra os hooks na ordem em que os construtores
// rodam (forçados pelos fx.Invoke, na ordem dos módulos) e os para na ordem
// inversa. Workers e consumidor vêm antes do servidor HTTP, então o servidor
// é o PRIMEIRO a parar (deixa de aceitar tráfego e fica DOWN no readiness).
// O publisher da outbox vem antes do worker de pendências: o worker para
// primeiro e os eventos que ele gravar na drenagem ainda saem pelo publisher
// desta instância.
// Pool pgx, cliente SQS e transporte do JWKS são dependências de todos e
// por isso sempre são construídos antes e parados DEPOIS deles.
func Options(cfg config.Config) []fx.Option {
	return []fx.Option{
		fx.Supply(cfg),
		fx.StartTimeout(StartTimeout(cfg)),
		fx.StopTimeout(StopTimeout(cfg)),
		logging.Module,
		logging.FxEventLogger(),
		metrics.Module,
		postgres.Module,
		app.Module,
		auth.Module,
		outboxpublisher.Module,
		pendingworker.Module,
		sqs.Module,
		httpadapter.Module,
		httpserver.Module,
		wiring,
	}
}

// newApp monta a aplicação; extra permite substituições em testes.
func newApp(cfg config.Config, extra ...fx.Option) *fx.App {
	return fx.New(append(Options(cfg), extra...)...)
}

// bootstrapLogger registra falhas anteriores ao logger da aplicação.
func bootstrapLogger() *slog.Logger {
	return logging.New(os.Stderr, logging.Options{}).With(slog.String("service", logging.ServiceName))
}

func main() {
	os.Exit(run())
}

// run carrega a configuração, monta e executa a aplicação e devolve o exit
// code: 0 após SIGINT/SIGTERM com parada limpa; 1 se a config for inválida,
// o grafo falhar, o start ou o stop falharem, ou um componente pedir a parada
// com erro (fx.Shutdowner com ExitCode, ex.: o Serve do HTTP morreu).
func run() int {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		bootstrapLogger().Error("configuração inválida", logging.Err(err))
		return 1
	}
	fxApp := newApp(cfg)
	if err := fxApp.Err(); err != nil {
		bootstrapLogger().Error("falha ao montar a aplicação", logging.Err(err))
		return 1
	}
	return runApp(fxApp)
}

// runApp equivale a fx.App.Run, mas devolve o exit code em vez de chamar
// os.Exit. app.Wait recebe SIGINT/SIGTERM (e fx.Shutdowner). Os erros de
// start/stop já são logados pelo logger de eventos do Fx.
func runApp(a *fx.App) int {
	startCtx, cancel := context.WithTimeout(context.Background(), a.StartTimeout())
	defer cancel()
	if err := a.Start(startCtx); err != nil {
		// O Fx já executou os OnStop dos hooks iniciados (rollback) com o
		// mesmo ctx: os OnStart que repetem desistem StartReserve antes do
		// prazo, então o rollback roda mesmo quando uma dependência
		// indisponível esgota o orçamento do start (ADR-025). Não adianta
		// chamar a.Stop aqui: depois do rollback o lifecycle fica "stopped"
		// e o Stop não executa nada.
		return 1
	}
	sig := <-a.Wait()
	stopCtx, cancelStop := context.WithTimeout(context.Background(), a.StopTimeout())
	defer cancelStop()
	if err := a.Stop(stopCtx); err != nil {
		return 1
	}
	return sig.ExitCode
}
