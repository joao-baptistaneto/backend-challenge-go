package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Decisões (ver ARCHITECTURE.md, ADR "Config, logging e métricas"):
//
//   - Registry próprio (prometheus.NewRegistry), nunca o DefaultRegisterer
//     global: cada app Fx (e cada teste) tem seu conjunto isolado, sem panics
//     de registro duplicado e sem métricas de bibliotecas de terceiros vazando
//     para /metrics. Go runtime e processo são registrados explicitamente.
//   - Todas as métricas usam o namespace "wallet" e são criadas e registradas
//     em New, que devolve erro (em vez de MustRegister/panic) para que uma
//     colisão apareça como falha de start no Fx.
//   - Rótulos têm cardinalidade limitada POR CONSTRUÇÃO, não por disciplina
//     do chamador: os métodos auxiliares normalizam cada valor contra um
//     conjunto fechado (labels.go) e trocam qualquer valor desconhecido por
//     "other" ("OTHER" para método HTTP). Rotas HTTP e nomes de fila são
//     fechados por registro explícito (AllowRoutes/AllowQueues) feito pelo
//     adapter na inicialização. IDs de carteira, transação, provider externo,
//     mensagem etc. NUNCA viram rótulo; eles vão para os logs
//     (platform/logging). Os campos *Vec continuam exportados para leitura e
//     testes, mas incrementá-los diretamente contorna a normalização: use os
//     métodos auxiliares.
//   - Valores monetários NÃO são exportados como métricas: Prometheus
//     representa amostras em float64, e a regra do projeto proíbe dinheiro em
//     ponto flutuante. Divergências de reconciliação são contadas (ocorrências),
//     e o valor exato da diferença fica na resposta da API e no log. float64
//     aparece aqui apenas para durações em segundos e contagens.
//   - Os coletores são expostos como campos (*CounterVec etc.) e também por
//     métodos auxiliares com assinatura tipada, que são a forma preferida de uso
//     pelos adapters.

// Namespace comum a todas as métricas da aplicação.
const Namespace = "wallet"

// Nomes de rótulos.
const (
	LabelSource    = "source"
	LabelType      = "type"
	LabelStatus    = "status"
	LabelKind      = "kind"
	LabelOperation = "operation"
	LabelReason    = "reason"
	LabelOutcome   = "outcome"
	LabelQueue     = "queue"
	LabelCurrency  = "currency"
	LabelMethod    = "method"
	LabelRoute     = "route"
	LabelCode      = "code"
)

// Valores do rótulo source (canal de entrada).
const (
	SourceHTTP   = "http"
	SourceSQS    = "sqs"
	SourceWorker = "worker"
)

// Valores do rótulo kind de DuplicatesTotal.
const (
	// DuplicateReplay: mesma operação reenviada; resultado persistido devolvido.
	DuplicateReplay = "idempotent_replay"
	// DuplicateInbox: mensagem SQS já registrada na inbox (reentrega).
	DuplicateInbox = "inbox_redelivery"
	// DuplicateConflict: mesma chave com payload diferente, ou outra chave
	// para a mesma operação.
	DuplicateConflict = "idempotency_conflict"
)

// Valores do rótulo kind de ConcurrencyConflictsTotal.
const (
	ConflictVersionMismatch = "version_mismatch"      // locking otimista (WHERE version = $old)
	ConflictSerialization   = "serialization_failure" // SQLSTATE 40001
	ConflictDeadlock        = "deadlock"              // SQLSTATE 40P01
	ConflictLockTimeout     = "lock_timeout"          // SQLSTATE 55P03
	ConflictUniqueViolation = "unique_violation"      // corrida resolvida por UNIQUE (23505)
)

// Valores do rótulo operation de RetriesTotal e ProcessingDuration.
const (
	OperationProcessTransaction = "process_transaction"
	OperationOpenWallet         = "open_wallet"
	OperationReconcile          = "reconcile"
	OperationQuery              = "query"
	OperationOutboxPublish      = "outbox_publish"
	OperationSQSConsume         = "sqs_consume"
	OperationPendingReference   = "pending_reference"
)

// Valores do rótulo outcome.
const (
	OutcomeSuccess   = "success"
	OutcomeRejected  = "rejected"
	OutcomeConflict  = "conflict"
	OutcomeTransient = "transient_error"
	OutcomeError     = "error"
)

// Valores do rótulo reason de DLQMessagesTotal.
const (
	DLQReasonInvalidMessage = "invalid_message"
	DLQReasonPermanentError = "permanent_error"
	DLQReasonMaxReceives    = "max_receives"
)

// Metrics agrupa todos os coletores da aplicação.
type Metrics struct {
	// routes e queues são os conjuntos fechados registrados em runtime.
	routes allowList
	queues allowList

	// TransactionsTotal conta transações de aposta por canal, tipo e status
	// final (PROCESSED, REJECTED, PENDING_REFERENCE, ...).
	TransactionsTotal *prometheus.CounterVec
	// DuplicatesTotal conta requisições/mensagens duplicadas por canal e kind.
	DuplicatesTotal *prometheus.CounterVec
	// RetriesTotal conta novas tentativas após falha transitória.
	RetriesTotal *prometheus.CounterVec
	// DLQMessagesTotal conta mensagens que a aplicação deixou seguir para a
	// DLQ (o redrive em si é feito pelo SQS via maxReceiveCount).
	DLQMessagesTotal *prometheus.CounterVec
	// DLQDepth é a profundidade aproximada observada da DLQ.
	DLQDepth *prometheus.GaugeVec
	// ConcurrencyConflictsTotal conta conflitos de concorrência por carteira.
	ConcurrencyConflictsTotal *prometheus.CounterVec
	// OutboxPendingEvents é o número de eventos ainda não publicados.
	OutboxPendingEvents prometheus.Gauge
	// OutboxOldestPendingAge é a idade, em segundos, do evento pendente mais
	// antigo (atraso da outbox; 0 quando vazia).
	OutboxOldestPendingAge prometheus.Gauge
	// OutboxPublishDelay mede created_at → publicação efetiva no SQS.
	OutboxPublishDelay prometheus.Histogram
	// ProcessingDuration mede a latência dos casos de uso por canal,
	// operação e outcome.
	ProcessingDuration *prometheus.HistogramVec
	// HTTPRequestDuration mede a latência HTTP por método, rota (template,
	// não o path concreto) e código.
	HTTPRequestDuration *prometheus.HistogramVec
	// ReconciliationsTotal conta reconciliações por outcome
	// (consistent/divergent).
	ReconciliationsTotal *prometheus.CounterVec
	// ReconciliationDivergencesTotal conta reconciliações com divergência
	// por moeda.
	ReconciliationDivergencesTotal *prometheus.CounterVec
}

// Outcomes de ReconciliationsTotal.
const (
	ReconciliationConsistent = "consistent"
	ReconciliationDivergent  = "divergent"
)

// latencyBuckets cobre de 1ms a 10s: operações financeiras com lock de
// carteira típicas ficam em milissegundos; caudas longas indicam contenção.
var latencyBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

// outboxDelayBuckets cobre de 10ms a 10min (polling de ~1s + backoff).
var outboxDelayBuckets = []float64{.01, .05, .1, .25, .5, 1, 2, 5, 10, 30, 60, 120, 300, 600}

// NewRegistry cria o registry isolado da aplicação já com os coletores de Go
// runtime e de processo.
func NewRegistry() (*prometheus.Registry, error) {
	reg := prometheus.NewRegistry()
	if err := reg.Register(collectors.NewGoCollector()); err != nil {
		return nil, err
	}
	if err := reg.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
		return nil, err
	}
	return reg, nil
}

// New cria e registra todas as métricas em reg.
func New(reg prometheus.Registerer) (*Metrics, error) {
	m := &Metrics{
		TransactionsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "wager_transactions_total",
			Help: "Transações de aposta por canal, tipo e status final.",
		}, []string{LabelSource, LabelType, LabelStatus}),
		DuplicatesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "duplicates_total",
			Help: "Requisições/mensagens duplicadas (replay idempotente, reentrega de inbox, conflito de idempotência).",
		}, []string{LabelSource, LabelKind}),
		RetriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "retries_total",
			Help: "Novas tentativas após falha transitória, por operação.",
		}, []string{LabelOperation}),
		DLQMessagesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "dlq_messages_total",
			Help: "Mensagens encaminhadas à DLQ, por motivo.",
		}, []string{LabelReason}),
		DLQDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "dlq_depth_messages",
			Help: "Profundidade aproximada da DLQ (ApproximateNumberOfMessages).",
		}, []string{LabelQueue}),
		ConcurrencyConflictsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "concurrency_conflicts_total",
			Help: "Conflitos de concorrência (versão, serialização, deadlock, lock timeout, unique).",
		}, []string{LabelKind}),
		OutboxPendingEvents: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "outbox_pending_events",
			Help: "Eventos da outbox ainda não publicados.",
		}),
		OutboxOldestPendingAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "outbox_oldest_pending_age_seconds",
			Help: "Idade do evento pendente mais antigo da outbox (atraso da outbox).",
		}),
		OutboxPublishDelay: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace, Name: "outbox_publish_delay_seconds",
			Help:    "Atraso entre a criação do evento na outbox e sua publicação no SQS.",
			Buckets: outboxDelayBuckets,
		}),
		ProcessingDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace, Name: "processing_duration_seconds",
			Help:    "Latência de processamento dos casos de uso.",
			Buckets: latencyBuckets,
		}, []string{LabelSource, LabelOperation, LabelOutcome}),
		HTTPRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace, Name: "http_request_duration_seconds",
			Help:    "Latência das requisições HTTP por método, rota e código.",
			Buckets: latencyBuckets,
		}, []string{LabelMethod, LabelRoute, LabelCode}),
		ReconciliationsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "reconciliations_total",
			Help: "Reconciliações executadas, por resultado.",
		}, []string{LabelOutcome}),
		ReconciliationDivergencesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "reconciliation_divergences_total",
			Help: "Reconciliações em que saldo armazenado != soma do ledger, por moeda.",
		}, []string{LabelCurrency}),
	}
	for _, c := range m.collectors() {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (m *Metrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.TransactionsTotal, m.DuplicatesTotal, m.RetriesTotal, m.DLQMessagesTotal,
		m.DLQDepth, m.ConcurrencyConflictsTotal, m.OutboxPendingEvents,
		m.OutboxOldestPendingAge, m.OutboxPublishDelay, m.ProcessingDuration,
		m.HTTPRequestDuration, m.ReconciliationsTotal, m.ReconciliationDivergencesTotal,
	}
}

// --- Métodos auxiliares (forma preferida de uso) --------------------------

// Todos os auxiliares abaixo normalizam os rótulos (labels.go): valores fora
// do conjunto fechado viram LabelValueOther.

// TransactionResult registra o status final de uma transação de aposta.
// txType e status são os valores do domínio (BET, PROCESSED...).
func (m *Metrics) TransactionResult(source, txType, status string) {
	m.TransactionsTotal.WithLabelValues(
		normSource(source), closedLabel(transactionTypes, txType), closedLabel(transactionStatuses, status),
	).Inc()
}

// Duplicate registra uma duplicata (kind: Duplicate*).
func (m *Metrics) Duplicate(source, kind string) {
	m.DuplicatesTotal.WithLabelValues(normSource(source), closedLabel(duplicateKinds, kind)).Inc()
}

// Retry registra uma nova tentativa da operação (Operation*).
func (m *Metrics) Retry(operation string) {
	m.RetriesTotal.WithLabelValues(closedLabel(operations, operation)).Inc()
}

// SentToDLQ registra uma mensagem encaminhada à DLQ (DLQReason*).
func (m *Metrics) SentToDLQ(reason string) {
	m.DLQMessagesTotal.WithLabelValues(closedLabel(dlqReasons, reason)).Inc()
}

// SetDLQDepth publica a profundidade observada de uma DLQ. queue deve ser o
// nome lógico da fila (ex.: "wager-transactions-dlq.fifo"), não a URL, e
// precisa ter sido registrado com AllowQueues; caso contrário vira "other".
func (m *Metrics) SetDLQDepth(queue string, depth int64) {
	m.DLQDepth.WithLabelValues(m.queues.label(queue)).Set(float64(depth))
}

// ConcurrencyConflict registra um conflito de concorrência (kind: Conflict*).
func (m *Metrics) ConcurrencyConflict(kind string) {
	m.ConcurrencyConflictsTotal.WithLabelValues(closedLabel(conflictKinds, kind)).Inc()
}

// SetOutboxBacklog publica o tamanho da outbox pendente e a idade do evento
// mais antigo (0 quando não há pendências).
func (m *Metrics) SetOutboxBacklog(pending int64, oldestAge time.Duration) {
	if pending <= 0 {
		pending, oldestAge = 0, 0
	}
	if oldestAge < 0 {
		oldestAge = 0
	}
	m.OutboxPendingEvents.Set(float64(pending))
	m.OutboxOldestPendingAge.Set(oldestAge.Seconds())
}

// OutboxPublished registra a publicação de um evento criado em createdAt.
func (m *Metrics) OutboxPublished(createdAt, publishedAt time.Time) {
	d := publishedAt.Sub(createdAt)
	if d < 0 {
		d = 0 // relógios distintos entre banco e aplicação
	}
	m.OutboxPublishDelay.Observe(d.Seconds())
}

// ObserveProcessing registra a duração de uma operação de caso de uso.
func (m *Metrics) ObserveProcessing(source, operation, outcome string, d time.Duration) {
	m.ProcessingDuration.WithLabelValues(
		normSource(source), closedLabel(operations, operation), closedLabel(outcomes, outcome),
	).Observe(d.Seconds())
}

// ObserveHTTP registra a duração de uma requisição HTTP. method é
// normalizado para os métodos padrão (demais → "OTHER"). route deve ser o
// template da rota (ex.: "/wallets/{id}"), nunca o path com IDs, e precisa ter
// sido registrado com AllowRoutes; caso contrário (inclusive 404 sem rota)
// vira "other".
func (m *Metrics) ObserveHTTP(method, route string, code int, d time.Duration) {
	m.HTTPRequestDuration.WithLabelValues(normMethod(method), m.routes.label(route), httpCode(code)).Observe(d.Seconds())
}

// AllowRoutes registra templates de rota aceitos como rótulo por ObserveHTTP.
// Deve ser chamado pelo adapter HTTP na inicialização, com os templates do
// roteador. Valores vazios são ignorados. Seguro para uso concorrente.
func (m *Metrics) AllowRoutes(routes ...string) { m.routes.add(routes...) }

// AllowQueues registra nomes lógicos de fila aceitos como rótulo por
// SetDLQDepth. Seguro para uso concorrente.
func (m *Metrics) AllowQueues(queues ...string) { m.queues.add(queues...) }

// Reconciliation registra o resultado de uma reconciliação. A diferença
// monetária não é exportada (ver decisões no topo do arquivo).
func (m *Metrics) Reconciliation(currency string, divergent bool) {
	if divergent {
		m.ReconciliationsTotal.WithLabelValues(ReconciliationDivergent).Inc()
		m.ReconciliationDivergencesTotal.WithLabelValues(closedLabel(currencies, currency)).Inc()
		return
	}
	m.ReconciliationsTotal.WithLabelValues(ReconciliationConsistent).Inc()
}

// Handler devolve o handler HTTP de /metrics para o registry informado. O
// roteamento (montagem em /metrics) é responsabilidade do adapter HTTP.
func Handler(g prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(g, promhttp.HandlerOpts{
		ErrorHandling:     promhttp.ContinueOnError,
		EnableOpenMetrics: false,
	})
}
