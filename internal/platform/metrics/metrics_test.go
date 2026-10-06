package metrics

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/fx"
)

func newTestMetrics(t *testing.T) (*prometheus.Registry, *Metrics) {
	t.Helper()
	reg, err := NewRegistry()
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	m, err := New(reg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return reg, m
}

// find devolve a métrica de nome name cujos rótulos contêm labels.
func find(t *testing.T, reg prometheus.Gatherer, name string, labels map[string]string) *dto.Metric {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
	next:
		for _, m := range mf.GetMetric() {
			got := map[string]string{}
			for _, lp := range m.GetLabel() {
				got[lp.GetName()] = lp.GetValue()
			}
			for k, v := range labels {
				if got[k] != v {
					continue next
				}
			}
			return m
		}
	}
	t.Fatalf("métrica %s%v não encontrada", name, labels)
	return nil
}

func TestAllRequiredMetricsAreRegistered(t *testing.T) {
	reg, m := newTestMetrics(t)
	m.AllowQueues("wager-transactions-dlq.fifo")
	m.AllowRoutes("/wagering/transactions")

	// Cada item exigido pela T09 / README §12.
	m.TransactionResult(SourceHTTP, "BET", "PROCESSED")
	m.TransactionResult(SourceSQS, "WIN", "REJECTED")
	m.Duplicate(SourceHTTP, DuplicateReplay)
	m.Duplicate(SourceSQS, DuplicateInbox)
	m.Retry(OperationProcessTransaction)
	m.SentToDLQ(DLQReasonInvalidMessage)
	m.SetDLQDepth("wager-transactions-dlq.fifo", 3)
	m.ConcurrencyConflict(ConflictVersionMismatch)
	m.SetOutboxBacklog(7, 90*time.Second)
	m.OutboxPublished(time.Unix(100, 0), time.Unix(102, 500_000_000))
	m.ObserveProcessing(SourceHTTP, OperationProcessTransaction, OutcomeSuccess, 12*time.Millisecond)
	m.ObserveHTTP(http.MethodPost, "/wagering/transactions", 201, 15*time.Millisecond)
	m.Reconciliation("BRL", true)
	m.Reconciliation("BRL", false)

	checks := []struct {
		name   string
		labels map[string]string
		value  func(*dto.Metric) float64
		want   float64
	}{
		{"wallet_wager_transactions_total", map[string]string{"source": "http", "type": "BET", "status": "PROCESSED"}, counter, 1},
		{"wallet_wager_transactions_total", map[string]string{"source": "sqs", "type": "WIN", "status": "REJECTED"}, counter, 1},
		{"wallet_duplicates_total", map[string]string{"source": "http", "kind": DuplicateReplay}, counter, 1},
		{"wallet_duplicates_total", map[string]string{"source": "sqs", "kind": DuplicateInbox}, counter, 1},
		{"wallet_retries_total", map[string]string{"operation": OperationProcessTransaction}, counter, 1},
		{"wallet_dlq_messages_total", map[string]string{"reason": DLQReasonInvalidMessage}, counter, 1},
		{"wallet_dlq_depth_messages", map[string]string{"queue": "wager-transactions-dlq.fifo"}, gauge, 3},
		{"wallet_concurrency_conflicts_total", map[string]string{"kind": ConflictVersionMismatch}, counter, 1},
		{"wallet_outbox_pending_events", nil, gauge, 7},
		{"wallet_outbox_oldest_pending_age_seconds", nil, gauge, 90},
		{"wallet_outbox_publish_delay_seconds", nil, histSum, 2.5},
		{"wallet_processing_duration_seconds", map[string]string{"source": "http", "operation": OperationProcessTransaction, "outcome": OutcomeSuccess}, histCount, 1},
		{"wallet_http_request_duration_seconds", map[string]string{"method": "POST", "route": "/wagering/transactions", "code": "201"}, histCount, 1},
		{"wallet_reconciliations_total", map[string]string{"outcome": ReconciliationDivergent}, counter, 1},
		{"wallet_reconciliations_total", map[string]string{"outcome": ReconciliationConsistent}, counter, 1},
		{"wallet_reconciliation_divergences_total", map[string]string{"currency": "BRL"}, counter, 1},
	}
	for _, c := range checks {
		if got := c.value(find(t, reg, c.name, c.labels)); got != c.want {
			t.Errorf("%s%v = %v, want %v", c.name, c.labels, got, c.want)
		}
	}

	// Coletores de runtime/processo presentes no registry próprio.
	mfs, _ := reg.Gather()
	var hasGo bool
	for _, mf := range mfs {
		if mf.GetName() == "go_goroutines" {
			hasGo = true
		}
	}
	if !hasGo {
		t.Error("go collector ausente")
	}
}

func counter(m *dto.Metric) float64   { return m.GetCounter().GetValue() }
func gauge(m *dto.Metric) float64     { return m.GetGauge().GetValue() }
func histSum(m *dto.Metric) float64   { return m.GetHistogram().GetSampleSum() }
func histCount(m *dto.Metric) float64 { return float64(m.GetHistogram().GetSampleCount()) }

func TestOutboxHelpersClampNegativeValues(t *testing.T) {
	reg, m := newTestMetrics(t)
	m.SetOutboxBacklog(0, time.Hour) // vazia → idade 0
	if got := gauge(find(t, reg, "wallet_outbox_oldest_pending_age_seconds", nil)); got != 0 {
		t.Fatalf("idade com outbox vazia = %v", got)
	}
	m.SetOutboxBacklog(1, -time.Second)
	if got := gauge(find(t, reg, "wallet_outbox_oldest_pending_age_seconds", nil)); got != 0 {
		t.Fatalf("idade negativa não foi limitada: %v", got)
	}
	m.OutboxPublished(time.Unix(10, 0), time.Unix(5, 0)) // skew de relógio
	if got := histSum(find(t, reg, "wallet_outbox_publish_delay_seconds", nil)); got != 0 {
		t.Fatalf("atraso negativo não foi limitado: %v", got)
	}
}

func TestObserveHTTPInvalidCode(t *testing.T) {
	reg, m := newTestMetrics(t)
	m.ObserveHTTP("GET", "/health/live", 0, time.Millisecond)
	find(t, reg, "wallet_http_request_duration_seconds", map[string]string{"code": "unknown"})
}

// seriesCount devolve o número de séries de cada métrica do namespace.
func seriesCount(t *testing.T, reg prometheus.Gatherer) map[string]int {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	out := map[string]int{}
	for _, mf := range mfs {
		if strings.HasPrefix(mf.GetName(), Namespace+"_") {
			out[mf.GetName()] = len(mf.GetMetric())
		}
	}
	return out
}

// Regressão (avaliação T09): qualquer string virava rótulo (1000 métodos
// arbitrários = 1000 séries).
func TestArbitraryLabelInputHasBoundedCardinality(t *testing.T) {
	reg, m := newTestMetrics(t)
	m.AllowRoutes("/wallets/{id}")
	m.AllowQueues("wager-transactions-dlq.fifo")

	const n = 1000
	for i := range n {
		s := fmt.Sprintf("x-%d", i)
		m.ObserveHTTP(s, "/wallets/"+s, 200, time.Millisecond)
		m.ObserveHTTP(http.MethodGet, "/wallets/"+s, 200+i, time.Millisecond)
		m.TransactionResult(s, s, s)
		m.Duplicate(s, s)
		m.Retry(s)
		m.SentToDLQ(s)
		m.SetDLQDepth(s, int64(i))
		m.ConcurrencyConflict(s)
		m.ObserveProcessing(s, s, s, time.Millisecond)
		m.Reconciliation(s, true)
		// Tentativa de abrir a allowlist em runtime com valores arbitrários.
		m.AllowRoutes("/dyn/" + s)
	}
	m.ObserveHTTP("get", "/wallets/{id}", 200, time.Millisecond) // métodos diferenciam maiúsculas
	if got := len(m.routes.values); got > maxAllowListSize {
		t.Fatalf("allowlist de rotas cresceu além do limite: %d", got)
	}

	// Limites por métrica: produto dos conjuntos fechados (+ "other").
	limits := map[string]int{
		"wallet_http_request_duration_seconds":    2 * (maxAllowListSize + 1) * 501,
		"wallet_wager_transactions_total":         1,
		"wallet_duplicates_total":                 1,
		"wallet_retries_total":                    1,
		"wallet_dlq_messages_total":               1,
		"wallet_dlq_depth_messages":               1,
		"wallet_concurrency_conflicts_total":      1,
		"wallet_processing_duration_seconds":      1,
		"wallet_reconciliation_divergences_total": 1,
	}
	got := seriesCount(t, reg)
	for name, limit := range limits {
		if got[name] == 0 || got[name] > limit {
			t.Errorf("%s: %d séries, limite %d", name, got[name], limit)
		}
	}
	// Entradas arbitrárias caíram em "other"/"OTHER".
	find(t, reg, "wallet_http_request_duration_seconds", map[string]string{"method": MethodOther, "route": LabelValueOther, "code": "200"})
	find(t, reg, "wallet_http_request_duration_seconds", map[string]string{"method": MethodOther, "route": "/wallets/{id}"})
	find(t, reg, "wallet_wager_transactions_total", map[string]string{"source": "other", "type": "other", "status": "other"})
	find(t, reg, "wallet_dlq_depth_messages", map[string]string{"queue": "other"})
	find(t, reg, "wallet_reconciliation_divergences_total", map[string]string{"currency": "other"})
	// Códigos fora de 100-599 colapsam em "unknown"; a rota registrada fica
	// limitada a maxAllowListSize.
	if n := got["wallet_http_request_duration_seconds"]; n > 2+400+1 {
		t.Errorf("séries HTTP inesperadas: %d", n)
	}
}

func TestClosedLabelSetsAcceptKnownValues(t *testing.T) {
	reg, m := newTestMetrics(t)
	for _, typ := range []string{"OPENING", "BET", "WIN", "LOSS", "REFUND", "ROLLBACK"} {
		for _, st := range []string{"PENDING", "PENDING_REFERENCE", "PROCESSED", "REJECTED", "FAILED"} {
			m.TransactionResult(SourceWorker, typ, st)
			find(t, reg, "wallet_wager_transactions_total", map[string]string{"source": SourceWorker, "type": typ, "status": st})
		}
	}
	for _, cur := range []string{"BRL", "USD", "EUR", "GBP", "CAD", "AUD", "CHF", "MXN", "ARS", "COP", "PEN", "UYU"} {
		m.Reconciliation(cur, true)
		find(t, reg, "wallet_reconciliation_divergences_total", map[string]string{"currency": cur})
	}
	for _, method := range []string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "CONNECT", "OPTIONS", "TRACE"} {
		m.ObserveHTTP(method, "/x", 204, time.Millisecond)
		find(t, reg, "wallet_http_request_duration_seconds", map[string]string{"method": method, "route": "other"})
	}
	m.ObserveProcessing(SourceSQS, OperationPendingReference, OutcomeTransient, time.Millisecond)
	find(t, reg, "wallet_processing_duration_seconds", map[string]string{"source": SourceSQS, "operation": OperationPendingReference, "outcome": OutcomeTransient})
}

func TestAllowListIsConcurrencySafe(t *testing.T) {
	_, m := newTestMetrics(t)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(2)
		go func() { defer wg.Done(); m.AllowRoutes(fmt.Sprintf("/r/%d", i)) }()
		go func() { defer wg.Done(); m.ObserveHTTP(http.MethodGet, "/r/1", 200, time.Millisecond) }()
	}
	wg.Wait()
}

func TestRegistryIsIsolatedFromGlobal(t *testing.T) {
	// Dois conjuntos independentes não colidem (seria panic/erro no global).
	newTestMetrics(t)
	newTestMetrics(t)

	// Registrar duas vezes no mesmo registry é erro, não panic.
	reg, _ := newTestMetrics(t)
	if _, err := New(reg); err == nil {
		t.Fatal("esperava erro de registro duplicado")
	}

	// Nada foi registrado no registry global.
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if strings.HasPrefix(mf.GetName(), Namespace+"_") {
			t.Fatalf("métrica %s vazou para o registry global", mf.GetName())
		}
	}
}

func TestHandlerExposesMetrics(t *testing.T) {
	reg, m := newTestMetrics(t)
	m.Retry(OperationOutboxPublish)
	srv := httptest.NewServer(Handler(reg))
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), `wallet_retries_total{operation="outbox_publish"} 1`) {
		t.Fatalf("métrica ausente na exposição:\n%s", body)
	}
}

func TestModuleValidateApp(t *testing.T) {
	err := fx.ValidateApp(Module, fx.Invoke(func(
		*Metrics, *prometheus.Registry, prometheus.Registerer, prometheus.Gatherer,
	) {
	}))
	if err != nil {
		t.Fatalf("ValidateApp: %v", err)
	}
}

func TestModuleWiresSameRegistry(t *testing.T) {
	var p struct {
		fx.In
		Metrics  *Metrics
		Registry *prometheus.Registry
		Gatherer prometheus.Gatherer
		Handler  http.Handler `name:"metricsHandler"`
	}
	app := fx.New(fx.NopLogger, Module, fx.Populate(&p))
	if err := app.Err(); err != nil {
		t.Fatalf("app.Err: %v", err)
	}
	if p.Gatherer != prometheus.Gatherer(p.Registry) {
		t.Fatal("Gatherer deveria ser o mesmo registry")
	}
	p.Metrics.ConcurrencyConflict(ConflictDeadlock)
	rec := httptest.NewRecorder()
	p.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(rec.Body.String(), `wallet_concurrency_conflicts_total{kind="deadlock"} 1`) {
		t.Fatalf("handler não expõe o registry do módulo:\n%s", rec.Body.String())
	}
}

// Dois apps Fx no mesmo processo (ex.: testes multi-instância) não colidem.
func TestTwoAppsDoNotCollide(t *testing.T) {
	for range 2 {
		app := fx.New(fx.NopLogger, Module, fx.Invoke(func(*Metrics) {}))
		if err := app.Err(); err != nil {
			t.Fatalf("app.Err: %v", err)
		}
	}
}
