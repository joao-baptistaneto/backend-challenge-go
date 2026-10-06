package httpadapter

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

// API HTTP (ARCHITECTURE.md, ADR-020).
//
// Decisões:
//
//   - net/http puro com os padrões de rota do ServeMux (Go 1.22+: método e
//     curingas {id}). Sem roteador de terceiros.
//   - Cadeia de middlewares, de fora para dentro: correlation-id →
//     observação (métrica de latência por rota e log de acesso) → recover
//     (panic → 500) → autenticação (auth.Authenticator.Middleware, com só os
//     health checks públicos por path exato) → roteador → autorização por
//     rota.
//     O correlation-id vem antes da autenticação para que até um 401 traga o
//     correlationId no corpo e no cabeçalho.
//   - Autorização por rota (ADR-019): carteira e reconciliação exigem o
//     serviço interno; transações exigem provedor e usam SEMPRE o providerId
//     da identidade. /metrics exige o serviço interno por padrão
//     (MetricsAccess).
//   - Os handlers dependem de interfaces estreitas (WalletOpener,
//     WagerProcessor, ...) satisfeitas pelos casos de uso de internal/app,
//     para que os testes de contrato rodem com httptest.

// WalletOpener abre carteiras (*app.OpenWallet).
type WalletOpener interface {
	Execute(ctx context.Context, cmd app.OpenWalletCommand) (app.OpenWalletResult, error)
}

// WagerProcessor processa operações de provedor (*app.ProcessWagerTransaction).
type WagerProcessor interface {
	Execute(ctx context.Context, cmd app.ProcessWagerCommand) (app.WagerResult, error)
}

// WalletReconciler reconcilia carteiras (*app.ReconcileWallet).
type WalletReconciler interface {
	Execute(ctx context.Context, cmd app.ReconcileCommand) (app.ReconciliationReport, error)
}

// QueryService atende as consultas (*app.Queries).
type QueryService interface {
	GetWallet(ctx context.Context, walletID uuid.UUID) (app.WalletSnapshot, error)
	ListLedger(ctx context.Context, in app.ListLedgerQuery) (app.LedgerPageView, error)
	GetTransaction(ctx context.Context, in app.GetTransactionQuery) (app.TransactionView, error)
	GetTransactionByProvider(ctx context.Context, in app.GetTransactionByProviderQuery) (app.TransactionView, error)
}

// Authenticator monta o middleware de autenticação (*auth.Authenticator).
type Authenticator interface {
	Middleware(publicPaths ...string) func(http.Handler) http.Handler
}

// Authorizer aplica o modelo de permissões (*auth.Authorizer).
type Authorizer interface {
	Internal(ctx context.Context) error
	Provider(ctx context.Context) (string, error)
	MatchProvider(ctx context.Context, requested string) (string, error)
}

// ReadinessCheck verifica uma dependência para /health/ready (PostgreSQL,
// SQS). Check deve respeitar o prazo do contexto.
type ReadinessCheck struct {
	// Name aparece no corpo da resposta (ex.: "postgres"). Não deve conter
	// detalhes de conexão.
	Name  string
	Check func(ctx context.Context) error
}

// MetricsAccess define quem pode ler /metrics.
type MetricsAccess int

const (
	// MetricsInternal (padrão): exige token válido com o papel de serviço
	// interno.
	MetricsInternal MetricsAccess = iota
	// MetricsPublic: sem autenticação (path exato), para scrapers sem OAuth
	// numa rede privada.
	MetricsPublic
)

// Rotas (templates). Também são os rótulos de rota da métrica de latência.
const (
	RouteOpenWallet                 = "/wallets"
	RouteGetWallet                  = "/wallets/{walletId}"
	RouteListLedger                 = "/wallets/{walletId}/ledger"
	RouteReconcile                  = "/wallets/{walletId}/reconciliation"
	RouteSubmitTransaction          = "/wagering/transactions"
	RouteGetTransaction             = "/wagering/transactions/{transactionId}"
	RouteGetTransactionByExternalID = "/providers/{providerId}/wagering/transactions/{externalTransactionId}"
	RouteHealthLive                 = "/health/live"
	RouteHealthReady                = "/health/ready"
	RouteMetrics                    = "/metrics"
)

// AllRoutes devolve os templates de rota (para metrics.AllowRoutes).
func AllRoutes() []string {
	return []string{
		RouteOpenWallet, RouteGetWallet, RouteListLedger, RouteReconcile,
		RouteSubmitTransaction, RouteGetTransaction, RouteGetTransactionByExternalID,
		RouteHealthLive, RouteHealthReady, RouteMetrics,
	}
}

// Deps são as dependências da API.
type Deps struct {
	OpenWallet WalletOpener
	Process    WagerProcessor
	Reconcile  WalletReconciler
	Queries    QueryService

	Authenticator Authenticator
	Authorizer    Authorizer

	// Metrics é opcional (nil = sem métricas).
	Metrics *metrics.Metrics
	// MetricsHandler serve /metrics; nil = rota não montada.
	MetricsHandler http.Handler
	// MetricsAccess: MetricsInternal (padrão) ou MetricsPublic.
	MetricsAccess MetricsAccess
	// Readiness são as verificações de /health/ready.
	Readiness []ReadinessCheck
	// Logger é opcional (nil = descarta).
	Logger *slog.Logger
}

func (d Deps) validate() error {
	var missing []string
	if d.OpenWallet == nil {
		missing = append(missing, "OpenWallet")
	}
	if d.Process == nil {
		missing = append(missing, "Process")
	}
	if d.Reconcile == nil {
		missing = append(missing, "Reconcile")
	}
	if d.Queries == nil {
		missing = append(missing, "Queries")
	}
	if d.Authenticator == nil {
		missing = append(missing, "Authenticator")
	}
	if d.Authorizer == nil {
		missing = append(missing, "Authorizer")
	}
	for i, c := range d.Readiness {
		if c.Name == "" || c.Check == nil {
			missing = append(missing, "Readiness["+strconv.Itoa(i)+"]")
		}
	}
	if len(missing) > 0 {
		return errors.New("httpadapter: missing dependencies: " + strings.Join(missing, ", "))
	}
	return nil
}

// api guarda as dependências usadas pelos handlers.
type api struct {
	Deps
	logger *slog.Logger
}

// NewHandler monta o http.Handler completo da API (middlewares + rotas).
func NewHandler(d Deps) (http.Handler, error) {
	if err := d.validate(); err != nil {
		return nil, err
	}
	logger := d.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	a := &api{Deps: d, logger: logger.With(slog.String("component", "http"))}
	if d.Metrics != nil {
		d.Metrics.AllowRoutes(AllRoutes()...)
	}

	mux := http.NewServeMux()
	internal := a.requireInternal
	provider := a.requireProvider

	a.handle(mux, http.MethodPost, RouteOpenWallet, internal(a.handleOpenWallet))
	a.handle(mux, http.MethodGet, RouteGetWallet, internal(a.handleGetWallet))
	a.handle(mux, http.MethodGet, RouteListLedger, internal(a.handleListLedger))
	a.handle(mux, http.MethodPost, RouteReconcile, internal(a.handleReconcile))
	a.handle(mux, http.MethodPost, RouteSubmitTransaction, provider(a.handleSubmitTransaction))
	a.handle(mux, http.MethodGet, RouteGetTransaction, provider(a.handleGetTransaction))
	a.handle(mux, http.MethodGet, RouteGetTransactionByExternalID, provider(a.handleGetTransactionByExternal))
	a.handle(mux, http.MethodGet, RouteHealthLive, a.handleLive)
	a.handle(mux, http.MethodGet, RouteHealthReady, a.handleReady)

	public := []string{RouteHealthLive, RouteHealthReady}
	if d.MetricsHandler != nil {
		h := d.MetricsHandler.ServeHTTP
		if d.MetricsAccess == MetricsPublic {
			public = append(public, RouteMetrics)
		} else {
			h = internal(h)
		}
		a.handle(mux, http.MethodGet, RouteMetrics, h)
	}
	// Qualquer outro caminho: 404 no formato padrão (depois da autenticação,
	// então não revela rotas a quem não tem token).
	// Caminho conhecido com outro método: 405 com Allow.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if allow := allowedMethods(mux, r); len(allow) > 0 {
			w.Header().Set("Allow", strings.Join(allow, ", "))
			writeError(w, r, http.StatusMethodNotAllowed, CodeMethodNotAllowed, "método não permitido")
			return
		}
		writeError(w, r, http.StatusNotFound, CodeRouteNotFound, "rota não encontrada")
	})

	var h http.Handler = mux
	h = d.Authenticator.Middleware(public...)(h)
	h = a.recoverPanics(h)
	h = a.observe(h)
	h = a.correlation(h)
	return h, nil
}

// handle registra "METHOD template" e anota o template no contexto de
// observação (rótulo da métrica e do log de acesso).
func (a *api) handle(mux *http.ServeMux, method, route string, h http.HandlerFunc) {
	mux.HandleFunc(method+" "+route, func(w http.ResponseWriter, r *http.Request) {
		if info := routeInfoFrom(r.Context()); info != nil {
			info.route = route
		}
		h(w, r)
	})
}

// requireInternal: só o serviço interno (operações de carteira).
func (a *api) requireInternal(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := a.Authorizer.Internal(r.Context()); err != nil {
			writeAuthError(w, r, err)
			return
		}
		next(w, r)
	}
}

// requireProvider: só provedores com providerId na identidade.
func (a *api) requireProvider(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, err := a.Authorizer.Provider(r.Context()); err != nil {
			writeAuthError(w, r, err)
			return
		}
		next(w, r)
	}
}

// allowedMethods devolve os métodos com rota própria para o path de r
// (consulta o próprio mux; o curinga "/" não conta).
func allowedMethods(mux *http.ServeMux, r *http.Request) []string {
	var allow []string
	for _, m := range []string{http.MethodGet, http.MethodPost} {
		probe := r.Clone(r.Context())
		probe.Method = m
		if _, pattern := mux.Handler(probe); pattern != "" && pattern != "/" {
			allow = append(allow, m)
		}
	}
	return allow
}
