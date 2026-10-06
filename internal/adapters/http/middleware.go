package httpadapter

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/platform/logging"
)

// HeaderCorrelationID é o cabeçalho de correlação (entrada e saída).
const HeaderCorrelationID = "X-Correlation-ID"

// correlation define o correlationId da requisição: o do cabeçalho
// X-Correlation-ID, se válido (mesmas regras do envelope de eventos:
// 1..255 bytes, sem controle, sem espaço nas pontas), ou um UUIDv7 gerado.
// Vários cabeçalhos ou valor inválido → 400 INVALID_CORRELATION_ID (o valor
// recebido não é ecoado). O id vai para o contexto (logs, corpo de erro,
// eventos) e para o cabeçalho de resposta.
func (a *api) correlation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vals := r.Header.Values(HeaderCorrelationID)
		var id string
		switch len(vals) {
		case 0:
			gen, err := uuid.NewV7()
			if err != nil {
				gen = uuid.New()
			}
			id = gen.String()
		case 1:
			id = vals[0]
			if err := (domain.EventMetadata{CorrelationID: id}).Validate(); err != nil {
				id = ""
			}
		}
		if id == "" {
			writeError(w, r, http.StatusBadRequest, CodeInvalidCorrelationID,
				"cabeçalho "+HeaderCorrelationID+" inválido")
			return
		}
		w.Header().Set(HeaderCorrelationID, id)
		next.ServeHTTP(w, r.WithContext(logging.WithCorrelationID(r.Context(), id)))
	})
}

// routeInfo é preenchido pelo handler da rota casada (template), para que a
// observação use o template, nunca o path com ids, como rótulo.
type routeInfo struct{ route string }

type routeInfoKey struct{}

func routeInfoFrom(ctx context.Context) *routeInfo {
	info, _ := ctx.Value(routeInfoKey{}).(*routeInfo)
	return info
}

// statusRecorder guarda o status escrito.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// observe registra a latência por rota (template) e um log de acesso. O log
// leva método, rota, status e duração; nunca cabeçalhos, query ou corpo.
func (a *api) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		info := &routeInfo{}
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r.WithContext(context.WithValue(r.Context(), routeInfoKey{}, info)))
		status := rec.status
		if status == 0 {
			status = http.StatusOK
		}
		elapsed := time.Since(start)
		if a.Metrics != nil {
			a.Metrics.ObserveHTTP(r.Method, info.route, status, elapsed)
		}
		level := slog.LevelInfo
		if status >= http.StatusInternalServerError {
			level = slog.LevelWarn
		}
		route := info.route
		if route == "" {
			route = "unmatched"
		}
		a.logger.LogAttrs(r.Context(), level, "requisição HTTP",
			slog.String("method", r.Method),
			slog.String("route", route),
			slog.Int("status", status),
			slog.Duration("elapsed", elapsed),
		)
	})
}

// recoverPanics converte um panic do handler em 500 INTERNAL_ERROR (se nada
// foi escrito ainda) e registra o erro. http.ErrAbortHandler é repassado.
func (a *api) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w}
		defer func() {
			v := recover()
			if v == nil {
				return
			}
			if v == http.ErrAbortHandler { //nolint:errorlint // valor sentinela do panic
				panic(v)
			}
			a.logger.ErrorContext(r.Context(), "panic no handler HTTP", slog.Any("panic", v))
			if rec.status == 0 {
				writeError(rec, r, http.StatusInternalServerError, CodeInternalError, "erro interno")
			}
		}()
		next.ServeHTTP(rec, r)
	})
}
