package httpadapter

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// ReadinessTimeout é o prazo de cada verificação de /health/ready.
const ReadinessTimeout = 2 * time.Second

const (
	statusUp   = "UP"
	statusDown = "DOWN"
)

type healthResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

// GET /health/live: o processo responde. Não consulta dependências (uma
// dependência fora não deve reiniciar o processo).
func (a *api) handleLive(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: statusUp})
}

// GET /health/ready: executa as verificações em paralelo, cada uma com
// ReadinessTimeout. Todas UP → 200; alguma DOWN → 503 + Retry-After. O corpo
// traz só nome e UP/DOWN; a causa vai para o log (WARN).
func (a *api) handleReady(w http.ResponseWriter, r *http.Request) {
	checks := make(map[string]string, len(a.Readiness))
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	for _, c := range a.Readiness {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(r.Context(), ReadinessTimeout)
			defer cancel()
			err := runCheck(ctx, c)
			status := statusUp
			if err != nil {
				status = statusDown
				a.logger.WarnContext(r.Context(), "dependência indisponível",
					slog.String("check", c.Name), slog.String("error", err.Error()))
			}
			mu.Lock()
			checks[c.Name] = status
			mu.Unlock()
		}()
	}
	wg.Wait()

	resp := healthResponse{Status: statusUp, Checks: checks}
	for _, s := range checks {
		if s != statusUp {
			resp.Status = statusDown
		}
	}
	if resp.Status != statusUp {
		w.Header().Set("Retry-After", RetryAfterSeconds)
		writeJSON(w, http.StatusServiceUnavailable, resp)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// runCheck executa a verificação convertendo panic em erro.
func runCheck(ctx context.Context, c ReadinessCheck) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("readiness check panicked: %v", v)
		}
	}()
	return c.Check(ctx)
}
