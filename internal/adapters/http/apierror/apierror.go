// Package apierror define o corpo JSON padronizado das respostas de erro da
// API HTTP (ARCHITECTURE.md, ADR-019). É compartilhado pelo middleware de
// autenticação (internal/adapters/auth) e pelos handlers (ADR-020), para que todo
// erro tenha o mesmo formato independentemente de onde foi produzido.
package apierror

import (
	"encoding/json"
	"net/http"

	"github.com/junglegaming/backend-challenge-go/internal/platform/logging"
)

// Body é o corpo de erro da API:
//
//	{"code":"UNAUTHENTICATED","message":"token ausente","correlationId":"..."}
//
// Code é estável e serve ao contrato; Message é texto para humanos e nunca
// carrega dados sensíveis (token, credenciais, detalhes de validação
// internos). CorrelationID vem do contexto da requisição, quando houver.
type Body struct {
	Code          string `json:"code"`
	Message       string `json:"message"`
	CorrelationID string `json:"correlationId,omitempty"`
}

// Write escreve status e corpo de erro JSON. Os cabeçalhos extras (ex.:
// WWW-Authenticate, Retry-After) devem ser definidos antes da chamada.
// Respostas de erro não são cacheáveis.
func Write(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	body := Body{Code: code, Message: message}
	if r != nil {
		body.CorrelationID = logging.IDsFromContext(r.Context()).CorrelationID
	}
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	// Falha de escrita significa cliente desconectado; não há o que fazer.
	_ = json.NewEncoder(w).Encode(body)
}
