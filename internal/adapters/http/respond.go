package httpadapter

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/auth"
	"github.com/junglegaming/backend-challenge-go/internal/adapters/http/apierror"
	"github.com/junglegaming/backend-challenge-go/internal/app"
)

// Códigos de erro produzidos pela própria borda HTTP (os demais vêm de
// app.Code*, domain.FailureCode e auth.Code*).
const (
	CodeInvalidRequest         = "INVALID_REQUEST"
	CodeIdempotencyKeyRequired = "IDEMPOTENCY_KEY_REQUIRED"
	CodeInvalidIdempotencyKey  = "INVALID_IDEMPOTENCY_KEY"
	CodeInvalidCorrelationID   = app.CodeInvalidCorrelationID
	CodeUnsupportedMediaType   = "UNSUPPORTED_MEDIA_TYPE"
	CodePayloadTooLarge        = "PAYLOAD_TOO_LARGE"
	CodeRouteNotFound          = "ROUTE_NOT_FOUND"
	CodeMethodNotAllowed       = "METHOD_NOT_ALLOWED"
	CodeInternalError          = auth.CodeInternalError
	CodeTemporarilyUnavailable = app.CodeTemporarilyUnavailable
)

// RetryAfterSeconds é o Retry-After sugerido nos 503 da aplicação.
const RetryAfterSeconds = "1"

// writeJSON escreve status e corpo JSON (sem cache).
func writeJSON(w http.ResponseWriter, status int, body any) {
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	// Falha de escrita: cliente desconectado; nada a fazer.
	_ = json.NewEncoder(w).Encode(body)
}

// writeError escreve o corpo de erro padrão (apierror.Body).
func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	apierror.Write(w, r, status, code, message)
}

// writeAuthError delega a resposta de autenticação/autorização ao pacote auth
// (401/403/503 com os cabeçalhos corretos).
func writeAuthError(w http.ResponseWriter, r *http.Request, err error) {
	auth.WriteError(w, r, err)
}

// writeUnavailable: 503 + Retry-After.
func writeUnavailable(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Retry-After", RetryAfterSeconds)
	writeError(w, r, http.StatusServiceUnavailable, CodeTemporarilyUnavailable,
		"serviço temporariamente indisponível; tente novamente")
}

// writeAppError mapeia o erro de um caso de uso para HTTP:
//
//   - app.KindInvalidInput → 400 com o código do erro;
//   - app.KindConflict     → 409 (IDEMPOTENCY_KEY_CONFLICT,
//     EXTERNAL_TRANSACTION_CONFLICT, WALLET_ALREADY_EXISTS...);
//   - app.KindNotFound     → 404 (WALLET_NOT_FOUND, TRANSACTION_NOT_FOUND);
//   - app.KindUnavailable e prazo/cancelamento → 503 + Retry-After;
//   - qualquer outro → 500 INTERNAL_ERROR (detalhe só no log).
//
// A mensagem é genérica por classe: a causa técnica nunca vai ao cliente.
func (a *api) writeAppError(w http.ResponseWriter, r *http.Request, err error) {
	if ae, ok := app.AsError(err); ok {
		switch ae.Kind {
		case app.KindInvalidInput:
			writeError(w, r, http.StatusBadRequest, ae.Code, "requisição inválida")
			return
		case app.KindConflict:
			writeError(w, r, http.StatusConflict, ae.Code, conflictMessage(ae.Code))
			return
		case app.KindNotFound:
			writeError(w, r, http.StatusNotFound, ae.Code, "recurso não encontrado")
			return
		case app.KindUnavailable:
			writeUnavailable(w, r)
			return
		}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		writeUnavailable(w, r)
		return
	}
	a.logger.ErrorContext(r.Context(), "erro interno no handler HTTP", slog.String("error", err.Error()))
	writeError(w, r, http.StatusInternalServerError, CodeInternalError, "erro interno")
}

func conflictMessage(code string) string {
	switch code {
	case app.CodeIdempotencyKeyConflict:
		return "Idempotency-Key já usada com outro conteúdo"
	case app.CodeExternalTransactionConflict:
		return "operação já registrada sob outra Idempotency-Key"
	case app.CodeWalletAlreadyExists:
		return "já existe carteira para o jogador e a moeda"
	default:
		return "conflito com o estado atual"
	}
}
