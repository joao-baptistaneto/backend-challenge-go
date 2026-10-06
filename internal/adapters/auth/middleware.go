package auth

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/http/apierror"
	"github.com/junglegaming/backend-challenge-go/internal/platform/logging"
)

// Realm anunciado no WWW-Authenticate.
const Realm = "wallet"

// Códigos estáveis do corpo de erro (apierror.Body.Code).
const (
	CodeUnauthenticated        = "UNAUTHENTICATED"
	CodeForbidden              = "FORBIDDEN"
	CodeProviderMismatch       = "PROVIDER_MISMATCH"
	CodeTemporarilyUnavailable = "TEMPORARILY_UNAVAILABLE"
	CodeInternalError          = "INTERNAL_ERROR"
)

// retryAfterSeconds é sugerido no 503 quando o IdP está indisponível.
const retryAfterSeconds = "5"

// Middleware devolve um middleware net/http que exige um token Bearer válido
// em toda rota, exceto as publicPaths (comparação exata com r.URL.Path, ex.:
// "/health/live"). Qualquer variação ("/health/live/", "/health/../x") não é
// pública: na dúvida, exige token. Em rota pública o cabeçalho Authorization
// é ignorado.
//
// Com token válido, o Principal vai para o contexto (PrincipalFromContext) e o
// providerId, se houver, para os IDs de log (logging.WithProviderID).
// Falhas viram 401 (ou 503 se as chaves do IdP estiverem indisponíveis) via
// WriteError. publicPaths precisam começar com "/" (panic na montagem,
// como http.ServeMux faz com padrões inválidos).
func (a *Authenticator) Middleware(publicPaths ...string) func(http.Handler) http.Handler {
	public := make(map[string]struct{}, len(publicPaths))
	for _, p := range publicPaths {
		if !strings.HasPrefix(p, "/") {
			panic("auth: rota pública deve começar com /: " + p)
		}
		public[p] = struct{}{}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, ok := public[r.URL.Path]; ok {
				next.ServeHTTP(w, r)
				return
			}
			raw, err := bearerToken(r)
			var p Principal
			if err == nil {
				p, err = a.Authenticate(r.Context(), raw)
			}
			if err != nil {
				a.logFailure(r, err)
				WriteError(w, r, err)
				return
			}
			ctx := ContextWithPrincipal(r.Context(), p)
			if p.ProviderID != "" {
				ctx = logging.WithProviderID(ctx, p.ProviderID)
			}
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// bearerToken extrai o token do único cabeçalho Authorization "Bearer <t>".
func bearerToken(r *http.Request) (string, error) {
	vals := r.Header.Values("Authorization")
	switch len(vals) {
	case 0:
		return "", newError(ErrMissingToken, ReasonMissing)
	case 1:
	default:
		return "", newError(ErrInvalidToken, ReasonMultipleHeaders)
	}
	v := strings.TrimSpace(vals[0])
	if v == "" {
		return "", newError(ErrMissingToken, ReasonMissing)
	}
	scheme, tok, _ := strings.Cut(v, " ")
	if !strings.EqualFold(scheme, "Bearer") {
		// Outro esquema (Basic, ...): não há credencial Bearer.
		return "", newError(ErrMissingToken, ReasonUnsupportedScheme)
	}
	tok = strings.TrimLeft(tok, " ")
	if tok == "" || strings.ContainsAny(tok, " \t\r\n") {
		return "", newError(ErrInvalidToken, ReasonMalformed)
	}
	return tok, nil
}

func (a *Authenticator) logFailure(r *http.Request, err error) {
	attrs := []slog.Attr{
		slog.String("reason", Reason(err)),
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
	}
	if errors.Is(err, ErrUnavailable) {
		var e *Error
		if errors.As(err, &e) && e.Err != nil {
			attrs = append(attrs, slog.String("error", e.Err.Error()))
		}
		a.logger.LogAttrs(r.Context(), slog.LevelWarn, "auth: chaves do IdP indisponíveis", attrs...)
		return
	}
	// Nunca o token, o cabeçalho ou valores de claims: só a categoria.
	a.logger.LogAttrs(r.Context(), slog.LevelInfo, "auth: requisição não autenticada", attrs...)
}

// WriteError escreve a resposta de um erro de autenticação/autorização:
//
//   - ErrMissingToken → 401, WWW-Authenticate: Bearer realm="wallet";
//   - ErrTokenExpired → 401, ... error="invalid_token", error_description="token expired";
//   - demais ErrUnauthenticated → 401, ... error="invalid_token";
//   - ErrProviderMismatch → 403 PROVIDER_MISMATCH;
//   - demais ErrForbidden → 403 FORBIDDEN, ... error="insufficient_scope";
//   - ErrUnavailable → 503 TEMPORARILY_UNAVAILABLE com Retry-After;
//   - outro erro → 500 INTERNAL_ERROR.
//
// O corpo segue apierror.Body e nunca descreve qual verificação falhou.
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	h := w.Header()
	switch {
	case errors.Is(err, ErrMissingToken):
		h.Set("WWW-Authenticate", `Bearer realm="`+Realm+`"`)
		apierror.Write(w, r, http.StatusUnauthorized, CodeUnauthenticated, "token de acesso ausente")
	case errors.Is(err, ErrTokenExpired):
		h.Set("WWW-Authenticate", `Bearer realm="`+Realm+`", error="invalid_token", error_description="token expired"`)
		apierror.Write(w, r, http.StatusUnauthorized, CodeUnauthenticated, "token de acesso expirado")
	case errors.Is(err, ErrUnauthenticated):
		h.Set("WWW-Authenticate", `Bearer realm="`+Realm+`", error="invalid_token", error_description="invalid token"`)
		apierror.Write(w, r, http.StatusUnauthorized, CodeUnauthenticated, "token de acesso inválido")
	case errors.Is(err, ErrProviderMismatch):
		apierror.Write(w, r, http.StatusForbidden, CodeProviderMismatch,
			"providerId não corresponde à identidade autenticada")
	case errors.Is(err, ErrForbidden):
		h.Set("WWW-Authenticate", `Bearer realm="`+Realm+`", error="insufficient_scope"`)
		apierror.Write(w, r, http.StatusForbidden, CodeForbidden, "acesso não permitido para esta identidade")
	case errors.Is(err, ErrUnavailable):
		h.Set("Retry-After", retryAfterSeconds)
		apierror.Write(w, r, http.StatusServiceUnavailable, CodeTemporarilyUnavailable,
			"provedor de identidade temporariamente indisponível")
	default:
		apierror.Write(w, r, http.StatusInternalServerError, CodeInternalError, "erro interno")
	}
}
