package auth

import (
	"context"
	"io"
	"log/slog"
	"net/http"
)

// Authorizer aplica o modelo de permissões (ADR-019):
//
//   - operações de carteira (abrir, consultar, ledger, reconciliar) exigem o
//     papel de serviço interno (Internal / RequireInternal);
//   - operações de transação de aposta exigem o papel de provedor e um
//     providerId na identidade (Provider / RequireProvider); o providerId
//     usado nos casos de uso é sempre o da identidade, e o informado pelo
//     cliente (corpo, path) precisa casar com ele (MatchProvider).
//
// Os métodos registram a negação (INFO, só categoria e papel exigido) e
// devolvem *Error; WriteError produz o 401/403.
type Authorizer struct {
	providerRole string
	internalRole string
	logger       *slog.Logger
}

// NewAuthorizer cria o Authorizer com os papéis configurados.
func NewAuthorizer(s Settings, logger *slog.Logger) *Authorizer {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	return &Authorizer{providerRole: s.ProviderRole, internalRole: s.InternalRole, logger: logger}
}

// Internal autoriza operações de carteira: exige o papel de serviço interno.
func (z *Authorizer) Internal(ctx context.Context) error {
	p, ok := PrincipalFromContext(ctx)
	if !ok {
		return newError(ErrUnauthenticated, ReasonNoPrincipal)
	}
	if !p.HasRole(z.internalRole) {
		return z.deny(ctx, newError(ErrMissingRole, ReasonMissingRole), z.internalRole)
	}
	return nil
}

// Provider autoriza operações de transação e devolve o providerId da
// identidade, que deve ser o usado nas consultas e no processamento.
func (z *Authorizer) Provider(ctx context.Context) (string, error) {
	p, ok := PrincipalFromContext(ctx)
	if !ok {
		return "", newError(ErrUnauthenticated, ReasonNoPrincipal)
	}
	if !p.HasRole(z.providerRole) {
		return "", z.deny(ctx, newError(ErrMissingRole, ReasonMissingRole), z.providerRole)
	}
	if p.ProviderID == "" {
		return "", z.deny(ctx, newError(ErrMissingRole, ReasonMissingProviderID), z.providerRole)
	}
	return p.ProviderID, nil
}

// MatchProvider autoriza como Provider e exige que requested (providerId do
// corpo de POST /wagering/transactions ou do path
// /providers/{providerId}/...) seja exatamente o da identidade. Devolve o
// providerId da identidade. requested vazio também diverge: a borda deve
// validar a presença do campo antes (400).
func (z *Authorizer) MatchProvider(ctx context.Context, requested string) (string, error) {
	id, err := z.Provider(ctx)
	if err != nil {
		return "", err
	}
	if requested != id {
		return "", z.deny(ctx, newError(ErrProviderMismatch, ReasonProviderMismatch), z.providerRole)
	}
	return id, nil
}

// RequireInternal envolve um handler que só o serviço interno pode chamar.
func (z *Authorizer) RequireInternal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := z.Internal(r.Context()); err != nil {
			WriteError(w, r, err)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// RequireProvider envolve um handler que só provedores (com providerId)
// podem chamar. O handler obtém o providerId por Provider/MatchProvider.
func (z *Authorizer) RequireProvider(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := z.Provider(r.Context()); err != nil {
			WriteError(w, r, err)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (z *Authorizer) deny(ctx context.Context, err *Error, requiredRole string) error {
	p, _ := PrincipalFromContext(ctx)
	z.logger.LogAttrs(ctx, slog.LevelInfo, "auth: acesso negado",
		slog.String("reason", err.Reason),
		slog.String("requiredRole", requiredRole),
		slog.String("clientId", p.ClientID),
	)
	return err
}
