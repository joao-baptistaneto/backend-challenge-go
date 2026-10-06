package auth

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/coreos/go-oidc/v3/oidc"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// maxTokenBytes limita o tamanho do token aceito (tokens do Keycloak têm
// poucos KiB); evita parse de entradas arbitrariamente grandes.
const maxTokenBytes = 16 << 10

// Authenticator valida tokens de acesso JWT e extrai o Principal.
type Authenticator struct {
	settings Settings
	verifier *oidc.IDTokenVerifier
	keys     *keySet
	now      func() time.Time
	logger   *slog.Logger
}

// Option ajusta o Authenticator (usado em testes e na composição).
type Option func(*options)

type options struct {
	client       *http.Client
	now          func() time.Time
	minRefresh   time.Duration
	maxAge       time.Duration
	fetchTimeout time.Duration
}

// WithHTTPClient define o cliente HTTP do JWKS/discovery.
func WithHTTPClient(c *http.Client) Option { return func(o *options) { o.client = c } }

// WithClock define o relógio (exp/nbf/iat e cache de chaves).
func WithClock(now func() time.Time) Option { return func(o *options) { o.now = now } }

// WithJWKSRefreshInterval define o intervalo mínimo entre buscas do JWKS
// (default 10s).
func WithJWKSRefreshInterval(d time.Duration) Option {
	return func(o *options) { o.minRefresh = d }
}

// WithJWKSMaxAge define a idade máxima das chaves em cache (default 10m).
func WithJWKSMaxAge(d time.Duration) Option { return func(o *options) { o.maxAge = d } }

// WithJWKSFetchTimeout define o timeout de cada busca (default 5s).
func WithJWKSFetchTimeout(d time.Duration) Option {
	return func(o *options) { o.fetchTimeout = d }
}

// NewAuthenticator valida as configurações e cria o Authenticator. Não acessa
// a rede: as chaves são buscadas no primeiro token (o start não depende do
// IdP estar no ar).
func NewAuthenticator(s Settings, logger *slog.Logger, opts ...Option) (*Authenticator, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	o := options{
		client:       &http.Client{Timeout: defaultFetchTimeout},
		now:          time.Now,
		minRefresh:   defaultMinRefreshInterval,
		maxAge:       defaultKeysMaxAge,
		fetchTimeout: defaultFetchTimeout,
	}
	for _, opt := range opts {
		opt(&o)
	}
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	ks := &keySet{
		client:       o.client,
		issuer:       s.Issuer,
		staticURL:    s.JWKSURL,
		jwksURL:      s.JWKSURL,
		minRefresh:   o.minRefresh,
		maxAge:       o.maxAge,
		fetchTimeout: o.fetchTimeout,
		now:          o.now,
		logger:       logger,
	}
	verifier := oidc.NewVerifier(s.Issuer, ks, &oidc.Config{
		ClientID:             s.Audience,
		SupportedSigningAlgs: allowedAlgorithmNames(),
		// exp/nbf/iat são verificados abaixo, com a tolerância configurada
		// (o go-oidc não tem tolerância em exp e usa 5 min fixos em nbf).
		SkipExpiryCheck: true,
	})
	return &Authenticator{settings: s, verifier: verifier, keys: ks, now: o.now, logger: logger}, nil
}

// Settings devolve as configurações em uso.
func (a *Authenticator) Settings() Settings { return a.settings }

// tokenClaims são as claims lidas além das que o go-oidc valida (iss, aud,
// exp). Tipos errados (ex.: nbf string) fazem o token ser recusado.
type tokenClaims struct {
	NotBefore      *float64 `json:"nbf"`
	IssuedAt       *float64 `json:"iat"`
	Type           *string  `json:"typ"`
	AuthorizedPart string   `json:"azp"`
	RealmAccess    *struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
	ResourceAccess map[string]struct {
		Roles []string `json:"roles"`
	} `json:"resource_access"`
}

// Authenticate valida o token bruto (sem o prefixo "Bearer") e devolve o
// principal. Erros: *Error com classe ErrInvalidToken, ErrTokenExpired ou
// ErrUnavailable.
func (a *Authenticator) Authenticate(ctx context.Context, raw string) (Principal, error) {
	if raw == "" {
		return Principal{}, newError(ErrMissingToken, ReasonMissing)
	}
	if len(raw) > maxTokenBytes {
		return Principal{}, newError(ErrInvalidToken, ReasonTooLarge)
	}

	st := &verifyState{}
	tok, err := a.verifier.Verify(withVerifyState(ctx, st), raw)
	if err != nil {
		return Principal{}, classifyVerifyError(st)
	}

	var c tokenClaims
	if err := tok.Claims(&c); err != nil {
		return Principal{}, newError(ErrInvalidToken, ReasonClaims)
	}

	// Tempo: exp obrigatório; nbf e iat opcionais; tolerância ClockSkew.
	now := a.now()
	skew := a.settings.ClockSkew
	if tok.Expiry.IsZero() {
		return Principal{}, newError(ErrInvalidToken, ReasonMissingExpiry)
	}
	if now.After(tok.Expiry.Add(skew)) {
		return Principal{}, newError(ErrTokenExpired, ReasonExpired)
	}
	if c.NotBefore != nil {
		nbf, ok := unixTime(*c.NotBefore)
		if !ok || now.Add(skew).Before(nbf) {
			return Principal{}, newError(ErrInvalidToken, ReasonNotYetValid)
		}
	}
	if c.IssuedAt != nil {
		iat, ok := unixTime(*c.IssuedAt)
		if !ok || now.Add(skew).Before(iat) {
			return Principal{}, newError(ErrInvalidToken, ReasonIssuedInFuture)
		}
	}
	// typ do Keycloak: "Bearer" no token de acesso, "ID"/"Refresh" nos
	// demais. Um ID token com a mesma audiência não serve como credencial.
	if c.Type != nil && !strings.EqualFold(*c.Type, "Bearer") {
		return Principal{}, newError(ErrInvalidToken, ReasonTokenType)
	}

	providerID, err := a.providerID(tok)
	if err != nil {
		return Principal{}, err
	}

	var roles []string
	if c.RealmAccess != nil {
		roles = append(roles, c.RealmAccess.Roles...)
	}
	if ra, ok := c.ResourceAccess[a.settings.rolesClient()]; ok {
		roles = append(roles, ra.Roles...)
	}
	roles = slices.DeleteFunc(roles, func(r string) bool { return r == "" })
	slices.Sort(roles)
	roles = slices.Compact(roles)

	return Principal{
		Subject:    tok.Subject,
		ClientID:   c.AuthorizedPart,
		ProviderID: providerID,
		Roles:      roles,
		ExpiresAt:  tok.Expiry,
	}, nil
}

// providerID lê a claim configurada: ausente/null → ""; presente, precisa
// ser string válida como identificador de provedor (mesmos limites com que o
// providerId é gravado), senão o token é recusado.
func (a *Authenticator) providerID(tok *oidc.IDToken) (string, error) {
	var all map[string]json.RawMessage
	if err := tok.Claims(&all); err != nil {
		return "", newError(ErrInvalidToken, ReasonClaims)
	}
	raw, ok := all[a.settings.ProviderClaim]
	if !ok || string(raw) == "null" {
		return "", nil
	}
	var v string
	if err := json.Unmarshal(raw, &v); err != nil || !validProviderID(v) {
		return "", newError(ErrInvalidToken, ReasonProviderClaim)
	}
	return v, nil
}

func validProviderID(v string) bool {
	if v == "" || len(v) > domain.MaxWagerIdentifierLength || !utf8.ValidString(v) {
		return false
	}
	for _, r := range v {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func classifyVerifyError(st *verifyState) error {
	switch {
	case !st.called:
		// Falhou antes da assinatura: JWT malformado ou alg não permitido
		// (inclusive "none" e HS*).
		return newError(ErrInvalidToken, ReasonMalformed)
	case st.err == nil:
		// Assinatura válida, mas iss/aud diferentes ou claims malformadas.
		return newError(ErrInvalidToken, ReasonClaims)
	case errors.Is(st.err, errKeysUnavailable):
		return &Error{Kind: ErrUnavailable, Reason: ReasonKeysUnavailable, Err: st.err}
	case errors.Is(st.err, errUnknownKey):
		return newError(ErrInvalidToken, ReasonUnknownKey)
	case errors.Is(st.err, errMalformedJWS):
		return newError(ErrInvalidToken, ReasonMalformed)
	default:
		return newError(ErrInvalidToken, ReasonSignature)
	}
}

// unixTime converte NumericDate (segundos, possivelmente fracionários).
func unixTime(v float64) (time.Time, bool) {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > 1e11 {
		return time.Time{}, false
	}
	sec, frac := math.Modf(v)
	return time.Unix(int64(sec), int64(frac*1e9)), true
}
