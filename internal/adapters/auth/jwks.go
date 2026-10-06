package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/go-jose/go-jose/v4"
)

// allowedAlgorithms são os únicos algoritmos aceitos no cabeçalho do JWT:
// todos assimétricos. "none" e HS* (simétricos, sujeitos a confusão de
// algoritmo com a chave pública) nunca são aceitos. O tipo da chave do JWKS
// precisa casar com o algoritmo (go-jose recusa RSA com ES256 etc.).
var allowedAlgorithms = []jose.SignatureAlgorithm{
	jose.RS256, jose.RS384, jose.RS512,
	jose.PS256, jose.PS384, jose.PS512,
	jose.ES256, jose.ES384, jose.ES512,
	jose.EdDSA,
}

func allowedAlgorithmNames() []string {
	out := make([]string, len(allowedAlgorithms))
	for i, a := range allowedAlgorithms {
		out[i] = string(a)
	}
	return out
}

// Limites do cliente do JWKS.
const (
	defaultMinRefreshInterval = 10 * time.Second
	defaultKeysMaxAge         = 10 * time.Minute
	defaultFetchTimeout       = 5 * time.Second
	maxJWKSBytes              = 1 << 20
)

// Erros internos do keySet; o Authenticator os traduz em *Error.
var (
	errKeysUnavailable = errors.New("jwks: keys unavailable")
	errUnknownKey      = errors.New("jwks: no key matches the token")
	errBadSignature    = errors.New("jwks: signature verification failed")
	errMalformedJWS    = errors.New("jwks: malformed jws")
	errThrottled       = errors.New("jwks: refresh throttled")
)

// keySet implementa oidc.KeySet com cache de chaves públicas:
//
//   - a primeira verificação busca o JWKS (lazy: o start não depende do IdP);
//   - kid desconhecido força uma nova busca (rotação de chaves), limitada a
//     uma a cada minRefresh para que tokens com kid aleatório não virem um
//     ataque ao IdP;
//   - chaves com mais de maxAge são renovadas no próximo uso (remove chaves
//     revogadas); se a renovação falhar, as chaves antigas continuam valendo;
//   - buscas são serializadas: requisições concorrentes esperam a busca em
//     andamento e reaproveitam o resultado (geração).
//
// O erro "chaves indisponíveis" (IdP fora do ar) é distinto de "assinatura
// inválida" para que a borda responda 503 em vez de 401.
type keySet struct {
	client       *http.Client
	issuer       string
	staticURL    string
	minRefresh   time.Duration
	maxAge       time.Duration
	fetchTimeout time.Duration
	now          func() time.Time
	logger       *slog.Logger

	fetchMu sync.Mutex // serializa buscas

	mu          sync.RWMutex
	jwksURL     string // staticURL ou o descoberto
	keys        []jose.JSONWebKey
	fetchedAt   time.Time // último sucesso
	attemptedAt time.Time // última tentativa
	lastErr     error     // resultado da última tentativa
	gen         uint64    // incrementa a cada tentativa
}

// verifyState registra, por chamada de Verify, o resultado do keySet: o
// go-oidc embrulha o erro com %v, perdendo a cadeia, e o Authenticator
// precisa distinguir IdP indisponível de assinatura inválida.
type verifyState struct {
	called bool
	err    error
}

type verifyStateKey struct{}

func withVerifyState(ctx context.Context, st *verifyState) context.Context {
	return context.WithValue(ctx, verifyStateKey{}, st)
}

// VerifySignature implementa oidc.KeySet.
func (k *keySet) VerifySignature(ctx context.Context, raw string) ([]byte, error) {
	payload, err := k.verify(ctx, raw)
	if st, ok := ctx.Value(verifyStateKey{}).(*verifyState); ok && st != nil {
		st.called = true
		st.err = err
	}
	return payload, err
}

func (k *keySet) verify(ctx context.Context, raw string) ([]byte, error) {
	jws, err := jose.ParseSigned(raw, allowedAlgorithms)
	if err != nil || len(jws.Signatures) != 1 {
		return nil, errMalformedJWS
	}
	hdr := jws.Signatures[0].Header

	keys, gen, fresh := k.snapshot()
	refreshed := false
	if !fresh {
		var rerr error
		keys, gen, rerr = k.refresh(ctx, gen)
		refreshed = rerr == nil
		if len(keys) == 0 {
			return nil, keysUnavailable(rerr)
		}
	}

	cands := candidates(keys, hdr)
	if len(cands) == 0 && !refreshed {
		// kid desconhecido: talvez o IdP tenha rotacionado as chaves.
		var rerr error
		keys, _, rerr = k.refresh(ctx, gen)
		cands = candidates(keys, hdr)
		if len(cands) == 0 && rerr != nil && !errors.Is(rerr, errThrottled) {
			return nil, keysUnavailable(rerr)
		}
	}
	if len(cands) == 0 {
		return nil, errUnknownKey
	}
	for _, key := range cands {
		if payload, err := jws.Verify(key.Key); err == nil {
			return payload, nil
		}
	}
	return nil, errBadSignature
}

// candidates devolve as chaves compatíveis com o cabeçalho: mesmo kid (ou
// todas, se o token não tiver kid) e, se a chave declarar alg, o mesmo alg.
func candidates(keys []jose.JSONWebKey, hdr jose.Header) []jose.JSONWebKey {
	var out []jose.JSONWebKey
	for _, key := range keys {
		if hdr.KeyID != "" && key.KeyID != hdr.KeyID {
			continue
		}
		if key.Algorithm != "" && key.Algorithm != hdr.Algorithm {
			continue
		}
		out = append(out, key)
	}
	return out
}

// snapshot devolve as chaves atuais, a geração e se ainda estão frescas.
func (k *keySet) snapshot() ([]jose.JSONWebKey, uint64, bool) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	fresh := len(k.keys) > 0 && k.now().Sub(k.fetchedAt) < k.maxAge
	return k.keys, k.gen, fresh
}

// refresh busca o JWKS, a menos que outra goroutine já tenha tentado depois
// da geração vista pelo chamador (reaproveita o resultado) ou que a última
// tentativa seja recente demais (devolve o último erro ou errThrottled).
// Sempre devolve as chaves atuais (possivelmente antigas).
func (k *keySet) refresh(ctx context.Context, seenGen uint64) ([]jose.JSONWebKey, uint64, error) {
	k.fetchMu.Lock()
	defer k.fetchMu.Unlock()

	k.mu.RLock()
	keys, gen, attempted, lastErr := k.keys, k.gen, k.attemptedAt, k.lastErr
	k.mu.RUnlock()
	if gen != seenGen {
		return keys, gen, lastErr
	}
	if !attempted.IsZero() && k.now().Sub(attempted) < k.minRefresh {
		if lastErr != nil {
			return keys, gen, lastErr
		}
		return keys, gen, errThrottled
	}

	fetched, err := k.fetch(ctx)

	k.mu.Lock()
	defer k.mu.Unlock()
	k.gen++
	k.attemptedAt = k.now()
	k.lastErr = err
	if err == nil {
		k.keys = fetched
		k.fetchedAt = k.attemptedAt
	} else {
		k.logger.Warn("auth: falha ao obter chaves do IdP", slog.String("error", err.Error()),
			slog.Int("cachedKeys", len(k.keys)))
	}
	return k.keys, k.gen, err
}

// fetch baixa e filtra o JWKS. Usa um contexto desligado do cancelamento da
// requisição (a busca é compartilhada) e com timeout próprio.
func (k *keySet) fetch(ctx context.Context) ([]jose.JSONWebKey, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), k.fetchTimeout)
	defer cancel()

	jwksURL, err := k.resolveJWKSURL(ctx)
	if err != nil {
		return nil, err
	}
	var doc struct {
		Keys []json.RawMessage `json:"keys"`
	}
	if err := k.getJSON(ctx, jwksURL, &doc); err != nil {
		return nil, fmt.Errorf("jwks: %w", err)
	}
	var keys []jose.JSONWebKey
	for _, raw := range doc.Keys {
		var key jose.JSONWebKey
		if err := key.UnmarshalJSON(raw); err != nil {
			continue // tipo de chave não suportado: ignora só essa chave
		}
		if key.Use != "" && key.Use != "sig" {
			continue // ex.: chave RSA-OAEP de cifragem do Keycloak
		}
		if !key.Valid() || !key.IsPublic() {
			continue // simétrica, privada ou inválida: nunca usada
		}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return nil, errors.New("jwks: nenhuma chave pública de assinatura utilizável")
	}
	return keys, nil
}

// resolveJWKSURL devolve a URL configurada ou a descoberta (e a memoriza).
func (k *keySet) resolveJWKSURL(ctx context.Context) (string, error) {
	k.mu.RLock()
	u := k.jwksURL
	k.mu.RUnlock()
	if u != "" {
		return u, nil
	}
	var doc struct {
		Issuer  string `json:"issuer"`
		JWKSURI string `json:"jwks_uri"`
	}
	wellKnown := strings.TrimSuffix(k.issuer, "/") + "/.well-known/openid-configuration"
	if err := k.getJSON(ctx, wellKnown, &doc); err != nil {
		return "", fmt.Errorf("discovery: %w", err)
	}
	if doc.Issuer != k.issuer {
		return "", fmt.Errorf("discovery: issuer %q difere do configurado %q", doc.Issuer, k.issuer)
	}
	if !validHTTPURL(doc.JWKSURI) {
		return "", errors.New("discovery: jwks_uri ausente ou inválido")
	}
	k.mu.Lock()
	k.jwksURL = doc.JWKSURI
	k.mu.Unlock()
	return doc.JWKSURI, nil
}

func (k *keySet) getJSON(ctx context.Context, rawURL string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := k.client.Do(req)
	if err != nil {
		var uerr *url.Error
		if errors.As(err, &uerr) {
			return uerr.Err // sem repetir a URL inteira
		}
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxJWKSBytes))
		return fmt.Errorf("status HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxJWKSBytes+1))
	if err != nil {
		return err
	}
	if len(body) > maxJWKSBytes {
		return fmt.Errorf("resposta maior que %d bytes", maxJWKSBytes)
	}
	if err := json.Unmarshal(body, dst); err != nil {
		return fmt.Errorf("JSON inválido: %w", err)
	}
	return nil
}

func keysUnavailable(cause error) error {
	if cause == nil {
		return errKeysUnavailable
	}
	return fmt.Errorf("%w: %w", errKeysUnavailable, cause)
}
