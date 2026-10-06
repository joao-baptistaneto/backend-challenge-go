package config

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Decisões (ver ARCHITECTURE.md, ADR "Config, logging e métricas"):
//
//   - A configuração vem exclusivamente de variáveis de ambiente (12-factor). Os
//     nomes são exatamente os de .env.example, que é a fonte de verdade das chaves.
//   - Load recebe uma função de lookup em vez de ler os.Getenv diretamente, para que
//     os testes não dependam (nem alterem) o ambiente do processo.
//   - A validação é completa e agregada: todos os problemas são reportados de uma
//     vez (errors.Join), cada um prefixado pelo nome da variável. Isso falha o
//     fx.New no start com uma mensagem acionável, em vez de falhar tarde, no
//     primeiro uso de uma dependência.
//   - Valores sensíveis (DATABASE_URL, AWS_SECRET_ACCESS_KEY) são do tipo Secret,
//     que se redige ao ser formatado/logado/serializado. Mensagens de erro de
//     validação NUNCA incluem o valor recebido de uma variável sensível.
//   - Variáveis com default seguro têm default igual ao .env.example; variáveis
//     que identificam recursos externos (banco, filas, IdP) são obrigatórias, para
//     que um deploy mal configurado não aponte silenciosamente para localhost.

// Nomes das variáveis de ambiente (mesmos de .env.example).
const (
	EnvAppEnv          = "APP_ENV"
	EnvHTTPAddr        = "HTTP_ADDR"
	EnvLogLevel        = "LOG_LEVEL"
	EnvShutdownTimeout = "SHUTDOWN_TIMEOUT"

	EnvDatabaseURL            = "DATABASE_URL"
	EnvDatabaseMaxConns       = "DATABASE_MAX_CONNS"
	EnvDatabaseConnectTimeout = "DATABASE_CONNECT_TIMEOUT"

	EnvAWSRegion          = "AWS_REGION"
	EnvAWSAccessKeyID     = "AWS_ACCESS_KEY_ID"
	EnvAWSSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
	EnvAWSEndpointURL     = "AWS_ENDPOINT_URL"

	EnvSQSInboundQueueURL = "SQS_INBOUND_QUEUE_URL"
	EnvSQSInboundDLQURL   = "SQS_INBOUND_DLQ_URL"
	EnvSQSEventsQueueURL  = "SQS_EVENTS_QUEUE_URL"

	EnvOIDCIssuerURL     = "OIDC_ISSUER_URL"
	EnvOIDCAudience      = "OIDC_AUDIENCE"
	EnvOIDCJWKSURL       = "OIDC_JWKS_URL"
	EnvOIDCProviderClaim = "OIDC_PROVIDER_CLAIM"
	EnvOIDCProviderRole  = "OIDC_PROVIDER_ROLE"
	EnvOIDCInternalRole  = "OIDC_INTERNAL_ROLE"
	EnvOIDCRolesClient   = "OIDC_ROLES_CLIENT"
	EnvOIDCClockSkew     = "OIDC_CLOCK_SKEW"

	EnvOutboxPollInterval          = "OUTBOX_POLL_INTERVAL"
	EnvOutboxBatchSize             = "OUTBOX_BATCH_SIZE"
	EnvOutboxLease                 = "OUTBOX_LEASE"
	EnvPendingReferenceMaxAttempts = "PENDING_REFERENCE_MAX_ATTEMPTS"
	EnvPendingReferenceTTL         = "PENDING_REFERENCE_TTL"
)

// Ambientes aceitos em APP_ENV.
const (
	EnvLocal       = "local"
	EnvTest        = "test"
	EnvDevelopment = "development"
	EnvStaging     = "staging"
	EnvProduction  = "production"
)

// Limites de validação. São guardas contra erros grosseiros de configuração,
// não ajuste fino de capacidade.
const (
	maxDatabaseConns   = 1000
	maxOutboxBatchSize = 1000
	// maxOIDCClockSkew limita a tolerância de relógio na validação de
	// exp/nbf/iat: uma tolerância grande estende a vida útil de tokens
	// expirados.
	maxOIDCClockSkew = 2 * time.Minute
)

// Defaults de autorização (ARCHITECTURE.md, ADR-019). Os nomes são os
// provisionados no realm do Keycloak (T24).
const (
	DefaultOIDCProviderClaim = "provider_id"
	DefaultOIDCProviderRole  = "wager-provider"
	DefaultOIDCInternalRole  = "wallet-internal"
	DefaultOIDCClockSkew     = 30 * time.Second
)

// ErrInvalidConfig é envolvido por todo erro devolvido por Load, permitindo
// errors.Is(err, ErrInvalidConfig) independentemente da variável que falhou.
var ErrInvalidConfig = errors.New("configuração inválida")

// Config é a configuração completa e já validada da aplicação. Uma vez
// devolvida por Load, é tratada como imutável (passada por valor).
type Config struct {
	App      AppConfig
	Database DatabaseConfig
	AWS      AWSConfig
	SQS      SQSConfig
	OIDC     OIDCConfig
	Workers  WorkersConfig
}

// AppConfig agrupa parâmetros do processo.
type AppConfig struct {
	Env             string
	HTTPAddr        string
	LogLevel        slog.Level
	ShutdownTimeout time.Duration
}

// DatabaseConfig agrupa parâmetros do PostgreSQL. URL contém credenciais e por
// isso é Secret.
type DatabaseConfig struct {
	URL            Secret
	MaxConns       int32
	ConnectTimeout time.Duration
}

// AWSConfig agrupa parâmetros do SDK AWS. AccessKeyID e SecretAccessKey são
// opcionais (ausentes → cadeia padrão de credenciais do SDK), mas devem ser
// informados juntos. EndpointURL é opcional (LocalStack em ambiente local).
type AWSConfig struct {
	Region          string
	AccessKeyID     string
	SecretAccessKey Secret
	EndpointURL     string
}

// HasStaticCredentials informa se credenciais estáticas foram configuradas.
func (c AWSConfig) HasStaticCredentials() bool {
	return c.AccessKeyID != "" && !c.SecretAccessKey.IsEmpty()
}

// SQSConfig agrupa as URLs das filas. Todas são FIFO.
//
// Nota: .env.example usa a fila de eventos "wallet-events.fifo", enquanto
// task_plan.md (T24) cita "wager-events.fifo". O nome é configuração, não
// código: este pacote só exige uma URL FIFO válida, e o valor de referência
// adotado é o de .env.example.
type SQSConfig struct {
	InboundQueueURL string
	// InboundDLQURL é opcional: o redrive para a DLQ é feito pelo próprio SQS
	// (maxReceiveCount); a URL só é usada para observabilidade (profundidade).
	InboundDLQURL  string
	EventsQueueURL string
}

// OIDCConfig agrupa parâmetros do IdP (Keycloak) e do modelo de permissões
// (ADR-019).
type OIDCConfig struct {
	// IssuerURL é comparado exatamente com a claim iss do token.
	IssuerURL string
	// Audience precisa constar da claim aud do token.
	Audience string
	// JWKSURL é opcional: vazio → descoberto em
	// {IssuerURL}/.well-known/openid-configuration no primeiro uso.
	JWKSURL string
	// ProviderClaim é a claim (string) que carrega o providerId autorizado.
	ProviderClaim string
	// ProviderRole e InternalRole são os papéis (realm_access ou
	// resource_access[RolesClient]) de provedor e de serviço interno.
	ProviderRole string
	InternalRole string
	// RolesClient é o client cujos papéis em resource_access são
	// considerados, além dos papéis de realm. Default: Audience.
	RolesClient string
	// ClockSkew é a tolerância de relógio para exp/nbf/iat.
	ClockSkew time.Duration
}

// WorkersConfig agrupa parâmetros dos workers de outbox e de referências
// pendentes.
type WorkersConfig struct {
	OutboxPollInterval          time.Duration
	OutboxBatchSize             int
	OutboxLease                 time.Duration
	PendingReferenceMaxAttempts int
	PendingReferenceTTL         time.Duration
}

// LookupFunc tem a mesma assinatura de os.LookupEnv.
type LookupFunc func(key string) (string, bool)

// LoadFromEnv carrega e valida a configuração a partir do ambiente do processo.
func LoadFromEnv() (Config, error) {
	return Load(os.LookupEnv)
}

// Load carrega e valida a configuração usando lookup. Devolve todos os
// problemas encontrados de uma vez; o erro satisfaz errors.Is(err,
// ErrInvalidConfig).
func Load(lookup LookupFunc) (Config, error) {
	p := parser{lookup: lookup}
	var c Config

	c.App.Env = p.oneOf(EnvAppEnv, EnvLocal, []string{EnvLocal, EnvTest, EnvDevelopment, EnvStaging, EnvProduction})
	c.App.HTTPAddr = p.listenAddr(EnvHTTPAddr, ":8080")
	c.App.LogLevel = p.logLevel(EnvLogLevel, slog.LevelInfo)
	c.App.ShutdownTimeout = p.positiveDuration(EnvShutdownTimeout, 15*time.Second)

	c.Database.URL = p.postgresURL(EnvDatabaseURL)
	c.Database.MaxConns = int32(p.intRange(EnvDatabaseMaxConns, 20, 1, maxDatabaseConns))
	c.Database.ConnectTimeout = p.positiveDuration(EnvDatabaseConnectTimeout, 30*time.Second)

	c.AWS.Region = p.required(EnvAWSRegion)
	c.AWS.AccessKeyID = p.optional(EnvAWSAccessKeyID)
	c.AWS.SecretAccessKey = Secret(p.optional(EnvAWSSecretAccessKey))
	if (c.AWS.AccessKeyID == "") != c.AWS.SecretAccessKey.IsEmpty() {
		p.fail(EnvAWSAccessKeyID+"/"+EnvAWSSecretAccessKey, "devem ser informadas juntas ou ambas omitidas")
	}
	c.AWS.EndpointURL = p.httpURL(EnvAWSEndpointURL, false)

	c.SQS.InboundQueueURL = p.fifoQueueURL(EnvSQSInboundQueueURL, true)
	c.SQS.InboundDLQURL = p.fifoQueueURL(EnvSQSInboundDLQURL, false)
	c.SQS.EventsQueueURL = p.fifoQueueURL(EnvSQSEventsQueueURL, true)
	if c.SQS.InboundQueueURL != "" && c.SQS.InboundQueueURL == c.SQS.EventsQueueURL {
		p.fail(EnvSQSEventsQueueURL, "deve ser diferente de "+EnvSQSInboundQueueURL)
	}
	if c.SQS.InboundDLQURL != "" && c.SQS.InboundDLQURL == c.SQS.InboundQueueURL {
		p.fail(EnvSQSInboundDLQURL, "deve ser diferente de "+EnvSQSInboundQueueURL)
	}

	c.OIDC.IssuerURL = p.httpURL(EnvOIDCIssuerURL, true)
	c.OIDC.Audience = p.required(EnvOIDCAudience)
	c.OIDC.JWKSURL = p.httpURL(EnvOIDCJWKSURL, false)
	c.OIDC.ProviderClaim = p.claimName(EnvOIDCProviderClaim, DefaultOIDCProviderClaim)
	c.OIDC.ProviderRole = p.roleName(EnvOIDCProviderRole, DefaultOIDCProviderRole)
	c.OIDC.InternalRole = p.roleName(EnvOIDCInternalRole, DefaultOIDCInternalRole)
	if c.OIDC.ProviderRole != "" && c.OIDC.ProviderRole == c.OIDC.InternalRole {
		p.fail(EnvOIDCInternalRole, "deve ser diferente de "+EnvOIDCProviderRole)
	}
	c.OIDC.RolesClient = p.optional(EnvOIDCRolesClient)
	if c.OIDC.RolesClient == "" {
		c.OIDC.RolesClient = c.OIDC.Audience
	}
	c.OIDC.ClockSkew = p.durationRange(EnvOIDCClockSkew, DefaultOIDCClockSkew, 0, maxOIDCClockSkew)

	c.Workers.OutboxPollInterval = p.positiveDuration(EnvOutboxPollInterval, time.Second)
	c.Workers.OutboxBatchSize = p.intRange(EnvOutboxBatchSize, 50, 1, maxOutboxBatchSize)
	c.Workers.OutboxLease = p.positiveDuration(EnvOutboxLease, 30*time.Second)
	c.Workers.PendingReferenceMaxAttempts = p.intRange(EnvPendingReferenceMaxAttempts, 10, 1, math.MaxInt32)
	c.Workers.PendingReferenceTTL = p.positiveDuration(EnvPendingReferenceTTL, 10*time.Minute)

	// O lease precisa ser maior que o intervalo de polling; caso contrário um
	// lote reivindicado expira antes do próximo ciclo e outro publisher o
	// reassume, gerando publicações duplicadas desnecessárias.
	if c.Workers.OutboxLease > 0 && c.Workers.OutboxPollInterval > 0 && c.Workers.OutboxLease <= c.Workers.OutboxPollInterval {
		p.fail(EnvOutboxLease, "deve ser maior que "+EnvOutboxPollInterval)
	}

	if err := p.err(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// parser acumula erros de validação por variável.
type parser struct {
	lookup LookupFunc
	errs   []error
}

func (p *parser) fail(key, msg string) {
	p.errs = append(p.errs, fmt.Errorf("%s: %s", key, msg))
}

func (p *parser) err() error {
	if len(p.errs) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrInvalidConfig, errors.Join(p.errs...))
}

// get devolve o valor sem espaços nas bordas; vazio é tratado como ausente.
func (p *parser) get(key string) (string, bool) {
	v, ok := p.lookup(key)
	if !ok {
		return "", false
	}
	v = strings.TrimSpace(v)
	return v, v != ""
}

func (p *parser) optional(key string) string {
	v, _ := p.get(key)
	return v
}

func (p *parser) required(key string) string {
	v, ok := p.get(key)
	if !ok {
		p.fail(key, "obrigatória")
	}
	return v
}

func (p *parser) oneOf(key, def string, allowed []string) string {
	v, ok := p.get(key)
	if !ok {
		return def
	}
	for _, a := range allowed {
		if v == a {
			return v
		}
	}
	p.fail(key, fmt.Sprintf("valor %q inválido (aceitos: %s)", v, strings.Join(allowed, ", ")))
	return ""
}

func (p *parser) logLevel(key string, def slog.Level) slog.Level {
	v, ok := p.get(key)
	if !ok {
		return def
	}
	switch strings.ToLower(v) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	}
	p.fail(key, fmt.Sprintf("valor %q inválido (aceitos: debug, info, warn, error)", v))
	return def
}

func (p *parser) positiveDuration(key string, def time.Duration) time.Duration {
	v, ok := p.get(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		p.fail(key, fmt.Sprintf("duração %q inválida (ex.: 500ms, 15s, 10m)", v))
		return 0
	}
	if d <= 0 {
		p.fail(key, fmt.Sprintf("duração deve ser positiva, recebido %q", v))
		return 0
	}
	return d
}

// durationRange aceita durações em [minV, maxV] (zero permitido se minV = 0).
func (p *parser) durationRange(key string, def, minV, maxV time.Duration) time.Duration {
	v, ok := p.get(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		p.fail(key, fmt.Sprintf("duração %q inválida (ex.: 0s, 30s, 1m)", v))
		return 0
	}
	if d < minV || d > maxV {
		p.fail(key, fmt.Sprintf("deve estar entre %s e %s, recebido %q", minV, maxV, v))
		return 0
	}
	return d
}

// reservedClaims não podem carregar o providerId: são claims registradas
// (RFC 7519) ou de papéis, com semântica própria validada pelo verificador.
var reservedClaims = map[string]bool{
	"iss": true, "aud": true, "exp": true, "nbf": true, "iat": true, "jti": true,
	"typ": true, "realm_access": true, "resource_access": true,
}

// claimName aceita um nome de claim simples (1–128 caracteres
// [A-Za-z0-9_.:-]) fora das claims reservadas.
func (p *parser) claimName(key, def string) string {
	v, ok := p.get(key)
	if !ok {
		return def
	}
	if !IsSimpleName(v) {
		p.fail(key, fmt.Sprintf("nome de claim %q inválido (1-128 caracteres [A-Za-z0-9_.:-])", v))
		return ""
	}
	if reservedClaims[v] {
		p.fail(key, fmt.Sprintf("a claim %q é reservada e não pode carregar o providerId", v))
		return ""
	}
	return v
}

// roleName aceita um nome de papel (1–128 caracteres [A-Za-z0-9_.:-]).
func (p *parser) roleName(key, def string) string {
	v, ok := p.get(key)
	if !ok {
		return def
	}
	if !IsSimpleName(v) {
		p.fail(key, fmt.Sprintf("nome de papel %q inválido (1-128 caracteres [A-Za-z0-9_.:-])", v))
		return ""
	}
	return v
}

// IsSimpleName informa se v tem 1–128 caracteres [A-Za-z0-9_.:-]. É a
// regra dos nomes de claim e de papel (também usada pelo adapter de auth).
func IsSimpleName(v string) bool {
	if v == "" || len(v) > 128 {
		return false
	}
	for _, r := range v {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '_', r == '.', r == ':', r == '-':
		default:
			return false
		}
	}
	return true
}

// IsReservedClaim informa se a claim não pode carregar o providerId.
func IsReservedClaim(name string) bool { return reservedClaims[name] }

func (p *parser) intRange(key string, def, minV, maxV int) int {
	v, ok := p.get(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		p.fail(key, fmt.Sprintf("inteiro %q inválido", v))
		return 0
	}
	if n < minV || n > maxV {
		p.fail(key, fmt.Sprintf("deve estar entre %d e %d, recebido %d", minV, maxV, n))
		return 0
	}
	return n
}

func (p *parser) listenAddr(key, def string) string {
	v, ok := p.get(key)
	if !ok {
		return def
	}
	_, port, err := net.SplitHostPort(v)
	if err != nil {
		p.fail(key, fmt.Sprintf("endereço %q inválido (formato host:porta, ex.: :8080)", v))
		return ""
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 0 || n > 65535 {
		p.fail(key, fmt.Sprintf("porta %q inválida", port))
		return ""
	}
	return v
}

// postgresURL valida DATABASE_URL sem jamais ecoar o valor (contém senha).
func (p *parser) postgresURL(key string) Secret {
	v, ok := p.get(key)
	if !ok {
		p.fail(key, "obrigatória")
		return ""
	}
	u, err := url.Parse(v)
	if err != nil {
		// O erro de url.Parse inclui a URL inteira; por isso não é propagado.
		p.fail(key, "URL malformada (valor omitido por conter credenciais)")
		return ""
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		p.fail(key, "esquema deve ser postgres:// ou postgresql://")
		return ""
	}
	if u.Host == "" {
		p.fail(key, "host ausente")
		return ""
	}
	return Secret(v)
}

func (p *parser) httpURL(key string, isRequired bool) string {
	v, ok := p.get(key)
	if !ok {
		if isRequired {
			p.fail(key, "obrigatória")
		}
		return ""
	}
	u, err := url.Parse(v)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		p.fail(key, "URL http(s) inválida: "+urlForError(v))
		return ""
	}
	if u.User != nil {
		// URLs não sensíveis não podem carregar credenciais embutidas, pois
		// são logadas livremente.
		p.fail(key, "URL não pode conter credenciais")
		return ""
	}
	return v
}

// fifoQueueURL exige URL http(s) cujo último segmento termine em ".fifo": o
// desafio exige filas FIFO (ordem por MessageGroupId e deduplicação).
func (p *parser) fifoQueueURL(key string, isRequired bool) string {
	v := p.httpURL(key, isRequired)
	if v == "" {
		return ""
	}
	u, _ := url.Parse(v)
	name := u.Path[strings.LastIndex(u.Path, "/")+1:]
	if !strings.HasSuffix(name, ".fifo") || len(name) <= len(".fifo") {
		p.fail(key, "a fila deve ser FIFO (nome terminado em .fifo): "+urlForError(v))
		return ""
	}
	return v
}

// urlForError devolve uma forma da URL segura para mensagens de erro: só
// scheme, host e path. Userinfo, query e fragment são descartados porque podem
// carregar credenciais (user:senha@, ?X-Amz-Security-Token=..., #access_token=).
// Se a URL nem pode ser analisada, o valor é omitido por inteiro.
func urlForError(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "(valor omitido: URL malformada)"
	}
	safe := url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}
	if u.Opaque != "" {
		// "scheme:opaco" (ex.: "mailto:x", "user:pass@h" sem //) pode
		// esconder credenciais: só o esquema é mantido.
		return strconv.Quote(u.Scheme + ":(omitido)")
	}
	return strconv.Quote(safe.String())
}
