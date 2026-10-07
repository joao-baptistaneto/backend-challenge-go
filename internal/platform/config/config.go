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

	EnvHTTPReadHeaderTimeout = "HTTP_READ_HEADER_TIMEOUT"
	EnvHTTPReadTimeout       = "HTTP_READ_TIMEOUT"
	EnvHTTPWriteTimeout      = "HTTP_WRITE_TIMEOUT"
	EnvHTTPIdleTimeout       = "HTTP_IDLE_TIMEOUT"
	EnvHTTPDrainDelay        = "HTTP_DRAIN_DELAY"

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

	EnvSQSConsumerName        = "SQS_CONSUMER_NAME"
	EnvSQSConsumerConcurrency = "SQS_CONSUMER_CONCURRENCY"
	EnvSQSWaitTime            = "SQS_WAIT_TIME"
	EnvSQSVisibilityTimeout   = "SQS_VISIBILITY_TIMEOUT"
	EnvSQSProcessTimeout      = "SQS_PROCESS_TIMEOUT"
	EnvSQSRetryBaseDelay      = "SQS_RETRY_BASE_DELAY"
	EnvSQSRetryMaxDelay       = "SQS_RETRY_MAX_DELAY"
	EnvSQSDrainTimeout        = "SQS_DRAIN_TIMEOUT"
	EnvSQSMaxAttempts         = "SQS_MAX_ATTEMPTS"

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
	EnvOutboxConcurrency           = "OUTBOX_CONCURRENCY"
	EnvOutboxPublishTimeout        = "OUTBOX_PUBLISH_TIMEOUT"
	EnvOutboxDrainTimeout          = "OUTBOX_DRAIN_TIMEOUT"
	EnvOutboxRetryBaseDelay        = "OUTBOX_RETRY_BASE_DELAY"
	EnvOutboxRetryMaxDelay         = "OUTBOX_RETRY_MAX_DELAY"
	EnvPendingReferenceMaxAttempts = "PENDING_REFERENCE_MAX_ATTEMPTS"
	EnvPendingReferenceTTL         = "PENDING_REFERENCE_TTL"

	EnvPendingReferenceRetryBaseDelay = "PENDING_REFERENCE_RETRY_BASE_DELAY"
	EnvPendingReferenceRetryMaxDelay  = "PENDING_REFERENCE_RETRY_MAX_DELAY"
	EnvPendingWorkerPollInterval      = "PENDING_WORKER_POLL_INTERVAL"
	EnvPendingWorkerBatchSize         = "PENDING_WORKER_BATCH_SIZE"
	EnvPendingWorkerConcurrency       = "PENDING_WORKER_CONCURRENCY"
	EnvPendingWorkerLease             = "PENDING_WORKER_LEASE"
	EnvPendingWorkerProcessTimeout    = "PENDING_WORKER_PROCESS_TIMEOUT"
	EnvPendingWorkerDrainTimeout      = "PENDING_WORKER_DRAIN_TIMEOUT"
	EnvPendingOrphanGrace             = "PENDING_ORPHAN_GRACE"
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
	// maxHTTPDrainDelay limita a espera entre readiness DOWN e o Shutdown do
	// servidor HTTP (ADR-021).
	maxHTTPDrainDelay = time.Minute
)

// Defaults do servidor HTTP (ARCHITECTURE.md, ADR-021).
const (
	DefaultHTTPReadHeaderTimeout = 5 * time.Second
	DefaultHTTPReadTimeout       = 30 * time.Second
	DefaultHTTPWriteTimeout      = 30 * time.Second
	DefaultHTTPIdleTimeout       = 120 * time.Second
)

// Defaults e limites do consumidor SQS (ARCHITECTURE.md, ADR-022). Os
// limites de espera e visibilidade são os do próprio SQS.
const (
	DefaultSQSConsumerName        = "wager-transactions-consumer"
	DefaultSQSConsumerConcurrency = 4
	DefaultSQSWaitTime            = 20 * time.Second
	DefaultSQSVisibilityTimeout   = 30 * time.Second
	DefaultSQSProcessTimeout      = 15 * time.Second
	DefaultSQSRetryBaseDelay      = time.Second
	DefaultSQSRetryMaxDelay       = 5 * time.Minute
	DefaultSQSDrainTimeout        = 10 * time.Second
	DefaultSQSMaxAttempts         = 3

	maxSQSConsumerConcurrency = 100
	// maxSQSMaxAttempts: o maxReceiveCount do SQS vai até 1000, e o limite da
	// aplicação precisa ser menor que ele (verificado no start do consumidor).
	maxSQSMaxAttempts       = 999
	maxSQSWaitTime          = 20 * time.Second
	maxSQSVisibilityTimeout = 12 * time.Hour
)

// Defaults e limites do publisher da outbox (ARCHITECTURE.md, ADR-024). O
// lease máximo é o aceito por postgres.OutboxRepository.Claim (1h).
const (
	DefaultOutboxPollInterval   = time.Second
	DefaultOutboxBatchSize      = 50
	DefaultOutboxLease          = 30 * time.Second
	DefaultOutboxConcurrency    = 4
	DefaultOutboxPublishTimeout = 5 * time.Second
	DefaultOutboxDrainTimeout   = 10 * time.Second
	DefaultOutboxRetryBaseDelay = time.Second
	DefaultOutboxRetryMaxDelay  = 5 * time.Minute

	maxOutboxLease       = time.Hour
	maxOutboxConcurrency = 100
	maxOutboxDelay       = 24 * time.Hour
)

// Defaults e limites do worker de pendências (ARCHITECTURE.md, ADR-023).
const (
	DefaultPendingReferenceRetryBaseDelay = 2 * time.Second
	DefaultPendingReferenceRetryMaxDelay  = time.Minute
	DefaultPendingWorkerPollInterval      = time.Second
	DefaultPendingWorkerBatchSize         = 20
	DefaultPendingWorkerConcurrency       = 4
	DefaultPendingWorkerLease             = 30 * time.Second
	DefaultPendingWorkerProcessTimeout    = 5 * time.Second
	DefaultPendingWorkerDrainTimeout      = 10 * time.Second
	DefaultPendingOrphanGrace             = 30 * time.Second

	maxPendingDelay             = 24 * time.Hour
	maxPendingWorkerBatchSize   = 1000
	maxPendingWorkerConcurrency = 100
	maxPendingWorkerLease       = 24 * time.Hour
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
	HTTP     HTTPConfig
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

// HTTPConfig agrupa os timeouts do http.Server e a espera de drenagem
// (ADR-021). O endereço fica em AppConfig.HTTPAddr e o prazo total de parada
// em AppConfig.ShutdownTimeout.
type HTTPConfig struct {
	// ReadHeaderTimeout limita a leitura dos cabeçalhos (mitiga Slowloris).
	ReadHeaderTimeout time.Duration
	// ReadTimeout limita a leitura da requisição inteira (cabeçalhos + corpo).
	ReadTimeout time.Duration
	// WriteTimeout limita o tempo entre o fim da leitura dos cabeçalhos e o
	// fim da escrita da resposta; precisa cobrir o handler mais lento.
	WriteTimeout time.Duration
	// IdleTimeout limita conexões keep-alive ociosas.
	IdleTimeout time.Duration
	// DrainDelay é a espera entre /health/ready passar a DOWN e o fechamento
	// do listener, para o balanceador retirar a instância antes. 0 = sem
	// espera. Consome o orçamento de SHUTDOWN_TIMEOUT.
	DrainDelay time.Duration
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
// A fila de eventos de saída padronizada é "wager-events.fifo" (T22,
// ADR-024; antes .env.example citava "wallet-events.fifo"). O nome é
// configuração, não código: este pacote só exige uma URL FIFO válida.
type SQSConfig struct {
	InboundQueueURL string
	// InboundDLQURL é opcional: o consumidor envia à DLQ da RedrivePolicy da
	// fila de entrada (resolvida pelo ARN, ADR-022); esta URL
	// só é usada para observabilidade (profundidade).
	InboundDLQURL  string
	EventsQueueURL string
	// Consumer são os parâmetros do consumidor da fila de entrada.
	Consumer SQSConsumerConfig
}

// SQSConsumerConfig agrupa os parâmetros do consumidor da fila de entrada
// (ADR-022). Durações enviadas ao SQS (espera, visibilidade, backoff) são
// segundos inteiros.
type SQSConsumerConfig struct {
	// Name identifica o consumidor na inbox (consumer_name).
	Name string
	// Concurrency é o número máximo de grupos de mensagens (MessageGroupId)
	// processados em paralelo.
	Concurrency int
	// WaitTime é a espera do long polling (0s a 20s).
	WaitTime time.Duration
	// VisibilityTimeout é a visibilidade pedida no ReceiveMessage (1s a 12h).
	VisibilityTimeout time.Duration
	// ProcessTimeout limita o processamento de uma mensagem; menor que
	// VisibilityTimeout, para que a mensagem não reapareça durante o
	// processamento.
	ProcessTimeout time.Duration
	// RetryBaseDelay e RetryMaxDelay definem o backoff exponencial das falhas
	// transitórias (ChangeMessageVisibility pelo ApproximateReceiveCount).
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	// DrainTimeout é o prazo, no shutdown, para concluir as mensagens em
	// processamento antes de liberá-las (visibilidade 0).
	DrainTimeout time.Duration
	// MaxAttempts limita as tentativas de uma mensagem com falha transitória;
	// esgotado, o consumidor a envia à DLQ explicitamente. Precisa ser menor
	// que o maxReceiveCount da RedrivePolicy (verificado no start do
	// consumidor), que fica como rede de segurança.
	MaxAttempts int
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
	// Publisher da outbox (ADR-024).
	//
	// OutboxPollInterval é a espera entre ciclos quando o último claim veio
	// vazio. OutboxBatchSize é o tamanho máximo do lote reivindicado (no
	// máximo um evento por agregado). OutboxLease é o lease persistido do
	// lote (1s a 1h): um evento não marcado nesse prazo (crash) volta a ser
	// elegível para outra instância.
	OutboxPollInterval time.Duration
	OutboxBatchSize    int
	OutboxLease        time.Duration
	// OutboxConcurrency é o número de eventos do lote publicados em
	// paralelo (agregados distintos).
	OutboxConcurrency int
	// OutboxPublishTimeout limita cada chamada externa de um evento
	// (SendMessage; MarkPublished/MarkFailed). Duas chamadas precisam caber no
	// lease: 2·OutboxPublishTimeout < OutboxLease.
	OutboxPublishTimeout time.Duration
	// OutboxDrainTimeout é o prazo, no shutdown, para concluir as publicações
	// em voo.
	OutboxDrainTimeout time.Duration
	// OutboxRetryBaseDelay/MaxDelay: backoff persistido após falha de
	// publicação (base·2^(tentativa−1), com teto).
	OutboxRetryBaseDelay time.Duration
	OutboxRetryMaxDelay  time.Duration
	// PendingReferenceMaxAttempts é o máximo de buscas da referência,
	// contando a do aceite; PendingReferenceTTL é o prazo desde a criação.
	// Esgotado qualquer um, a operação vira REJECTED REFERENCE_NOT_FOUND
	// (ADR-023).
	PendingReferenceMaxAttempts int
	PendingReferenceTTL         time.Duration
	// PendingReferenceRetryBaseDelay/MaxDelay: backoff entre buscas
	// (base·2^(tentativa−1), com teto).
	PendingReferenceRetryBaseDelay time.Duration
	PendingReferenceRetryMaxDelay  time.Duration
	// PendingWorker são os parâmetros do worker de pendências.
	PendingWorker PendingWorkerConfig
}

// PendingWorkerConfig agrupa os parâmetros do worker de referências
// pendentes e PENDING órfãos (ADR-023).
type PendingWorkerConfig struct {
	// PollInterval é a espera entre ciclos quando o último lote não veio
	// cheio.
	PollInterval time.Duration
	// BatchSize é o tamanho máximo de um lote reivindicado.
	BatchSize int
	// Concurrency é o número de itens do lote processados em paralelo.
	Concurrency int
	// Lease é a duração da reivindicação persistida; um item não processado
	// dentro dele (crash) volta a ser elegível. Maior que ProcessTimeout.
	Lease time.Duration
	// ProcessTimeout limita a retomada de um item.
	ProcessTimeout time.Duration
	// DrainTimeout é o prazo, no shutdown, para concluir os itens em voo.
	DrainTimeout time.Duration
	// OrphanGrace é a idade mínima de um PENDING (sem next_attempt_at) para
	// ser retomado como órfão.
	OrphanGrace time.Duration
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

	c.HTTP.ReadHeaderTimeout = p.positiveDuration(EnvHTTPReadHeaderTimeout, DefaultHTTPReadHeaderTimeout)
	c.HTTP.ReadTimeout = p.positiveDuration(EnvHTTPReadTimeout, DefaultHTTPReadTimeout)
	c.HTTP.WriteTimeout = p.positiveDuration(EnvHTTPWriteTimeout, DefaultHTTPWriteTimeout)
	c.HTTP.IdleTimeout = p.positiveDuration(EnvHTTPIdleTimeout, DefaultHTTPIdleTimeout)
	c.HTTP.DrainDelay = p.durationRange(EnvHTTPDrainDelay, 0, 0, maxHTTPDrainDelay)
	if c.HTTP.ReadHeaderTimeout > 0 && c.HTTP.ReadTimeout > 0 && c.HTTP.ReadHeaderTimeout > c.HTTP.ReadTimeout {
		p.fail(EnvHTTPReadHeaderTimeout, "não pode ser maior que "+EnvHTTPReadTimeout)
	}
	// A espera de drenagem consome o prazo total de parada; sem folga, o
	// Shutdown não teria tempo de drenar nada e cairia sempre no Close.
	if c.HTTP.DrainDelay > 0 && c.App.ShutdownTimeout > 0 && c.HTTP.DrainDelay >= c.App.ShutdownTimeout {
		p.fail(EnvHTTPDrainDelay, "deve ser menor que "+EnvShutdownTimeout)
	}

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

	c.SQS.Consumer = p.sqsConsumer()

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

	p.outboxPublisher(&c.Workers)
	c.Workers.PendingReferenceMaxAttempts = p.intRange(EnvPendingReferenceMaxAttempts, 10, 1, math.MaxInt32)
	c.Workers.PendingReferenceTTL = p.positiveDuration(EnvPendingReferenceTTL, 10*time.Minute)
	c.Workers.PendingReferenceRetryBaseDelay = p.durationRange(EnvPendingReferenceRetryBaseDelay,
		DefaultPendingReferenceRetryBaseDelay, time.Millisecond, maxPendingDelay)
	c.Workers.PendingReferenceRetryMaxDelay = p.durationRange(EnvPendingReferenceRetryMaxDelay,
		DefaultPendingReferenceRetryMaxDelay, time.Millisecond, maxPendingDelay)
	if b, m := c.Workers.PendingReferenceRetryBaseDelay, c.Workers.PendingReferenceRetryMaxDelay; b > 0 && m > 0 && m < b {
		p.fail(EnvPendingReferenceRetryMaxDelay, "não pode ser menor que "+EnvPendingReferenceRetryBaseDelay)
	}
	c.Workers.PendingWorker = p.pendingWorker()

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

// sqsConsumer lê e valida os parâmetros do consumidor SQS.
func (p *parser) sqsConsumer() SQSConsumerConfig {
	var c SQSConsumerConfig
	c.Name = p.optional(EnvSQSConsumerName)
	if c.Name == "" {
		c.Name = DefaultSQSConsumerName
	} else if !IsSimpleName(c.Name) {
		p.fail(EnvSQSConsumerName, fmt.Sprintf("nome %q inválido (1-128 caracteres [A-Za-z0-9_.:-])", c.Name))
		c.Name = ""
	}
	c.Concurrency = p.intRange(EnvSQSConsumerConcurrency, DefaultSQSConsumerConcurrency, 1, maxSQSConsumerConcurrency)
	c.WaitTime = p.wholeSeconds(EnvSQSWaitTime, DefaultSQSWaitTime, 0, maxSQSWaitTime)
	c.VisibilityTimeout = p.wholeSeconds(EnvSQSVisibilityTimeout, DefaultSQSVisibilityTimeout, time.Second, maxSQSVisibilityTimeout)
	c.ProcessTimeout = p.positiveDuration(EnvSQSProcessTimeout, DefaultSQSProcessTimeout)
	c.RetryBaseDelay = p.wholeSeconds(EnvSQSRetryBaseDelay, DefaultSQSRetryBaseDelay, time.Second, maxSQSVisibilityTimeout)
	c.RetryMaxDelay = p.wholeSeconds(EnvSQSRetryMaxDelay, DefaultSQSRetryMaxDelay, time.Second, maxSQSVisibilityTimeout)
	c.DrainTimeout = p.positiveDuration(EnvSQSDrainTimeout, DefaultSQSDrainTimeout)
	c.MaxAttempts = p.intRange(EnvSQSMaxAttempts, DefaultSQSMaxAttempts, 1, maxSQSMaxAttempts)
	if c.ProcessTimeout > 0 && c.VisibilityTimeout > 0 && c.ProcessTimeout >= c.VisibilityTimeout {
		p.fail(EnvSQSProcessTimeout, "deve ser menor que "+EnvSQSVisibilityTimeout)
	}
	if c.RetryBaseDelay > 0 && c.RetryMaxDelay > 0 && c.RetryMaxDelay < c.RetryBaseDelay {
		p.fail(EnvSQSRetryMaxDelay, "não pode ser menor que "+EnvSQSRetryBaseDelay)
	}
	return c
}

// outboxPublisher lê e valida os parâmetros do publisher da outbox.
func (p *parser) outboxPublisher(w *WorkersConfig) {
	w.OutboxPollInterval = p.durationRange(EnvOutboxPollInterval, DefaultOutboxPollInterval, time.Millisecond, maxOutboxDelay)
	w.OutboxBatchSize = p.intRange(EnvOutboxBatchSize, DefaultOutboxBatchSize, 1, maxOutboxBatchSize)
	w.OutboxLease = p.durationRange(EnvOutboxLease, DefaultOutboxLease, time.Second, maxOutboxLease)
	w.OutboxConcurrency = p.intRange(EnvOutboxConcurrency, DefaultOutboxConcurrency, 1, maxOutboxConcurrency)
	w.OutboxPublishTimeout = p.durationRange(EnvOutboxPublishTimeout, DefaultOutboxPublishTimeout, time.Millisecond, maxOutboxLease)
	w.OutboxDrainTimeout = p.durationRange(EnvOutboxDrainTimeout, DefaultOutboxDrainTimeout, time.Millisecond, maxOutboxDelay)
	w.OutboxRetryBaseDelay = p.durationRange(EnvOutboxRetryBaseDelay, DefaultOutboxRetryBaseDelay, time.Millisecond, maxOutboxDelay)
	w.OutboxRetryMaxDelay = p.durationRange(EnvOutboxRetryMaxDelay, DefaultOutboxRetryMaxDelay, time.Millisecond, maxOutboxDelay)
	// O lease precisa ser maior que o intervalo de polling; caso contrário um
	// lote reivindicado expira antes do próximo ciclo e outro publisher o
	// reassume, gerando publicações duplicadas desnecessárias.
	if w.OutboxLease > 0 && w.OutboxPollInterval > 0 && w.OutboxLease <= w.OutboxPollInterval {
		p.fail(EnvOutboxLease, "deve ser maior que "+EnvOutboxPollInterval)
	}
	// Envio e marcação de um evento precisam caber no lease; senão outro
	// publisher reassume o evento no meio do trabalho (o fencing evita
	// marcação errada, mas a publicação seria repetida).
	if w.OutboxLease > 0 && w.OutboxPublishTimeout > 0 && 2*w.OutboxPublishTimeout >= w.OutboxLease {
		p.fail(EnvOutboxPublishTimeout, "o dobro deve ser menor que "+EnvOutboxLease)
	}
	if b, m := w.OutboxRetryBaseDelay, w.OutboxRetryMaxDelay; b > 0 && m > 0 && m < b {
		p.fail(EnvOutboxRetryMaxDelay, "não pode ser menor que "+EnvOutboxRetryBaseDelay)
	}
}

// pendingWorker lê e valida os parâmetros do worker de pendências.
func (p *parser) pendingWorker() PendingWorkerConfig {
	var c PendingWorkerConfig
	c.PollInterval = p.durationRange(EnvPendingWorkerPollInterval, DefaultPendingWorkerPollInterval, time.Millisecond, maxPendingDelay)
	c.BatchSize = p.intRange(EnvPendingWorkerBatchSize, DefaultPendingWorkerBatchSize, 1, maxPendingWorkerBatchSize)
	c.Concurrency = p.intRange(EnvPendingWorkerConcurrency, DefaultPendingWorkerConcurrency, 1, maxPendingWorkerConcurrency)
	c.Lease = p.durationRange(EnvPendingWorkerLease, DefaultPendingWorkerLease, time.Second, maxPendingWorkerLease)
	c.ProcessTimeout = p.positiveDuration(EnvPendingWorkerProcessTimeout, DefaultPendingWorkerProcessTimeout)
	c.DrainTimeout = p.positiveDuration(EnvPendingWorkerDrainTimeout, DefaultPendingWorkerDrainTimeout)
	c.OrphanGrace = p.durationRange(EnvPendingOrphanGrace, DefaultPendingOrphanGrace, 0, maxPendingDelay)
	// Um item precisa caber no lease; senão outra instância o reassume no
	// meio do processamento (o fencing impede efeito duplicado, mas o
	// trabalho seria perdido).
	if c.Lease > 0 && c.ProcessTimeout > 0 && c.ProcessTimeout >= c.Lease {
		p.fail(EnvPendingWorkerProcessTimeout, "deve ser menor que "+EnvPendingWorkerLease)
	}
	return c
}

// wholeSeconds aceita durações em segundos inteiros dentro de [minV, maxV]
// (a API do SQS só aceita segundos inteiros).
func (p *parser) wholeSeconds(key string, def, minV, maxV time.Duration) time.Duration {
	v, ok := p.get(key)
	if !ok {
		return def
	}
	before := len(p.errs)
	d := p.durationRange(key, def, minV, maxV)
	if len(p.errs) != before {
		return 0 // já reportado
	}
	if d%time.Second != 0 {
		p.fail(key, fmt.Sprintf("deve ser um número inteiro de segundos, recebido %q", v))
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
