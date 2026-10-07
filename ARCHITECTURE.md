# ARCHITECTURE

Registro das decisões arquiteturais do serviço de carteira (desafio Junglegaming).
Cada decisão traz contexto, escolha e consequências. Atualizado a cada tarefa.

## ADR-001 — Módulo, versão Go e dependências base (T01)

- **Módulo:** `github.com/junglegaming/backend-challenge-go` (mesmo path do repositório).
- **Versão Go:** diretiva `go 1.25.0` no `go.mod` (mínimo exigido pelas versões atuais de
  `golang.org/x/*` puxadas pelas dependências). Toolchain local usada: go1.27.
- **Dependências base** (versões fixadas em `go.mod`/`go.sum`, ambos versionados):
  - `go.uber.org/fx` — composição/DI e ciclo de vida (OnStart/OnStop).
  - `github.com/jackc/pgx/v5` (+ `pgxpool`) — PostgreSQL com SQL explícito.
  - `github.com/aws/aws-sdk-go-v2/service/sqs` + `config` — SQS (LocalStack local).
  - `github.com/google/uuid` — identidades internas.
  - `github.com/coreos/go-oidc/v3` — validação de tokens OIDC do IdP externo (Keycloak).
  - `github.com/prometheus/client_golang` — métricas.
- **Fixação antes do uso:** `tools.go` (build tag `tools`, raiz do módulo) importa as
  dependências com `_` para que `go mod tidy` não as remova enquanto ainda não são usadas
  pelo código de produção. Não entra no binário. Cada import deve ser removido quando a
  dependência passar a ser usada diretamente.

## ADR-002 — Layout de pacotes (hexagonal / ports & adapters) (T01)

```
cmd/server/                 composição Fx (main) — único lugar que conhece todos os módulos
internal/domain/            modelo puro: Money, Wallet, ledger, WagerTransaction, regras
internal/app/               casos de uso + portas (interfaces de repositório, tx, publisher)
internal/adapters/http/     handlers net/http (package httpadapter, evita colisão com net/http)
internal/adapters/postgres/ repositórios pgx, gerenciador de transações
internal/adapters/sqs/      consumidor de entrada e publisher da outbox
internal/adapters/auth/     verificação OIDC e autorização
internal/adapters/pendingworker/ worker temporizado de referências pendentes e PENDING órfãos (T21)
internal/platform/config/   configuração por variáveis de ambiente
internal/platform/logging/  slog JSON
internal/platform/metrics/  registro Prometheus
```

Regras de dependência (direção única, de fora para dentro):

- `domain` não importa nada de `app`, `adapters`, `platform`, Fx, pgx, SQS ou HTTP.
- `app` depende apenas de `domain` e de interfaces que ele mesmo declara.
- `adapters/*` implementam as portas de `app`; `platform/*` provê infraestrutura transversal.
- Apenas `cmd/server` e os arquivos `module.go` de app/adapters/platform conhecem Fx
  (`fx.Module`, `fx.Provide`, `fx.Invoke`). Em `app`, só `module.go` importa Fx; os casos
  de uso em si não dependem dele (ADR-016).

## ADR-003 — Ferramentas de build e verificação (T01)

- `Makefile`: `build`, `run`, `test`, `test-race`, `test-integration` (build tag
  `integration`, containers reais), `vet`, `fmt`, `fmt-check`, `tidy`, `check`
  (`fmt-check` + `vet` + `test-race`).
- `-race` exige cgo (gcc). Em máquinas Windows sem gcc, `make test-race-docker` executa
  `go test -race` dentro de `golang:1.25`.
- `cmd/server` expõe `appOptions()` para que testes validem o grafo com `fx.ValidateApp`
  e exercitem Start/Stop sem bloquear em `Run()`.
- `.env.example` lista as variáveis previstas (HTTP, PostgreSQL, SQS/LocalStack, OIDC,
  workers) com valores locais de exemplo, sem segredos reais; `.env` é ignorado pelo git.
  A validação dessas variáveis é responsabilidade de `platform/config` (T09).

## ADR-004 — Representação de Money (T02)

- **Escolha:** `int64` em unidades mínimas (centavos, escala fixa 2) + `Currency` (código
  ISO 4217 alfabético). Sem biblioteca decimal e sem ponto flutuante: o parsing é feito
  dígito a dígito sobre a string, e a formatação usa divisão/resto inteiros
  (`internal/domain/money.go`).
- **Limites:** `[-92233720368547758.08, 92233720368547758.07]` (`math.MinInt64` /
  `math.MaxInt64` centavos). Parsing, `Add`, `Sub` e `Neg` detectam overflow e retornam
  `ErrMoneyOverflow` (ex.: `Neg` de `MinInt64`), nunca fazem wrap-around.
- **Moedas:** apenas códigos de três letras maiúsculas numa lista de moedas cujo expoente
  ISO 4217 é 2 (BRL, USD, EUR, ...). JPY/KWD etc. são rejeitadas porque a escala é fixa.
  Não há normalização de caixa (`"brl"` é inválido).
- **Gramática do valor:** `["-"] ("0" | [1-9][0-9]*) ["." [0-9]{1,2}]`. Rejeita vazio,
  `NaN`, `Infinity`, notação científica, `+`, espaços, zeros à esquerda, `.5`, `5.`,
  separadores, `-0` e escala > 2 (sem arredondamento: `ErrAmountScale`).
- **Normalização (relevante para o hash de idempotência, T08):** `"25"`, `"25.5"` e
  `"25.50"` são aceitos e equivalentes; a forma canônica é `Amount()` com exatamente duas
  casas (`"25.50"`). O hash deve usar a forma canônica, não a string recebida.
- **Sinal:** `Parse` aceita negativos, pois Money também representa diferenças internas
  (reconciliação). Entradas financeiras externas usam `ParseNonNegative`
  (`ErrNegativeAmount`). A não negatividade do saldo é regra da Wallet e do schema.
- **Imutabilidade e zero-value:** campos privados; operações devolvem novos valores. O
  zero-value (`Money{}`, moeda vazia) é "não inicializado": `Add/Sub/Neg/Cmp/MarshalJSON`
  retornam `ErrUninitializedMoney` e os predicados `IsZero/IsPositive/IsNegative` retornam
  `false`. Aritmética/comparação entre moedas diferentes retorna `ErrCurrencyMismatch`.
- **JSON:** contrato `{"amount":"25.00","currency":"BRL"}`. O unmarshal é estrito:
  `amount` precisa ser string JSON (número é rejeitado, evitando que `encoding/json` o
  interprete como ponto flutuante), campos desconhecidos/ausentes e `null` são rejeitados.
- **Erros:** sentinelas compatíveis com `errors.Is`: `ErrInvalidAmount`, `ErrAmountScale`,
  `ErrMoneyOverflow`, `ErrNegativeAmount`, `ErrInvalidCurrency`, `ErrCurrencyMismatch`,
  `ErrUninitializedMoney`. Erros de escala/overflow no parsing também satisfazem
  `errors.Is(err, ErrInvalidAmount)`.
- **Persistência (T12):** `MinorUnits()` → `BIGINT`, `Currency()` → `CHAR(3)`;
  reidratação via `FromMinorUnits`. Os repositórios checam `IsValid()` antes de gravar
  (ADR-014).

## ADR-005 — Agregado Wallet, versionamento e concorrência (T03)

- **Construção separada:** `NewWallet(NewWalletParams)` abre a carteira e `RehydrateWallet(RehydrateWalletParams)`
  reconstrói a partir do banco validando invariantes (ids não nulos, saldo inicializado e
  `>= 0`, versão `>= 1`, `createdAt <= updatedAt`) sem efeitos colaterais. `Wallet{}`/`nil`
  é "não inicializada" (`ErrUninitializedWallet`). IDs de carteira, jogador, transação e
  lançamento são `uuid.UUID` (UUIDv7 recomendado: ordenável no tempo, favorece keyset).
- **Abertura:** a versão inicial é `1`. Com saldo inicial `> 0`, `NewWallet` exige o id da
  transação `OPENING` e devolve o lançamento `CREDIT` de abertura (0 → saldo inicial) **sem**
  incrementar a versão. Com saldo zero, nenhum lançamento é devolvido (sem OPENING/ledger/eventos).
- **Movimentos:** `Debit/Credit(money, txID, now)` são o único caminho para alterar saldo;
  validam carteira inicializada, Money inicializado (`ErrInvalidAmount`+`ErrUninitializedMoney`),
  moeda (`ErrCurrencyMismatch`), valor `> 0` (`ErrInvalidAmount`; negativo também
  `ErrNegativeAmount`), `txID` e instante (`ErrInvalidMovement`), saldo não negativo
  (`ErrInsufficientFunds`) e overflow (`ErrMoneyOverflow`). Em sucesso, incrementam a versão
  em exatamente 1 e devolvem o `WalletLedgerEntry`; em erro, o agregado fica intacto.
  `LOSS` e rejeições não chamam o agregado, logo não alteram a versão. A distinção
  `INSUFFICIENT_FUNDS` × `REVERSAL_INSUFFICIENT_FUNDS` é feita pela camada de regras (T06),
  ambas a partir de `ErrInsufficientFunds`.
- **Concorrência:** o agregado não é thread-safe; vive dentro de uma transação SQL. A
  coordenação é por carteira, no banco (T12): `SELECT ... FOR UPDATE` da linha (pessimista)
  e `UPDATE ... SET balance, version WHERE id = $1 AND version = PersistedVersion()`
  (otimista, defesa em profundidade). 0 linhas afetadas ⇒ escrita concorrente ⇒ retry
  limitado; nenhuma atualização confirmada é descartada. `PersistedVersion()` é a versão
  lida em `RehydrateWallet` (0 para carteira criada por `NewWallet`, que é inserida).
- **Tempo:** `now` é injetado e normalizado para UTC; `updatedAt` é monotônico
  (`max(updatedAt, now)`) para tolerar pequenas diferenças de relógio entre instâncias; o
  lançamento registra o `now` recebido.
- **Ledger:** todo lançamento devolvido pela Wallet é construído por `NewWalletLedgerEntry`
  (ver ADR-006). A versão da carteira após o movimento (exigida em `WalletBalanceChanged`,
  T07) é lida de `Wallet.Version()` da mesma instância, logo após o `Debit/Credit`; o
  construtor do evento confere isso pelo id do último lançamento (ADR-012).

## ADR-006 — WalletLedgerEntry imutável (T04)

- **Local:** `internal/domain/ledger_entry.go` (`LedgerDirection`, `WalletLedgerEntry`,
  `ErrInvalidLedgerEntry`).
- **Imutabilidade:** campos privados; apenas getters com receptor por valor
  (`ID/WalletID/TransactionID/Direction/Amount/BalanceBefore/BalanceAfter/CreatedAt` e
  `IsInitialized`). Sem setters; o tipo não contém ponteiros/slices, então cópias são
  independentes. Um teste por reflexão fixa o conjunto de métodos. Correções exigem novos
  lançamentos; a proibição de `UPDATE/DELETE` no banco é do schema (T10).
- **Construção separada:** `NewWalletLedgerEntry(NewWalletLedgerEntryParams)` para movimentos
  novos (usado por `Wallet.Debit/Credit` e pela abertura) e
  `RehydrateWalletLedgerEntry(RehydrateWalletLedgerEntryParams)` para linhas lidas do banco.
  Ambas aplicam as **mesmas** invariantes (função interna única): a reidratação não confia
  no banco, e dados persistidos inconsistentes falham com erro marcado `rehydrate ledger
  entry <id>`, em vez de alimentar reconciliação/paginação com valores errados.
- **Invariantes (`ErrInvalidLedgerEntry`, mais o sentinela de Money aplicável):** ids de
  lançamento/carteira/transação não nulos; direção exatamente `DEBIT|CREDIT` (sensível a
  caixa); `amount`, `balanceBefore`, `balanceAfter` inicializados (`ErrUninitializedMoney`)
  e na mesma moeda (`ErrCurrencyMismatch`); `amount > 0` (`ErrInvalidAmount`; negativo
  também `ErrNegativeAmount`) — `LOSS` e rejeições não geram lançamento; saldos `>= 0`
  (`ErrNegativeAmount`); `balanceAfter = balanceBefore + amount` (CREDIT) ou `- amount`
  (DEBIT), com overflow detectado (`ErrMoneyOverflow`); `createdAt` não zero, normalizado
  para UTC. Em erro, devolve o zero-value (`IsInitialized() == false`).

## ADR-007 — Config, logging JSON e métricas (T09)

Pacotes `internal/platform/{config,logging,metrics}`, cada um com um `fx.Module`. Ainda não
conectados em `cmd/server` (T23); cada módulo tem teste de `fx.ValidateApp`.

- **Config (`config.Module` → `config.Config`):** somente variáveis de ambiente, com os nomes
  exatos de `.env.example`. `Load(lookup)` recebe a função de lookup (testável sem tocar no
  ambiente) e `LoadFromEnv` usa `os.LookupEnv`. A validação roda no construtor, ou seja,
  durante `fx.New`: config inválida impede o start antes de abrir qualquer conexão. Os erros
  são agregados (`errors.Join`), prefixados pelo nome da variável e envolvem
  `ErrInvalidConfig`. São obrigatórias as variáveis que identificam recursos externos
  (`DATABASE_URL`, `AWS_REGION`, `SQS_INBOUND_QUEUE_URL`, `SQS_EVENTS_QUEUE_URL`,
  `OIDC_ISSUER_URL`, `OIDC_AUDIENCE`), para que um deploy mal configurado nunca aponte
  silenciosamente para localhost. As demais têm defaults iguais aos de `.env.example`. Regras:
  filas precisam ser FIFO (`*.fifo`) e distintas entre si; durações > 0;
  `OUTBOX_LEASE > OUTBOX_POLL_INTERVAL`; credenciais AWS estáticas são informadas juntas ou
  omitidas (nesse caso vale a cadeia padrão do SDK); URLs que não são segredo não podem ter
  userinfo.
- **Segredos:** `config.Secret` (`DATABASE_URL`, `AWS_SECRET_ACCESS_KEY`) imprime
  `[REDACTED]` em todo verbo válido do fmt (`fmt.Formatter`: `%v/%+v/%#v/%s/%q/%d/%x/...`,
  com largura e flags, inclusive aninhado em `Config`/`*Config`), em slog (`LogValuer`) e em
  JSON/texto. Exceção: `%p` e `%w` fora de `Errorf` são verbos inválidos que o fmt resolve
  sem consultar métodos, e nesses casos o valor bruto sai. Só o `%w` é acusado pelo `go vet`
  (printf). O `%p` aplicado direto a um `Secret` passa no vet, porque o analisador aceita
  qualquer verbo para um tipo que implementa `fmt.Formatter`; esse uso fica a cargo da revisão.
  Outro caso de borda: `Secret` como chave de map. O `encoding/json` usa o valor bruto de
  chaves de tipo string sem consultar `MarshalText`, então `json.Marshal(map[Secret]T)` vaza.
  No logger isso é coberto, porque a redação consulta `MarshalText` antes e a chave sai
  mascarada. Fora do logger, não use `Secret` como chave.
  O valor real só sai por `Reveal()`, então os pontos de uso podem ser auditados com grep.
  Os erros de validação nunca ecoam o valor de uma variável sensível. Em URLs inválidas não
  sensíveis, o erro ecoa só scheme+host+path: userinfo, query e fragment são descartados.
- **Fila de eventos:** padronizada em `wager-events.fifo` na T22 (ADR-024). Antes, o
  `.env.example` citava `wallet-events.fifo`. O código só exige uma URL FIFO, então o nome
  está na configuração e no provisionamento (T24), não no código.
- **Logging (`logging.Module` → `*slog.Logger`, depende de `config.Config`):** `log/slog`
  com `JSONHandler` em stdout, nível vindo de `LOG_LEVEL` e atributos fixos `service`/`env`.
  As chaves de rastreio são `correlationId`, `messageId`, `transactionId`, `walletId` e
  `providerId`, fornecidas por helpers de atributo (`logging.WalletID(id)`, ...) ou propagadas
  pelo `context` (`logging.WithCorrelationID(ctx, id)`, ...). O handler injeta esses IDs na
  raiz do JSON em todo log `*Context(ctx, ...)`, mesmo quando o logger usa `WithGroup`.
  Redação em profundidade (`logging/redact.go`) com estas garantias:
  (1) chave sensível (senha, token, secret, authorization, cookie, dsn, credenciais,
  signature, payload/body etc., normalizada e por sufixo) tem o valor inteiro trocado por
  `[REDACTED]` em qualquer nível: atributo, grupo, chave de map, campo de struct (nome Go ou
  tag json), membro de `LogValuer` em grupo e parâmetro de query/fragment de URL.
  (2) Todo valor `KindAny` (map, slice/array, struct, ponteiro, `http.Header`, `url.URL`,
  `error`, `json.Marshaler`, `TextMarshaler`, `LogValuer`) é percorrido recursivamente e
  vira uma árvore já redigida. A recursão tem limite de profundidade (ciclos viram
  `[TRUNCATED]`), e um panic de terceiros dá falha fechada (`[REDACTED]`). Números de
  `json.Marshaler` são preservados exatos.
  (3) Toda string, incluindo `msg` e o texto de erros, passa por limpeza textual: valor que é
  inteiro um header `Bearer`/`Basic` (qualquer whitespace) vira `[REDACTED]`; `Bearer <token>`
  e `Basic <credencial>` no meio do texto, senha/userinfo de URLs, query sensível e pares
  `chave=valor`, `chave: valor`, `"chave":"valor"` e `\"chave\":\"valor\"` (JSON escapado
  dentro de uma string) com chave sensível têm só o trecho trocado. Como efeito colateral
  aceito, a palavra seguinte a "basic" em prosa também é trocada. `[]byte` com UTF-8 válido
  sai como string limpa (não base64), e `[]byte` binário sai só como `[N bytes]`.
  Limites: um segredo em texto livre, sem chave nem prefixo reconhecível, não é detectável.
  JSON escapado em mais de um nível (`\\\"chave\\\"`) não é reconhecido.
  Por isso `config.Secret` continua sendo a primeira barreira. Payloads completos não são
  logados (README §12). O logger global não é alterado (nada de
  `slog.SetDefault`). `logging.FxEventLogger()` envia os eventos do Fx para o mesmo logger e
  deve ser usado no nível raiz em T23. `logging.WithOutput(w)` troca o destino em testes.
- **Métricas (`metrics.Module` → `*Metrics`, `*prometheus.Registry` também como
  `Registerer`/`Gatherer`, `http.Handler` nomeado `metricsHandler`):** usam um registry
  próprio, nunca o global, com coletores de Go/processo registrados explicitamente. Assim
  vários apps no mesmo processo (testes multi-instância) não colidem, e o registro devolve
  erro em vez de panic. Namespace `wallet`, cobrindo cada item da T09/README §12:
  `wager_transactions_total{source,type,status}`, `duplicates_total{source,kind}`,
  `retries_total{operation}`, `dlq_messages_total{reason}`, `dlq_depth_messages{queue}`,
  `concurrency_conflicts_total{kind}`, `outbox_pending_events`,
  `outbox_oldest_pending_age_seconds`, `outbox_publish_delay_seconds`,
  `processing_duration_seconds{source,operation,outcome}`,
  `http_request_duration_seconds{method,route,code}`, `reconciliations_total{outcome}` e
  `reconciliation_divergences_total{currency}`. A cardinalidade é limitada pelo próprio
  código (`metrics/labels.go`), não pela disciplina de quem chama. Os métodos auxiliares
  normalizam cada rótulo contra um conjunto fechado e mandam o desconhecido para `other`:
  `source`, `kind`, `operation`, `outcome` e `reason` pelas constantes do pacote; `type`
  (OPENING, BET, WIN, LOSS, REFUND, ROLLBACK), `status` (PENDING, PENDING_REFERENCE,
  PROCESSED, REJECTED, FAILED) e `currency` (BRL, USD, EUR, GBP, CAD, AUD, CHF, MXN, ARS, COP,
  PEN, UYU) por cópias locais das listas do domínio, porque platform não importa domain.
  `method` aceita só os métodos padrão e o resto vira `OTHER`. `code` fica em 100–599 ou
  `unknown`. `route` e `queue` são fechados por registro explícito (`AllowRoutes`, com os
  templates do roteador, e `AllowQueues`), que o adapter faz na inicialização. A allowlist
  tem teto de 256 entradas, e rota/fila não registrada (inclusive 404) vira `other`. IDs
  (carteira, transação, mensagem) vão para os logs, nunca para rótulos. Os campos `*Vec`
  continuam exportados, mas incrementá-los direto contorna a normalização, então adapters
  devem usar os métodos auxiliares.
- **Sem float monetário em métricas:** as amostras Prometheus são `float64`, então nenhum valor
  monetário é exportado. A divergência de reconciliação é contada em ocorrências, e a diferença
  exata (Money) fica na resposta da API e no log. `float64` aparece só em segundos e contagens.

## ADR-008 — WagerTransaction e máquina de estados (T05)

- **Local:** `internal/domain/wager_transaction.go` (`WagerKind`, `WagerOrigin`, `WagerStatus`,
  `FailureCode`/`FailureCategory`, `WagerTransaction`). Só lógica pura: a decisão de débito,
  crédito e reversão é da T06, a persistência da T10/T12 e a orquestração da T15/T21.
- **Origem:** `OPENING` é a única operação `INTERNAL`. `BET/WIN/LOSS/REFUND/ROLLBACK` são
  `EXTERNAL`. A origem é derivada do tipo (`WagerKind.Origin()`), e a reidratação rejeita
  combinações divergentes. O schema (T10) deve persistir `origin` e usar CHECKs: em `INTERNAL`,
  provedor, ID externo, chave, hash, rodada, jogo e referências ficam `NULL` e o tipo é
  `OPENING`; em `EXTERNAL`, esses campos são `NOT NULL` (a referência é opcional).
- **Construção:**
  - `NewExternalWagerTransaction(NewExternalWagerTransactionParams)` cria a transação em
    `PENDING`. `OPENING` é rejeitado com `ErrOpeningNotExternal`, que também é
    `ErrInvalidWagerKind`, ou seja, `INVALID_KIND`. `ParseExternalWagerKind` aplica a mesma
    regra ao campo `kind` de HTTP/SQS e é sensível a caixa.
  - `NewOpeningWagerTransaction(NewOpeningWagerTransactionParams)` cria a transação já em
    `PROCESSED`, com `balanceAfter = amount`, porque a abertura é confirmada no mesmo commit
    da carteira e não tem estado intermediário observável. Seu ID é o
    `OpeningTransactionID` passado a `NewWallet`. Com saldo zero nada é criado (o construtor
    rejeita `0.00`).
  - `RehydrateWagerTransaction(RehydrateWagerTransactionParams)` não transiciona nada e
    revalida tudo. Dado persistido inconsistente devolve `ErrCorruptedWagerTransaction`,
    que é erro interno e nunca código de entrada.
  - Construtores, reidratação e transições usam a mesma função `validate()`, então as
    invariantes nunca divergem.
- **Máquina de estados** (`CanTransitionWagerStatus`):

  | De \ Para | PENDING_REFERENCE | PROCESSED | REJECTED | FAILED |
  | --- | --- | --- | --- | --- |
  | `PENDING` | `MarkPendingReference` | `MarkProcessed` | `Reject` | `Fail` |
  | `PENDING_REFERENCE` | `RescheduleReferenceRetry` | `MarkProcessed` | `Reject` | `Fail` |

  `PROCESSED`, `REJECTED` e `FAILED` são terminais e não aceitam transição
  (`ErrWagerTransactionTerminal`, que também é `ErrWagerTransitionNotAllowed`). Não existe
  aresta de volta para `PENDING`: o worker resolve a referência a partir de
  `PENDING_REFERENCE` na mesma transação SQL. Cada transição muta uma cópia, revalida e só
  então substitui o receptor, de modo que um erro deixa a transação intacta. `updatedAt` é
  monotônico e `completedAt` é igual a `updatedAt` na transição terminal.
- **Transitório x permanente:** falha transitória (timeout, serialização, deadlock, conexão)
  **não** vira estado. Ela faz rollback e é refeita por retry da aplicação, por reentrega SQS
  ou pelo worker, de modo que o registro permanece `PENDING`/`PENDING_REFERENCE` ou nem é
  criado. `FAILED` é reservado à falha permanente de infraestrutura que esgotou as tentativas
  e exige código da categoria `INFRASTRUCTURE` (`PROCESSING_FAILED`, `RETRIES_EXHAUSTED`).
  `REJECTED` exige código `INPUT` ou `BUSINESS`.
- **Política de zero:** `LOSS` exige exatamente `0.00`. `BET/WIN/REFUND/ROLLBACK/OPENING`
  exigem `> 0`. Negativo e Money não inicializado são sempre rejeitados (`ErrInvalidAmount`
  junto com `ErrNegativeAmount`/`ErrUninitializedMoney`).
- **Referências:** `REFUND/ROLLBACK` exigem `referenceExternalTransactionId`
  (`ErrReferenceRequired`). `WIN` aceita a referência como opcional. `BET/LOSS` não aceitam
  (`ErrReferenceNotAllowed`), e a autorreferência também é recusada.
  `ReferenceTransactionID` (a referência interna resolvida) é obrigatório em `PROCESSED`
  quando há referência externa, opcional em `REJECTED`/`PENDING_REFERENCE` e proibido sem
  referência externa. `PENDING_REFERENCE` só é permitido com referência externa.
- **Retomada durável:**
  - `attempts` (`int32` → `INTEGER`) conta **somente** as buscas de referência sem sucesso.
    `MarkPendingReference` registra a primeira (attempts = 1) e cada
    `RescheduleReferenceRetry` soma 1. Consequências validadas no domínio (inclusive na
    reidratação): `attempts > 0` exige `referenceExternalTransactionId` e é proibido em
    `PENDING` (não há aresta de volta para `PENDING`). BET, LOSS, WIN sem referência e
    OPENING têm sempre `attempts = 0`. O schema (T10) deve espelhar com
    `CHECK (attempts = 0 OR (reference_external_transaction_id IS NOT NULL AND status <> 'PENDING'))`.
  - `nextAttemptAt` é obrigatório em `PENDING_REFERENCE`, opcional em `PENDING` e é zerado em
    estado terminal. `attempts` é mantido para auditoria. A retomada de `PENDING` órfão (T21)
    usa apenas `nextAttemptAt`/lease e **não** consome `attempts`; se a T21 precisar limitar
    retomadas de órfãos, usará um contador próprio, não este.
  - O backoff e o limite de tentativas/TTL são política da aplicação (T21): ao esgotar, ela
    chama `Reject(REFERENCE_NOT_FOUND)`.
- **Resultado para replay:** `balanceAfter` é obrigatório em `PROCESSED` (em `LOSS`, é o
  saldo inalterado), opcional em `REJECTED` (saldo observado) e proibido nos demais estados.
  Precisa estar na moeda da operação e ser `>= 0`.
- **Identificadores externos:** não são normalizados (sem trim nem mudança de caixa), porque
  a mesma string entra no hash (T08) e nas chaves únicas. Precisam ser não vazios, ter no
  máximo `MaxWagerIdentifierLength` = 255 bytes e não podem ter espaço nas bordas nem
  caracteres de controle. `requestHash` tem exatamente 64 caracteres hex minúsculos
  (SHA-256, T08).
- **FailureCodes** (contrato público estável, `AllFailureCodes()`):

  | Categoria | Códigos |
  | --- | --- |
  | `INPUT` (corrigível: o provedor corrige e envia uma nova operação) | `INVALID_REQUEST`, `INVALID_KIND`, `INVALID_AMOUNT`, `INVALID_CURRENCY`, `REFERENCE_REQUIRED`, `REFERENCE_NOT_ALLOWED`, `WALLET_NOT_FOUND`, `WALLET_PLAYER_MISMATCH`, `CURRENCY_MISMATCH`, `REFERENCE_MISMATCH`, `AMOUNT_MISMATCH` |
  | `BUSINESS` (definitivo) | `INSUFFICIENT_FUNDS`, `REVERSAL_INSUFFICIENT_FUNDS`, `REFERENCE_NOT_FOUND`, `REFERENCE_NOT_REVERSIBLE`, `REFERENCE_NOT_PROCESSED`, `ALREADY_REVERSED` |
  | `INFRASTRUCTURE` (só `FAILED`) | `PROCESSING_FAILED`, `RETRIES_EXHAUSTED` |

  Restrições validadas pelo domínio:
  - Códigos de referência (`REFERENCE_NOT_FOUND`, `REFERENCE_MISMATCH`,
    `REFERENCE_NOT_REVERSIBLE`, `REFERENCE_NOT_PROCESSED`, `ALREADY_REVERSED`,
    `AMOUNT_MISMATCH`) exigem referência externa.
  - `INSUFFICIENT_FUNDS` só vale para `BET`.
  - `REVERSAL_INSUFFICIENT_FUNDS` só vale para `ROLLBACK`, o único tipo de reversão que pode
    debitar.
  - `ALREADY_REVERSED` e `AMOUNT_MISMATCH` só valem para reversões.
  - `REFERENCE_NOT_FOUND` não pode coexistir com `referenceTransactionId` resolvido (a
    referência nunca foi encontrada). Os demais códigos de referência podem registrar a
    referência resolvida.

  `REFERENCE_MISMATCH` cobre divergência de provedor, jogador, carteira ou rodada.
  `REFERENCE_NOT_PROCESSED` indica referência terminada sem sucesso (`REJECTED`/`FAILED`).
  Referência ainda pendente não gera rejeição: a operação aguarda.
  `ValidationFailureCode(err)` mapeia erros de construção/parsing para o código do corpo
  HTTP/SQS.

## ADR-009 — Regras de operação e reversão (T06)

- **Local:** `internal/domain/wager_rules.go`. Serviço de domínio puro, sem I/O e sem relógio
  global, em duas etapas:
  - `DecideWager(WagerRuleInput) (WagerDecision, error)` só lê. A entrada traz a transação
    nova (`PENDING`/`PENDING_REFERENCE`), o snapshot da carteira (`nil` = inexistente), a
    referência resolvida por `(providerId, referenceExternalTransactionId)` (`nil` = ainda não
    recebida) e as reversões já existentes dessa referência, em qualquer estado. A saída é
    `APPLY_MOVEMENT` (`DEBIT`/`CREDIT` + valor + saldo projetado), `NO_MOVEMENT` (`LOSS`),
    `AWAIT_REFERENCE` (motivo `REFERENCE_MISSING`/`REFERENCE_PENDING`) ou `REJECT`
    (`FailureCode` + saldo observado).
  - `ApplyWagerDecision(ApplyWagerDecisionParams) (WalletLedgerEntry, error)` aplica a decisão:
    `Wallet.Debit/Credit` e `MarkProcessed`/`MarkPendingReference`/
    `RescheduleReferenceRetry`/`Reject`. As mudanças são feitas em cópias e só publicadas se
    tudo der certo, então um erro deixa carteira e transação intactas. Recebe `now` e
    `nextAttemptAt` (backoff da T21).
- **Rejeição é decisão, não erro.** O `error` é reservado a entrada inconsistente
  (`ErrInvalidWagerRuleInput`: transação terminal ou não inicializada, carteira de outro id,
  referência com outro ID externo, reversão listada que não aponta para a referência) e a
  overflow de saldo em crédito (`ErrMoneyOverflow`). A aplicação trata os dois como falha
  interna. Transação terminal nunca é reavaliada: replay lê o resultado persistido.
- **Ordem de avaliação** (o primeiro critério que falha decide):
  1. Carteira: inexistente → `WALLET_NOT_FOUND`; de outro jogador → `WALLET_PLAYER_MISMATCH`;
     outra moeda → `CURRENCY_MISMATCH`. Essas três rejeições não carregam saldo, para não
     expor a carteira de outro jogador.
  2. Sem referência: `BET` debita (sem saldo → `INSUFFICIENT_FUNDS`), `LOSS` não movimenta
     (`balanceAfter` = saldo atual, versão inalterada) e `WIN` credita.
  3. Com referência (`WIN` opcional, `REFUND`/`ROLLBACK` obrigatória):
     - referência ausente → `AWAIT_REFERENCE`;
     - critérios estáticos: `OPENING` → `REFERENCE_NOT_REVERSIBLE`; provedor, jogador,
       carteira ou rodada divergentes → `REFERENCE_MISMATCH`; moeda → `CURRENCY_MISMATCH`;
       tipo não aceito → `REFERENCE_NOT_REVERSIBLE`; valor da reversão ≠ valor referenciado
       → `AMOUNT_MISMATCH`;
     - estado da referência: `PENDING`/`PENDING_REFERENCE` → `AWAIT_REFERENCE`;
       `REJECTED`/`FAILED` → `REFERENCE_NOT_PROCESSED` (definitivo);
     - reversão já existente → `ALREADY_REVERSED`;
     - saldo: estorno que debitaria além do saldo → `REVERSAL_INSUFFICIENT_FUNDS`, distinto de
       `INSUFFICIENT_FUNDS`.

  A identidade é checada antes de tipo e estado para não revelar dados de uma transação de
  outro jogador ou rodada. Os critérios estáticos vêm antes do estado para que o resultado
  não dependa do momento em que a referência é processada: uma reversão com valor errado é
  rejeitada já, sem esperar a referência terminar.
- **Tipos aceitos como referência:** `WIN` → `BET`. `REFUND` → `BET`. `ROLLBACK` → `BET`, `WIN`
  ou `REFUND`. `LOSS` (sem movimento), `ROLLBACK` (não se desfaz um desfazimento) e `OPENING`
  são `REFERENCE_NOT_REVERSIBLE`.
- **Movimento:** `REFUND(BET)` e `ROLLBACK(BET)` creditam. `ROLLBACK(WIN)` e
  `ROLLBACK(REFUND)` debitam. O valor é sempre o da referência (reversão parcial não existe).
  O `WIN` com referência credita o próprio valor: a `BET` serve só de vínculo de rodada.
- **Política REFUND × ROLLBACK:** cada referência admite **no máximo uma reversão
  bem-sucedida, de qualquer tipo**. Contam apenas reversões `PROCESSED`, e a própria
  transação é ignorada se aparecer na lista. Isso cobre "não duas reversões do mesmo tipo" e
  impede devolver duas vezes o mesmo débito:

  | Já existe (PROCESSED) sobre a BET | Nova operação | Resultado |
  | --- | --- | --- |
  | `REFUND` | `REFUND` ou `ROLLBACK` da BET | `ALREADY_REVERSED` |
  | `ROLLBACK` | `REFUND` ou `ROLLBACK` da BET | `ALREADY_REVERSED` |
  | `REFUND` | `ROLLBACK` do **REFUND** | debita, desfazendo a devolução (uma única vez) |
  | rejeitadas/pendentes/falhas | qualquer reversão | não contam |

  Limitação consciente: uma `BET` cuja devolução foi desfeita (`REFUND` + `ROLLBACK(REFUND)`)
  não pode ser devolvida de novo. Em troca, a regra cabe num único índice único parcial do
  schema (T10), a defesa final contra corrida:
  `UNIQUE (reference_transaction_id) WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK')`.
- **Concorrência:** o caso de uso (T15/T21) chama decidir e aplicar dentro da transação SQL,
  com a carteira travada (`SELECT ... FOR UPDATE`, ADR-005). Referência e reversões ficam na
  **mesma** carteira (caso contrário, `REFERENCE_MISMATCH`), então esse lock também serializa
  as reversões concorrentes de uma referência e o snapshot lido é atual. Para que um
  snapshot antigo nunca seja aplicado, `ApplyWagerDecision` confere id, versão e saldo da
  carteira contra os da decisão e devolve `ErrStaleWagerDecision` se divergirem. O caso de uso
  deve então reler sob lock e decidir de novo. Sequencialmente, duas `BET` de 80.00 sobre
  100.00 resultam em uma `PROCESSED`, uma `REJECTED INSUFFICIENT_FUNDS`, saldo 20.00 e um único
  débito (teste `TestWagerRules_TwoBetsOf80On100`).
- **Referência ausente/pendente:** o worker (T21) chama de novo `DecideWager` e
  `ApplyWagerDecision`. A partir de `PENDING_REFERENCE`, um novo `AWAIT_REFERENCE` vira
  `RescheduleReferenceRetry` (`attempts + 1`). Esgotado o limite, a aplicação chama
  `Reject(REFERENCE_NOT_FOUND)` sem referência resolvida (ADR-008).

## ADR-010 — Hash canônico de idempotência (T08)

- **Local:** `internal/domain/idempotency` (`RequestFields`, `RequestHash`, `CanonicalJSON`,
  `HashSchemaVersion`). É a única função de hash de requisição, usada pelo HTTP (T18) e
  pelo consumidor SQS (T20) antes de chamar o caso de uso (T15); a inbox (T13) compara o
  mesmo valor em reentregas.
- **Campos:** `providerId` (da identidade autenticada), `externalTransactionId`, `playerId`,
  `walletId`, `roundId`, `gameId`, `kind`, `money` (`amount` = `Money.Amount()` com duas casas
  e `currency`), `referenceExternalTransactionId` e `v` (versão do esquema, hoje `1`).
  Ficam de fora `idempotencyKey` e todo metadado de transporte (headers, `messageId`,
  `eventId`, `correlationId`, `occurredAt`).
- **Normalização:** a entrada é tipada (`domain.Money`, `domain.WagerKind`, `uuid.UUID`).
  Por isso `"25"`/`"25.00"` e `"25.5"`/`"25.50"` são equivalentes (ADR-004) e UUIDs entram na
  forma minúscula de `uuid.UUID.String()`. Strings de identificadores não são normalizadas
  (ADR-008): caixa e espaços mudam o hash.
- **Ausente x vazio:** todas as chaves estão sempre presentes. Referência vazia é serializada
  como `null`. O transporte mapeia omissão, `null` e `""` para `""`, já que o domínio não
  distingue esses casos. Os demais campos são sempre strings, mesmo vazias.
- **Formato:** JSON compacto, chaves em ordem de bytes (inclusive em `money`) e escape no estilo
  RFC 8785 (só `"`, `\` e controles `< 0x20`; o resto é UTF-8 literal). UTF-8 inválido e Money
  não inicializado são rejeitados com `ErrInvalidHashInput`, porque encoders trocariam bytes
  inválidos por U+FFFD e poderiam gerar colisões.
- **Digest:** SHA-256 em 64 hex minúsculos (`domain.RequestHashLength`). Um vetor golden
  (`TestGoldenVector`) fixa o algoritmo. Qualquer mudança intencional incrementa `v`; hashes
  antigos deixam de casar e o resultado é conflito explícito, nunca colisão silenciosa.

## ADR-011 — Migrations PostgreSQL e schema (T10)

- **Local:** SQL versionado em `migrations/NNNN_nome.{up,down}.sql`, embutido por `migrations.FS`
  (`embed.FS`). Runner em `internal/adapters/postgres/migrate` e CLI em `cmd/migrate`
  (`up`, `down N`, `down -all`, `status`), que lê só `DATABASE_URL` (+ `MIGRATE_TIMEOUT`) e
  pode rodar como job do Compose (T24) antes das réplicas. Makefile: `migrate-up`,
  `migrate-down` (1 passo), `migrate-status`.
- **Runner próprio em vez de golang-migrate:**
  - É mínimo (cerca de 300 linhas), usa só o pgx/v5 e não acrescenta dependência.
  - Cada migration roda numa transação junto com o `INSERT/DELETE` em `schema_migrations`,
    aproveitando o DDL transacional do PostgreSQL. Não existe estado "dirty" que exija
    `force` manual. Uma falha no meio desfaz a migration inteira (há teste).
  - **Réplicas simultâneas:** `pg_advisory_lock(AdvisoryLockKey)` de sessão, adquirido antes
    de criar/ler `schema_migrations`. As demais réplicas esperam, releem o estado e não
    aplicam nada. Se um processo morrer, a sessão cai e o lock é liberado. Por isso a API
    recebe um `*pgx.Conn` (uma sessão), nunca um pool. O teste com 3 conexões simultâneas
    aplica cada versão exatamente uma vez.
  - **Integridade do histórico:** `schema_migrations.checksum` guarda o SHA-256 do arquivo
    up. Três situações abortam antes de qualquer DDL: arquivo aplicado alterado
    (`ErrChecksumMismatch`), versão aplicada que o binário não conhece (`ErrUnknownVersion`,
    banco à frente do código) e lacunas (`ErrInconsistentHistory`). As versões precisam
    ser 1..N sem buracos, com pares up/down obrigatórios (validado em `Load` e em teste
    unitário).
  - **Limitação:** comandos que não rodam em transação (`CREATE INDEX CONCURRENTLY`) não
    são suportados.
- **Tabelas e invariantes no banco:**
  - Money é sempre `BIGINT` (centavos) + `CHAR(3)` com `CHECK ~ '^[A-Z]{3}$'`. Não há
    ponto flutuante no schema.
  - `wallets`:
    - `UNIQUE(player_id, currency)`, `balance >= 0`, `version >= 1`.
    - `UNIQUE(id, currency)` é o alvo das FKs do ledger.
    - Trigger `wallets_guard_update`: id, player, moeda e `created_at` são imutáveis. A
      versão sobe **exatamente 1** quando o saldo muda e não muda sem mudança de saldo.
      `updated_at` é monotônico.
  - `wager_transactions`:
    - `origin` e os CHECKs de coerência espelham `WagerTransaction.validate()`: INTERNAL
      só para OPENING, sem metadados externos, `PROCESSED`, `balance_after = amount` e
      `attempts = 0`. EXTERNAL exige provedor, ID externo, chave, hash, rodada e jogo.
    - Política de zero, regras de referência e autorreferência.
    - Status × `failure_code` (REJECTED: INPUT/BUSINESS; FAILED: INFRASTRUCTURE), mais a
      aplicabilidade de cada código por tipo e por referência.
    - Status × `balance_after`, status × `completed_at` e agendamento (`PENDING_REFERENCE`
      exige `attempts >= 1` e `next_attempt_at`; terminal não fica agendado).
    - `attempts > 0` exige referência e é proibido em `PENDING`.
    - Identificadores são `TEXT` com 1..255 **bytes** (`octet_length`, igual ao domínio).
      `request_hash` é `CHAR(64)` em hex minúsculo.
    - Únicos: `(provider_id, external_transaction_id)`, `(provider_id, idempotency_key)`,
      OPENING por carteira (parcial) e **uma reversão bem-sucedida por referência, de
      qualquer tipo** (`UNIQUE(reference_transaction_id) WHERE status='PROCESSED' AND kind
      IN ('REFUND','ROLLBACK')`, política do ADR-009). Esse índice implica o
      "uma por (referência, tipo)" da T10.
    - Trigger `wager_transactions_guard_update`: estados terminais são imutáveis, não há
      `PENDING_REFERENCE → PENDING`, colunas de entrada são imutáveis e `attempts` nunca
      diminui.
    - **Sem FK de `wallet_id` para `wallets`:** rejeições `WALLET_NOT_FOUND`,
      `WALLET_PLAYER_MISMATCH` e `CURRENCY_MISMATCH` precisam ser persistidas para o
      replay. A integridade financeira vem das FKs compostas do ledger.
  - `wallet_ledger_entries`:
    - `UNIQUE(wallet_id, transaction_id)`, `amount > 0` e saldos `>= 0`.
    - CHECK aritmético em `NUMERIC`: exato e sem overflow.
    - FK `(wallet_id, currency) → wallets(id, currency)` e FK
      `(transaction_id, wallet_id, currency) → wager_transactions(id, wallet_id, currency)`.
    - Triggers bloqueiam `UPDATE`/`DELETE` (por linha) e `TRUNCATE`, inclusive o
      `TRUNCATE ... CASCADE` vindo de `wallets`/`wager_transactions`.
  - `inbox`: PK `(consumer_name, message_id)` (constraint `inbox_consumer_message_key`),
    `request_hash CHAR(64)`, `transaction_id` opcional (FK), `received_at` e `completed_at`.
  - `outbox`:
    - `seq` IDENTITY (ordem) e `event_id UUID UNIQUE` (= `MessageDeduplicationId`).
    - `payload JSON` (não JSONB), para guardar o snapshot byte a byte.
    - Colunas `attempts`, `next_attempt_at`, `locked_until`/`locked_by` (sempre em par),
      `last_error` e `published_at`.
    - Trigger `outbox_guard`: o snapshot é imutável, evento publicado é imutável e evento
      não publicado não pode ser apagado.
- **Índices dos workers:** `PENDING_REFERENCE` por `(next_attempt_at, id)`, `PENDING` por
  `(created_at, id)`, `(provider_id, reference_external_transaction_id)` para referências,
  outbox não publicada por `(next_attempt_at, seq)` e extrato por
  `(wallet_id, created_at, id)`. A paginação do extrato passou a usar
  `(wallet_id, seq)` na migration 0006 (ADR-014).
- **Erros para a T11:** CHECK → `23514`, UNIQUE → `23505`, FK → `23503`. As guardas de
  trigger usam `23001` (`restrict_violation`) com `CONSTRAINT` nomeado
  (`wallet_ledger_entries_append_only`, `wallets_version_increment`,
  `wager_transactions_terminal_immutable`, `outbox_snapshot_immutable`...). Assim o
  classificador decide pelo nome da constraint.
- **Limitações:** o dono das tabelas ou um superusuário pode desabilitar triggers
  (`DISABLE TRIGGER`, `session_replication_role`). Em produção a aplicação deve usar um
  papel que não seja dono (T24). `down` destrói dados (DROP não dispara as guardas) e é
  destinado a ambientes descartáveis.
- **Testes:** os unitários (sem tag) cobrem pares up/down, convenção, validação de
  histórico e o casamento dos enums do domínio com os CHECKs. Os de integração
  (`-tags=integration`) usam testcontainers (`postgres:17-alpine`) ou `TEST_DATABASE_URL`
  (precisa de `CREATE DATABASE`, um banco novo por teste) e cobrem up/down/up, down
  parcial, concorrência entre 3 sessões, adulteração, atomicidade e cada
  constraint/trigger.

## ADR-012 — Eventos de integração tipados (T07)

- **Local:** `internal/domain/events.go` (`IntegrationEvent`, `EventType`, `EventMetadata`,
  `EventData` e os payloads `WagerTransactionProcessedData`, `WagerTransactionRejectedData`,
  `WalletBalanceChangedData`, `WagerTransactionPendingReferenceData`). Testes golden em
  `internal/domain/testdata/*.golden.json` (regravar com `go test ./internal/domain/ -run
  TestIntegrationEvents_Golden -update`).
- **Envelope:** `{eventId, eventType, aggregateId, correlationId, causationId?, occurredAt,
  version, data}`. `causationId` é omitido quando ausente. `eventId` é UUIDv7 (gerador em
  variável de pacote `newEventID`, substituída só nos testes).
- **Construtores (únicos caminhos de criação):**
  `NewWagerTransactionProcessedEvent(tx, meta)` (exige `PROCESSED`),
  `NewWagerTransactionRejectedEvent(tx, meta)` (exige `REJECTED`),
  `NewWagerTransactionPendingReferenceEvent(tx, meta)` (exige `PENDING_REFERENCE`) e
  `NewWalletBalanceChangedEvent(entry, wallet, meta)`. Tipo e versão (`1` em todos) são
  fixados pelo tipo do payload, uma interface selada; o chamador não os escolhe. Erros
  satisfazem `ErrInvalidIntegrationEvent`.
- **Snapshot imutável:** o construtor serializa o envelope uma vez e guarda os bytes.
  `Payload()` e `MarshalJSON()` devolvem cópias, `Data()` devolve cópia profunda, não há
  campos exportados nem setters (teste por reflexão fixa o conjunto de métodos). Mudanças
  posteriores na transação/carteira não alteram o evento. A outbox (T13) persiste
  `Payload()` e o publisher (T22) envia esses bytes sem reconstruir, preservando o
  `eventId` em republicações.
- **aggregateId e ordenação FIFO:** eventos de transação usam o id da transação (o ciclo
  `PENDING_REFERENCE` → `PROCESSED`/`REJECTED` fica no mesmo `MessageGroupId`).
  `WalletBalanceChanged` usa o id da carteira (mudanças de saldo da carteira em ordem).
  Não há ordem garantida entre grupos; consumidores ordenam/deduplicam saldo por
  `walletVersion`.
- **occurredAt:** RFC 3339 em UTC (`Z`), truncado a microssegundos para coincidir com o
  `timestamptz` da coluna da outbox. Conclusões (`Processed`/`Rejected`) usam `CompletedAt`
  (ADR-008); `PendingReference` usa `UpdatedAt` (instante do registro da espera);
  `WalletBalanceChanged` usa `CreatedAt` do lançamento. `nextAttemptAt` segue o mesmo
  formato.
- **Dinheiro:** sempre `{"amount":"25.00","currency":"BRL"}` (ADR-004). Os únicos números
  JSON do envelope são `version`, `walletVersion` e `attempts` (teste verifica).
- **WalletBalanceChanged:** `walletId`, `transactionId`, `direction`, `money`,
  `balanceBefore`, `balanceAfter`, `walletVersion`. O evento é construído com a **mesma
  instância** de `Wallet` que produziu o lançamento, antes de qualquer outro movimento, e
  `walletVersion = wallet.Version()`. A `Wallet` guarda só em memória o id do último
  lançamento que produziu (`NewWallet`/`Debit`/`Credit`; `uuid.Nil` após `RehydrateWallet`),
  e o construtor exige `entry.ID == esse id`, além de `entry.WalletID == wallet.ID` e
  `wallet.Balance == entry.BalanceAfter`. Comparar só o saldo não bastava: no caso A-B-A
  (outro movimento leva o saldo de volta ao mesmo valor) o saldo bate e a versão não, e o
  evento sairia com `walletVersion` errada. Carteira reidratada, desatualizada ou
  movimentada de novo é recusada (`TestWalletBalanceChanged_ABA`). Um `Debit/Credit` que
  falha não altera o último lançamento. A versão não foi colocada no `WalletLedgerEntry`
  porque ela não é persistida no ledger e a reidratação não conseguiria reproduzi-la.
- **Dados sensíveis:** `idempotencyKey` e `requestHash` nunca entram nos eventos.
  `WagerTransactionRejected` nunca carrega `referenceTransactionId`: em
  `REFERENCE_MISMATCH` ele seria o id interno de uma transação de outra carteira/jogador
  (ressalva da T06); nos demais códigos a referência externa do próprio provedor
  (`referenceExternalTransactionId`) basta. Rejeições `WALLET_NOT_FOUND`,
  `WALLET_PLAYER_MISMATCH` e `CURRENCY_MISMATCH` nunca levam `balanceAfter`, mesmo que a
  transação tenha um (defesa em profundidade sobre o ADR-009). `WagerTransactionProcessed`
  pode levar `referenceTransactionId`, porque em `PROCESSED` a referência é da mesma
  carteira.
- **Rejeições:** todo `REJECTED` gera `WagerTransactionRejected`, com `failureCode` e
  `failureCategory` (`INPUT` corrigível × `BUSINESS` definitivo). Erros de validação que
  impedem criar a transação (ex.: `INVALID_AMOUNT` no parsing) não geram evento.
- **OPENING:** abertura com saldo `> 0` gera `WagerTransactionProcessed` (`origin
  INTERNAL`; provedor, IDs externos, rodada, jogo e referências são omitidos do JSON) e
  `WalletBalanceChanged` (`CREDIT` 0 → saldo inicial, `walletVersion` 1), no mesmo commit
  da carteira (T14). Saldo zero não gera eventos. OPENING nunca fica pendente nem é
  rejeitado.
- **FAILED:** não gera evento de integração (não é rejeição de negócio); fica visível em
  métricas, logs e na consulta da transação.
- **PendingReference:** a política (T15/T21) é emitir na transição `PENDING` →
  `PENDING_REFERENCE` (`attempts = 1`); reagendamentos do worker não precisam emitir de
  novo. O domínio não impõe isso; `attempts` vai no payload.
- **Metadados:** `correlationId` obrigatório (request HTTP, envelope SQS ou contexto do
  worker); `causationId` opcional (ex.: `messageId` SQS). Ambos: não vazios, ≤ 255 bytes,
  sem espaço nas bordas, sem caracteres de controle e UTF-8 válido.

## ADR-013 — Pool pgx, TxManager e classificação de erros (T11)

Pacote `internal/adapters/postgres` (`pool.go`, `tx.go`, `errors.go`, `module.go`).

- **Pool (`*postgres.DB`):** `pgxpool` montado a partir de `config.DatabaseConfig`
  (`MaxConns`, `ConnectTimeout`, `application_name=wallet` se o DSN não definir outro). O DSN só
  sai do `config.Secret` por um único `Reveal()`, direto para `pgxpool.ParseConfig`. Erro de parse
  vira `ErrPoolConfig` sem o texto do pgx, porque `url.Error` cita a URL inteira. O construtor não
  faz I/O. `OnStart` (`DB.Start`) faz ping com backoff exponencial (100ms→2s) até o prazo do ctx de
  start menos a reserva de rollback (`SetStartReserve`, ADR-025), mas só repete falha transitória (conexão recusada, `57P03`, timeout). Falha permanente
  (`28P01`, `3D000`) aborta na hora. `OnStop` (`DB.Stop`) faz `Close`, limitado pelo ctx de stop.
  Como os dependentes param antes no Fx, as conexões já estão devolvidas. `DB.Ping(ctx)` é o
  probe de readiness (T18); quem chama define o prazo.
- **Fronteira transacional explícita:** o caso de uso chama
  `Transactor.WithinTx(ctx, fn func(ctx, pgx.Tx) error, opts...)` e passa o `pgx.Tx` a cada
  repositório. Os repositórios (T12/T13) recebem `postgres.Querier` (`Exec/Query/QueryRow`,
  satisfeito por `pgx.Tx`, `*pgx.Conn` e `*pgxpool.Pool`). Não há tx escondida no `context`, então
  dá para auditar que wallet, transação, ledger, outbox e inbox entram no mesmo commit.
- **Isolamento:** o padrão é `READ COMMITTED`. A consistência por carteira vem de
  `SELECT ... FOR UPDATE` + `UPDATE ... WHERE version = $old` (T12), não do nível de isolamento.
  `WithIsolation(pgx.RepeatableRead|Serializable)` e `ReadOnly()` existem para a reconciliação
  (T16) e para casos especiais.
- **Rollback:** acontece quando `fn` devolve erro, entra em panic ou chama `runtime.Goexit`. O
  `defer` não usa `recover`, então o panic original segue propagando. O ROLLBACK roda em
  `context.WithoutCancel(ctx)` com prazo de 5s, para funcionar mesmo com o ctx da requisição já
  cancelado. Uma falha de rollback só é logada: o pgx descarta a conexão e o servidor desfaz a
  transação. O erro de `fn` volta **sem tradução**, para que erros de domínio passem intactos.
- **Retry: mecanismo aqui, decisão no caso de uso.** Reexecutar exige refazer BEGIN..COMMIT
  inteiro, e só o TxManager sabe se o COMMIT chegou a ser enviado. Por isso o loop mora aqui. Ele
  é opt-in por chamada (`WithRetry(RetryPolicy)`), porque só o caso de uso (T14/T15/T21) sabe que
  `fn` é reexecutável: não pode ter efeito fora do banco e precisa recalcular tudo a partir do
  que lê no tx. Sem `WithRetry` há uma única tentativa. `DefaultRetryPolicy(op)` faz 4 execuções
  com backoff de 10ms, dobrando até 250ms, com equal jitter. Só se repete o que é
  `IsRetryable`: `40001`, `40P01`, `55P03`, `57014`, `08xxx`, `57P01-03` e `53300` vindos do
  servidor (inclusive no COMMIT, porque se o servidor respondeu, não confirmou), além de falha de
  conexão antes do COMMIT. `RetryPolicy.RetryIf` amplia isso. Uso previsto na T15: corrida no
  UNIQUE de idempotência, em que a nova tentativa lê a linha vencedora e devolve o replay. Cada
  nova tentativa incrementa `wallet_retries_total{operation}` e, nos conflitos,
  `wallet_concurrency_conflicts_total{kind}` (serialization/deadlock/lock_timeout/unique), e
  gera um log `warn` sem DSN.
- **Commit ambíguo:** quando o COMMIT foi enviado e a resposta não voltou (conexão caiu, ctx
  cancelado durante o commit), o erro sai com `ErrCommitOutcomeUnknown` e **nunca** é
  reexecutado, nem com `RetryIf`. Ele é `IsTransient` (HTTP 503/Retry-After; no SQS a mensagem
  não é removida), e a nova tentativa externa passa pela idempotência (replay), nunca por
  reaplicação cega. `pgx.ErrTxCommitRollback` (fn engoliu um erro SQL) é falha definitiva.
- **Classificação (sem importar `internal/domain`):**
  - `IsTransient`/`TransientReasonOf`: as falhas listadas acima, mais timeouts
    (`context.DeadlineExceeded`, `pgconn.Timeout`, `net.Error`), EOF, `ConnectError` e
    `pgconn.SafeToRetry`. `context.Canceled` **nunca** é transitório, porque quem chamou
    desistiu. Um `PgError` decide pelo SQLSTATE mesmo dentro de um `ConnectError`.
  - `IsRetryable` = transitório sem `ErrCommitOutcomeUnknown`.
  - Constraints (classe 23): `AsConstraintViolation(err)` devolve `*ConstraintViolation{Kind,
    Constraint, Table, Column, SQLState}`. Os helpers `IsUniqueViolation/IsCheckViolation/
    IsForeignKeyViolation/IsRestrictViolation(err, nome)` aceitam `""` como "qualquer". As guardas
    de trigger (`23001`, ADR-011) são identificadas pelo nome (`wallets_version_increment`,
    `wager_transactions_terminal_immutable`...). A mensagem não inclui o `Detail` do PostgreSQL,
    que traz valores da linha.
  - `Translate(err)` embrulha em `*ConstraintViolation` (que casa com `errors.Is` em
    `ErrConstraintViolation` e na sentinela do tipo: `ErrUniqueViolation`, `ErrCheckViolation`,
    `ErrForeignKeyViolation`, `ErrRestrictViolation`...) ou em `*TransientError`
    (`ErrTransient`), preservando a cadeia e o `*pgconn.PgError`. É idempotente. Todos os helpers
    funcionam também no erro cru. O mapeamento final para erros de domínio é feito nos
    repositórios (T12/T13), pelo nome da constraint.
- **Fx (`postgres.Module`):** provê `*DB` (com os hooks), `*pgxpool.Pool`, `*TxManager` e
  `Transactor`. Depende de `config.Config`; `*slog.Logger` e `*metrics.Metrics` são opcionais.
  Não registra coletor de estatísticas do pgxpool (fica para T23, se for necessário).
- **Testes:** os unitários usam `*pgconn.PgError` sintéticos, erros de contexto/rede e um `pgx.Tx`
  falso para commit, rollback (erro, panic, Goexit, ctx cancelado), commit ambíguo, retry, backoff
  e métricas. Os de integração (`-tags=integration`, mesmo setup da T10) cobrem commit, rollback
  por erro/panic, cancelamento e prazo durante um `pg_sleep`, classificação real (UNIQUE `23505`,
  CHECK `23514`, FK `23503`, trigger `23001` com nome), retry em `40001` (SERIALIZABLE) e em
  `40P01` sem lost update, start fail-fast em `3D000` sem vazar a senha e o ciclo start/stop do
  módulo Fx sem goroutines vazadas.

## ADR-014 — Repositórios de wallet, ledger e transação (T12)

Pacote `internal/adapters/postgres` (`repository.go`, `wallet_repository.go`,
`ledger_repository.go`, `wager_transaction_repository.go`) e migration
`0006_add_wallet_ledger_entries_seq`.

- **Forma:** SQL explícito com pgx, repositórios stateless (`WalletRepository`,
  `LedgerRepository`, `WagerTransactionRepository`, providos pelo `postgres.Module`). Cada
  método recebe o `Querier` da chamada. Dentro de `WithinTx` ele é o `pgx.Tx` do caso de
  uso, então carteira, transação, ledger (e depois inbox/outbox) entram no mesmo commit
  (ADR-013). Os métodos que travam linhas (`WalletRepository.GetForUpdate`,
  `WagerTransactionRepository.GetByIDForUpdate` e `GetClaimedForUpdate`, T21) recusam um
  `Querier` que não seja `pgx.Tx` (`ErrTxRequired`), porque `FOR UPDATE` em autocommit
  solta o lock na hora.
- **Money ↔ BIGINT + CHAR(3):** escrita por `MinorUnits()`/`Currency()` e leitura por
  `domain.FromMinorUnits`. Todo Money passa por `IsValid()` antes de ser gravado
  (`ErrInvalidAggregate` + `ErrUninitializedMoney`), porque os getters do zero-value não
  devolvem erro. `balance_after` usa a moeda da operação e é `NULL` quando ausente.
  Valores "vazios" do domínio (`""`, `uuid.Nil`, tempo zero) viram `NULL`.
- **Leitura sempre reidrata pelo domínio** (`RehydrateWallet`,
  `RehydrateWalletLedgerEntry`, `RehydrateWagerTransaction`), que revalida tudo: linha
  inconsistente falha alto, sem virar dado errado.
- **Concorrência por carteira (ADR-005):**
  - `GetForUpdate`: `SELECT ... FROM wallets WHERE id = $1 FOR UPDATE`. Trava só a linha
    da carteira; carteiras diferentes seguem em paralelo (teste com `lock_timeout`: a
    mesma carteira dá `55P03`, outra carteira trava na hora). Vinte débitos concorrentes
    terminam com saldo e versão exatos e um ledger encadeado.
  - `Update`: `UPDATE wallets SET balance, version, updated_at WHERE id = $1 AND
    version = $PersistedVersion`. 0 linhas ⇒ `ErrStaleWallet` (lost update detectado; nada
    gravado; o caso de uso relê sob lock e decide de novo). Teste: dois escritores leem a
    mesma versão sem lock; o segundo bloqueia no lock da linha, e após o commit do
    primeiro o `WHERE version` reavaliado (READ COMMITTED) não casa.
  - `Update` aceita exatamente **um** movimento por gravação (`Version() =
    PersistedVersion() + 1`), espelhando o trigger `wallets_guard_update`. Sem movimento
    (LOSS) não executa SQL. A instância não deve ser gravada duas vezes; para outro
    movimento, releia a carteira.
  - `Insert` só aceita carteira nova (`NewWallet`, versão 1, `PersistedVersion` 0).
- **Ledger:** `Append` só insere; não há método de alteração (o schema também bloqueia).
  `GetByTransaction` lê o lançamento de (carteira, transação).
- **Paginação keyset com cursor opaco:** a ordem é a coluna `seq BIGINT GENERATED ALWAYS
  AS IDENTITY` (migration 0006, índice único `(wallet_id, seq)`), não `(created_at, id)`.
  `created_at` e o UUIDv7 vêm do relógio de cada instância. Com clock skew, um lançamento
  confirmado depois de um cursor poderia ordenar antes dele e sumir da paginação. O
  `seq` é atribuído no INSERT, e todo lançamento de uma carteira é inserido com a linha
  dela travada até o commit. Logo, dentro da carteira, `seq` cresce na ordem de commit e
  lançamento novo sempre aparece depois do cursor. A consulta é `WHERE wallet_id = $1 AND
  seq > $cursor ORDER BY seq LIMIT n+1`, sem OFFSET. A linha extra decide se há
  `NextCursor`, então a última página não devolve cursor. O cursor é base64url de
  `[versão=1][walletId 16 bytes][seq 8 bytes]`. Ele fica preso à carteira (usado em
  outra, dá `ErrInvalidCursor`) e não é assinado: adulterar o `seq` só desloca a leitura
  dentro de uma carteira que o chamador já pode ler. Limites: `DefaultLedgerPageSize` =
  50 (sugestão para o HTTP) e `MaxLedgerPageSize` = 200; fora de `[1, 200]` dá
  `ErrInvalidPageLimit`. Teste: lançamentos com `created_at` uma ou duas horas no
  passado, inseridos entre leituras de página, aparecem no fim, sem duplicar nem pular,
  e a leitura paginada é igual à leitura completa.
- **Transações:**
  - Consultas por id, `(provider, externalTransactionId)`, `(provider,
    idempotencyKey)` e `ListByProviderReference(provider, referenceExternalId)` (as
    reversões existentes que o `DecideWager` recebe). Todas filtram pelo provedor, o que
    garante o isolamento entre provedores no nível do SQL.
  - `Insert` grava qualquer estado (PENDING, OPENING PROCESSED, rejeição já decidida).
  - `Update` grava só as colunas de resultado e agendamento, com `WHERE id = $1 AND
    status IN ('PENDING','PENDING_REFERENCE')`. Uma linha terminal devolve
    `ErrTransactionFinalized` e uma inexistente devolve `ErrNotFound`. O trigger
    `wager_transactions_guard_update` continua sendo a barreira final. Quem atualiza
    precisa ter lido sob lock: o lock da carteira serializa as operações dela, e o
    worker usa o lock da própria linha.
  - *(Substituído na T21, ADR-023.)* O antigo `ClaimDuePendingReferences` (lock da linha
    até o commit do lote) deu lugar a `ClaimDue` (lease persistido em
    `claimed_by`/`claimed_until`, `FOR NO KEY UPDATE SKIP LOCKED`) e
    `GetClaimedForUpdate` (fencing por token). `Update` também solta o lease.
- **Erros:** as violações viram sentinelas pelo nome da constraint, preservando a causa
  (`*ConstraintViolation`/`*pgconn.PgError` continuam na cadeia):
  `wallets_player_currency_key` → `ErrWalletAlreadyExists`; `*_pkey` →
  `ErrDuplicateKey`; `wager_transactions_provider_external_key` →
  `ErrDuplicateExternalTransaction`; `wager_transactions_provider_idempotency_key` →
  `ErrDuplicateIdempotencyKey`; `wager_transactions_opening_per_wallet_key` →
  `ErrOpeningAlreadyExists`; `wager_transactions_reversal_once_key` →
  `ErrReferenceAlreadyReversed`; `wager_transactions_terminal_immutable` →
  `ErrTransactionFinalized`; `wallet_ledger_entries_wallet_transaction_key` →
  `ErrLedgerEntryExists`. As demais passam por `Translate` (por exemplo, FK →
  `ErrForeignKeyViolation`). `pgx.ErrNoRows` vira `ErrNotFound`. As sentinelas ficam no
  adapter porque os casos de uso já dependem dele (`Transactor`/`pgx.Tx`, ADR-013); o
  mapeamento para respostas HTTP e SQS é feito na T15/T18.
- **Testes:**
  - Unitários: cursor (round-trip e rejeições), validação antes do SQL (Money
    zero-value, agregado não inicializado, um movimento por `Update`, `ErrTxRequired`,
    limites) e mapeamento por nome de constraint.
  - Integração (`-tags=integration`, banco novo por teste): CRUD de carteira e de
    transação com round-trip exato, conflitos de unicidade, lock por carteira, lost
    update, ledger append/duplicata/FK/atomicidade, cursor estável com clock skew e claim
    `SKIP LOCKED` entre duas instâncias.

## ADR-015 — Repositórios de inbox e outbox (T13)

Pacote `internal/adapters/postgres` (`inbox_repository.go`, `outbox_repository.go`,
`backoff.go`). Sem migration nova: a `outbox` já tem `seq BIGINT GENERATED ALWAYS AS
IDENTITY` (0005).

- **Inbox (`InboxRepository`):**
  - `Register(consumer, messageId, requestHash, now)` e `Complete(consumer, messageId,
    requestHash, transactionId, now)` exigem `pgx.Tx` (`ErrTxRequired`). A inbox só evita
    reprocessamento se registro, conclusão e efeitos de domínio entrarem no mesmo commit
    (T15/T20).
  - `Register` faz `INSERT ... ON CONFLICT (consumer_name, message_id) DO NOTHING
    RETURNING`. Se a chave já existe, lê a linha com `FOR UPDATE`. Uma entrega concorrente
    da mesma mensagem espera no índice único até o commit de quem registrou e depois vê o
    estado final. Teste: 10 entregas paralelas resultam em 1 `NEW` e 9 `COMPLETED`.
  - **Comparação de hash:** hash diferente para a mesma `(consumer, messageId)` devolve
    `ErrInboxHashMismatch` junto com a linha registrada, que fica intacta. Hash igual devolve
    `InboxCompleted` (com o `transactionId` do resultado; não reprocessar) ou
    `InboxInProgress`. `Complete` também confere o hash. Ele devolve
    `ErrInboxAlreadyCompleted`, `ErrNotFound` ou `ErrForeignKeyViolation` quando a transação
    não existe.
  - `completed_at = GREATEST(now, received_at)`, para que o relógio de outra instância um
    pouco atrasado não viole o CHECK `inbox_completed_after_received`.
  - Validação antes do SQL (`ErrInvalidInboxMessage`) espelha os CHECKs da 0004: 1..255
    bytes e hash `^[0-9a-f]{64}$`.
- **Outbox (`OutboxRepository`):**
  - `Enqueue(q, events...)` grava `IntegrationEvent.Payload()` byte a byte (coluna `JSON`) na
    ordem recebida, com o `Querier` do caso de uso. Um `eventId` repetido devolve
    `ErrDuplicateEvent` (`outbox_event_id_key`).
  - **Claim com lease, não com lock até o commit:** `Claim(owner, lease, limit)` aceita
    qualquer `Querier`. O publisher (T22) reivindica numa transação curta ou em autocommit,
    publica no SQS **fora** de transação e depois marca o resultado. Não há transação aberta
    durante I/O externo. A consulta é uma CTE `SELECT seq ... WHERE published_at IS NULL
    AND next_attempt_at <= agora AND (locked_until IS NULL OR locked_until <= agora) ORDER
    BY seq LIMIT n FOR UPDATE SKIP LOCKED`, seguida de `UPDATE ... SET locked_by,
    locked_until = agora + lease, attempts = attempts + 1 RETURNING`.
  - Claimers concorrentes não esperam uns pelos outros e não duplicam. Linhas travadas por
    um claim em andamento são puladas. Uma linha cujo lease foi confirmado entre o snapshot
    e o lock é reavaliada pelo PostgreSQL (READ COMMITTED) e sai do lote. O lease expirado é
    reassumido.
  - **Ordem por `seq`** (IDENTITY, ordem de inserção), nunca por `occurred_at`, que vem do
    relógio de cada instância. O resultado volta ordenado por `seq`, porque `RETURNING` não
    garante ordem.
  - **Relógio único:** leases e `next_attempt_at` usam `statement_timestamp()` do
    PostgreSQL. Instâncias com relógios divergentes não roubam nem estendem leases umas das
    outras.
  - **Fencing:** cada claim incrementa `attempts`. O lease (`OutboxLease`) é
    `(seq, eventId, owner, attempt)` e só vale com `locked_until > agora`.
    `MarkPublished` e `MarkFailed` filtram por todos esses campos e por `published_at IS
    NULL`. Sem efeito, devolvem `ErrOutboxLeaseLost` e não alteram nada. Isso cobre o
    claimer com lease expirado (reassumido ou não), o worker do mesmo `owner` com token
    antigo e a marcação repetida.
  - Um lease expirado e não reassumido também é recusado, o que pode causar uma republicação
    com o mesmo `eventId`. Essa republicação é absorvida pela deduplicação FIFO
    (`MessageDeduplicationId = eventId`) e pelos consumidores idempotentes.
  - Como `attempts` conta claims, um crash entre o claim e a marcação também entra no
    backoff.
  - **Backoff (`OutboxBackoff{Base, Max}`):** `MarkFailed` grava `last_error` (até 2048
    bytes, cortado sem partir UTF-8, sem NUL) e `next_attempt_at = agora +
    min(Base·2^(attempt-1), Max)`, e solta o lease. O atraso é determinístico (sem jitter),
    porque é por evento e não há manada a dispersar. `Max` é obrigatório: `Validate` exige
    `0 < Base <= Max`, senão devolve `ErrInvalidBackoff`. O padrão é 1s→5min. Teste:
    1/2/4/8/10/10s medidos contra o relógio do banco, e `attempts = 2·10⁹` resulta
    exatamente no teto.
  - Limites: lote de 1 a 1000 (`MaxOutboxClaimBatch`), lease de 1ms a 1h e owner de 1 a
    255 bytes (`ErrInvalidLease`).
- **Backoff sem overflow (pendência da T11):** `exponentialDelay(base, max, attempt)` é
  compartilhado por `RetryPolicy.backoff` e `OutboxBackoff`. Ele para de dobrar no teto
  ou quando dobrar passaria de `MaxInt64`, e nesse caso satura em `MaxInt64`. Por isso
  não há overflow nem panic em `rand.Int64N` com `MaxDelay <= 0`, e o laço roda no
  máximo cerca de 63 vezes. O comentário de `errors.go` foi corrigido: `WithinTx` não
  chama `Translate`. Ele devolve o erro de `fn` intacto e embrulha os de BEGIN/COMMIT só
  com `%w`. A forma tipada vem dos repositórios ou de quem chama `Translate`.
- **Limitação para a T22 (resolvida no ADR-024):** a ordem global por `seq` não impedia que
  eventos posteriores do mesmo `MessageGroupId` fossem publicados enquanto um anterior
  estava em backoff. A T22 acrescentou ao claim o bloqueio head-of-line por agregado. Não
  foi preciso serializar os claims.
- **Testes:**
  - Unitários: `exponentialDelay` (teto, saturação, attempt enorme ou negativo),
    `RetryPolicy` sem `MaxDelay` nas tentativas 41, 64 e `MaxInt`, `OutboxBackoff`,
    truncamento, validações antes do SQL e `ErrTxRequired`.
  - Integração: inbox (rollback, IN_PROGRESS, COMPLETED, hash divergente, FK, relógio
    atrasado, corrida de 10 entregas), outbox (ordem por `seq` com `occurred_at`
    decrescente, payload idêntico byte a byte, duplicata, 8 claimers concorrentes
    drenando 300 eventos sem duplicar, SKIP LOCKED com claim não confirmado, lease
    expirado reassumido com `attempt=2`, fencing de owner antigo e de mesmo owner com
    token antigo, lease expirado não reassumido e backoff com teto).

## ADR-016 — Portas da aplicação, UnitOfWork e caso de uso OpenWallet (T14)

Pacotes `internal/app` (`ports.go`, `errors.go`, `open_wallet.go`, `module.go`) e
`internal/adapters/postgres/unit_of_work.go`.

- **Portas no lado da aplicação, sem pgx.** `app.UnitOfWork.WithinTx(ctx, TxOptions, fn
  func(ctx, app.Tx) error)` abre uma transação e entrega um `app.Tx` com os stores
  `Wallets()`, `Transactions()`, `Ledger()` e `Outbox()`, todos ligados ao **mesmo**
  `pgx.Tx`. A fronteira transacional continua explícita e auditável, como no ADR-013: o
  caso de uso só grava pelos stores do `Tx` recebido, e não há transação escondida no
  `context`. Cada store expõe apenas o que algum caso de uso usa. T15, T16 e T21 acrescentam
  métodos (`GetForUpdate`, consultas, inbox) conforme precisarem.
- **Adapter (`postgres.UnitOfWork`).** Implementa `app.UnitOfWork` sobre `Transactor` e os
  repositórios do ADR-014/015. `TxOptions.Retry` liga `WithRetry(DefaultRetryPolicy(
  Operation))`. `Operation` usa os mesmos rótulos de `metrics.Operation*`, e um teste no
  adapter garante essa igualdade, porque `app` não importa `platform`. O
  `postgres.Module` provê `*UnitOfWork` como `app.UnitOfWork`, e o `app.Module` provê
  `*OpenWallet` a partir dela, com `*slog.Logger` opcional. A composição em `cmd/server`
  fica para a T23.
- **Tradução de erros na borda do adapter (substitui a nota do ADR-014).** O caso de uso
  não conhece as sentinelas do adapter. `WalletStore.Insert` embrulha
  `postgres.ErrWalletAlreadyExists` (constraint `wallets_player_currency_key`) com
  `app.ErrWalletAlreadyExists`. Colisão de PK não é tratada como conflito de negócio.
  Falhas transitórias (`IsTransient`, inclusive `ErrCommitOutcomeUnknown`) saem de
  `WithinTx` embrulhadas com `app.ErrUnavailable`. Em todos os casos a causa original
  continua na cadeia de erros.
- **Erros tipados da aplicação.** `*app.Error{Kind, Code, Op, Err}`. `Kind` casa por
  `errors.Is` com `ErrInvalidInput` (400), `ErrConflict` (409), `ErrNotFound` (404) e
  `ErrUnavailable` (503). `Code` é estável (`WALLET_ALREADY_EXISTS`,
  `INVALID_PLAYER_ID`, `INVALID_INITIAL_BALANCE`, `INVALID_CORRELATION_ID`,
  `TEMPORARILY_UNAVAILABLE`). Erros sem classe (falha permanente, bug) não viram
  `*app.Error` e devem ser tratados como erro interno. O mapeamento HTTP é feito na T18.
- **OpenWallet.**
  - Tudo o que é puro é montado **antes** da transação e validado pelo domínio: ids
    UUIDv7, `NewWallet` (versão 1), `NewOpeningWagerTransaction` (PROCESSED, INTERNAL),
    o lançamento CREDIT 0 → saldo e os eventos `WagerTransactionProcessed` (agregado =
    OPENING) e `WalletBalanceChanged` (agregado = carteira, `walletVersion` 1, construído
    com a mesma instância de `Wallet`, ADR-012).
  - O corpo da transação só grava, nesta ordem: carteira, OPENING, lançamento e outbox
    (Processed e depois BalanceChanged, ordem de `seq`). A carteira é inserida antes do
    `Ledger.Append`. Até o commit, essa linha é invisível e exclusiva da transação, o que
    equivale a tê-la travada. Assim fica cumprido o contrato do ledger (`seq` em ordem de
    commit, ADR-014).
  - O corpo não tem efeitos externos e grava objetos já prontos. Por isso é reexecutável,
    e o caso de uso liga `Retry` (`open_wallet`). Um retry reaproveita os mesmos ids e
    eventos, o que é seguro porque a tentativa anterior foi desfeita.
  - Saldo zero grava só a carteira, sem OPENING, ledger nem eventos (ADR-012).
  - Quem decide o `(playerId, currency)` duplicado é o UNIQUE do banco, inclusive em
    corrida entre instâncias. O perdedor espera no índice até o commit do vencedor e recebe
    `*app.Error` `KindConflict`/`WALLET_ALREADY_EXISTS`, sem efeito colateral (rollback).
  - Entradas inválidas (`playerId` nulo, `Money` não inicializado ou negativo,
    `correlationId` ausente ou malformado) viram `KindInvalidInput` antes de abrir
    transação. Desde a T15 (ADR-017), o formato de `correlationId` e `causationId` é
    validado sempre, mesmo com saldo zero, por `domain.EventMetadata.Validate`. O
    `causationId` inválido tem código próprio (`INVALID_CAUSATION_ID`). A borda (T18)
    sempre gera um id válido.
  - **Limitação:** o contrato `POST /wallets` não tem chave de idempotência. Se o commit
    tiver resultado desconhecido (503) e o cliente repetir a chamada, ele recebe 409 e deve
    consultar a carteira existente. Não há risco financeiro, porque o UNIQUE impede uma
    segunda carteira e o índice `wager_transactions_opening_per_wallet_key` impede um
    segundo crédito inicial.
- **Testes:**
  - Unitários (`internal/app`, fakes das portas): ordem das escritas (o fake de ledger
    recusa `Append` sem a carteira inserida na mesma tx), eventos e metadados, saldo zero,
    conflito, indisponibilidade, entradas inválidas sem abrir tx, ctx cancelado, falha do
    gerador de ids, corpo reexecutável e `fx.ValidateApp` do `app.Module`.
  - Adapter: tradução de erros, opção de retry e igualdade dos rótulos de operação.
  - Integração (`-tags=integration`, banco novo por teste, mesmo harness do adapter):
    - saldo positivo: carteira v1, OPENING PROCESSED sem metadados externos, um CREDIT e
      dois eventos com payload conferido. O mesmo `xmin` em todas as linhas prova o
      mesmo commit;
    - saldo zero: só a carteira;
    - conflito: sem efeitos colaterais, e outra moeda funciona;
    - 10 aberturas concorrentes: uma vence e nove recebem conflito;
    - rollback total com falha injetada depois de cada passo (carteira, OPENING, ledger,
      outbox) e com falha real do PostgreSQL (trigger) no INSERT do 2º evento da outbox;
    - composição Fx `postgres.Module` + `app.Module` com start/stop.

## ADR-017 — Caso de uso ProcessWagerTransaction (T15)

Pacotes `internal/app` (`process_wager.go`, `metadata.go`, portas em `ports.go`, erros em
`errors.go`) e `internal/adapters/postgres/unit_of_work.go`. O mesmo caso de uso atende
HTTP (T18) e SQS (T20), com as mesmas garantias de idempotência.

- **Uma transação SQL por operação, sem aceite intermediário.** A operação é gravada já no
  estado final (`PROCESSED`, `REJECTED` ou `PENDING_REFERENCE`), junto com carteira,
  ledger, outbox e inbox. O fluxo síncrono não deixa `PENDING` confirmado. A retomada de
  `PENDING` órfão (T21) só se aplica a um eventual aceite assíncrono futuro.
- **Ordem dentro da transação:**
  1. `Inbox.Register` (só com `cmd.Inbox`, entrada SQS). Hash divergente → `409
     MESSAGE_CONFLICT`. `COMPLETED` → replay do resultado gravado (lido por
     `inbox.transaction_id`), sem tocar na carteira.
  2. Leitura **sem lock** por `(provider, idempotencyKey)`. Se encontrar → replay ou
     conflito. É o caminho rápido: replays não esperam o lock da carteira.
  3. `Wallets.GetForUpdate`: lock pessimista por carteira (ADR-005). Carteira inexistente
     segue sem lock e o domínio decide `WALLET_NOT_FOUND`.
  4. Nova leitura por `(provider, key)` e por `(provider, externalId)`. Em `READ
     COMMITTED`, cada comando vê os commits anteriores, então, depois do lock, a duplicata
     confirmada por quem segurava o lock fica visível.
  5. Referência por `(provider, referenceExternalTransactionId)` e reversões existentes
     (`ListByProviderReference`, filtrada para `REFUND`/`ROLLBACK`, porque um `WIN` também
     cita a `BET`). Em seguida, `DecideWager` + `ApplyWagerDecision` (ADR-009). Uma
     referência válida está na mesma carteira, então o mesmo lock serializa as reversões
     dela.
  6. Escritas: `Wallets.Update` (só com movimento, condicional à versão),
     `Transactions.Insert` (antes do ledger, por causa da FK), `Ledger.Append` (carteira
     travada, contrato do ADR-014), `Outbox.Enqueue` e `Inbox.Complete`.
- **Idempotência e conflitos** (`replayOrConflict`, aplicado à linha encontrada por chave,
  externalId ou inbox):
  - Outra chave para a mesma `(provider, externalId)` → `409
    EXTERNAL_TRANSACTION_CONFLICT`, mesmo com conteúdo idêntico (README §9: não reaplicar
    com outra chave).
  - Mesma chave com outro hash → `409 IDEMPOTENCY_KEY_CONFLICT`.
  - Mesma chave com o mesmo hash → replay: `idempotentReplay=true` e o resultado
    persistido. O `balance` é o `balance_after` gravado no processamento original, não o
    saldo atual. O mesmo vale para rejeições (`failureCode` + saldo observado) e para
    `PENDING_REFERENCE` (estado atual, sem saldo).
  - Chaves são escopadas pelo provedor: a mesma key de outro provedor é outra operação.
    Um provedor nunca recebe replay de outro (isolamento no SQL, ADR-014).
  - **Corrida sem lock comum** (carteira inexistente): os `UNIQUE` decidem. O perdedor
    recebe `app.ErrDuplicateWagerTransaction`, que o adapter mapeia de
    `provider_external_key`, `provider_idempotency_key` e `reversal_once_key`.
    `TxOptions.RetryIf` repete o corpo; a nova tentativa lê a linha vencedora e devolve o
    replay.
  - Bug encontrado pelos testes e corrigido: o vencedor pode confirmar **entre** a leitura
    por chave e a leitura por externalId. Por isso o acerto por externalId também passa
    por `replayOrConflict`, que compara a chave em vez de presumir "outra chave".
- **Corpo reexecutável e retry limitado.**
  - Tudo é lido do tx e nada sai do banco. A cada tentativa, a transação nova é clonada do
    modelo `PENDING` que foi validado antes da transação (`plan`).
  - `Retry: true` aplica `DefaultRetryPolicy` (4 execuções) às falhas transitórias, mais o
    `RetryIf` de duplicata.
  - Commit de resultado desconhecido nunca é repetido: vira 503, e a nova chamada externa
    cai no replay (ADR-013).
  - Uma duplicata que persiste após as tentativas também vira `KindUnavailable`
    (`TEMPORARILY_UNAVAILABLE`). Quando o chamador repetir, recebe o replay.
- **Eventos do estado final** (em ordem de `seq`):
  - `PROCESSED` → `WagerTransactionProcessed`, mais `WalletBalanceChanged` se houver
    lançamento.
  - `LOSS` não gera `WalletBalanceChanged`, ledger nem versão nova, e `Wallets.Update` não
    executa SQL.
  - `REJECTED` → `WagerTransactionRejected`.
  - `PENDING_REFERENCE` → `WagerTransactionPendingReference`, só nesta transição
    (`attempts = 1`, ADR-012).
  - O `causationId` do comando (ex.: `messageId` SQS) vai para os eventos.
- **Referência ausente ou pendente:** `PENDING_REFERENCE` com `attempts = 1` e
  `next_attempt_at = now + WithReferenceRetryDelay` (padrão `DefaultReferenceRetryDelay`
  = 2s). Backoff seguinte, limite/TTL e resolução ficam com a T21. O replay de uma operação
  pendente devolve `PENDING_REFERENCE` sem reavaliar: só o worker transiciona a linha.
- **Resultado (`WagerResult`):** `transactionId`, `walletId`, `kind`, `status`, `balance`,
  `failureCode`, `referenceTransactionId` e `idempotentReplay`.
  - `referenceTransactionId` só aparece em `PROCESSED`, porque numa rejeição poderia ser de
    outra carteira (pendência T18/T20).
  - `balance` é omitido em `WALLET_NOT_FOUND`, `WALLET_PLAYER_MISMATCH` e
    `CURRENCY_MISMATCH`, mesma regra do evento (ADR-012).
- **Inbox:**
  - O hash registrado é o hash canônico da operação (T08), calculado pelo caso de uso. A
    borda informa só `consumerName`/`messageId` (1..255 bytes; senão `INVALID_MESSAGE`).
  - Mesmo `messageId` com outro conteúdo → `MESSAGE_CONFLICT`. Mesmo `messageId` e mesmo
    conteúdo com outra chave → `EXTERNAL_TRANSACTION_CONFLICT`.
  - Mensagem nova para uma operação já processada (por HTTP ou por outra mensagem) é
    replay; a mensagem é concluída apontando para a transação original.
  - Em `PENDING_REFERENCE`, a mensagem é concluída no mesmo commit da pendência (README
    §6.5); a continuidade é do worker.
  - Conflitos e entradas inválidas fazem rollback e não deixam linha na inbox. Remover a
    mensagem ou mandá-la à DLQ é decisão da T20.
- **Entradas inválidas** (antes de abrir a transação, `KindInvalidInput`): nada é
  persistido e nenhum evento é gerado (ADR-012). O `Code` é o `FailureCode` do domínio:
  `INVALID_KIND` (inclusive `OPENING`), `INVALID_AMOUNT`, `REFERENCE_REQUIRED`,
  `REFERENCE_NOT_ALLOWED` e `INVALID_REQUEST` (identificadores ou UTF-8 inválidos). Há
  ainda `INVALID_CORRELATION_ID`, `INVALID_CAUSATION_ID` e `INVALID_MESSAGE`.
- **Pendências resolvidas:**
  - Contrato de lock do ledger e do Update (T14/T15/T21): `Ledger.Append` e
    `Wallets.Update` só são chamados após `Wallets.GetForUpdate` na mesma transação, e a
    transação nova entra por `Transactions.Insert`. O caso de uso não chama
    `Transactions.Update`; a porta existe para a T21, com o contrato documentado (linha
    lida sob lock).
  - Validação de `correlationId`/`causationId`: `app.validateEventMetadata`, sobre o novo
    método exportado `domain.EventMetadata.Validate`, roda sempre antes da transação,
    inclusive em `OpenWallet` com saldo zero. `causationId` tem código próprio
    (`INVALID_CAUSATION_ID`).
- **Portas acrescentadas** (ADR-016):
  - `WalletStore.GetForUpdate/Update`.
  - `WagerTransactionStore.Update/GetByID/GetByProviderIdempotencyKey/
    GetByProviderExternalID/ListByProviderReference`.
  - `InboxStore.Register/Complete` (`Tx.Inbox()`) e `TxOptions.RetryIf`.
  - Sentinelas: `ErrRecordNotFound` (casa com `ErrNotFound`), `ErrDuplicateWagerTransaction`
    e `ErrInboxHashMismatch` (casam com `ErrConflict`). O adapter traduz preservando a
    causa.
  - `NewUnitOfWork` recebe o `*InboxRepository`, e `app.Module` provê
    `*ProcessWagerTransaction`. O rótulo de operação é `process_transaction` (igual a
    `metrics.OperationProcessTransaction`, verificado por teste).
- **Fora do escopo:** métricas por resultado e de duplicatas (`app` não importa `platform`;
  a borda T18/T20 as registra a partir de `WagerResult`), mapeamento HTTP/SQS dos códigos
  e worker de referências.
- **Testes unitários** (`process_wager_test.go`, UnitOfWork em memória com rollback e
  retry): BET, replay com saldo original, conflitos, 14 entradas inválidas sem abrir
  transação, retry por duplicata, classificação de indisponibilidade, `PENDING_REFERENCE`
  com `next_attempt_at`, rejeição sem saldo, inbox (conclusão, reentrega, hash divergente,
  outra chave, mensagem nova para operação já feita) e `fx.ValidateApp`.
- **Testes de integração** (`process_wager_integration_test.go`, PostgreSQL real):
  - Dez passos com BET, WIN com e sem referência, LOSS, REFUND e ROLLBACK de BET, WIN e
    REFUND. Conferem saldo, versão, direção do lançamento, eventos e reconciliação (saldo =
    soma do ledger, encadeamento before/after), além do mesmo `xmin` em carteira,
    transação, ledger e outbox.
  - LOSS sem ledger, sem versão e sem alterar `updated_at`.
  - Replay após outras movimentações, inclusive de uma rejeição.
  - Conflitos sem efeito e isolamento entre provedores.
  - 50 duplicatas concorrentes, com carteira existente e inexistente (exatamente um
    efeito); 10 keys diferentes para a mesma operação (um efeito, nove conflitos).
  - Rejeições persistidas, cada uma com evento e sem `referenceTransactionId`:
    `INSUFFICIENT_FUNDS`, `REFERENCE_MISMATCH`, `REVERSAL_INSUFFICIENT_FUNDS`,
    `ALREADY_REVERSED`, `AMOUNT_MISMATCH`, `REFERENCE_NOT_REVERSIBLE`,
    `WALLET_PLAYER_MISMATCH` e `WALLET_NOT_FOUND`.
  - ROLLBACK antes da referência.
  - 80+80 sobre 100 e 30 débitos concorrentes de 7.00 (nunca negativo, sem lost update).
  - Rollback total com falha injetada após cada escrita, inclusive `Inbox.Complete`.
  - 10 entregas concorrentes da mesma mensagem, com a inbox no mesmo commit (`xmin`).
  - Composição Fx.

## ADR-018 — Reconciliação e consultas (T16)

Pacotes `internal/app` (`reconcile.go`, `queries.go`, portas em `ports.go`, erros em
`errors.go`, `module.go`) e `internal/adapters/postgres` (`unit_of_work.go`,
`ledger_repository.go`). Sem migration nova.

- **Isolamento e modo de acesso pelas portas.** `app.TxOptions` ganhou `Isolation`
  (`IsolationReadCommitted` = valor zero, `IsolationRepeatableRead`,
  `IsolationSerializable`) e `ReadOnly`. O adapter traduz para `WithIsolation`/`ReadOnly()`
  do TxManager (ADR-013), ou seja, `BEGIN ISOLATION LEVEL ... READ ONLY`. Um nível
  desconhecido é recusado antes do BEGIN, nunca cai silenciosamente no padrão.
- **Reconciliação (`ReconcileWallet`).**
  - Uma transação **REPEATABLE READ + READ ONLY**, sem lock. O snapshot é fixado no
    primeiro comando (`Wallets.Get`), então o saldo gravado e a soma do ledger
    (`Ledger.Summarize`) vêm do mesmo estado confirmado, mesmo com operações
    confirmando no meio. A reconciliação não bloqueia escritores nem é bloqueada por eles.
  - `READ ONLY` faz o PostgreSQL recusar qualquer escrita (`25006`): a reconciliação
    não pode alterar saldo nem nenhum outro dado, nem por engano. O corpo só lê, então
    usa o retry limitado de falhas transitórias.
  - `calculated` = Σ créditos − Σ débitos do ledger (inclusive a abertura), somado no
    banco em `NUMERIC` e devolvido como texto. Um total fora de `int64` dá
    `ErrLedgerSumOutOfRange` (erro interno), nunca é truncado. A moeda é a da
    carteira, garantida pelas FKs compostas do ledger (ADR-011).
    `difference = stored − calculated` (pode ser negativa); `consistent = difference == 0`;
    `checkedEntries` = lançamentos somados; o relatório traz também `walletVersion`.
    Optamos pela soma em vez de percorrer a cadeia `balance_before/after`: a cadeia já é
    garantida pelo CHECK aritmético + lock por carteira, e a soma é uma única consulta
    agregada, independente da ordem.
  - **Divergência não é erro.** O relatório volta com `consistent=false`, um log `WARN`
    "divergência de reconciliação" (walletId, currency, storedBalance, calculatedBalance,
    difference em string exata, checkedEntries, walletVersion, correlationId) e a
    métrica `wallet_reconciliation_divergences_total{currency}`. Toda reconciliação
    concluída conta em `wallet_reconciliations_total{outcome=consistent|divergent}`.
    Nada é corrigido automaticamente: correção exige lançamento compensatório
    auditável, fora do escopo.
  - A métrica entra por uma porta (`app.ReconciliationRecorder`, satisfeita por
    `*metrics.Metrics.Reconciliation`), porque `app` não importa `platform`. No Fx ela é
    opcional; a composição (T23) deve prover `fx.Provide(func(m *metrics.Metrics)
    app.ReconciliationRecorder { return m })` (fiação coberta por teste de integração).
  - Carteira inexistente → `404 WALLET_NOT_FOUND`; `walletId` nulo → `INVALID_WALLET_ID`.
- **Consultas (`Queries`).**
  - Todas rodam numa transação **READ ONLY** (READ COMMITTED), sem lock e com retry
    (rótulo de operação `query`, acrescentado a `metrics.Operation*`).
  - `GetWallet(walletId)` → `WalletSnapshot` ou `WALLET_NOT_FOUND`.
  - `ListLedger{walletId, cursor, limit}`: verifica a carteira e lê a página na mesma
    transação (carteira inexistente → `WALLET_NOT_FOUND`, não página vazia). Usa a
    paginação keyset por `seq` com cursor opaco do ADR-014. `limit` 0 → 50; fora de
    `[1, 200]` → `INVALID_PAGE_LIMIT`. Cursor malformado ou de outra carteira →
    `INVALID_CURSOR` (o adapter traduz `ErrInvalidCursor` para `app.ErrInvalidCursor`,
    que casa com `ErrInvalidInput`).
  - `GetTransaction{transactionId, providerId}` e
    `GetTransactionByProvider{providerId, externalTransactionId}` recebem o
    **providerId explícito**. A borda (T18) o preenche com a identidade autenticada. Na
    rota `/providers/{providerId}/...`, o path precisa casar com a identidade antes de
    chamar. A transação de outro provedor (ou interna, como OPENING) responde exatamente
    como inexistente (`TRANSACTION_NOT_FOUND`), sem revelar a existência. Por
    externalId, o filtro está no SQL. Por id, a linha é descartada no caso de uso se o
    provedor não casar.
  - Identificadores externos inválidos (vazio, > 255 bytes, UTF-8 inválido, caracteres
    de controle) → `INVALID_PROVIDER_ID`/`INVALID_EXTERNAL_TRANSACTION_ID` sem abrir
    transação.
  - `TransactionView` aplica as regras de exposição do ADR-017: `referenceTransactionId`
    só em `PROCESSED`; saldo (`balance_after`) omitido em `WALLET_NOT_FOUND`,
    `WALLET_PLAYER_MISMATCH` e `CURRENCY_MISMATCH`; a chave de idempotência e o hash
    não são expostos. Inclui `attempts`/`nextAttemptAt` para acompanhar
    `PENDING_REFERENCE`.
  - A autorização de carteira (só serviço interno) é da borda (T17/T18): `GetWallet`,
    `ListLedger` e a reconciliação não recebem identidade.
- **Portas acrescentadas:** `WalletStore.Get` (sem lock), `LedgerStore.List` e
  `LedgerStore.Summarize` (`app.LedgerPage`, `app.LedgerSummary`), `TxOptions.Isolation`
  e `TxOptions.ReadOnly`, `ErrInvalidCursor`, os códigos `WALLET_NOT_FOUND`,
  `TRANSACTION_NOT_FOUND`, `INVALID_WALLET_ID`, `INVALID_TRANSACTION_ID`,
  `INVALID_PROVIDER_ID`, `INVALID_EXTERNAL_TRANSACTION_ID`, `INVALID_CURSOR` e
  `INVALID_PAGE_LIMIT`, e `app.Module` provendo `*ReconcileWallet` e `*Queries`.
- **Testes:**
  - Unitários (`queries_test.go`, memUoW): relatório consistente e divergente (diferença
    positiva e negativa), recorder e log JSON, estado inalterado, opções da transação
    (RR + READ ONLY + retry), erros (inválido sem tx, 404, 503, interno, ctx
    cancelado), consultas (encontrado, inexistente, outro provedor, OPENING invisível,
    entradas inválidas sem tx, paginação, limites, cursor), visão sem dados alheios e
    `fx.ValidateApp`. No adapter: mapeamento de `TxOptions` (inclusive nível
    desconhecido), `ErrInvalidCursor`, parse e overflow do `Summarize` e igualdade dos
    rótulos de operação.
  - Integração (PostgreSQL real):
    - consistente: 985.00 / 3 lançamentos, métrica `consistent`;
    - divergente: saldo alterado direto no banco com os triggers desligados só na sessão
      do teste (`SET LOCAL session_replication_role = replica`), relatório 980.50 ×
      975.00 = 5.50, métrica e log;
    - o estado do banco é comparado antes e depois por uma impressão digital de todas as
      tabelas (conteúdo + `xmin`), que prova que nada foi escrito;
    - escrita dentro da transação da reconciliação recusada com `25006`;
    - snapshot: uma BET confirmada entre a leitura da carteira e a soma não aparece, e o
      controle negativo forçado para READ COMMITTED mostra a falsa divergência que o
      snapshot evita;
    - `current_setting('transaction_isolation'/'transaction_read_only')` pela própria
      UnitOfWork;
    - consultas: paginação com commit concorrente, cursor de outra carteira, isolamento
      por provedor com o mesmo externalId, pendência e OPENING invisível;
    - composição Fx com a métrica.

## ADR-019 — Autenticação OIDC (Keycloak) e autorização (T17)

Pacotes `internal/adapters/auth` (validação, middleware, autorização, módulo Fx) e
`internal/adapters/http/apierror` (corpo de erro padronizado, compartilhado com T18).
Config em `internal/platform/config` (variáveis `OIDC_*`).

- **IdP e fluxo.** Keycloak como IdP externo OAuth 2.0/OIDC; os serviços chamadores
  (provedores e o serviço interno) obtêm tokens por `client_credentials` (cada um é um
  client confidencial com service account). A aplicação só **valida** tokens: não emite
  tokens nem guarda credenciais. Validação local por JWT assinado + JWKS (sem
  introspecção por requisição): latência baixa e o IdP não vira dependência síncrona de
  cada chamada. Custo aceito: revogação só vale quando o token expira (tokens de acesso
  curtos no realm, T24).
- **Bibliotecas.** `go-oidc` (`oidc.NewVerifier`) faz parse com allowlist de algoritmos,
  assinatura primeiro e depois `iss` (igualdade exata) e `aud` (precisa conter
  `OIDC_AUDIENCE`). O `KeySet` é próprio (`jwks.go`, sobre `go-jose/v4`, agora dependência
  direta; `go-oidc` saiu de `tools.go`), porque o `RemoteKeySet` do go-oidc não distingue
  "IdP fora do ar" de "assinatura inválida" (o verificador embrulha o erro com `%v`). O
  resultado do KeySet volta ao Authenticator por um estado no `context` da chamada.
- **Regras de validação** (tudo → 401, exceto IdP indisponível → 503):
  - algoritmos permitidos fixos e só assimétricos (RS/PS/ES 256-512, EdDSA); `none` e
    `HS*` são recusados no parse (proteção contra confusão de algoritmo); a chave do JWKS
    precisa casar em `kid` e, se declarar, em `alg`;
  - chaves do JWKS: só públicas, `use` vazio ou `sig`; chaves `oct`/privadas/`enc`/de tipo
    desconhecido são descartadas individualmente (o JWKS do Keycloak traz uma chave
    RSA-OAEP de cifragem);
  - `exp` obrigatório; `exp`, `nbf` e `iat` com tolerância `OIDC_CLOCK_SKEW` (default
    30s, máx. 2m). O verificador do go-oidc roda com `SkipExpiryCheck`, porque não tem
    tolerância em `exp` e usa 5 min fixos em `nbf`;
  - `typ`, se presente, precisa ser `Bearer` (o Keycloak marca `ID`/`Refresh` nos outros
    tokens; um ID token com a mesma audiência não serve de credencial);
  - claims com tipo errado (ex.: `nbf` string, `realm_access` string) e token > 16 KiB
    são recusados;
  - cabeçalho: exatamente um `Authorization: Bearer <token>`; outro esquema ou ausência
    → 401 sem `error`; vários cabeçalhos, token vazio ou com espaços → `invalid_token`.
- **JWKS e rotação.** Busca preguiçosa (o start não depende do Keycloak no ar). `kid`
  desconhecido força nova busca (rotação), limitada a uma a cada 10s, para que tokens com
  `kid` aleatório não virem ataque ao IdP; chaves com mais de 10 min são renovadas no
  próximo uso (remove chaves revogadas), e, se a renovação falhar, as antigas continuam
  valendo. Buscas são serializadas e reaproveitadas por geração (N requisições com cache
  frio = 1 busca). Busca com timeout de 5s, desligada do cancelamento da requisição e
  corpo limitado a 1 MiB. Sem chaves utilizáveis (IdP fora, discovery inválido) → **503
  `TEMPORARILY_UNAVAILABLE` + `Retry-After: 5`**, não 401: o cliente não errou.
- **`OIDC_JWKS_URL` opcional.** Vazio → discovery em
  `{OIDC_ISSUER_URL}/.well-known/openid-configuration`, exigindo `issuer` igual ao
  configurado. Informado → usado direto; serve ao Docker Compose, em que o app alcança o
  Keycloak por `http://keycloak:8080` mas o `iss` dos tokens é o host público
  (`KC_HOSTNAME`), situação que T24 precisa acertar.
- **Principal.** `sub`, `azp` (client), `providerId` da claim `OIDC_PROVIDER_CLAIM`
  (default `provider_id`, mapper "hardcoded claim" em cada client de provedor no realm)
  e papéis = `realm_access.roles` ∪ `resource_access[OIDC_ROLES_CLIENT].roles` (default:
  o client da audiência). Papéis de outros clients são ignorados. A claim do provedor é
  desacoplada do `client_id` para que trocar credenciais não mude a identidade de
  negócio. Se presente, ela precisa ser string válida como identificador (mesmos limites
  com que o providerId é gravado), senão 401. Claims reservadas (`iss`, `aud`, `exp`,
  `realm_access`...) não podem ser a claim do provedor.
- **Modelo de permissões** (`Authorizer`):
  - papel `OIDC_INTERNAL_ROLE` (default `wallet-internal`): operações de carteira
    (`POST /wallets`, `GET /wallets/{id}`, ledger, reconciliação), por `RequireInternal`
    ou `Internal(ctx)`;
  - papel `OIDC_PROVIDER_ROLE` (default `wager-provider`) **e** `providerId` na
    identidade: transações de aposta (envio, consultas por id e por externalId), por
    `RequireProvider` ou `Provider(ctx)`, que devolve o providerId a usar nos casos de
    uso;
  - `MatchProvider(ctx, requested)`: o providerId do corpo de
    `POST /wagering/transactions` e o do path `/providers/{providerId}/...` precisam ser
    exatamente (byte a byte) o da identidade, senão **403 `PROVIDER_MISMATCH`**. Assim um
    provedor nunca consulta nem faz replay de transação alheia, mesmo conhecendo a chave
    de idempotência, porque o caso de uso sempre recebe o providerId da identidade. Por
    id, a transação de outro provedor responde como inexistente (ADR-018);
  - os papéis são disjuntos e validados no start (precisam ser diferentes). Um token com
    os dois papéis tem os dois acessos (decisão do realm);
  - sem papel, ou provedor sem providerId → **403 `FORBIDDEN`** com
    `WWW-Authenticate: Bearer ... error="insufficient_scope"`.
- **Rotas públicas.** `Authenticator.Middleware(publicPaths...)` envolve o roteador
  inteiro, ou seja, o padrão é exigir token. Só caminhos declarados com comparação exata
  de `r.URL.Path` (T18: `/health/live`, `/health/ready`) passam sem token. Variações
  (`/health/live/`, `..`) exigem token. Em rota pública, o cabeçalho é ignorado.
  `/metrics` foi decidido na T18 (ADR-020): exige o serviço interno por padrão.
- **Respostas** (`WriteError`, corpo `apierror.Body {code, message, correlationId?}`,
  `Cache-Control: no-store`): 401 `UNAUTHENTICATED` com `WWW-Authenticate: Bearer
  realm="wallet"` (+ `error="invalid_token"` e `error_description` genérica
  `invalid token`/`token expired`); 403 `FORBIDDEN`/`PROVIDER_MISMATCH`; 503
  `TEMPORARILY_UNAVAILABLE`. A mensagem nunca diz qual verificação falhou.
- **Logs.** Falhas de autenticação em INFO e negações em INFO, só com a categoria
  (`reason`: `expired`, `signature`, `unknown_key`, `claims`, `provider_mismatch`...),
  método, path, papel exigido e `clientId`. Nunca o token, o cabeçalho ou valores de
  claims (testado: nem a assinatura aparece). IdP indisponível em WARN com a causa
  técnica. Com token válido, o `providerId` entra nos IDs de log do contexto.
- **Config** (validada no start, `errors.Is(err, config.ErrInvalidConfig)`):
  `OIDC_ISSUER_URL`, `OIDC_AUDIENCE` (obrigatórias), `OIDC_JWKS_URL` (http(s) sem
  credenciais), `OIDC_PROVIDER_CLAIM`, `OIDC_PROVIDER_ROLE`, `OIDC_INTERNAL_ROLE` (nomes
  `[A-Za-z0-9_.:-]{1,128}`, papéis distintos), `OIDC_ROLES_CLIENT` e `OIDC_CLOCK_SKEW`
  (0–2m). `auth.Settings.Validate` repete as regras para quem constrói sem env.
- **Fx.** `auth.Module` provê `Settings`, `*Authenticator` e `*Authorizer` a partir de
  `config.Config`, com logger opcional. Config inválida faz `fx.New` falhar, e o start
  não abre conexão com o IdP. Ainda não está ligado em `cmd/server` (T23).
- **Testes.**
  - Unitários (`go test -race`): IdP falso com `httptest` e chaves RSA geradas no teste,
    cobrindo:
    - os casos de 401: ausente, vazio, Basic, Bearer sem token, múltiplos cabeçalhos,
      malformado, grande demais, assinatura de outra chave com mesmo `kid`, payload
      adulterado, `kid` desconhecido, expirado, sem `exp`, `nbf`/`iat` futuros, `aud`
      errada/ausente, `iss` errado, `alg none`, HS256 com a chave pública, `typ`
      ID/Refresh, claim de provedor inválida;
    - tolerância de relógio;
    - principal no contexto, com papéis de realm e do client e só deles;
    - rotas públicas e variações;
    - rotação: `kid` novo → nova busca, chave retirada deixa de valer, limite de
      frequência e expiração do cache;
    - 503 com IdP fora, recuperação e chaves antigas reaproveitadas;
    - discovery (inclusive issuer divergente);
    - chaves inutilizáveis no JWKS;
    - concorrência com cache frio (1 busca);
    - matriz de autorização (403 `FORBIDDEN`/`PROVIDER_MISMATCH`, inclusive diferença
      só de caixa);
    - mapeamento de erros, `Settings.Validate`, `fx.ValidateApp` e start com config
      inválida (direta e por env).
  - Integração (`-tags=integration`, `keycloak_integration_test.go`): obtém um token real
    por `client_credentials` (`KEYCLOAK_URL`, `KEYCLOAK_REALM`, `KEYCLOAK_CLIENT_ID`,
    `KEYCLOAK_CLIENT_SECRET`, opcionais `KEYCLOAK_ISSUER`, `OIDC_AUDIENCE`,
    `KEYCLOAK_EXPECT_PROVIDER_ID`, `KEYCLOAK_EXPECT_ROLE`) e o valida por discovery + JWKS
    reais. Cobre middleware 200/401, assinatura adulterada e aud/iss errados. Sem
    `KEYCLOAK_URL` faz `t.Skip` com mensagem explícita: nesta máquina não há
    Docker/Keycloak. **Ele roda de verdade em T24/T27** com o Keycloak do Docker
    Compose. O realm precisa ter: mapper de audiência `wallet-api` nos clients,
    hardcoded claim `provider_id` nos clients de provedor, papéis `wager-provider` e
    `wallet-internal`, e `iss` consistente com `OIDC_ISSUER_URL`.

## ADR-020 — API HTTP (T18)

Pacotes `internal/adapters/http` (`package httpadapter`: `api.go`, `middleware.go`,
`request.go`, `handlers.go`, `health.go`, `dto.go`, `respond.go`, `module.go`) e
`internal/adapters/strictjson` (decoder estrito, reaproveitável pelo consumidor SQS na
T20).

- **Roteamento.** Usamos `net/http` puro, com os padrões do `ServeMux` (método + curingas
  `{walletId}`), sem roteador de terceiros. Os handlers dependem de interfaces estreitas
  (`WalletOpener`, `WagerProcessor`, `WalletReconciler`, `QueryService`, `Authenticator`,
  `Authorizer`), satisfeitas pelos casos de uso de `internal/app` e pelos tipos de
  `internal/adapters/auth`. `NewHandler(Deps)` recusa dependências ausentes.
- **Cadeia de middlewares** (de fora para dentro):
  1. correlation-id;
  2. observação: histograma `wallet_http_request_duration_seconds` com o rótulo = template
     da rota (registrado em `AllowRoutes`; um path sem rota vira `other`) e log de acesso
     com método, rota, status e duração (nunca cabeçalhos, query ou corpo);
  3. recover: panic → 500 `INTERNAL_ERROR`;
  4. `auth.Authenticator.Middleware("/health/live", "/health/ready")`, com rotas públicas
     só por path exato;
  5. `ServeMux`;
  6. autorização por rota.

  O template da rota casada é anotado num holder do contexto pelo wrapper de cada rota.
  Isso é necessário porque o `r.Pattern` do mux fica na cópia da requisição criada pelo
  middleware de autenticação.
- **Correlation-id.** Usamos o cabeçalho `X-Correlation-ID` quando ele é válido: mesmas
  regras do envelope de eventos (1..255 bytes, sem controle, sem espaço nas pontas). Na
  ausência dele, geramos um UUIDv7. Valor inválido ou repetido → **400
  `INVALID_CORRELATION_ID`**, sem ecoar o valor. O id volta no cabeçalho de toda resposta
  e no `correlationId` de todo corpo de erro, inclusive 401. Ele também segue para os
  casos de uso (eventos da outbox) e para os logs, pelo contexto.
- **Autorização por rota** (modelo do ADR-019):
  - `POST /wallets`, `GET /wallets/{id}`, `GET /wallets/{id}/ledger` e
    `POST /wallets/{id}/reconciliation` exigem o serviço interno (`Internal`). Um provedor
    ou uma identidade sem papel recebe 403 `FORBIDDEN`.
  - As três rotas de transação exigem provedor com `providerId` (`Provider`) e usam
    **sempre** o providerId da identidade. O serviço interno não acessa transações (403).
  - No `POST /wagering/transactions`, o `providerId` do corpo precisa casar exatamente com
    o da identidade (`MatchProvider`), senão 403 `PROVIDER_MISMATCH`. Essa verificação
    vem antes de qualquer acesso a dados.
- **Acesso cruzado (decisão 404 vs 403).** A resposta nunca depende da existência de
  dados de outro provedor:
  - quando a decisão depende só da identidade versus o que o próprio pedido nomeia
    (`providerId` no corpo ou em `/providers/{providerId}/...`), a resposta é **403
    `PROVIDER_MISMATCH`**, igual para um externalId existente ou não;
  - quando o pedido não nomeia o provedor (`GET /wagering/transactions/{id}`), a transação
    de outro provedor, ou interna como `OPENING`, responde **404 `TRANSACTION_NOT_FOUND`**
    com corpo byte a byte igual ao de um id inexistente (testado);
  - em `/providers/{seu-id}/...`, o externalId de outro provedor também é 404, porque o
    filtro está no SQL.

  Acessos negados não abrem unidade de trabalho e não produzem ledger, outbox nem
  transação (testado contando as transações da UoW). Um replay com a chave e o corpo de
  outro provedor é 403 e não devolve resultado nem saldo. O mesmo
  `externalTransactionId`/chave no espaço de outro provedor é outra operação.
- **Decoder estrito (sem float).** `strictjson.DecodeObject` lê cada campo como
  `json.RawMessage` e nada é decodificado para `interface{}` ou tipo numérico. Rejeita:
  - chave desconhecida;
  - chave com outra caixa (o `encoding/json` padrão casaria);
  - chave repetida (o padrão ficaria com a última);
  - dado depois do objeto;
  - corpo que não seja objeto.

  Para `Money`, exige `{"amount":"<string>","currency":"<string>"}` com as mesmas regras
  e faz o parse com `domain.ParseNonNegative`: rejeita amount numérico (`25.00` sem
  aspas), `NaN`, `Infinity`, notação científica, escala > 2, negativos e overflow.
  Resolve, na borda HTTP, a pendência "Money.UnmarshalJSON aceita outra caixa/chaves
  duplicadas", porque o HTTP não usa aquele `UnmarshalJSON`. A forma equivalente `"25"`
  é normalizada para `25.00` antes do hash (ADR-010) e o replay com `"25"` casa com
  `"25.00"` (testado). UUIDs (`playerId`, `walletId`, ids no path) só na forma canônica
  de 36 caracteres, não nulos. `kind` por `ParseExternalWagerKind` (`OPENING` e outra
  caixa → `INVALID_KIND`). `referenceExternalTransactionId`, se presente, precisa ser
  string não vazia (`null`/`""` → 400). Só então se monta o comando: o
  `idempotency.RequestFields` sai de valores já parseados (pendência T18/T20 resolvida
  no HTTP).
- **Transporte.** `Content-Type` diferente de `application/json` (charset só `utf-8`) dá
  415 `UNSUPPORTED_MEDIA_TYPE`. Corpo acima de 64 KiB dá 413 `PAYLOAD_TOO_LARGE`. UTF-8
  inválido dá 400. Path sem rota dá 404 `ROUTE_NOT_FOUND`, mas só depois da
  autenticação, para não revelar rotas sem token. Path conhecido com outro método dá 405
  `METHOD_NOT_ALLOWED` com `Allow`. Toda resposta JSON leva `Cache-Control: no-store` e
  `X-Content-Type-Options: nosniff`.
- **Idempotency-Key.** O cabeçalho é obrigatório em `POST /wagering/transactions`:
  - ausente ou vazio → 400 `IDEMPOTENCY_KEY_REQUIRED`;
  - repetido, com mais de 255 bytes, com caracteres de controle ou com espaço nas pontas
    → 400 `INVALID_IDEMPOTENCY_KEY`.

  O valor é usado exatamente como recebido, nunca substituído por `{providerId}:{externalId}`.
- **Contrato de respostas** (corpo de erro `apierror.Body {code, message, correlationId}`;
  mensagens genéricas, a causa técnica fica só no log):

  | Situação | HTTP | Corpo |
  | --- | --- | --- |
  | Carteira aberta | 201 + `Location` | `{id, playerId, balance, version, createdAt, updatedAt}` |
  | Operação `PROCESSED` (nova ou replay) | 200 | `{transactionId, status, balance, idempotentReplay}` (+`referenceTransactionId` em reversões) |
  | `PENDING_REFERENCE` (nova ou replay) | 202 + `Location: /wagering/transactions/{id}` | `{transactionId, status, idempotentReplay}` |
  | Rejeição de negócio `REJECTED` ou falha permanente `FAILED` (nova ou replay) | 422 | `{transactionId, status, failureCode, failureCategory, balance?, idempotentReplay}` |
  | Entrada inválida | 400 | `code` = `INVALID_REQUEST`, `INVALID_AMOUNT`, `INVALID_CURRENCY`, `INVALID_KIND`, `REFERENCE_REQUIRED`, `REFERENCE_NOT_ALLOWED`, `IDEMPOTENCY_KEY_REQUIRED`, `INVALID_IDEMPOTENCY_KEY`, `INVALID_PLAYER_ID`, `INVALID_INITIAL_BALANCE`, `INVALID_WALLET_ID`, `INVALID_TRANSACTION_ID`, `INVALID_PAGE_LIMIT`, `INVALID_CURSOR`, `INVALID_CORRELATION_ID` |
  | Sem token / token inválido ou expirado | 401 + `WWW-Authenticate` | `UNAUTHENTICATED` |
  | Sem papel / provedor divergente | 403 | `FORBIDDEN` / `PROVIDER_MISMATCH` |
  | Inexistente (ou de outro provedor) | 404 | `WALLET_NOT_FOUND` / `TRANSACTION_NOT_FOUND` |
  | Conflito | 409 | `IDEMPOTENCY_KEY_CONFLICT`, `EXTERNAL_TRANSACTION_CONFLICT`, `WALLET_ALREADY_EXISTS` |
  | Indisponibilidade transitória (banco, prazo, IdP) | 503 + `Retry-After` | `TEMPORARILY_UNAVAILABLE` |
  | Erro inesperado / panic | 500 | `INTERNAL_ERROR` |

  O 422 vale para os dois terminais sem sucesso porque ambos são resultados definitivos e
  persistidos. O campo `status` os distingue, e `failureCategory` (`INPUT`, `BUSINESS`,
  `INFRASTRUCTURE`) diz se a entrada é corrigível. O saldo do replay é o `balance_after`
  gravado, e ele é omitido onde o ADR-017 o esconde (`WALLET_NOT_FOUND`...). Não é um
  `Money` zero.
- **Consultas.** `GET /wagering/transactions/{id}` e `/providers/{p}/wagering/transactions/{ext}`
  devolvem `{transactionId, providerId, externalTransactionId, walletId, playerId, roundId,
  gameId, kind, status, money, referenceExternalTransactionId?, referenceTransactionId?,
  balance?, failureCode?, failureCategory?, attempts, nextAttemptAt?, createdAt, updatedAt,
  completedAt?}`, sem chave de idempotência nem hash. O extrato
  `GET /wallets/{id}/ledger?cursor=&limit=` devolve `{walletId, entries[], nextCursor?}`:
  - `limit` só em dígitos decimais;
  - `limit`/`cursor` repetidos → 400;
  - lista vazia serializada como `[]`.

  A reconciliação devolve `{walletId, storedBalance, calculatedBalance, difference,
  consistent, checkedEntries, walletVersion}`, ignora qualquer corpo e não altera dados.
- **Health e /metrics.**
  - `GET /health/live`: 200 `{"status":"UP"}`, sem consultar dependências.
  - `GET /health/ready`: roda as `ReadinessCheck` em paralelo, com 2s cada; um panic numa
    verificação vira DOWN. Todas UP → 200; alguma DOWN → 503 + `Retry-After` com
    `{"status":"DOWN","checks":{"postgres":"DOWN",...}}`. A causa vai só para o log WARN.
  - As verificações entram pelo grupo Fx `readinessChecks`. **T23** deve contribuir
    `postgres` (`db.Ping`) e `sqs`.
  - **Decisão sobre `/metrics`:** por padrão exige o serviço interno
    (`MetricsInternal`). As métricas não têm valores monetários nem ids (ADR-007), mas
    expõem volume e taxas de negócio, e a API compartilha a porta com o tráfego externo.
    `MetricsAccess = MetricsPublic` torna o path público (exato), para um scraper sem
    OAuth em rede privada. A escolha fica para a composição (T23/T24), mantendo o padrão
    seguro.
- **Métricas de negócio do canal HTTP.**
  - `TransactionResult(http, kind, status)` para operações novas.
  - `Duplicate(http, idempotent_replay)` nos replays e `Duplicate(http,
    idempotency_conflict)` nos 409.
  - `ObserveProcessing(http, process_transaction, outcome)`.
- **Fx.** `httpadapter.Module` provê o `http.Handler` nomeado `apiHandler`:
  - **obrigatórias:** casos de uso (`app.Module`) e `*auth.Authenticator`/`*auth.Authorizer`;
  - **opcionais:** `*metrics.Metrics`, `metricsHandler`, `MetricsAccess`, logger e o grupo
    de readiness.

  O servidor (listen/shutdown) é o `httpserver.Module` da T19 (ADR-021).
- **Testes** (`go test -race`, sem banco):
  - **Contrato por `httptest`.** Rodam os casos de uso **reais** sobre uma `UnitOfWork`
    em memória (`memstore_test.go`) e verificam status, código e formato de cada rota.
  - **Replay.** Saldo original após outras movimentações, forma equivalente do valor,
    409 nos dois conflitos e replays de 422 e 202.
  - **Decoder estrito.** Float, notação científica, escala, chaves desconhecidas,
    repetidas e com outra caixa.
  - **Acesso cruzado.** 403/404 sem vazamento, corpos idênticos, nenhuma escrita e
    nenhuma transação aberta.
  - **Transversais.** Matriz 401/403 das rotas de carteira, mapeamento de erros
    (503 + `Retry-After`, 500 sem detalhe), panic, correlation-id, health, `/metrics`,
    405/404 e `fx.ValidateApp` + start/stop.
  - **Autenticação real.** Um teste usa o `auth.Authenticator` real com JWKS em
    `httptest`: expirado, assinatura de outra chave, token falso e providerId vindo da
    claim.
  - **Falsos.** Os tokens do autenticador falso montam o `Principal` com papéis
    ordenados, porque `HasRole` usa busca binária.

## ADR-021 — Servidor HTTP com fx.Lifecycle e parada graciosa (T19)

- **Pacote.** `internal/adapters/http/server` (package `httpserver`). Ele fica separado de
  `httpadapter` por dois motivos. Os testes de lifecycle usam `goleak.VerifyTestMain`
  sem a carga dos testes de contrato da API. E os testes de handler não precisam de
  `config.Config`. O pacote recebe o `http.Handler` nomeado `apiHandler` (ADR-020) e não
  conhece rotas.
- **Start (OnStart).** O listen é **síncrono** (`net.ListenConfig.Listen` com o ctx do
  OnStart): uma porta ocupada faz o `app.Start` falhar com `listen HTTP em "<addr>"`, e
  a instância não fica "de pé" sem servir. Só o `Serve` roda em goroutine. Se o `Serve`
  terminar com erro que não seja `http.ErrServerClosed` (por exemplo, o listener morreu),
  o servidor loga ERROR e chama `fx.Shutdowner.Shutdown(fx.ExitCode(1))`. Assim o
  processo não continua vivo sem a API.
- **Stop (OnStop)**, nesta ordem:
  1. **Readiness DOWN.** `*Drain` é um sinal atômico compartilhado. O módulo contribui a
     verificação `http-server` ao grupo `readinessChecks` (ADR-020). Depois de
     `Drain.Begin()`, `/health/ready` responde 503 enquanto o listener ainda aceita
     conexões.
  2. **Espera `HTTP_DRAIN_DELAY`** (default `0s`, de 0 a 1m) para o balanceador retirar a
     instância antes de o listener fechar. No compose local o valor 0 basta. Atrás de
     um balanceador com health check, use alguns segundos.
  3. **`http.Server.Shutdown(ctx)`.** Fecha o listener, de modo que novas conexões são
     recusadas. Fecha também as conexões keep-alive ociosas e espera as requisições em
     voo terminarem. A conexão de uma requisição em voo é fechada logo depois da
     resposta.
  4. **Prazo estourado → `Close` forçado.** O contexto-base das requisições
     (`BaseContext`) é cancelado e `Close` derruba as conexões restantes. Handlers que
     respeitam `r.Context()`, como os casos de uso via pgx, abortam e a transação faz
     rollback. O Stop devolve um erro que envolve `ErrForcedClose`, e o Fx o loga.
  5. Espera a goroutine do `Serve` terminar, para não deixar goroutine para trás
     (goleak).
- **Prazo.** `SHUTDOWN_TIMEOUT` (default 15s) é o orçamento **total** do Stop do
  servidor, já com a espera de drenagem. Por isso `HTTP_DRAIN_DELAY < SHUTDOWN_TIMEOUT`,
  validado na config. Vale o prazo que vencer antes: `SHUTDOWN_TIMEOUT` ou o deadline do
  ctx do OnStop. **Nota para a T23:** quando o ctx do OnStop vence, o Fx devolve
  `ctx.Err()` por conta própria e pode abandonar o hook. A composição deve então usar
  `fx.StopTimeout` maior que `SHUTDOWN_TIMEOUT` mais o tempo de parada dos workers.
- **Timeouts do `http.Server`** (config, ADR-007):

  | Variável | Default |
  |---|---|
  | `HTTP_READ_HEADER_TIMEOUT` | 5s |
  | `HTTP_READ_TIMEOUT` | 30s |
  | `HTTP_WRITE_TIMEOUT` | 30s |
  | `HTTP_IDLE_TIMEOUT` | 120s |

  Todos precisam ser positivos, porque zero no net/http significa "sem limite". Também
  se exige `READ_HEADER <= READ`. O `ReadHeaderTimeout` mitiga Slowloris. O
  `WriteTimeout` precisa cobrir o handler mais lento: o pior caso hoje é o retry de
  serialização do caso de uso somado ao readiness, de 2s por verificação. O `ErrorLog`
  do servidor vai para o slog em WARN.
- **Ordem de parada.** O Fx executa os OnStop na ordem inversa dos OnStart. O servidor
  depende, via handler, dos casos de uso, e estes do pool e dos clients. Logo ele inicia
  depois e para antes deles: as requisições em voo drenam com o banco ainda aberto.
- **Fx.** `httpserver.Module` provê `*Drain`, a `ReadinessCheck` `http-server` (grupo) e
  `*Server`. O `*Server` já tem o hook registrado e é forçado por `fx.Invoke`. O módulo
  depende de `config.Config` e do `apiHandler`, e o logger é opcional.
- **Testes** (`go test -race`, `goleak.VerifyTestMain` + `goleak.VerifyNone` por teste,
  porta efêmera `127.0.0.1:0`, Fx real via `fxtest`):
  - start/stop sem vazamento;
  - porta ocupada falha o start;
  - requisição em voo é **drenada** durante o stop, e o stop só termina após a resposta;
  - conexões novas e keep-alive ociosas são **recusadas** após o início do stop;
  - com `DrainDelay`, `/health/ready` passa a 503 com o servidor ainda atendendo;
  - prazo estourado (por `SHUTDOWN_TIMEOUT` ou pelo ctx) → `ErrForcedClose`, contexto da
    requisição cancelado;
  - `ReadHeaderTimeout` efetivo contra cliente lento;
  - falha do `Serve` → `Shutdown` com exit code 1;
  - `fx.ValidateApp`.
- **Dependência nova:** `go.uber.org/goleak` (só em testes).

## ADR-022 — Consumidor SQS com inbox (T20)

Pacote `internal/adapters/sqs` (`envelope.go`, `consumer.go`, `client.go`, `module.go`).
Usa `aws-sdk-go-v2/service/sqs` atrás de uma interface estreita `sqs.API`
(`ReceiveMessage`, `DeleteMessage`, `ChangeMessageVisibility`, `GetQueueAttributes`,
`GetQueueUrl`, `SendMessage`), que
o `*awssqs.Client` satisfaz e os testes unitários substituem por uma fila FIFO em memória.

- **Envelope** (`DecodeEnvelope`): `{messageId, type, occurredAt, data}` com
  `type = "WagerTransactionRequested"`. Reaproveita `internal/adapters/strictjson`
  (ADR-020), então as regras são as do corpo HTTP: chaves com caixa exata, sem chaves
  desconhecidas ou repetidas, Money só como strings (`ParseNonNegative`), UUIDs canônicos e
  nada depois do objeto. `data` tem os campos do HTTP mais `idempotencyKey` (usada como
  recebida, nunca recalculada) e `providerId`. O `kind` passa por
  `domain.ParseExternalWagerKind`, então **`OPENING` é inválido** (`INVALID_KIND`).
  `messageId` e `idempotencyKey`: 1..255 bytes, UTF-8, sem controle e sem espaço nas pontas.
  Corpo até 64 KiB. Os demais identificadores são validados pelo domínio no caso de uso,
  com os mesmos códigos do HTTP. O hash de idempotência é o mesmo do HTTP (ADR-010): a
  mesma operação por HTTP e por SQS é replay, não duplicata.
- **Comando.** `CorrelationID = CausationID = messageId` do envelope (vai para os eventos
  e logs) e `Inbox{ConsumerName: SQS_CONSUMER_NAME, MessageID: messageId}`. A identidade
  durável é o `messageId` **do envelope**, não o `MessageId` do SQS (que muda a cada envio).
  O `providerId` vem do envelope: o acesso à fila é controlado pelas credenciais e
  políticas do broker (T24), e o domínio continua validando provedor, carteira, jogador e
  referência.
- **Inbox na mesma transação.** O consumidor só chama `ProcessWagerTransaction.Execute`.
  Registro da inbox, efeitos financeiros, ledger, outbox e `Inbox.Complete` entram no
  mesmo commit (ADR-017). Uma reentrega depois do commit encontra a inbox `COMPLETED` e
  devolve o replay sem tocar na carteira. O mesmo `messageId` com outro conteúdo é
  `MESSAGE_CONFLICT`.
- **Destino de cada mensagem:**

  | Situação | Ação | Resultado |
  |---|---|---|
  | `PROCESSED`, `REJECTED` (regra de negócio), `PENDING_REFERENCE`, replay | `DeleteMessage` **depois** do commit | Removida |
  | Envelope inválido (JSON, tipo, `OPENING`, Money...) | `SendMessage` na DLQ e então `DeleteMessage` | DLQ explícita (`invalid_message`), 1ª entrega |
  | `app.KindInvalidInput` / `app.KindConflict` (`MESSAGE_CONFLICT`, `IDEMPOTENCY_KEY_CONFLICT`, `EXTERNAL_TRANSACTION_CONFLICT`) | `SendMessage` na DLQ e então `DeleteMessage` | DLQ explícita (`permanent_error`), 1ª tentativa |
  | `app.KindUnavailable`, `ProcessTimeout` estourado, erro sem classe | `ChangeMessageVisibility(backoff)` | Retry; esgotado `SQS_MAX_ATTEMPTS`, DLQ explícita (`max_receives`) |
  | Falha no `SendMessage` à DLQ | `ChangeMessageVisibility(backoff)`, não apaga | Retry do envio (transitório) |
  | Shutdown (contexto do processamento cancelado) | visibilidade 0 | Reentrega imediata |

  - **Rejeição de negócio é terminal e persistida** (estado + `failureCode` + evento):
    a mensagem é apagada e não vai à DLQ. `PENDING_REFERENCE` também é apagada: a
    pendência já está persistida e o worker de referências (T21) assume (README §6.5).
  - **Inválidas, permanentes e transitórias esgotadas vão à DLQ por envio explícito.**
    *Por quê (achado da avaliação):* a primeira versão deixava o redrive nativo mover a
    mensagem (visibilidade 0 até passar de `maxReceiveCount`). Mas cada `ReceiveMessage`
    na fila FIFO incrementa o `ApproximateReceiveCount` também das seguintes do mesmo
    grupo que vieram no lote e foram devolvidas sem processar. A venenosa gastava
    exatamente `maxReceiveCount` recebimentos, as seguintes subiam junto e o redrive as
    movia também: uma mensagem **válida** ia para a DLQ sem ser processada (grupo
    `["not json", válida]`, `maxReceiveCount=2` → DLQ = as duas). Folga no
    `maxReceiveCount` não resolve, porque os contadores andam juntos.
    *Como:* o consumidor copia a mensagem para a DLQ (`SendMessage`) e **só depois do
    envio confirmado** a apaga da fila de entrada. Nenhum recebimento extra é gasto e o
    grupo não fica bloqueado: as seguintes do mesmo lote são processadas em seguida, na
    mesma goroutine e em ordem. A cópia preserva:
    - o corpo original, byte a byte;
    - o `MessageGroupId` (ou `dlq-ungrouped`, se não houver);
    - um `MessageDeduplicationId` determinístico (`sha256(fila de origem, MessageId do
      SQS)`), estável entre reentregas da mesma mensagem;
    - `MessageAttributes` com `dlqReason` (`invalid_message`, `permanent_error`,
      `max_receives`), `dlqCode`, `dlqField` (caminho do campo, nunca o nome de chave
      desconhecida nem valores), `sourceQueue`, `sourceMessageId`, `envelopeMessageId`,
      `receiveCount`, `attempts` e `consumerName`.

    Nenhuma escrita acontece no banco (a validação ocorre antes da transação, e
    conflitos fazem rollback, inclusive da inbox). Falhas:
    - `SendMessage` falhou → transitório: a mensagem **não** é apagada, volta com
      backoff e o envio é repetido; o grupo é interrompido.
    - Envio confirmado mas `DeleteMessage` falhou → a original reaparece ao fim da
      visibilidade e o reenvio é deduplicado pela DLQ FIFO (janela de 5 min; depois
      dela, no pior caso, uma cópia duplicada na DLQ, inofensiva para reprocessamento
      manual porque a inbox/idempotência absorvem). O grupo é interrompido para que as
      seguintes não passem à frente dela.
  - **Transitório:** backoff `SQS_RETRY_BASE_DELAY · 2^(tentativa−1)`, com teto
    `SQS_RETRY_MAX_DELAY` (segundos inteiros, até 12h), aplicado via
    `ChangeMessageVisibility`. Esgotadas `SQS_MAX_ATTEMPTS` tentativas (ou no último
    recebimento antes do redrive nativo, `ApproximateReceiveCount >= maxReceiveCount`), a
    mensagem vai à DLQ explicitamente, com o código (`TEMPORARILY_UNAVAILABLE`,
    `PROCESS_TIMEOUT`, `UNCLASSIFIED_ERROR`...). Erro sem classe (bug, falha permanente de
    infraestrutura) também é tratado como transitório: esgota as tentativas e chega à
    DLQ, em vez de ser descartado.
  - **Tentativa ≠ `ApproximateReceiveCount`.** A tentativa é o `ApproximateReceiveCount`
    menos os recebimentos que a própria instância devolveu sem tentar processar
    (seguintes de um grupo interrompido, shutdown, falha na renovação da visibilidade).
    Esse desconto fica num mapa local por `MessageId` do SQS (`releaseTracker`,
    limitado a 10 000 entradas, poda por idade de 1h, apagado no delete). Assim a
    seguinte de uma transitória que esgotou não herda as tentativas dela: ainda tem
    `SQS_MAX_ATTEMPTS` tentativas próprias (limitadas pelo redrive nativo, abaixo).
    Depois de um reinício, ou em outra instância, o desconto se perde e vale a contagem
    bruta. Isso é conservador: a mensagem pode ir à DLQ mais cedo, nunca ficar em retry
    sem fim. Por isso o estado do retry continua sobrevivendo a reinícios e troca de
    instância.
  - **Delete que falha depois do commit** só gera log: a mensagem reaparece ao fim da
    visibilidade, a inbox devolve o replay e então ela é apagada.
- **RedrivePolicy obrigatória; o redrive nativo é só rede de segurança.** No `Start`, o
  consumidor faz três coisas, com retry até o prazo do OnStart (fila inexistente é erro
  imediato):
  1. Lê a `RedrivePolicy` da fila e falha o start se ela não existir ou não tiver
     `deadLetterTargetArn` (ARN de SQS) e `maxReceiveCount >= 1` (`ErrNoRedrivePolicy`).
  2. Exige `SQS_MAX_ATTEMPTS < maxReceiveCount` (`ErrMaxAttemptsTooHigh`). Senão o redrive
     nativo moveria a mensagem antes do envio explícito (sem motivo nos atributos).
  3. Resolve a URL da DLQ com `GetQueueUrl` (nome e conta do ARN). Os envios vão para
     essa DLQ, a da `RedrivePolicy`. Se `SQS_INBOUND_DLQ_URL` apontar para outra fila, um
     aviso é logado; ela continua servindo só para a profundidade
     (`wallet_dlq_depth_messages`, lida a cada 30s).

  O redrive nativo continua configurado para o que a aplicação não cobre:
  - crash ou kill repetido no meio do processamento;
  - `SendMessage` à DLQ falhando por muito tempo;
  - um envelope que derruba o processo.

  `wallet_dlq_messages_total{reason}` conta os envios explícitos confirmados (os
  movimentos do redrive nativo não são observáveis pela aplicação; a profundidade da DLQ
  os inclui).
- **Risco residual para seguintes (documentado).**
  - Garantido: uma única mensagem venenosa (inválida, `OPENING`, permanente de negócio,
    conflito) **nunca** leva uma válida do mesmo grupo à DLQ. Ela sai na primeira
    tentativa e as seguintes nem são devolvidas.
  - Transitória que esgota: as seguintes do lote são devolvidas a cada falha, então
    chegam com até `SQS_MAX_ATTEMPTS` recebimentos. Isso fica abaixo de
    `maxReceiveCount` (garantido no start), e o desconto local evita que herdem as
    tentativas.
  - O que ainda pode levar uma válida ao redrive nativo é acumular `maxReceiveCount`
    recebimentos sem desconto. Exemplos: várias transitórias longas em sequência no mesmo
    grupo com reinícios entre elas; shutdowns repetidos com a mensagem recebida e
    liberada; falha prolongada da DLQ.
  - Mitigação: `maxReceiveCount` com folga sobre `SQS_MAX_ATTEMPTS` (recomendado 5 para
    o default 3; mais em ambientes com deploys frequentes).
  - Mesmo nesses casos a mensagem não é perdida: fica na DLQ para reprocessamento, e a
    inbox/idempotência tornam a reinjeção segura.
- **Limites e visibilidade** (config, ADR-007; novos campos em `config.SQSConsumerConfig`):

  | Variável | Default | Regra |
  |---|---|---|
  | `SQS_CONSUMER_NAME` | `wager-transactions-consumer` | nome simples; é o `consumer_name` da inbox |
  | `SQS_CONSUMER_CONCURRENCY` | 4 | 1..100 grupos em paralelo por instância |
  | `SQS_WAIT_TIME` | 20s | long polling, 0..20s |
  | `SQS_VISIBILITY_TIMEOUT` | 30s | 1s..12h, pedido em cada `ReceiveMessage` |
  | `SQS_PROCESS_TIMEOUT` | 15s | `< SQS_VISIBILITY_TIMEOUT` |
  | `SQS_RETRY_BASE_DELAY` / `SQS_RETRY_MAX_DELAY` | 1s / 5m | `base <= max <= 12h` |
  | `SQS_DRAIN_TIMEOUT` | 10s | prazo de drenagem no shutdown |
  | `SQS_MAX_ATTEMPTS` | 3 | 1..999; tentativas de uma falha transitória antes da DLQ explícita; `< maxReceiveCount` (checado no start) |

  O `maxReceiveCount` é da fila (provisionamento, T24); o valor recomendado é 5 (folga
  de 2 sobre `SQS_MAX_ATTEMPTS`). O
  processamento de uma mensagem nunca passa de `SQS_PROCESS_TIMEOUT`, que é menor que a
  visibilidade. Antes de processar uma mensagem que esperou atrás de outras do mesmo grupo
  no lote, o consumidor **renova a visibilidade** se o que resta não cobre um
  processamento completo. Assim ela não reaparece para outra instância durante o
  trabalho. Se reaparecer mesmo assim, a inbox serializa as duas entregas: uma processa e
  a outra recebe o replay.
- **Ordem FIFO e concorrência.** `MessageGroupId` recomendado = `walletId` (ordem por
  carteira e paralelismo entre carteiras). `MessageDeduplicationId` recomendado =
  `messageId` do envelope (deduplicação de 5 min do SQS). Nenhum dos dois é necessário
  para a correção: a inbox, a idempotência por chave e o lock da carteira valem sem eles.
  - Um laço de recebimento só pede mensagens quando há vaga (`MaxNumberOfMessages` =
    vagas livres, até 10).
  - O lote é agrupado por `MessageGroupId`, e cada grupo vai para **uma** goroutine que
    processa em ordem. Grupos diferentes rodam em paralelo até
    `SQS_CONSUMER_CONCURRENCY`.
  - Uma mensagem enviada à DLQ (e apagada) conta como concluída: o grupo segue.
  - Se uma mensagem do grupo não for concluída (retry transitório, falha no envio à
    DLQ, shutdown), as seguintes do mesmo lote **não** são processadas e voltam com
    visibilidade 0. Enquanto a anterior estiver em voo, o SQS FIFO não entrega o grupo,
    e quando ela reaparece vem antes das outras.
  - Essas devoluções incrementam o `ApproximateReceiveCount` das seguintes. Elas são
    descontadas da tentativa (acima), e o efeito no redrive nativo está em "Risco
    residual".
  - Entre lotes não há bloqueio local: o próprio SQS segura o grupo enquanto houver
    mensagem dele em voo. Como nenhum lock é global, outras carteiras seguem
    normalmente.
- **Shutdown (OnStop).** *(Revisado na T24; ver "Correção" abaixo.)*
  1. Para de buscar: nenhum `ReceiveMessage` novo começa, mas o que está em curso
     **não** é cancelado. O Stop espera ele terminar (até `SQS_WAIT_TIME` +
     `CallTimeout` 5s). As mensagens que ele trouxer não são processadas: são liberadas
     com `ChangeMessageVisibility(0)`.
  2. Em paralelo, as mensagens em processamento terminam (commit + delete) até
     `SQS_DRAIN_TIMEOUT` (contado do início do Stop) ou o prazo do ctx do OnStop. As
     recebidas e ainda não iniciadas são liberadas com visibilidade 0.
  3. Estourado o prazo com mensagem em processamento, o contexto do processamento é
     cancelado (rollback). As interrompidas também são liberadas com visibilidade 0, e o
     Stop devolve `ErrDrainTimeout`. Se o prazo vence só esperando o último long polling
     (nada em processamento), não é estouro: o Stop devolve nil.
  4. **Toda liberação do shutdown acontece depois do último receive** (`release` espera
     o fim do laço quando `stopping`).
  5. O Stop sempre espera todas as goroutines do consumidor, salvo se o próprio ctx do
     OnStop vencer.

  **Correção (T24, achada contra o LocalStack real).** A versão original cancelava o
  contexto do long polling no Stop. Só que cancelar um `ReceiveMessage` no cliente não o
  encerra no servidor, nem no LocalStack nem na AWS: a chamada continua até o fim do
  `WaitTime`, e a resposta vai para uma conexão que ninguém lê. A mensagem interrompida,
  liberada com visibilidade 0 logo depois, era entregue a esse poll órfão e ficava
  invisível pelo `VisibilityTimeout` inteiro (30s). Não havia perda (at-least-once), mas
  a "reentrega imediata" do shutdown falhava, e um poll órfão também escondia mensagens
  novas que chegassem no fim do `WaitTime`. Alternativas consideradas:
  - cancelar o poll e liberar depois de `WaitTime`: o mesmo custo de espera, e ainda
    perde as mensagens que o poll órfão pegar;
  - `WaitTime` curto: mais chamadas vazias e mais custo, só para encurtar a janela.

  A escolha foi deixar o último receive terminar, liberar o que ele trouxer e só então
  liberar o resto. Custo: o Stop de uma réplica ociosa leva até `SQS_WAIT_TIME` (20s por
  padrão) em vez de ser imediato. Isso entra no `fx.StopTimeout` por
  `sqs.StopBudget` = max(`SQS_DRAIN_TIMEOUT`, `SQS_WAIT_TIME` + 5s), já que a espera corre
  em paralelo com a drenagem: 75s com os defaults (ADR-025, ADR-026). Só se o servidor não
  responder em `WaitTime` + 5s, ou o ctx do OnStop vencer, o receive é cancelado no
  cliente. Nesse caso volta o comportamento antigo, atraso até o fim da visibilidade, sem
  perda. Testes: `TestConsumerShutdownReleasesAfterLastLongPoll` (fila falsa com poll que
  continua no servidor depois do cancelamento, `serverSidePolls`),
  `TestConsumerStopReleasesMessagesFromLastLongPoll`,
  `TestConsumerIdleStopWaitingLongPollIsNotDrainTimeout`,
  `TestConsumerStopCancelsUnresponsiveLongPolling` e, contra o LocalStack,
  `TestIntegrationConsumerShutdownReleasesVisibility`.
- **Fx.**
  - `ClientModule` provê `*sqs.Client` e `sqs.API`. O cliente tem transporte HTTP
    próprio, e o OnStop fecha as conexões ociosas. Como é construído antes de quem o
    usa, ele para depois deles. `BaseEndpoint = AWS_ENDPOINT_URL` (LocalStack).
    Credenciais estáticas, quando configuradas, evitam qualquer chamada de rede na
    construção.
  - `ConsumerModule` provê o `*Consumer` (OnStart inicia o laço em goroutine, OnStop
    drena) e contribui a verificação `sqs` (`GetQueueAttributes` na fila de entrada; DOWN
    durante a parada) ao grupo `readinessChecks` (ADR-020).
  - `sqs.Module` = os dois mais `*app.ProcessWagerTransaction` como `WagerProcessor`. O
    consumidor depende do caso de uso, que depende do pool, logo para antes do pool.
- **Logs e métricas sem payload.** Os logs JSON levam `sqsMessageId`, `messageId`,
  `correlationId`, `providerId`, `walletId`, `transactionId`, `kind`, `status`,
  `failureCode`, `receiveCount` e o código/campo do erro. **Nunca** levam o corpo, o valor
  de um campo inválido ou o nome de uma chave desconhecida. As métricas usam os rótulos
  fechados de `platform/metrics`:
  - `wager_transactions_total{source="sqs"}`;
  - `duplicates_total` (`inbox_redelivery` quando um replay chega em reentrega,
    `idempotent_replay` na primeira entrega, `idempotency_conflict`);
  - `retries_total{operation="sqs_consume"}`;
  - `dlq_messages_total{reason}`;
  - `dlq_depth_messages`;
  - `processing_duration_seconds{source="sqs",operation="sqs_consume"}`.
- **Testes.**
  - **Unitários** (`go test -race`, `goleak.VerifyTestMain`, fila FIFO em memória com
    visibilidade, `ApproximateReceiveCount`, bloqueio por grupo, redrive nativo,
    `GetQueueUrl` e `SendMessage` na DLQ FIFO com deduplicação):
    - envelope válido e 35 formas inválidas, sem ecoar valores;
    - delete após o resultado (inclusive rejeição, pendência e replay), com o comando
      montado com inbox, correlation e causation;
    - inválidas (inclusive `OPENING`) vão à DLQ por envio explícito na 1ª entrega, sem
      chegar ao caso de uso, com corpo, grupo e atributos (motivo, código, campo);
    - conflitos e entrada inválida vão à DLQ na 1ª tentativa;
    - backoff 1/2/4s com teto;
    - transitórios esgotados (`SQS_MAX_ATTEMPTS`) vão à DLQ com o código (indisponível,
      erro sem classe, prazo), antes do `maxReceiveCount`;
    - **válidas atrás de venenosa no mesmo grupo** (JSON inválido, `OPENING`, permanente
      de negócio, conflito; `maxReceiveCount=2`, o cenário do achado) são processadas em
      ordem e apagadas, e só a venenosa vai à DLQ;
    - válidas atrás de transitória que esgota são processadas, e a falha transitória de
      uma delas é a tentativa 1 (não herda as devoluções);
    - falha no envio à DLQ é transitória (não apaga; backoff; o grupo espera);
    - delete falho após o envio: reenvio deduplicado, uma única cópia na DLQ;
    - start exige `SQS_MAX_ATTEMPTS < maxReceiveCount` e resolve a DLQ pela
      `RedrivePolicy` (retry; fila inexistente falha na hora);
    - `releaseTracker` (contagem, esquecimento, poda e limite);
    - ordem no grupo sob falha;
    - paralelismo entre grupos (barreira) e sequência dentro do grupo;
    - limite de concorrência;
    - drenagem no shutdown, com liberação das seguintes do grupo e nenhum receive
      depois do Stop;
    - prazo de drenagem com liberação (visibilidade 0);
    - long polling no shutdown: o último receive termina antes das liberações, o que
      ele traz é liberado, e um servidor que não responde é cancelado (T24);
    - delete que falha leva a replay;
    - retry do receive;
    - renovação de visibilidade;
    - `RedrivePolicy` obrigatória;
    - validação da configuração;
    - `fx.ValidateApp` e ciclo Fx do módulo.
  - **Integração** (`-tags=integration`, `consumer_integration_test.go`; LocalStack via
    `SQS_ENDPOINT`/`AWS_ENDPOINT_URL` e PostgreSQL via `TEST_DATABASE_URL` ou
    testcontainers; pulados sem LocalStack). Cada teste cria filas FIFO exclusivas com
    redrive:
    - processamento, com saldo, ledger, inbox e eventos com correlation/causation =
      `messageId`;
    - reentrega sem efeito: mesma mensagem duas vezes e outra mensagem com a mesma
      chave. Mesmo `messageId` com outro conteúdo vai à DLQ;
    - rejeição persistida e removida;
    - inválidas na DLQ (JSON, `OPENING`, valor zero, tipo), sem transação nem inbox,
      lidas da DLQ com grupo preservado e `dlqReason`/`dlqCode`/`receiveCount=1`;
    - válidas atrás de venenosas no mesmo grupo (`maxReceiveCount=2`): processadas em
      ordem, só as venenosas na DLQ;
    - shutdown gracioso com a mensagem em voo concluída e apagada;
    - prazo estourado: rollback, mensagem liberada e reprocessada uma única vez por
      outra instância;
    - composição Fx real (`postgres.Module` + `app.Module` + `sqs.Module`) sem
      vazamento de goroutines (`goleak.VerifyNone` dentro do teste).

    Vazamentos com a tag: o `TestMain` de integração roda todos os testes do pacote
    (unitários e de integração) e, ao fim, falha se sobrar goroutine criada por eles
    (`goleak.Find` com `IgnoreCurrent` tirado antes de `m.Run`, `runWithLeakCheck`).
    Nesta máquina há PostgreSQL (`TEST_DATABASE_URL`), mas não há Docker nem
    LocalStack. Os testes de integração do SQS compilam (`go vet -tags=integration`) e
    são pulados. O comportamento está provado pelos testes unitários com `-race`.
- **Para a T23:** compor `config.Module`, `postgres.Module`, `app.Module` e `sqs.Module`.
  Usar `fx.StopTimeout` > `SHUTDOWN_TIMEOUT` (HTTP) + `SQS_DRAIN_TIMEOUT` + folga para os
  releases (5s por chamada): o Fx não espera um OnStop além do seu prazo.

## ADR-023 — Worker de referências pendentes e PENDING órfãos (T21)

Pacotes `internal/app/resume_wager.go` (caso de uso `ResumeWagerTransaction` e
`PendingWagerPolicy`), `internal/adapters/pendingworker` (laço, lifecycle Fx) e
`internal/adapters/postgres` (`ClaimDue`, `GetClaimedForUpdate`), além da migration `0007`.

- **O que é retomado.**
  - `PENDING_REFERENCE` vencido (`next_attempt_at <= agora`).
  - `PENDING` órfão. O fluxo síncrono nunca confirma `PENDING` (ADR-017). Uma linha
    `PENDING` confirmada só existe se um aceite foi interrompido (ou se um aceite
    assíncrono futuro gravá-la). Ela é órfã quando vence o seu `next_attempt_at` ou, sem
    ele, `PENDING_ORPHAN_GRACE` desde `created_at`.
  - A retomada é idempotente: a linha órfã é a própria operação, e a idempotência por
    chave já a protege de uma segunda entrada. Uma linha terminal não é elegível nem
    atualizável.
- **Claim com lease persistido** (migration `0007`):
  - Colunas `claimed_by UUID` (token do lote) e `claimed_until TIMESTAMPTZ`.
  - `CHECK` de par (as duas juntas) e `CHECK` de que linha terminal não fica
    reivindicada.
  - `ClaimDue` roda dois comandos `UPDATE ... FROM (SELECT ... LIMIT n FOR NO KEY UPDATE
    SKIP LOCKED)`: primeiro as referências vencidas, depois os órfãos com o que sobrar do
    lote. Eles pegam só linhas sem lease vigente (`claimed_until IS NULL OR <= agora`) e
    gravam `claimed_by = token` e `claimed_until = agora + lease`.
  - **Por que dois comandos:** um único comando com duas CTEs travadas trava também os
    órfãos que não entram no lote. Achado no teste de integração: outro claimer
    concorrente os pulava.
  - **Por que lease e não lock até o commit do lote** (o desenho anterior,
    `ClaimDuePendingReferences`):
    - um lote não segura locks de várias carteiras;
    - cada item tem sua própria transação curta;
    - um worker que morre não perde o item, que volta quando o lease expira.
  - `FOR NO KEY UPDATE` não conflita com o `KEY SHARE` que a FK
    `reference_transaction_id` toma. Por isso uma operação nova que cita a linha não espera
    o claim.
  - Relógio: o da aplicação, o mesmo que grava `next_attempt_at`. Um desvio de relógio só
    antecipa ou atrasa uma reassunção e nunca duplica efeito, por causa do fencing.
- **Fencing e exatamente uma vez.**
  - `Resume(id, token)` lê a linha sem lock só para descobrir a carteira (`wallet_id` é
    imutável).
  - Trava a carteira (`GetForUpdate`) **antes** da linha. É a mesma ordem de quem insere
    uma operação nova (carteira → `KEY SHARE` da FK), então não há deadlock entre o worker
    e HTTP/SQS.
  - Em seguida chama `GetClaimedForUpdate(id, token)`: `SELECT ... WHERE id = $1 AND
    claimed_by = $2 FOR NO KEY UPDATE`. Uma instância cujo lease expirou e foi reassumido
    encontra outro token e devolve `ResumeSkipped` sem gravar nada.
  - A transição grava com `Transactions.Update`, que tem `WHERE status não terminal` e
    também solta o lease no mesmo `UPDATE`.
  - Barreiras finais: `UNIQUE(wallet_id, transaction_id)` do ledger, o índice de reversão
    única e o trigger de imutabilidade terminal.
- **Mesmo caminho do caso de uso** (pendência da T15). `process_wager.go` foi fatorado em
  `loadReference`, `wagerEvents` e `persistWagerOutcome`. `ProcessWagerTransaction` grava
  com `Insert` e o worker com `Update`. `DecideWager` e `ApplyWagerDecision` são os mesmos,
  com a carteira travada (contratos dos ADR-014/017).
- **Limite: tentativas e TTL** (`PendingWagerPolicy`):
  - `attempts` continua contando só as buscas sem sucesso (ADR-008).
    `PENDING_REFERENCE_MAX_ATTEMPTS` conta todas as buscas, inclusive a do aceite.
  - Quando a referência continua indisponível (ausente, ou existente mas ainda
    `PENDING`/`PENDING_REFERENCE`), a operação expira se esta busca for a de número
    `MaxAttempts` ou se `PENDING_REFERENCE_TTL` desde `created_at` tiver vencido. Ela vira
    `REJECTED REFERENCE_NOT_FOUND`, com o saldo observado e `WagerTransactionRejected` no
    mesmo commit. `attempts` não é incrementado na expiração.
  - A busca vem **antes** da expiração: uma referência que chegou é aplicada mesmo na
    última tentativa ou depois do TTL (se o worker atrasou).
  - Referência existente que terminou sem sucesso (`REJECTED`/`FAILED`) não espera:
    `DecideWager` rejeita na hora com `REFERENCE_NOT_PROCESSED` (ADR-009).
  - Referência pendente no TTL: `REFERENCE_NOT_FOUND`. O código significa "não ficou
    disponível no prazo" e nunca carrega `referenceTransactionId`.
- **Backoff exponencial persistido.**
  - `next_attempt_at = agora + PENDING_REFERENCE_RETRY_BASE_DELAY · 2^(attempts−1)`, com
    teto `PENDING_REFERENCE_RETRY_MAX_DELAY` e sem overflow. `attempts` é o valor já
    incrementado.
  - Nunca agenda depois do fim do TTL, para que a expiração aconteça no prazo.
  - A primeira espera é a do aceite (`WithReferenceRetryDelay`, padrão 2s). A T23 deve
    passar o mesmo `PENDING_REFERENCE_RETRY_BASE_DELAY`.
  - Com os defaults (2s, teto 1m, 10 buscas, TTL 10m), a última busca acontece cerca de
    5 min depois do aceite.
- **Eventos.**
  - `PROCESSED`: `WagerTransactionProcessed` e `WalletBalanceChanged`. `REJECTED`:
    `WagerTransactionRejected`.
  - `PENDING` → `PENDING_REFERENCE` gera `WagerTransactionPendingReference`. O
    reagendamento não gera evento (ADR-012).
  - `correlationId` = id da transação (o correlation da entrada original não é
    persistido). `causationId` = token do claim, rastreável até o lote.
- **Worker** (`pendingworker.Worker`):
  - Cada ciclo gera um token UUIDv7, chama `Claim` (lote `PENDING_WORKER_BATCH_SIZE`) e
    reaplica os itens com até `PENDING_WORKER_CONCURRENCY` em paralelo, cada um com
    `PENDING_WORKER_PROCESS_TIMEOUT`.
  - Lote cheio dispara novo ciclo imediato. Senão, o worker espera
    `PENDING_WORKER_POLL_INTERVAL`.
  - Um item cujo lease já não cobre um processamento completo não é iniciado. Ele volta
    para a próxima reivindicação.
  - Erro em um item (transitório ou não) não interrompe o ciclo: o lease fica gravado e o
    item volta quando ele expirar.
  - Erro no claim (banco indisponível): log e nova tentativa no próximo ciclo.
- **Lifecycle Fx.**
  - `pendingworker.Module` provê `*app.ResumeWagerTransaction` (política vinda da config)
    como `Resumer` e o `*Worker`, que é instanciado por `fx.Invoke`.
  - `OnStart` inicia o laço em goroutine.
  - `OnStop` para de reivindicar e de iniciar itens. Os itens em voo terminam até
    `PENDING_WORKER_DRAIN_TIMEOUT` (ou o prazo do OnStop). Estourado o prazo, o contexto do
    processamento é cancelado (rollback; o lease expira e outra instância retoma) e o
    OnStop devolve `ErrDrainTimeout`.
  - O `Stop` sempre espera as goroutines do worker. O worker depende do caso de uso, que
    depende do pool, então para antes do pool.
  - Itens reivindicados e ainda não iniciados no shutdown ficam com o lease e voltam
    quando ele expirar (≤ `PENDING_WORKER_LEASE`).
- **Configuração** (validada no start; `.env.example`):

  | Variável | Default | Regra |
  |---|---|---|
  | `PENDING_REFERENCE_MAX_ATTEMPTS` | 10 | 1..2^31−1, buscas totais |
  | `PENDING_REFERENCE_TTL` | 10m | > 0, desde a criação |
  | `PENDING_REFERENCE_RETRY_BASE_DELAY` / `_MAX_DELAY` | 2s / 1m | 1ms..24h, base <= max |
  | `PENDING_WORKER_POLL_INTERVAL` | 1s | 1ms..24h |
  | `PENDING_WORKER_BATCH_SIZE` | 20 | 1..1000 |
  | `PENDING_WORKER_CONCURRENCY` | 4 | 1..100 |
  | `PENDING_WORKER_LEASE` | 30s | 1s..24h |
  | `PENDING_WORKER_PROCESS_TIMEOUT` | 5s | < `PENDING_WORKER_LEASE` |
  | `PENDING_WORKER_DRAIN_TIMEOUT` | 10s | > 0 |
  | `PENDING_ORPHAN_GRACE` | 30s | 0..24h |
- **Observabilidade.**
  - Logs: `transactionId`, `walletId`, `providerId`, `kind`, `previousStatus`, `status`,
    `attempts`, `failureCode`, `nextAttemptAt` e `outcome`. Resolução e expiração saem em
    INFO; reagendamento e skip em DEBUG; falhas em WARN.
  - Métricas: `wager_transactions_total{source="worker"}` (resolução, expiração e órfão
    que passou a aguardar) e `processing_duration_seconds{source="worker",
    operation="pending_reference", outcome}`. Os retries transitórios são contados pelo
    TxManager (`retries_total{operation="pending_reference"}`).
- **Limitações.**
  - Um item que falha sempre (ex.: dado corrompido que não reidrata) volta a cada lease,
    sem limite, com log WARN e métrica `outcome="error"`. Não há poison counter nem
    transição para `FAILED`.
  - Pendências não são "acordadas" quando a referência chega: a resolução espera o próximo
    `next_attempt_at`, que é no máximo `RETRY_MAX_DELAY` depois. O índice
    `wager_transactions_reference_external_idx` permite acrescentar isso depois.
- **Testes.**
  - Unitários (`go test -race`, UnitOfWork em memória que espelha claim, fencing e
    Update):
    - política (backoff, teto, overflow, validação);
    - item não vencido não é reivindicado;
    - reagendamento 4s/8s sem efeitos e com o lease solto;
    - resolução com ledger, eventos e metadados;
    - expiração por tentativas e por TTL, com o backoff limitado ao fim do TTL;
    - última tentativa ainda resolve;
    - `REFERENCE_NOT_PROCESSED`;
    - fencing (dono antigo não grava);
    - órfãos (BET concluída; REFUND sem referência → `PENDING_REFERENCE` + evento).
  - Unitários do worker (fake `Resumer`, `goleak.VerifyTestMain`): lotes com token
    próprio, limite de concorrência, erros de item, retry do claim, início barrado perto
    do fim do lease, drenagem, prazo de drenagem com cancelamento, `fx.ValidateApp` e
    ciclo Fx.
  - Integração do repositório: `ClaimDue` (ordem, carência, leases vigentes, claimer
    concorrente sem espera com `lock_timeout`, fencing, reassunção após expiração,
    `Update` solta o lease), 8 claimers × 60 linhas sem duplicidade, e `CHECK`s da
    `0007`. A migration é testada em up/down/up (`Down(4)`; `Down(5)` desde a `0008`).
  - Integração do worker (`pendingworker`, PostgreSQL real, pool próprio por instância,
    goleak ao fim):
    - **resolução após a chegada da referência**, com reagendamentos antes, um único
      lançamento e eventos, reconciliação, e replay devolvendo o estado final com o saldo
      da resolução;
    - **expiração** por tentativas e por TTL (`REFERENCE_NOT_FOUND`, um evento, sem
      ledger; a referência que chega depois não reabre);
    - **sobrevivência a restart**: worker parado e pool fechado preservam `attempts`; um
      item reivindicado por uma instância que "morreu" só é retomado depois do lease; um
      `PENDING` órfão é concluído pela nova instância;
    - **múltiplas instâncias**: 4 instâncias × (40 rollbacks + 5 órfãos), com as
      referências chegando em paralelo por instâncias diferentes. Cada pendência tem
      exatamente um lançamento e um `WagerTransactionProcessed`, nenhum `event_id`
      repetido, nenhum lease sobrando, e saldo = ledger.
- **Para a T23:**
  - Compor `pendingworker.Module` junto de `config.Module`, `postgres.Module` e
    `app.Module`.
  - Prover `app.WithReferenceRetryDelay(cfg.Workers.PendingReferenceRetryBaseDelay)` ao
    `ProcessWagerTransaction`. Hoje `app.Module` usa o padrão de 2s, igual ao default da
    config.
  - O `fx.StopTimeout` precisa cobrir também `PENDING_WORKER_DRAIN_TIMEOUT`.

## ADR-024 — Publisher da outbox (T22)

Pacote `internal/adapters/outboxpublisher` (`publisher.go`, `store.go`, `module.go`).
O repositório da T13 ganhou três mudanças: o claim com ordem por agregado, `Release` e
`Backlog`. A migration `0008` troca os índices do claim.

- **Ciclo (sem transação aberta durante I/O):**
  1. `Claim` em autocommit, sobre o pool. O lease conta do `statement_timestamp()` do
     próprio comando e já vale quando o lote volta.
  2. Publicação no SQS, fora de transação.
  3. `MarkPublished` ou `MarkFailed`, com fencing pelo lease `(seq, eventId, owner,
     attempts)` (ADR-015).

  Lote não vazio leva a um novo ciclo imediato, porque o próximo evento de cada agregado
  acabou de ficar elegível. Lote vazio ou falha no claim leva a uma espera de
  `OUTBOX_POLL_INTERVAL`. Não há estado em memória: qualquer instância retoma o trabalho de
  outra depois que o lease expira.
- **Roteamento (contrato da fila `wager-events.fifo`):**
  - `MessageGroupId = aggregateId`: id da transação nos eventos de transação, id da
    carteira em `WalletBalanceChanged` (ADR-012). Decisão mantida, e não `walletId` em
    todos, por três motivos:
    - o enunciado da T22 fixa `aggregateId`;
    - o ciclo `PENDING_REFERENCE` → `PROCESSED`/`REJECTED` de uma transação fica ordenado;
    - os saldos de uma carteira ficam em ordem total.

    Não há ordem entre grupos. Um consumidor que precise relacionar transação e saldo usa
    `walletVersion` (monotônica por carteira) e `transactionId` do `WalletBalanceChanged`.
  - `MessageDeduplicationId = eventId` (UUIDv7 gerado no construtor do evento e persistido
    na outbox).
  - Corpo: o snapshot JSON gravado no `Enqueue`, byte a byte. A republicação manda o mesmo
    corpo e o mesmo `eventId`.
  - Atributos SQS para rotear sem abrir o corpo: `eventType` (String), `eventVersion`
    (Number), `eventId` e `correlationId` (este quando presente).
  - **Consumo:** entrega at-least-once. O SQS FIFO descarta duplicatas do mesmo `eventId`
    dentro da janela de 5 minutos. Fora dela (lease expirado depois de muito tempo,
    backoff longo), o consumidor precisa deduplicar por `eventId`.
- **Ordem por agregado sob falha (pendência do ADR-015):**
  - O claim só devolve o evento não publicado mais antigo de cada agregado. A condição é
    `NOT EXISTS (SELECT 1 FROM outbox prev WHERE prev.aggregate_id = c.aggregate_id AND
    prev.seq < c.seq AND prev.published_at IS NULL)`. Enquanto o evento N estiver em
    lease, em backoff ou não marcado, N+1 do mesmo agregado não sai. Os outros agregados
    seguem.
  - A condição só depende de `published_at`, que só muda pelo `MarkPublished` do detentor
    do lease. Um snapshot antigo só pode ser mais restritivo, então a condição é segura
    entre claimers concorrentes sem serializar os claims (a T13 cogitava serializar). Por
    isso não há lock global.
  - Dentro de um agregado, `seq` é a ordem de commit: os eventos de uma carteira ou
    transação são gravados com a carteira travada, então N sempre é confirmado antes de
    N+1 (ADR-005/014).
  - Consequência: um lote tem no máximo um evento por agregado. Os eventos do lote são
    publicados em paralelo (`OUTBOX_CONCURRENCY`) sem quebrar a ordem.
  - Custo: um agregado com fila longa avança um evento por ciclo. Como lote não vazio
    dispara o ciclo seguinte na hora, isso dá cerca de um round-trip (claim + send + mark)
    por evento do mesmo agregado.
- **Falhas:**
  - **Envio falhou:** `MarkFailed` grava `last_error` e `next_attempt_at = agora +
    min(base·2^(attempt−1), teto)`, persistido (`OUTBOX_RETRY_BASE_DELAY` 1s,
    `OUTBOX_RETRY_MAX_DELAY` 5min). Conta em `retries_total{operation="outbox_publish"}`.
  - **Resposta sem `MessageId`:** tratada como falha.
  - **Sem limite de tentativas nem DLQ:** descartar um evento confirmado violaria a
    garantia de não perda (README §3). Um evento que falha sempre bloqueia só o seu
    agregado, e isso aparece em `outbox_oldest_pending_age_seconds` e em `last_error`.
    `attempts` (INTEGER) cresce cerca de uma vez por teto de backoff (5min), então não
    estoura na prática.
  - **Publicado, mas a marcação falhou:** se o lease foi perdido (`ErrOutboxLeaseLost`,
    outcome `conflict`) ou o banco caiu (outcome `transient_error`), o evento continua não
    publicado. O lease expira e outra instância (ou a mesma) republica com o mesmo
    `eventId`. O fencing impede que a marcação atrasada sobrescreva a do novo detentor.
- **Prazos:**
  - `OUTBOX_PUBLISH_TIMEOUT` limita cada chamada de um evento (`SendMessage`; marcação).
  - Um item só é iniciado se envio e marcação ainda couberem no lease
    (`claimedAt + lease − 2·timeout`, medido no relógio local antes do claim, ou seja, de
    forma conservadora). Os que não cabem voltam por `Release` (lease solto, sem backoff
    e sem `last_error`).
  - A config exige `2·OUTBOX_PUBLISH_TIMEOUT < OUTBOX_LEASE` e `OUTBOX_LEASE >
    OUTBOX_POLL_INTERVAL`. O lease vai de 1s a 1h (limite do repositório).
- **Shutdown (`fx.Lifecycle`):**
  - `Stop` para de reivindicar e de iniciar itens, e devolve (`Release`) os itens
    reivindicados ainda não iniciados.
  - Os envios em voo terminam até `OUTBOX_DRAIN_TIMEOUT`. Estourado o prazo, o contexto
    das publicações é cancelado e `Stop` devolve `ErrDrainTimeout`. O lease fica, expira, e
    outra instância republica com o mesmo `eventId`.
  - `Stop` sempre espera as goroutines, salvo se o próprio ctx do OnStop vencer.
  - O publisher depende do pool e do cliente SQS, construídos antes dele, então o Fx o
    para antes de fechar os dois.
- **Owner:** `hostname-UUIDv7`, único por processo. Duas réplicas no mesmo host não se
  confundem, e o `attempts` do lease já distingue claims do mesmo owner.
- **Métricas:**
  - `outbox_publish_delay_seconds`: `now − occurred_at` na publicação confirmada (atraso
    fim a fim, com retries). A ajuda da métrica passou de "criação" para "ocorrência".
  - `outbox_pending_events` e `outbox_oldest_pending_age_seconds`: vêm do
    `OutboxRepository.Backlog` (`count(*)`, `min(occurred_at)` e relógio do banco), medido
    no máximo uma vez por `OUTBOX_POLL_INTERVAL` por instância.
  - `processing_duration_seconds{source="worker",operation="outbox_publish",outcome}`.
- **Migration `0008`:** remove `outbox_unpublished_due_idx` (`next_attempt_at, seq`), que
  não servia ao `ORDER BY seq` nem ao `NOT EXISTS`. Cria `outbox_unpublished_seq_idx
  (seq)` e `outbox_unpublished_aggregate_seq_idx (aggregate_id, seq)`, ambos parciais em
  `published_at IS NULL`. O down recria o índice antigo.
- **Config** (`config.WorkersConfig`, validada no start, documentada em `.env.example`):
  `OUTBOX_POLL_INTERVAL` (1ms–24h), `OUTBOX_BATCH_SIZE` (1–1000), `OUTBOX_LEASE` (1s–1h),
  `OUTBOX_CONCURRENCY` (1–100), `OUTBOX_PUBLISH_TIMEOUT`, `OUTBOX_DRAIN_TIMEOUT`,
  `OUTBOX_RETRY_BASE_DELAY` e `OUTBOX_RETRY_MAX_DELAY` (1ms–24h, teto ≥ base).
  `SQS_EVENTS_QUEUE_URL` aponta para `wager-events.fifo`.
- **Fx:** `outboxpublisher.Module` provê:
  - `*PostgresStore` como `Store`, sobre o `*pgxpool.Pool` e o `*OutboxRepository` do
    `postgres.Module`, com o backoff da config;
  - `Sender`, a partir de `sqs.API` (`sqs.ClientModule`);
  - o `*Publisher`, com OnStart/OnStop e `fx.Invoke`.

  Métricas e logger são opcionais. A interface `Sender` é estreita (só `SendMessage`) e
  permite os testes com fila FIFO falsa.
- **Logs:** `eventId`, `eventType`, `aggregateId`, `attempt`, `correlationId` (pelo
  contexto) e `walletId` ou `transactionId`, conforme o tipo. Payloads nunca vão para o
  log.
- **Testes:**
  - Unitários (`go test -race`, `goleak.VerifyTestMain`, `Store` e fila FIFO em memória):
    - validação de config e owner;
    - montagem da mensagem (grupo, deduplicação, corpo e atributos);
    - publicação e marcação, falha de envio que vira `MarkFailed` + retry, resposta sem
      `MessageId`, lease perdido depois do envio (sem `MarkFailed`, outcome `conflict`) e
      erro de claim;
    - itens perto do fim do lease devolvidos;
    - laço Start/Stop com métricas de backlog, drenagem que conclui o envio em voo e
      devolve o resto do lote, drenagem estourada (`ErrDrainTimeout`, nada marcado) e ctx
      do OnStop vencido;
    - `fx.ValidateApp` (`postgres.Module` + `sqs.ClientModule` + `Module`) e ciclo Fx.
  - Integração do repositório: head-of-line (cabeças em lease ou em backoff retêm o
    agregado e os outros seguem; `Release` sem backoff; `Backlog`), 6 claimers × 8
    agregados × 6 eventos sem dois eventos do mesmo agregado em voo e em ordem de `seq`,
    e índices da `0008` com `Down(5)`.
  - Integração do publisher (PostgreSQL real, pool próprio por instância, fila FIFO falsa
    com deduplicação, goleak ao fim):
    - **dois publishers sem perda:** 96 eventos (12 carteiras × 8) com metade confirmada
      durante a publicação e 20% dos envios falhando. Cada `eventId` entra na fila uma
      vez, cada grupo sai na ordem de `seq`, os dois publishers publicam e tudo termina
      marcado;
    - **crash entre commit e publicação:** evento sem publisher vivo, e evento
      reivindicado por uma instância que morreu antes de enviar. Outra instância publica,
      o segundo só depois do lease (`attempts = 2`), com o mesmo `eventId` e corpo;
    - **crash entre publicação e marcação:** a republicação, depois do lease, repete
      `eventId` e corpo, a fila descarta a duplicata, o evento fica marcado pelo novo
      detentor e a marcação tardia da instância morta é recusada. O 2º evento do agregado
      só sai depois, em ordem;
    - **lease perdido durante o envio:** o envio lento chega depois que outra instância
      republicou e marcou. A marcação atrasada não altera nada;
    - **ordem sob falha:** o 1º evento de X em backoff persistido (`last_error`,
      `next_attempt_at`) retém o 2º, Y segue, e X sai em ordem quando o SQS volta;
    - métricas de backlog e atraso; composição Fx real com `goleak.VerifyNone`.
  - SQS real (LocalStack, pulado sem `SQS_ENDPOINT`): fila FIFO exclusiva, crash entre
    publicação e marcação, e na fila exatamente dois eventos em ordem, com
    `MessageGroupId`, `MessageDeduplicationId`, corpo e atributos conferidos. Não foi
    executado nesta máquina (sem Docker/LocalStack).
- **Para a T23:** compor `outboxpublisher.Module` com `config.Module`, `postgres.Module` e
  `sqs.ClientModule` (já incluído em `sqs.Module`). Prover `*metrics.Metrics` e
  `*slog.Logger`. O `fx.StopTimeout` precisa cobrir também `OUTBOX_DRAIN_TIMEOUT`.
- **Para a T24:** provisionar `wager-events.fifo` (`FifoQueue=true`,
  `ContentBasedDeduplication=false`; a deduplicação é por `eventId`). Dar à aplicação
  permissão de `sqs:SendMessage` nessa fila e aos consumidores de eventos
  `ReceiveMessage`/`DeleteMessage`. Rodar `TestIntegrationRealSQSPublishDedupAndRouting`
  com `SQS_ENDPOINT`.

## ADR-025 — Composição Fx da aplicação e ordem de parada (T23)

- **Composição (`cmd/server.Options`).** `logging.Module` + `logging.FxEventLogger()` (eventos
  do Fx no mesmo logger JSON/slog), `metrics.Module`, `postgres.Module`, `app.Module`,
  `auth.Module`, `outboxpublisher.Module`, `pendingworker.Module`, `sqs.Module` (que já inclui
  `sqs.ClientModule`; ele não é repetido), `httpadapter.Module`, `httpserver.Module` e as
  ligações que nenhum módulo conhece sozinho:
  - `*metrics.Metrics` como `app.ReconciliationRecorder` (ADR-018);
  - `app.ReferenceRetryDelay` = `PENDING_REFERENCE_RETRY_BASE_DELAY`: o
    `ProcessWagerTransaction` agenda a primeira nova busca com a mesma base do backoff do
    worker (ADR-023). Ausente, `app.Module` mantém o default de 2s;
  - `httpadapter.MetricsAccess` de `METRICS_ACCESS` (`internal`, padrão: exige token do
    serviço interno; `public`: sem autenticação, para scrapers sem OAuth em rede privada);
  - a verificação de prontidão `postgres` (`db.Ping`) no grupo `readinessChecks`, ao lado de
    `sqs` (consumidor) e `http-server` (drenagem).
- **Config antes do `fx.New`.** `main` carrega a config (`config.LoadFromEnv`) e a injeta com
  `fx.Supply`, em vez de `config.Module`, porque `fx.StartTimeout`/`fx.StopTimeout` são opções
  do `fx.New` e dependem dela. Config inválida → log JSON em stderr (sem valores sensíveis) e
  exit 1, sem abrir conexões.
- **Ordem de start/stop.** O Fx registra os hooks na ordem em que os construtores rodam e
  executa os OnStop na ordem **inversa**, em **sequência**, todos com o mesmo ctx. Pool pgx,
  cliente SQS e transporte do JWKS são dependências de quem os usa, então são sempre
  construídos antes e parados depois. Entre componentes independentes, a ordem vem da ordem
  dos módulos (os `fx.Invoke` dos módulos rodam na ordem de declaração): workers e consumidor
  vêm antes do servidor HTTP, para que o servidor seja o **primeiro** a parar, e o publisher
  da outbox vem antes do worker de pendências, para que o worker pare **antes** do publisher
  (eventos gravados na drenagem do worker ainda saem por esta instância). Ordem real,
  verificada pelo teste de integração:

  | Stop | Hook (construtor que o registrou) |
  |---|---|
  | 1 | servidor HTTP (`httpserver.NewFx`): readiness DOWN, drenagem, `Shutdown` |
  | 2 | transporte HTTP do JWKS (`auth.newAuthenticatorFx`): fecha conexões ociosas com o IdP |
  | 3 | consumidor SQS (`sqs.NewConsumerFx`): para de receber, drena, libera visibilidade |
  | 4 | worker de pendências (`pendingworker.NewWorkerFx`) |
  | 5 | publisher da outbox (`outboxpublisher.NewPublisherFx`) |
  | 6 | cliente SQS (`sqs.NewClientFx`): fecha conexões ociosas |
  | 7 | pool pgx (`postgres.newDBWithLifecycle`): `Close`, por último |

  O start é o inverso: o pool faz ping primeiro e o servidor só escuta depois que consumidor,
  publisher e worker iniciaram. Uma falha de start (ex.: fila sem RedrivePolicy, ou SQS/banco
  indisponível até o fim do orçamento) faz o Fx executar os OnStop dos hooks já iniciados
  (rollback): workers param, o pool fecha e o processo sai com 1 (ver `fx.StartTimeout`).
- **Transporte do JWKS.** `auth.Module` passou a criar o `Authenticator` com um
  `http.Transport` próprio (clone do padrão) e a fechar as conexões ociosas no OnStop. Antes,
  a conexão keep-alive com o IdP sobrevivia ao Stop (goroutines do net/http, detectadas pelo
  goleak).
- **`fx.StopTimeout`.** Como os OnStop rodam em sequência, o pior caso é a **soma** dos
  orçamentos: `SHUTDOWN_TIMEOUT` (servidor, já inclui `HTTP_DRAIN_DELAY`) +
  `SQS_DRAIN_TIMEOUT` + `PENDING_WORKER_DRAIN_TIMEOUT` + `OUTBOX_DRAIN_TIMEOUT` + folga de 15s.
  A folga cobre a liberação das mensagens SQS depois do estouro da drenagem (5s por chamada,
  em paralelo), a última marcação da outbox (`OUTBOX_PUBLISH_TIMEOUT`) e o fechamento do pool
  e dos clients. Com os defaults: 15 + 10 + 10 + 10 + 15 = **60s**. *(Revisado na T24: a
  parcela do SQS passou a ser `sqs.StopBudget` = max(`SQS_DRAIN_TIMEOUT`, `SQS_WAIT_TIME` +
  5s), porque o Stop espera o último long polling (ADR-022, "Shutdown"). Com os defaults:
  15 + 25 + 10 + 10 + 15 = **75s**. A reserva de rollback passa a 75s e o `StartTimeout` a
  120s.)* Se o ctx vencesse antes, o
  Fx abandonaria os hooks restantes e o pool/clients não fechariam em ordem. O orquestrador
  precisa dar ao processo pelo menos esse tempo entre SIGTERM e SIGKILL (no Compose,
  `stop_grace_period`, cujo default é 10s; T24).
- **`fx.StartTimeout` e reserva de rollback.** O Fx executa o rollback de um start que falhou
  com o **mesmo** ctx do start (`app.withRollback`), e o `lifecycle.Stop` não executa nenhum
  OnStop se esse ctx já venceu (e o lifecycle fica `stopped`: um `app.Stop` posterior também
  não faz nada). Um OnStart que repetisse até o prazo do ctx (ping do pool, chamadas iniciais
  ao SQS) deixaria pool, worker e publisher rodando até o `os.Exit`. Por isso o prazo é
  dividido: **orçamento** (`StartBudget` = `DATABASE_CONNECT_TIMEOUT` + 15s, default 45s) +
  **reserva de rollback** (`StartReserve` = `StopTimeout`, o pior caso de parar o que já
  iniciou; default 60s) = `StartTimeout` (default 105s). `cmd/server` fornece a reserva
  (`startbudget.Reserve`) ao grafo; `postgres.DB.Start` e `sqs.Consumer.Start` derivam um ctx
  com prazo = prazo(ctx) − reserva (`startbudget.Context`) e desistem nele, devolvendo o erro
  com a reserva inteira para o rollback. Os demais OnStart (listen do HTTP, goroutines dos
  workers) não repetem e retornam na hora. Optou-se por isso, e não por um `fx.Decorate` do
  `fx.Lifecycle` que embrulhasse todos os OnStart, porque o wrapper viraria o "caller" de
  todos os hooks nos eventos do Fx (logs e ordem de parada ficariam ilegíveis).
- **Sinais e exit codes.** `runApp` equivale a `fx.App.Run`, mas devolve o código em vez de
  chamar `os.Exit`: `app.Wait()` recebe SIGINT/SIGTERM (exit 0 com parada limpa) ou um
  `fx.Shutdowner.Shutdown(fx.ExitCode(n))` (ex.: o `Serve` do HTTP morreu → 1). Start ou stop
  com erro → 1. Os erros de start/stop são logados pelo logger de eventos do Fx.
- **Testes.**
  - Unitários (`cmd/server`): `fx.ValidateApp(Options(cfg)...)`; `StopTimeout` = soma +
    folga; o `fx.App` usa os prazos calculados; ligações (readiness `postgres`/`sqs`/
    `http-server`, `ReferenceRetryDelay` da config, recorder de reconciliação);
    `METRICS_ACCESS`; exit codes de `runApp` (Shutdowner, parada limpa, falha de start com
    rollback, falha de stop); `StartTimeout` = orçamento + reserva (= `StopTimeout`); um
    OnStart que repete até o prazo (com a reserva) falha a tempo de o Fx executar o OnStop já
    iniciado. `startbudget`, `postgres.DB.Start` e `sqs.Consumer.Start` têm testes de que
    desistem antes da reserva.
  - Integração (`-tags=integration`, banco novo com migrations): a aplicação completa sobe
    com `fx.Decorate` trocando `sqs.API` por uma fila em memória e um IdP `httptest`
    (`OIDC_ISSUER_URL`/`OIDC_JWKS_URL`). O teste faz requisições reais a `/health/live`,
    `/health/ready` (com `postgres`, `sqs` e `http-server`), `POST /wallets` e
    `GET /wallets/{id}` (401 sem token, 201/200 com token interno) e `/metrics` (401/200).
    Espera o publisher publicar o evento da abertura na fila de eventos falsa e para dentro
    do `StopTimeout`. Confere então a ordem dos OnStop (por um `fxevent.Logger` que os
    registra), que o pool foi fechado (`Ping` falha), que senha e token não aparecem nos logs
    e `goleak.VerifyNone`. Outros testes cobrem o rollback de um start que falha: fila
    inexistente (falha imediata) e SQS indisponível (erro transitório repetido até esgotar
    um orçamento de 2s, via `runApp`): exit 1, `fxevent.RolledBack` sem erro, OnStop de
    worker, publisher, cliente SQS e pool (por último), pool fechado e sem vazamento.
  - `TestIntegrationRealDependencies` usa LocalStack e Keycloak reais (`SQS_ENDPOINT`,
    `KEYCLOAK_URL`, `KEYCLOAK_CLIENT_SECRET`; Skip sem eles) e roda na T24. Para a aplicação
    num `defer` se algo falhar depois do start e confere que client secret, token e senha do
    banco não aparecem nos logs.

## ADR-026 — Dockerfile, Docker Compose e provisionamento (T24)

- **Imagem (`Dockerfile`).** Multi-stage: `golang:1.25-alpine` (a diretiva `go 1.25.0` do
  go.mod; `GOTOOLCHAIN=local` faz o build falhar em vez de baixar outro toolchain se o go.mod
  subir), `CGO_ENABLED=0`, `-trimpath -ldflags="-s -w"`, cache de módulos/build via BuildKit.
  Uma imagem com os dois binários: `server` (entrypoint) e `migrate` (job). Runtime
  `alpine:3.22` com usuário não-root (uid 10001). Alpine e não distroless para ter o `wget`
  do busybox no healthcheck sem um binário extra. `.dockerignore` restringe o contexto a
  `go.mod/go.sum`, `cmd/`, `internal/` e `migrations/` (nada de `.env`/`.git`).
- **Serviços (`docker-compose.yml`, projeto `junglegaming-wallet`).**

  | Serviço | Imagem | Host | Pronto quando |
  |---|---|---|---|
  | `postgres` | `postgres:17-alpine` (mesma dos testcontainers) | 5433 | `pg_isready` |
  | `keycloak` | `quay.io/keycloak/keycloak:26.0`, `start-dev --import-realm` | 8081 | discovery do realm `junglegaming` responde (realm importado) |
  | `localstack` | `localstack/localstack:4.4` (`SERVICES=sqs,iam,sts`) | 4566 | init hook terminou (marcador) e `/_localstack/health` ok |
  | `migrate` | imagem da app, `migrate up` | — | sai com 0 (`service_completed_successfully`) |
  | `app` | imagem da app | — (só `expose: 8080`) | `GET /health/ready` (postgres, sqs, http-server) |
  | `lb` | `nginx:1.28-alpine` | 8080 | `/lb/health` |

  `app` depende de `postgres`/`localstack`/`keycloak` com `service_healthy` e de `migrate` com
  `service_completed_successfully`; `lb` depende de `app` saudável. `docker compose up --build
  -d --wait` só retorna com tudo saudável (o job encerrado com 0 não quebra o `--wait`).
  O PostgreSQL é publicado em **5433** porque 5432 costuma estar ocupada por um PostgreSQL
  local. O healthcheck do Keycloak usa `/dev/tcp` do bash (a imagem não tem curl/wget) e
  precisa mandar `Host`: sem ele o Keycloak 26 responde 500 (NPE em `authority()`).
- **Escala (`--scale app=3`).** As réplicas não publicam porta, então não há conflito no
  host. O nginx publica a API em `localhost:8080`. `server app:8080 resolve` (nginx ≥ 1.27.3,
  com `zone` e `resolver 127.0.0.11 valid=5s`) re-resolve o DNS do Docker: réplicas
  acrescentadas ou removidas entram e saem do round-robin sem reiniciar o nginx (verificado:
  9 requisições → 3 por réplica, `upstream=` no access log). Com
  `proxy_next_upstream error timeout http_502`, uma réplica parando (listener fechado →
  conexão recusada) faz a requisição ir para outra. Requisições não idempotentes só são
  repetidas se não chegaram a ser enviadas (padrão do nginx), então um POST não é duplicado.
  Mesmo que fosse, a idempotência por chave cobriria (ADR-010). Medido: 60 requisições
  seguidas durante `--scale app=2` (a partir de 3) e durante `docker stop` de uma réplica →
  60× 200. Limitação observada: depois que o container some, o IP antigo fica no cache de
  DNS do nginx por até 5s. Com o nginx ocioso, a primeira requisição nesse intervalo pode ir
  para o IP morto, esperar o `proxy_connect_timeout` (2s) e voltar 502 ("no live upstreams").
  Isso não acontece com o listener fechado durante a drenagem, porque a conexão é recusada na
  hora e a requisição segue para outra réplica. Alternativa descartada: faixa
  de portas no host (`8080-8082:8080`), que expõe N endpoints e deixa o balanceamento para o
  cliente. As migrations rodam uma vez, no job; o runner já é seguro em paralelo (advisory
  lock, ADR-011).
- **Parada.** `stop_grace_period: 90s` ≥ `fx.StopTimeout` = SHUTDOWN_TIMEOUT 15s +
  max(SQS_DRAIN_TIMEOUT 10s, SQS_WAIT_TIME 20s + 5s) + PENDING_WORKER_DRAIN_TIMEOUT 10s +
  OUTBOX_DRAIN_TIMEOUT 10s + 15s = 75s (ADR-025; a parcela do SQS vem da correção do
  shutdown do consumidor, ADR-022). O default do Docker (10s) mataria o processo no meio da
  drenagem. Esses prazos, incluindo `SQS_WAIT_TIME`, ficam explícitos no `x-app-env` do
  Compose. `docker stop` de uma réplica ociosa sai com 0 em até ~`SQS_WAIT_TIME`: o
  consumidor espera o último long polling terminar (medido: 7s e 19s, exit 0).
- **Issuer OIDC (host × rede do Compose).** O app valida `iss`. O Keycloak roda com
  `KC_HOSTNAME=http://localhost:8081` (hostname v2, URL completa) e
  `KC_HOSTNAME_BACKCHANNEL_DYNAMIC=true`. Assim **todo** token tem
  `iss=http://localhost:8081/realms/junglegaming`, seja pedido do host, seja de dentro da rede.
  O app usa `OIDC_ISSUER_URL=http://localhost:8081/realms/junglegaming`, que só é comparado
  com o `iss` e nunca acessado, e
  `OIDC_JWKS_URL=http://keycloak:8080/realms/junglegaming/protocol/openid-connect/certs`
  (chaves pela rede interna, sem discovery). Um token obtido no host (curl em
  `localhost:8081`) é aceito pelas réplicas, e os testes de integração no host usam o
  discovery normal em `localhost:8081`. Com o app fora do Compose (no host), `.env.example`
  aponta os dois para `localhost:8081`.
- **Realm (`deploy/keycloak/realm-junglegaming.json`).** Papéis de realm `wager-provider` e
  `wallet-internal`. Os clients são confidenciais e só usam client_credentials (service
  account; sem standard flow nem direct grants):
  - `provider-a` e `provider-b`: papel `wager-provider` e mapper hardcoded `provider_id` = id
    do client (String, só no access token);
  - `internal-service`: papel `wallet-internal`, sem `provider_id`.

  Os três têm um audience mapper com `included.custom.audience=wallet-api`
  (= `OIDC_AUDIENCE`). Os papéis saem em `realm_access.roles` pelo client scope padrão
  `roles`, e `typ=Bearer`. **Os segredos (`provider-a-dev-secret`, `provider-b-dev-secret`,
  `internal-service-dev-secret`), o admin `admin/admin` e a senha do banco são valores fixos
  de DESENVOLVIMENTO**, versionados de propósito; o `displayName` do realm diz isso. Nenhum
  `.env` real é versionado (`.gitignore`). A senha do banco é `wallet-dev-pw`, e não
  `wallet`, porque o teste da composição confere que a senha não aparece nos logs, e
  `wallet` é o nome do serviço em todo log.
- **SQS (`deploy/localstack/init-sqs.sh`, hook `ready.d`).** O script é idempotente:
  `create-queue` só com `FifoQueue=true` e o resto via `set-queue-attributes`.
  Com `SQS_ENDPOINT_STRATEGY=path` o LocalStack devolve URLs
  `http://<host>:4566/queue/us-east-1/000000000000/<fila>` (sem DNS `*.localstack.cloud`),
  mas também aceita a forma `http://<host>:4566/000000000000/<fila>`, que é a usada no app e
  nos testes (verificado: réplicas saudáveis consumindo/publicando com ela).
  - `wager-transactions-dlq.fifo`: FIFO, retenção de 14 dias, `RedriveAllowPolicy` byQueue
    só para a fila de entrada.
  - `wager-transactions.fifo`: FIFO, `ContentBasedDeduplication=false` (produtores mandam
    `MessageGroupId=walletId` e `MessageDeduplicationId=messageId`), `VisibilityTimeout=30`,
    `RedrivePolicy` com `maxReceiveCount=5` → DLQ. O consumidor recusa o start sem ela
    (`ErrNoRedrivePolicy`) e exige `SQS_MAX_ATTEMPTS` (3) < 5 (ADR-022).
  - `wager-events.fifo`: FIFO, `ContentBasedDeduplication=false` (dedup por `eventId`,
    ADR-024).
  - Credenciais e políticas: o app usa `test/test` (conta `000000000000`). Cada fila tem uma
    `Policy` de recurso restrita à conta, e o usuário IAM `wallet-app` recebe a política
    mínima (Receive/Delete/ChangeVisibility/GetAttributes na entrada, SendMessage na DLQ e
    nos eventos). O LocalStack Community **não aplica IAM**: isso documenta, de forma
    executável, o que a AWS exigiria.
  - O healthcheck só passa depois do marcador gravado no fim do script, então o app nunca
    sobe antes de as filas e o redrive existirem.
- **Verificação.**
  - `make compose-up` / `make compose-up-scaled` (`REPLICAS=3`).
  - `make compose-smoke` (`scripts/compose-smoke.sh`): obtém do host tokens
    client_credentials dos três clients e confere `iss`/`aud`/`provider_id`/papel. Via nginx:
    401 sem token, 201 em `POST /wallets` com `internal-service`, 200 na consulta, 403 para
    `provider-a` na rota de carteira e 404 (autenticado) para `provider-a` em transação
    inexistente. Confere os atributos FIFO, RedrivePolicy e ContentBasedDeduplication via
    `docker compose exec localstack awslocal`.
  - `make test-integration-compose`: `-tags=integration -race -p 1`, com
    `TEST_DATABASE_URL` no PostgreSQL do Compose, `SQS_ENDPOINT`, `KEYCLOAK_URL` e o segredo
    de dev do `internal-service`.

  `.gitattributes` força LF em `.sh`/`.conf`/`.json`/`.yml`/Dockerfile, que são montados em
  containers Linux a partir de um checkout Windows com `core.autocrlf=true`.
- **Achado nos testes contra o LocalStack real (resolvido na T24).**
  `TestIntegrationConsumerShutdownReleasesVisibility` (T20) falhava sempre nesta máquina. No `Stop`, o consumidor cancela no cliente o `ReceiveMessage` em long
  polling, mas o servidor mantém a chamada até o fim do `WaitTime`. A mensagem liberada
  (visibilidade 0) é entregue a essa chamada abandonada e fica invisível pelo
  `VisibilityTimeout` inteiro (30s), o que estoura a espera do teste. Reproduzido só com
  `awslocal`: receive abandonado + `change-message-visibility 0` → 0 visíveis, 1 em voo. Na
  AWS real o comportamento é o mesmo. Não há perda (at-least-once: a mensagem volta depois
  da visibilidade), mas a "reentrega imediata" do shutdown não era garantida. **Correção
  aplicada** (ADR-022, "Shutdown"): o `Stop` não cancela mais o receive em curso. Ele espera
  o receive terminar (até `SQS_WAIT_TIME` + 5s, em paralelo com a drenagem), libera o que
  ele trouxer e só libera as mensagens depois dele. `fx.StopTimeout` e `stop_grace_period`
  foram ajustados (75s e 90s, item "Parada"). Agora o teste passa contra o LocalStack
  (`-count=3`, junto com o pacote inteiro).

## ADR-027 — Testes de concorrência obrigatórios (T25)

- **Onde.** Os cenários do README §8 e de "Testes de concorrência e recuperação" (itens 1–3 e
  o cruzamento HTTP × SQS) rodam pela **aplicação completa** (`cmd/server.Options`, o mesmo
  grafo Fx do binário) em `cmd/server/concurrency_integration_test.go`: servidor HTTP real com
  o middleware OIDC (IdP de teste do pacote, que assina tokens de serviço interno e de
  provedor com `provider_id` e papel `wager-provider`), casos de uso e repositórios PostgreSQL
  reais, consumidor SQS real. Banco exclusivo por teste (`TEST_DATABASE_URL`). Nenhum lock,
  fila ou deduplicação em memória participa do resultado. O nível do caso de uso já tinha
  disputa 80/80, 7.00 × 30 e duplicatas paralelas (T15). A T25 acrescenta em
  `internal/app/reversal_concurrency_integration_test.go` as reversões concorrentes da mesma
  referência.
- **Cenários.**
  - 50 POSTs paralelos da mesma aposta (mesma chave e corpo): um débito, uma resposta não
    replay, as 50 com a mesma transação e o saldo do processamento original (75.00).
    Reenvios posteriores não gravam eventos.
  - 80.00 × 2 sobre 100.00, em 5 carteiras seguidas para variar as intercalações:
    1 PROCESSED (200, saldo 20.00), 1 REJECTED `INSUFFICIENT_FUNDS` (422), um débito, versão 2.
    Cinco reenvios de cada, em paralelo, devolvem replays idênticos (status, código e
    transação) e não gravam outbox.
  - Carteiras distintas: com a linha de uma carteira travada (`FOR UPDATE` numa conexão do
    teste), as apostas das outras 7 concluem enquanto a da carteira travada continua
    esperando. Ela só conclui depois do unlock. Isso mostra que a coordenação é por linha de
    carteira, sem lock global. Depois, a disputa 80/80 nas 8 carteiras ao mesmo tempo
    (16 envios): cada carteira tem o seu resultado correto.
  - HTTP × SQS, mesma operação (LocalStack real, `SQS_ENDPOINT`; sem ele o teste é pulado):
    25 POSTs e 25 entregas SQS simultâneas. As entregas têm 20 `messageId` distintos e 5
    reentregas do mesmo `messageId`, cada uma com `MessageDeduplicationId` e `MessageGroupId`
    próprios, para o SQS não deduplicar nem serializar. Ou seja, o teste usa de propósito o pior
    caso de produtor, fora do contrato. Resultado: uma transação, um débito, 21 linhas na
    inbox (uma por `messageId`) apontando para ela. A fila e a DLQ terminam vazias, o que
    também prova que o hash de idempotência do HTTP e o do SQS coincidem (ADR-010).
  - HTTP × SQS, 80/80: em 4 carteiras ao mesmo tempo, uma aposta vai pelo HTTP e a outra pelo
    SQS (`MessageGroupId = walletId`, contrato do ADR-022). Em cada carteira, uma PROCESSED e
    uma `INSUFFICIENT_FUNDS`. Depois cada operação é reenviada pelo **outro** transporte. Os
    reenvios são replays, sem efeito e sem outbox novo.
  - Reversões concorrentes da mesma referência (pendência da T15/T21): 12 ROLLBACK/REFUND
    alternados, com externalIds distintos, sobre uma BET processada. Resultado: uma PROCESSED e
    11 REJECTED `ALREADY_REVERSED`, um crédito, replays idênticos. Na variante em que as 10
    reversões disputam com a própria BET, as que chegam antes ficam `PENDING_REFERENCE`. Três
    "workers" (`Claim`/`Resume` em paralelo) as retomam, e no fim há exatamente uma reversão
    PROCESSED, nenhuma pendência e saldo 100.00. A proteção é o lock da carteira + a regra
    `ALREADY_REVERSED`, com `wager_transactions_reversal_once_key` como última barreira.
- **Reconciliação final (todo cenário de `cmd/server`).** Feita no banco inteiro do teste:
  saldo armazenado == créditos − débitos do ledger em toda carteira, cadeia
  `balance_before`/`balance_after` íntegra por `seq`, nenhum saldo negativo, no máximo um
  lançamento por transação, lançamentos só de transações PROCESSED, nenhuma operação externa
  duplicada. Na outbox, um `WalletBalanceChanged` por lançamento e exatamente um evento
  terminal coerente com o status de cada transação (PROCESSED ↔ `WagerTransactionProcessed`,
  REJECTED ↔ `WagerTransactionRejected`). Além disso, `POST /wallets/{id}/reconciliation`
  devolve `consistent: true` e diferença 0.00 para cada carteira.
- **Resultado.** Nenhum defeito de concorrência foi encontrado. Não houve mudança no código de
  produção. `go test -race -tags=integration -count=3` passa nos pacotes novos e na suíte
  inteira (`-p 1`). Os cenários com várias instâncias independentes e com falhas ficam para a T26.
