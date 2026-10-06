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
- **Fila de eventos:** `.env.example` usa `wallet-events.fifo` e `task_plan.md` (T24) cita
  `wager-events.fifo`. Fica valendo o nome de `.env.example`. O código só exige uma URL FIFO,
  então a escolha está na configuração e no provisionamento (T24), não no código.
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
  start, mas só repete falha transitória (conexão recusada, `57P03`, timeout). Falha permanente
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
  `WagerTransactionRepository.GetByIDForUpdate` e `ClaimDuePendingReferences`) recusam um
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
  - `ClaimDuePendingReferences(now, limit)`: `status = 'PENDING_REFERENCE' AND
    next_attempt_at <= now ORDER BY next_attempt_at, id LIMIT n FOR UPDATE SKIP LOCKED`,
    com lote de 1 a 1000 (`MaxClaimBatch`). Instâncias concorrentes pegam lotes
    disjuntos sem esperar umas pelas outras, e o lock vale até o commit, no qual o worker
    grava o reagendamento ou a conclusão. Para a T21: a ordem de locks precisa ser
    consistente. O worker deve travar a carteira (`GetForUpdate`) antes de mexer em
    saldo, e um deadlock eventual (`40P01`) é retentável pelo TxManager.
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
- **Limitação para a T22:** a ordem global por `seq` não impede que eventos posteriores do
  mesmo `MessageGroupId` sejam publicados enquanto um anterior está em backoff. Garantir a
  ordem por grupo sob falha exige bloqueio head-of-line por grupo no claim, por exemplo um
  `NOT EXISTS` de evento anterior não publicado do mesmo grupo, combinado com a
  serialização dos claims. Isso será decidido junto com a escolha do `MessageGroupId`,
  hoje `aggregateId`.
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
