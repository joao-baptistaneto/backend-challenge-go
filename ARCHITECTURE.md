# ARCHITECTURE

Decisões arquiteturais do serviço de carteira (desafio Junglegaming).

O documento tem duas partes:

- **Parte I — Síntese (seções 1 a 18).** Uma seção por tema exigido no enunciado, com o estado
  **atual** do código, valores padrão e links para os ADRs detalhados. Quando um ADR antigo
  diverge da Parte I (valores revisados depois, pendências já resolvidas), vale a Parte I.
- **Parte II — Registro de decisões (ADR-001 a ADR-033).** Histórico incremental, um ADR por
  tarefa, com contexto, alternativas, testes e notas de revisão.

**Referências ao enunciado.** O enunciado do desafio está em [`docs/DESAFIO.md`](docs/DESAFIO.md)
(era o `README.md` até a T29, ADR-030). Neste documento, em `task_plan.md` e nos comentários do
código, "README §N" quer dizer a seção N **do enunciado**. O [`README.md`](README.md) atual é o
da solução: execução, variáveis, filas, migrations, exemplos e testes.

## Sumário

**Parte I — Síntese**

1. [Visão geral, componentes e estrutura de pastas](#1-visão-geral-componentes-e-estrutura-de-pastas)
2. [Fluxos principais (diagramas)](#2-fluxos-principais-diagramas)
3. [Dinheiro (`Money`)](#3-dinheiro-money)
4. [Persistência: pgx, transações e mapeamento](#4-persistência-pgx-transações-e-mapeamento)
5. [Idempotência e hash canônico](#5-idempotência-e-hash-canônico)
6. [Concorrência e locks](#6-concorrência-e-locks)
7. [Máquina de estados; falha transitória × permanente](#7-máquina-de-estados-falha-transitória--permanente)
8. [Referências pendentes: tentativas, backoff e TTL](#8-referências-pendentes-tentativas-backoff-e-ttl)
9. [Reversões: `REFUND` × `ROLLBACK`](#9-reversões-refund--rollback)
10. [`failureCode`s](#10-failurecodes)
11. [Inbox e outbox](#11-inbox-e-outbox)
12. [SQS: filas, `MessageGroupId`/`MessageDeduplicationId`, visibilidade, tentativas e DLQ](#12-sqs-filas-messagegroupidmessagededuplicationid-visibilidade-tentativas-e-dlq)
13. [Eventos de integração e contrato da fila de eventos](#13-eventos-de-integração-e-contrato-da-fila-de-eventos)
14. [Autenticação (OIDC) e autorização](#14-autenticação-oidc-e-autorização)
15. [Uber Fx: composição, ciclo de vida e shutdown](#15-uber-fx-composição-ciclo-de-vida-e-shutdown)
16. [Contrato HTTP: códigos e corpos](#16-contrato-http-códigos-e-corpos)
17. [Observabilidade](#17-observabilidade)
18. [Interpretações adotadas, limitações, trade-offs e trabalho não concluído](#18-interpretações-adotadas-limitações-trade-offs-e-trabalho-não-concluído)

**Parte II — Registro de decisões:** [ADR-001](#adr-001--módulo-versão-go-e-dependências-base-t01)
a [ADR-033](#adr-033--moneyunmarshaljson-estrito-t32).

**Mapa enunciado ([docs/DESAFIO.md](docs/DESAFIO.md)) → seção**

| Exigência do enunciado | Seção | ADRs |
| --- | --- | --- |
| §2 IdP, validação de credenciais e modelo de permissões | 14 | 019, 026, 029 |
| §2 acesso à mensageria por credenciais/políticas do broker | 12, 14 | 022, 026 |
| §4 biblioteca de banco, mapeamento de `Money`, delimitação da transação entre repositórios | 3, 4 | 004, 011, 013, 014, 016 |
| §4 Fx: módulos, `fx.Lifecycle`, prazos, shutdown, ordem de fechamento | 15 | 021, 022, 023, 024, 025 |
| §6.1 representação e limites de `Money`, normalização antes do hash | 3, 5 | 004, 010 |
| §6.2 estratégia de controle de concorrência | 6 | 005, 014, 017 |
| §6.3 máquina de estados, transitório × permanente | 7 | 008, 013, 017, 031 |
| §7 combinações `REFUND`/`ROLLBACK` | 9 | 009, 011 |
| §7 tentativas/TTL, referência pendente ou terminada sem sucesso | 8 | 009, 023, 031, 032 |
| §7 `failureCode` estável, corrigível × definitivo | 10 | 008, 009 |
| §9 algoritmo, campos e normalizações do hash; equivalência HTTP × SQS | 5 | 010, 020, 022 |
| §9 códigos HTTP e corpos (inválido, conflito, rejeição, pendente, indisponível) | 16 | 020 |
| §10 tentativas, visibility timeout, mensagens inválidas, `MessageGroupId`/`MessageDeduplicationId`, SIGTERM | 12, 15 | 022, 026 |
| §11 roteamento e consumo do destino dos eventos | 13 | 012, 024 |
| §15 dinheiro, transações, idempotência, locks, pendências, reversões, inbox/outbox, autenticação, autorização, Fx, shutdown | 3–15 | — |
| §15 limitações, interpretações e trabalho não concluído | 18 | — |

---

# Parte I — Síntese

## 1. Visão geral, componentes e estrutura de pastas

Um único binário (`cmd/server`) roda em N réplicas idênticas e sem estado em memória que
afete o resultado financeiro. Cada réplica tem quatro entradas e saídas de trabalho, todas
com garantias equivalentes:

- **API HTTP** (`net/http`), autenticada por OIDC;
- **consumidor SQS** da fila `wager-transactions.fifo`;
- **worker de pendências**, que retoma `PENDING_REFERENCE` vencidos e `PENDING` órfãos;
- **publisher da outbox**, que envia os eventos confirmados para `wager-events.fifo`.

Toda coordenação é feita no PostgreSQL: lock de linha por carteira, constraints e índices
únicos, leases persistidos com fencing. Nenhum lock global e nenhum estado em memória
compartilhado entre instâncias (ADR-005, 015, 023, 024).

Estrutura de pastas (arquitetura hexagonal, dependências de fora para dentro, ADR-002):

```
cmd/server/                      composição Fx (main), prazos de start/stop; testes da aplicação
                                 completa (integração, concorrência, multi-instância, E2E de auth)
cmd/migrate/                     CLI de migrations (up, down N, down -all, status)
migrations/                      SQL versionado NNNN_nome.{up,down}.sql, embutido (embed.FS)
internal/domain/                 modelo puro: Money, Wallet, WalletLedgerEntry, WagerTransaction,
                                 regras (DecideWager/ApplyWagerDecision), eventos de integração
internal/domain/idempotency/     hash canônico de requisição (SHA-256 de JSON canônico)
internal/app/                    casos de uso (OpenWallet, ProcessWagerTransaction,
                                 ResumeWagerTransaction, ReconcileWallet, Queries) e portas
internal/adapters/postgres/      pool pgx, TxManager, UnitOfWork, repositórios, classificação de erros
internal/adapters/postgres/migrate/  runner de migrations (advisory lock, checksum)
internal/adapters/http/          handlers net/http (package httpadapter), DTOs, health, middlewares
internal/adapters/http/server/   servidor HTTP com fx.Lifecycle e parada graciosa (package httpserver)
internal/adapters/http/apierror/ corpo de erro padronizado {code, message, correlationId}
internal/adapters/strictjson/    decoder JSON estrito, compartilhado por HTTP e SQS
internal/adapters/auth/          validação OIDC (JWKS próprio), middleware, autorização
internal/adapters/sqs/           cliente SQS, envelope e consumidor da fila de entrada
internal/adapters/outboxpublisher/ publisher da outbox para wager-events.fifo
internal/adapters/pendingworker/ worker de referências pendentes e PENDING órfãos
internal/platform/config/        configuração por variáveis de ambiente, validada no start
internal/platform/logging/       slog JSON com IDs de rastreio e redação de dados sensíveis
internal/platform/metrics/       registry Prometheus próprio, rótulos de cardinalidade fechada
internal/platform/startbudget/   divisão do prazo de start entre orçamento e reserva de rollback
internal/platform/failpoint/     pontos de crash para testes (no-op sem a build tag failpoint)
deploy/                          realm Keycloak, provisionamento das filas (LocalStack), nginx
scripts/                         smoke test do Docker Compose
```

Regras de dependência: `domain` não importa Fx, pgx, SQS, HTTP nem `app`/`adapters`/`platform`.
`app` depende só de `domain` e das portas que declara (`app.UnitOfWork`, `app.Tx` e stores); em
`app` só `module.go` importa Fx. Os adapters implementam as portas.

## 2. Fluxos principais (diagramas)

Entrada de uma operação (HTTP e SQS compartilham o caso de uso e a transação):

```mermaid
flowchart LR
    P[Provedor] -- "POST /wagering/transactions<br/>Bearer + Idempotency-Key" --> H[HTTP<br/>auth + decoder estrito]
    Q[(wager-transactions.fifo)] -- "ReceiveMessage" --> C[Consumidor SQS<br/>envelope estrito]
    H --> U[ProcessWagerTransaction]
    C -- "cmd + Inbox{consumer, messageId}" --> U
    U --> T{{"Transação SQL única (READ COMMITTED)<br/>1. inbox.Register (só SQS)<br/>2. idempotência por (provider, key)<br/>3. SELECT wallet FOR UPDATE<br/>4. releitura por key e externalId<br/>5. referência + reversões existentes<br/>6. DecideWager / ApplyWagerDecision<br/>7. UPDATE wallet WHERE version<br/>8. INSERT transação, ledger, outbox<br/>9. inbox.Complete (só SQS)"}}
    T -- COMMIT --> DB[(PostgreSQL)]
    T -.->|depois do commit| R1[HTTP 200/202/422]
    T -.->|depois do commit| R2[DeleteMessage]
```

Publicação dos eventos e retomada de pendências (workers em todas as réplicas):

```mermaid
flowchart LR
    DB[(PostgreSQL<br/>outbox / wager_transactions)]
    OP[Publisher da outbox] -- "1. claim com lease (autocommit,<br/>SKIP LOCKED, cabeça de cada agregado)" --> DB
    OP -- "2. SendMessage<br/>GroupId = aggregateId<br/>DedupId = eventId" --> EQ[(wager-events.fifo)]
    OP -- "3. MarkPublished / MarkFailed<br/>(fencing pelo lease)" --> DB
    PW[Worker de pendências] -- "ClaimDue (lease + token,<br/>FOR NO KEY UPDATE SKIP LOCKED)" --> DB
    PW -- "Resume: tx curta por item<br/>wallet FOR UPDATE → linha com token<br/>→ DecideWager → Update + ledger + outbox" --> DB
```

Mensagem SQS com falhas:

```mermaid
sequenceDiagram
    participant Q as wager-transactions.fifo
    participant C as Consumidor
    participant DB as PostgreSQL
    participant D as wager-transactions-dlq.fifo
    Q->>C: ReceiveMessage (visibilidade 30s)
    C->>C: envelope inválido?
    alt inválida / permanente / conflito
        C->>D: SendMessage (corpo original + dlqReason)
        C->>Q: DeleteMessage (só depois do envio confirmado)
    else válida
        C->>DB: BEGIN … inbox + efeitos + outbox + inbox.Complete … COMMIT
        alt PROCESSED / REJECTED / PENDING_REFERENCE / replay
            C->>Q: DeleteMessage
        else transitória (503, prazo, erro sem classe)
            C->>Q: ChangeMessageVisibility(backoff)
            Note over C,Q: esgotado SQS_MAX_ATTEMPTS → DLQ explícita (max_receives)
        end
    end
```

## 3. Dinheiro (`Money`)

Detalhes: ADR-004, ADR-011, ADR-014, ADR-020, ADR-033.

- **Representação:** `int64` em centavos (escala fixa 2) + código ISO 4217 alfabético. Nenhum
  `float32`/`float64` no parsing, no cálculo, na serialização, na persistência nem nas métricas
  (as métricas contam ocorrências, nunca valores).
- **Limites:** `[-92233720368547758.08, 92233720368547758.07]`. Parsing, `Add`, `Sub` e `Neg`
  detectam overflow (`ErrMoneyOverflow`), sem wrap-around. Só moedas com expoente ISO 2 (BRL,
  USD, EUR, GBP, CAD, AUD, CHF, MXN, ARS, COP, PEN, UYU); JPY/KWD são rejeitadas.
- **Gramática:** `["-"] ("0" | [1-9][0-9]*) ["." [0-9]{1,2}]`. Rejeita vazio, `NaN`,
  `Infinity`, notação científica, `+`, espaços, zeros à esquerda, `.5`, `5.`, `-0` e escala
  maior que 2 (sem arredondar). Entradas externas usam `ParseNonNegative` (negativo é
  rejeitado); negativos só existem em diferenças internas (reconciliação).
- **Normalização antes do hash:** `"25"`, `"25.5"` e `"25.50"` são aceitos e equivalentes; o
  hash usa sempre a forma canônica `Amount()` com duas casas (`"25.50"`). Moeda não é
  normalizada (`"brl"` é inválido).
- **Contrato externo:** `{"amount":"25.00","currency":"BRL"}`. No HTTP e no SQS o Money é lido
  por `strictjson` (amount precisa ser string JSON; chave desconhecida, repetida ou com outra
  caixa é rejeitada) e com `ParseNonNegative`. O `Money.UnmarshalJSON` do domínio (DTOs,
  eventos, testes, consumidores Go do contrato) aplica as mesmas regras de objeto sem
  importar o adapter (ADR-033): só `amount`/`currency` em caixa exata, sem repetição, sem
  extras, ambos obrigatórios e strings JSON, nada além de um objeto (`null` é erro). Ele
  aceita negativos, porque serializa diferenças (`difference` da reconciliação) e precisa de
  round-trip com `MarshalJSON`; a não negatividade é responsabilidade do ponto de entrada.
  Aritmética e comparação entre moedas diferentes → `ErrCurrencyMismatch`;
  `Money{}` não inicializado é rejeitado em toda operação.
- **Persistência:** `BIGINT` (centavos) + `CHAR(3)` com `CHECK ~ '^[A-Z]{3}$'`; reidratação por
  `FromMinorUnits`. O CHECK aritmético do ledger roda em `NUMERIC` (exato, sem overflow). A soma
  da reconciliação é feita em `NUMERIC` no banco e um total fora de `int64` é erro, nunca é
  truncado.

## 4. Persistência: pgx, transações e mapeamento

Detalhes: ADR-011, ADR-013, ADR-014, ADR-016, ADR-018.

- **Biblioteca:** `github.com/jackc/pgx/v5` + `pgxpool`, com **SQL explícito** (sem ORM, sem
  sqlc). Migrations próprias (runner de ~300 linhas sobre pgx; cada migration numa transação;
  `pg_advisory_lock` para réplicas simultâneas; checksum do arquivo aplicado).
- **Fronteira transacional explícita:** o caso de uso chama `app.UnitOfWork.WithinTx(ctx,
  TxOptions, fn)`; dentro de `fn`, `app.Tx` entrega os stores `Wallets()`, `Transactions()`,
  `Ledger()`, `Outbox()` e `Inbox()`, **todos ligados ao mesmo `pgx.Tx`**. Não há transação
  escondida no `context`: dá para auditar, lendo o caso de uso, que carteira, transação, ledger,
  outbox e inbox entram no mesmo commit. Os métodos que travam linhas recusam um `Querier` que
  não seja `pgx.Tx` (`ErrTxRequired`).
- **Isolamento:** `READ COMMITTED` nas escritas (a consistência vem do lock da carteira e da
  versão condicional, seção 6). `REPEATABLE READ READ ONLY` na reconciliação (snapshot único
  entre saldo e soma do ledger). `READ ONLY` nas consultas.
- **Rollback e retry:** erro, panic ou `Goexit` em `fn` → ROLLBACK (com 5s próprios, mesmo com
  o ctx cancelado). Retry opt-in por chamada (`DefaultRetryPolicy`: 4 execuções, 10ms→250ms com
  jitter) só para SQLSTATE transitórios (`40001`, `40P01`, `55P03`, `57014`, `08xxx`,
  `57P01-03`, `53000`, `53100`, `53200`, `53300`, `25006`) e, no `ProcessWagerTransaction`,
  para a duplicata perdedora de uma corrida de UNIQUE (a nova tentativa vê a linha vencedora e devolve o replay). **Commit
  ambíguo** (`ErrCommitOutcomeUnknown`) nunca é repetido internamente: vira 503 / mensagem não
  apagada, e a nova tentativa externa passa pela idempotência.
- **Mapeamento:** `Money` ↔ `BIGINT` + `CHAR(3)`; UUIDs ↔ `UUID`; valores vazios do domínio ↔
  `NULL`. Toda leitura reidrata pelo domínio (`Rehydrate*`), que revalida as invariantes; uma
  linha inconsistente falha alto. Violações de constraint são traduzidas pelo **nome** da
  constraint para sentinelas do adapter e depois para erros de `app`.
- **Invariantes no schema** (independentes de locks locais e do SQS): `UNIQUE(player_id,
  currency)`, `balance >= 0`, versão sobe exatamente 1 por mudança de saldo (trigger),
  `UNIQUE(provider_id, external_transaction_id)`, `UNIQUE(provider_id, idempotency_key)`,
  OPENING único por carteira, uma reversão bem-sucedida por referência, ledger com
  `UNIQUE(wallet_id, transaction_id)`, CHECK aritmético e triggers que bloqueiam
  `UPDATE`/`DELETE`/`TRUNCATE`, terminais imutáveis, snapshot da outbox imutável.

## 5. Idempotência e hash canônico

Detalhes: ADR-010, ADR-015, ADR-017, ADR-020, ADR-022.

- **Persistente:** a idempotência mora nas linhas de `wager_transactions` (únicos por
  `(provider_id, idempotency_key)` e `(provider_id, external_transaction_id)`) e na `inbox`
  (`(consumer_name, message_id)`). Sobrevive ao reinício de todos os processos.
- **Hash:** SHA-256 (64 hex minúsculos) do JSON canônico, chaves em ordem de bytes, JSON
  compacto, escape no estilo RFC 8785. Campos: `externalTransactionId`, `gameId`, `kind`,
  `money{amount,currency}`, `playerId`, `providerId`, `referenceExternalTransactionId` (`null`
  quando ausente), `roundId`, `v` (versão do esquema, hoje `1`) e `walletId`. Ficam de fora a
  `Idempotency-Key`/`idempotencyKey` e todo metadado de transporte (headers, `messageId`,
  `correlationId`, `occurredAt`).
- **Normalizações:** Money canônico com duas casas; UUIDs na forma minúscula de
  `uuid.UUID.String()`; referência omitida, `null` ou `""` → `null`. Identificadores de texto
  **não** são normalizados (caixa e espaços mudam o hash). No HTTP, `providerId` vem da
  identidade autenticada (que precisa casar com o corpo). Equivalência HTTP × SQS: a mesma
  função de hash e os mesmos campos, então a mesma operação pelos dois transportes é replay
  (testado na T25/T26).
- **Regras:**
  - mesma chave + mesmo hash → replay com `idempotentReplay: true` e o resultado persistido,
    inclusive o `balance` gravado no processamento original (não o saldo atual);
  - mesma chave + outro hash → 409 `IDEMPOTENCY_KEY_CONFLICT`;
  - mesmo `(providerId, externalTransactionId)` com outra chave → 409
    `EXTERNAL_TRANSACTION_CONFLICT` (nunca reaplica);
  - SQS: mesmo `messageId` com outro hash → `MESSAGE_CONFLICT` (DLQ); mensagem nova para uma
    operação já feita → replay, e a inbox aponta para a transação original.
- A `Idempotency-Key` é usada exatamente como recebida; o servidor nunca a substitui por
  `{providerId}:{externalTransactionId}`.

## 6. Concorrência e locks

Detalhes: ADR-005, ADR-009, ADR-014, ADR-017, ADR-023, ADR-027, ADR-028, ADR-032.

- **Coordenação por carteira, combinando pessimista e otimista:**
  `SELECT ... FROM wallets WHERE id = $1 FOR UPDATE` serializa as operações **da mesma
  carteira**; `UPDATE wallets ... WHERE id = $1 AND version = $lida` é a defesa em
  profundidade contra lost update (0 linhas → `ErrStaleWallet`, nada gravado). O trigger
  `wallets_guard_update` exige versão +1 exatamente quando o saldo muda.
- **Sem lock global:** carteiras diferentes avançam em paralelo (testado travando uma carteira
  e vendo as outras concluírem). Referência e reversões ficam na mesma carteira, então o mesmo
  lock serializa reversões concorrentes de uma referência.
- **Barreiras finais no schema:** `CHECK (balance >= 0)`, os únicos de idempotência, o índice
  de reversão única e `UNIQUE(wallet_id, transaction_id)` do ledger valem mesmo se uma regra
  de aplicação falhasse.
- **Replays não esperam o lock:** a leitura por `(provider, key)` é feita antes do
  `FOR UPDATE`; depois do lock há uma releitura (em `READ COMMITTED` cada comando vê os commits
  anteriores).
- **Workers:** outbox e pendências usam lease persistido (`locked_until`/`claimed_until`) com
  `SKIP LOCKED` e fencing por token/tentativa; a ordem de locks do worker de pendências é a
  mesma das entradas (carteira → linha), sem deadlock.
- **Wake-up de pendências** (ADR-032): a gravação terminal de uma operação toca, na mesma
  transação, linhas `PENDING_REFERENCE` que a citam (possivelmente de outra carteira) só com
  `FOR NO KEY UPDATE SKIP LOCKED`: nunca espera lock, então não cria ciclo com quem segura
  carteiras nem com o worker, e não conflita com o `KEY SHARE` da FK.
- **Demonstração exigida:** 100.00 BRL e duas apostas simultâneas de 80.00 → uma `PROCESSED`,
  uma `REJECTED INSUFFICIENT_FUNDS`, saldo 20.00, um débito; reenvios não mudam o resultado.
  Coberto no caso de uso, na aplicação completa e com 3 processos independentes
  (ADR-027/028).

## 7. Máquina de estados; falha transitória × permanente

Detalhes: ADR-008, ADR-013, ADR-017, ADR-031.

| De \ Para | `PENDING_REFERENCE` | `PROCESSED` | `REJECTED` | `FAILED` |
| --- | --- | --- | --- | --- |
| `PENDING` | `MarkPendingReference` | `MarkProcessed` | `Reject` | `Fail` |
| `PENDING_REFERENCE` | `RescheduleReferenceRetry` (attempts+1) | `MarkProcessed` | `Reject` | `Fail` |

- `PROCESSED`, `REJECTED` e `FAILED` são terminais: o domínio recusa transições
  (`ErrWagerTransactionTerminal`) e o trigger `wager_transactions_guard_update` as torna
  imutáveis no banco. Não há aresta de volta para `PENDING`.
- `OPENING` nasce `PROCESSED` (interno, no mesmo commit da carteira). Operações externas nascem
  `PENDING` no domínio, mas o fluxo síncrono grava **já no estado final** (`PROCESSED`,
  `REJECTED` ou `PENDING_REFERENCE`) numa única transação, sem commit intermediário de aceite.
  Um `PENDING` confirmado só existiria num aceite interrompido; o worker o retoma como órfão
  (`PENDING_ORPHAN_GRACE`).
- **Transitório:** timeout, serialização, deadlock, lock timeout, conexão, `57P0x`, `53300`,
  recursos do servidor esgotados (`53000`, `53100` disco cheio, `53200` sem memória), escrita
  em servidor somente leitura (`25006`, failover), IdP sem chaves. **Não vira estado**: a transação faz rollback e é refeita (retry interno
  limitado, reentrega SQS com backoff, nova chamada do provedor após 503, lease do worker). O
  registro continua inexistente, `PENDING` ou `PENDING_REFERENCE`.
- **Permanente de negócio:** `REJECTED` com `failureCode` `INPUT` ou `BUSINESS`, persistido e
  com evento.
- **Permanente de infraestrutura:** `FAILED RETRIES_EXHAUSTED`, gravado **só pelo worker de
  pendências** (ADR-031) quando a retomada de um item falha sempre:
  - **O que conta como falha:** todo erro da retomada que **não** é transitório. Ex.: a linha,
    a carteira ou a referência não reidratam (`ErrCorrupted*`), violação de constraint, erro
    de domínio inesperado, erro interno. **Não contam** `ErrUnavailable` (conexão, `57P0x`,
    serialização, deadlock, lock/statement timeout, commit ambíguo, prazo) nem o cancelamento
    do próprio ctx (shutdown, `PENDING_WORKER_PROCESS_TIMEOUT`). Assim, uma queda do
    PostgreSQL só adia os itens (eles voltam pelo lease, como antes) e nunca leva um item bom a
    `FAILED`.
  - **Contagem e backoff:** numa transação curta e própria, com o fencing do lease
    (`claimed_by = token`, `FOR NO KEY UPDATE`, sem reidratar), `resume_failures += 1` (coluna
    operacional da `0009`, fora do agregado) e `claimed_until = agora +
    PENDING_REFERENCE_RETRY_BASE_DELAY · 2^(falhas−1)` (teto `..._MAX_DELAY`). O próprio lease
    é o backoff: `status`, `attempts` e `next_attempt_at` não mudam. Uma retomada que grava a
    transição zera o contador (falhas **consecutivas**).
  - **Esgotamento:** na falha de número `PENDING_WORKER_MAX_FAILURES` (padrão 5: com os
    defaults, ~30s de 2s+4s+8s+16s) ou na primeira falha depois de `PENDING_REFERENCE_TTL`
    desde a criação, uma **segunda** transação curta, depois do commit da contagem, grava
    `FAILED RETRIES_EXHAUSTED`: pelo domínio (`WagerTransaction.Fail` + `Update`) quando a
    linha reidrata; quando a própria linha não reidrata (por **qualquer** erro não transitório,
    inclusive `Money` com moeda não suportada) ou a gravação pelo agregado falha,
    numa transação nova, `domain.ValidateFailTransition(status, código)` valida a aresta e um
    `UPDATE` com fencing grava só as colunas de resultado (os CHECKs de `FAILED` são a barreira
    final). Como a contagem já foi confirmada, um erro ao gravar `FAILED` **não desfaz o
    incremento**: o item volta depois do backoff (no máximo `..._MAX_DELAY`), conta de novo e
    tenta outra vez; nunca fica preso abaixo do limite.
    Sem lock de carteira, sem saldo, sem ledger e sem evento (seção 13); `resume_failures`
    fica na linha para auditoria; log ERROR e `wager_transactions_total{source="worker",
    status="FAILED"}`.
  - **Replay** de uma operação `FAILED` (HTTP ou SQS) devolve o estado final: HTTP 422
    `{status: FAILED, failureCode: RETRIES_EXHAUSTED, failureCategory: INFRASTRUCTURE}`; no
    SQS a mensagem é concluída e apagada como as demais terminais. Operações que citam uma
    referência `FAILED` são rejeitadas com `REFERENCE_NOT_PROCESSED` (seção 8).
  - Fora do worker continua sem `FAILED`: transitórios esgotados no SQS vão para a DLQ sem
    tocar no banco, e no HTTP o provedor recebe 503 e repete (não há linha pendente a encerrar,
    porque o aceite é síncrono). `PROCESSING_FAILED` continua reservado (nenhum fluxo o grava).
- Entradas inválidas que impedem construir a transação (parsing, `OPENING` externo) não criam
  linha nem evento: são 400 no HTTP e DLQ no SQS.

## 8. Referências pendentes: tentativas, backoff e TTL

Detalhes: ADR-009, ADR-017, ADR-023, ADR-031, ADR-032.

- Referência ausente → a operação é gravada `PENDING_REFERENCE` (`attempts = 1`,
  `next_attempt_at = agora + PENDING_REFERENCE_RETRY_BASE_DELAY`) com o evento
  `WagerTransactionPendingReference`, no mesmo commit (e, no SQS, com a inbox concluída e a
  mensagem apagada: o worker assume).
- O worker de cada réplica reivindica as vencidas (`ClaimDue`, lease + token,
  `FOR NO KEY UPDATE SKIP LOCKED`) e reaplica `DecideWager`/`ApplyWagerDecision` com a carteira
  travada. Backoff exponencial **persistido**: `base · 2^(attempts−1)`, teto
  `PENDING_REFERENCE_RETRY_MAX_DELAY`, nunca agendado depois do fim do TTL. Sobrevive a
  reinícios porque o estado está na linha.
- **Limite:** `PENDING_REFERENCE_MAX_ATTEMPTS` (10, buscas totais, inclusive a do aceite) ou
  `PENDING_REFERENCE_TTL` (10m desde `created_at`), o que vier antes. Esgotado →
  `REJECTED REFERENCE_NOT_FOUND` + `WagerTransactionRejected`. A busca vem antes da expiração:
  uma referência que chegou é aplicada mesmo na última tentativa.
- **Referência existente mas pendente** (`PENDING`/`PENDING_REFERENCE`): a operação continua
  aguardando; se o prazo esgotar, `REFERENCE_NOT_FOUND` ("não ficou disponível a tempo").
- **Referência terminada sem sucesso** (`REJECTED`/`FAILED`): rejeição imediata com
  `REFERENCE_NOT_PROCESSED`, sem esperar.
- **Wake-up quando a referência chega** (T31, ADR-032): toda transação que grava uma operação
  externa num estado **terminal** (`PROCESSED`, `REJECTED` ou `FAILED`, pelo fluxo síncrono ou
  pelo worker) executa, no mesmo commit, `WakePendingReferences`: as `PENDING_REFERENCE` **do
  mesmo provedor** que citam o seu `externalTransactionId` e têm `next_attempt_at` no futuro
  passam a vencer agora (nunca antes de `created_at`). Só `next_attempt_at` muda: `attempts`,
  `updated_at`, lease (`claimed_*`) e `resume_failures` ficam intactos, então um item
  reivindicado continua do seu worker e um item retido pelo backoff de falhas continua retido.
  A resolução passa a sair no próximo ciclo do worker (`PENDING_WORKER_POLL_INTERVAL`, 1s), não
  no fim do backoff. Uma referência ainda pendente não acorda nada (não resolveria); quando ela
  própria é resolvida, acorda as suas dependentes. Linhas travadas por outra transação são
  puladas (`SKIP LOCKED`) e ficam com o backoff que já tinham. Usa o índice parcial
  `wager_transactions_reference_external_idx` (`0002`).
- Defaults: base 2s, teto 1m, 10 buscas, TTL 10m (a última busca acontece ~5 min após o
  aceite); poll 1s, lote 20, concorrência 4, lease 30s, prazo por item 5s,
  5 falhas consecutivas não transitórias da retomada antes de `FAILED`.
- **Item envenenado** (a retomada falha sempre, por erro não transitório): falhas contadas com
  backoff no lease e, esgotado `PENDING_WORKER_MAX_FAILURES` ou o TTL, `FAILED
  RETRIES_EXHAUSTED` sem evento e sem movimento (seção 7). Não bloqueia os demais itens do lote.
- O replay de uma operação pendente devolve o estado atual (202 enquanto pendente; o estado
  final depois da resolução).

## 9. Reversões: `REFUND` × `ROLLBACK`

Detalhes: ADR-009, ADR-011.

- `REFUND` referencia só `BET` e credita. `ROLLBACK` referencia `BET` (credita), `WIN` ou
  `REFUND` (debita). `LOSS`, `ROLLBACK` e `OPENING` não são reversíveis
  (`REFERENCE_NOT_REVERSIBLE`). `WIN` pode citar uma `BET` da mesma rodada como vínculo e
  credita o próprio valor.
- A referência é resolvida por `(providerId, referenceExternalTransactionId)` e precisa
  concordar em provedor, jogador, carteira e rodada (`REFERENCE_MISMATCH`) e moeda
  (`CURRENCY_MISMATCH`); o valor precisa ser igual (`AMOUNT_MISMATCH`; sem reversão parcial).
- **Política:** cada referência admite **no máximo uma reversão `PROCESSED`, de qualquer
  tipo**. Uma segunda (`REFUND` ou `ROLLBACK`) → `ALREADY_REVERSED`. Assim o mesmo débito nunca
  é devolvido duas vezes. Um `ROLLBACK` do próprio `REFUND` é permitido (desfaz a devolução,
  uma única vez). Defesa final no schema:
  `UNIQUE (reference_transaction_id) WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK')`.
- Reversão que debitaria além do saldo → `REVERSAL_INSUFFICIENT_FUNDS` (distinto de
  `INSUFFICIENT_FUNDS` da `BET`), persistida e com evento.
- Limitação consciente: uma `BET` cuja devolução foi desfeita (`REFUND` + `ROLLBACK(REFUND)`)
  não pode ser devolvida de novo.

## 10. `failureCode`s

Detalhes: ADR-008, ADR-009, ADR-031. Contrato público estável (`domain.AllFailureCodes()`), com
`failureCategory` no corpo e nos eventos.

| Categoria | Significado | Códigos |
| --- | --- | --- |
| `INPUT` | corrigível: o provedor corrige e envia **uma nova** operação | `INVALID_REQUEST`, `INVALID_KIND`, `INVALID_AMOUNT`, `INVALID_CURRENCY`, `REFERENCE_REQUIRED`, `REFERENCE_NOT_ALLOWED`, `WALLET_NOT_FOUND`, `WALLET_PLAYER_MISMATCH`, `CURRENCY_MISMATCH`, `REFERENCE_MISMATCH`, `AMOUNT_MISMATCH` |
| `BUSINESS` | resultado definitivo | `INSUFFICIENT_FUNDS` (só `BET`), `REVERSAL_INSUFFICIENT_FUNDS` (só `ROLLBACK`), `REFERENCE_NOT_FOUND`, `REFERENCE_NOT_REVERSIBLE`, `REFERENCE_NOT_PROCESSED`, `ALREADY_REVERSED` |
| `INFRASTRUCTURE` | só em `FAILED`: falha permanente de infraestrutura registrada para auditoria; não é corrigível pelo provedor nem resultado de negócio | `RETRIES_EXHAUSTED` (gravado pelo worker de pendências quando a retomada de um item falha `PENDING_WORKER_MAX_FAILURES` vezes seguidas ou depois do TTL, seção 7), `PROCESSING_FAILED` (reservado; nenhum fluxo grava) |

Os códigos de `INPUT` de formato (`INVALID_*`, `REFERENCE_REQUIRED`, `REFERENCE_NOT_ALLOWED`)
normalmente saem como **400 sem persistência** (validação antes da transação); os de carteira
e referência (`WALLET_*`, `*_MISMATCH`) dependem do banco e saem como **422 `REJECTED`
persistido**. Os códigos dos erros de API (`IDEMPOTENCY_KEY_CONFLICT`, `TRANSACTION_NOT_FOUND`,
`PROVIDER_MISMATCH`...) são outro espaço de nomes: campo `code` do corpo de erro (seção 16).

## 11. Inbox e outbox

Detalhes: ADR-011, ADR-015, ADR-017, ADR-022, ADR-024.

- **Inbox** (`inbox`, PK `(consumer_name, message_id)`, `request_hash`, `transaction_id`,
  `received_at`, `completed_at`): `Register` e `Complete` rodam **na mesma transação** que
  carteira, ledger, transação e outbox. Uma reentrega depois do commit encontra `COMPLETED` e
  devolve o replay; uma entrega concorrente da mesma mensagem espera no índice único. Em
  `PENDING_REFERENCE` a mensagem é concluída no commit da pendência.
- **Outbox** (`outbox`: `seq` IDENTITY, `event_id` UNIQUE, `aggregate_id`, `event_type`,
  `payload JSON` imutável, `occurred_at`, `attempts`, `next_attempt_at`,
  `locked_by`/`locked_until`, `last_error`, `published_at`): os eventos são gravados no mesmo
  commit do estado que os originou; **nada é publicado antes do commit**.
- **Publisher:** claim em autocommit com lease (`statement_timestamp()` do banco), `SKIP
  LOCKED`, ordem por `seq` e só a **cabeça** de cada agregado (head-of-line: um evento em
  lease/backoff segura os seguintes do mesmo agregado). Envio fora de transação, depois
  `MarkPublished`/`MarkFailed` com fencing `(seq, eventId, owner, attempts)`. Falha →
  backoff persistido `min(1s·2^(n−1), 5min)`. Lease expirado (processo morto) → outra
  instância republica o **mesmo `eventId` e o mesmo corpo**.
- **Sem limite de tentativas nem DLQ na outbox:** descartar um evento confirmado violaria a
  garantia de não perda; um evento que falha sempre bloqueia só o seu agregado e aparece em
  `outbox_oldest_pending_age_seconds` e `last_error`.
- Crash entre commit e publicação, e entre publicação e marcação: testados com processos
  reais mortos (ADR-024, ADR-028).

## 12. SQS: filas, `MessageGroupId`/`MessageDeduplicationId`, visibilidade, tentativas e DLQ

Detalhes: ADR-022, ADR-024, ADR-026.

**Filas** (provisionadas por `deploy/localstack/init-sqs.sh`):

| Fila | Configuração | Uso |
| --- | --- | --- |
| `wager-transactions.fifo` | FIFO, `ContentBasedDeduplication=false`, `VisibilityTimeout=30`, retenção 4 dias, `RedrivePolicy` `maxReceiveCount=5` → DLQ | entrada (`WagerTransactionRequested`) |
| `wager-transactions-dlq.fifo` | FIFO, retenção 14 dias, `RedriveAllowPolicy` só para a entrada | mensagens inválidas, permanentes e transitórias esgotadas |
| `wager-events.fifo` | FIFO, `ContentBasedDeduplication=false`, retenção 4 dias | eventos de integração (seção 13) |

**Produtores da fila de entrada (contrato):** `MessageGroupId = walletId` (ordem por carteira,
paralelismo entre carteiras) e `MessageDeduplicationId = messageId` do envelope (dedup de 5 min
do SQS). Nenhum dos dois é necessário para a correção: inbox, idempotência por chave e lock da
carteira valem sem eles (o teste HTTP × SQS usa de propósito grupos e dedup ids aleatórios).
A identidade durável é o `messageId` do **envelope**, não o `MessageId` do SQS.

**Consumidor:**

- Long polling (`SQS_WAIT_TIME` 20s); visibilidade pedida em cada receive
  (`SQS_VISIBILITY_TIMEOUT` 30s); processamento limitado a `SQS_PROCESS_TIMEOUT` (15s, menor
  que a visibilidade); renovação da visibilidade antes de processar uma mensagem que esperou
  no lote.
- Mensagens agrupadas por `MessageGroupId`: uma goroutine por grupo, em ordem; até
  `SQS_CONSUMER_CONCURRENCY` (4) grupos em paralelo. Se uma mensagem do grupo não conclui, as
  seguintes do lote voltam com visibilidade 0, sem processar.
- `DeleteMessage` **só depois do commit**: `PROCESSED`, `REJECTED` (rejeição de negócio é
  terminal), `PENDING_REFERENCE` e replay.
- **Inválidas** (JSON, tipo, `OPENING`, Money...) e **permanentes** (`KindInvalidInput`,
  `MESSAGE_CONFLICT`, `IDEMPOTENCY_KEY_CONFLICT`, `EXTERNAL_TRANSACTION_CONFLICT`): enviadas
  **explicitamente** à DLQ na 1ª entrega (corpo original, mesmo grupo, `MessageDeduplicationId`
  determinístico, atributos `dlqReason`, `dlqCode`, `dlqField`, `sourceQueue`,
  `sourceMessageId`, `envelopeMessageId`, `receiveCount`, `attempts`, `consumerName`) e só
  depois apagadas. Nada é gravado no banco.
- **Transitórias** (`TEMPORARILY_UNAVAILABLE`, prazo, erro sem classe): `ChangeMessageVisibility`
  com backoff `SQS_RETRY_BASE_DELAY · 2^(n−1)` (1s, teto 5m). Esgotadas `SQS_MAX_ATTEMPTS` (3)
  tentativas → DLQ explícita (`max_receives`).
- **Redrive nativo** (`maxReceiveCount=5`) é só rede de segurança (crash repetido, DLQ
  indisponível). O start exige a `RedrivePolicy` e `SQS_MAX_ATTEMPTS < maxReceiveCount`, e
  resolve a DLQ pela própria política. Motivo do envio explícito: na FIFO, devolver as
  seguintes de um grupo também incrementa o `ApproximateReceiveCount` delas, e o redrive
  nativo levaria mensagens válidas à DLQ junto com a venenosa.
- **Acesso ao broker:** a aplicação usa credenciais próprias; cada fila tem `Policy` de recurso
  restrita à conta e o usuário IAM `wallet-app` recebe só Receive/Delete/ChangeVisibility/
  GetAttributes na entrada e SendMessage na DLQ e nos eventos. O LocalStack Community não
  aplica IAM: o provisionamento documenta de forma executável o que a AWS exigiria. O
  `providerId` do envelope é aceito porque o acesso à fila é controlado pelo broker; o domínio
  continua validando provedor, carteira, jogador e referência.
- **SIGTERM:** seção 15.

## 13. Eventos de integração e contrato da fila de eventos

Detalhes: ADR-012, ADR-024.

- Tipos concretos, construídos só pelos construtores do domínio (tipo e `version` = 1 fixados
  pelo payload): `WagerTransactionProcessed` (inclusive `LOSS` e `OPENING`),
  `WagerTransactionRejected`, `WalletBalanceChanged` (só com lançamento) e
  `WagerTransactionPendingReference` (só na transição para `PENDING_REFERENCE`).
- Envelope `{eventId, eventType, aggregateId, correlationId, causationId?, occurredAt, version,
  data}`; `eventId` UUIDv7; timestamps RFC 3339 UTC; dinheiro sempre em string decimal.
  `WalletBalanceChanged.data`: `walletId`, `transactionId`, `direction`, `money`,
  `balanceBefore`, `balanceAfter`, `walletVersion`. O payload é serializado uma vez e gravado
  byte a byte (snapshot imutável).
- **Roteamento em `wager-events.fifo`:** `MessageGroupId = aggregateId` (id da transação nos
  eventos de transação; id da carteira em `WalletBalanceChanged`), `MessageDeduplicationId =
  eventId`, corpo = snapshot; atributos `eventType` (String), `eventVersion` (Number), `eventId`
  e `correlationId`.
- **Consumo:** at-least-once; dedup do FIFO na janela de 5 min e, fora dela, por `eventId` no
  consumidor. Ordem garantida só dentro do grupo; para relacionar transação e saldo, usar
  `walletVersion` (monotônica por carteira) e `transactionId`.
- `idempotencyKey`, `requestHash` e (em rejeições) `referenceTransactionId` nunca entram nos
  eventos.
- **`FAILED` não gera evento** (reavaliado na T30, ADR-031): o enunciado (§11) exige eventos
  para conclusão, rejeição de negócio, saldo e espera por referência, e `FAILED` não é nenhum
  deles. É um registro de auditoria de falha de infraestrutura, que pede ação de operação, não
  de consumidores; fica visível na consulta da transação, no replay (422), no log ERROR e em
  `wager_transactions_total{status="FAILED"}`. Um evento novo (`WagerTransactionFailed`)
  ampliaria o contrato da fila sem consumidor que o exija; fica como evolução possível.

## 14. Autenticação (OIDC) e autorização

Detalhes: ADR-019, ADR-020, ADR-026, ADR-029.

- **IdP:** Keycloak (realm `junglegaming`, provisionado no Compose por import). Serviços obtêm
  tokens por `client_credentials`; a aplicação só **valida** tokens (sem senhas, sem emissão
  própria).
- **Validação local** de JWT por JWKS (sem introspecção por requisição): allowlist de
  algoritmos assimétricos (`none`/`HS*` recusados), `kid` (e `alg`, se declarado) da chave,
  `iss` exato, `aud` contendo `OIDC_AUDIENCE` (`wallet-api`), `exp` obrigatório, `exp`/`nbf`/
  `iat` com tolerância `OIDC_CLOCK_SKEW` (30s), `typ=Bearer` se presente, token ≤ 16 KiB. JWKS
  com cache, renovação por `kid` desconhecido (no máx. 1 busca a cada 10s) e por idade (10
  min). Sem chaves utilizáveis (IdP fora) → 503, não 401. Trade-off: revogação só vale na
  expiração do token (tokens curtos, 300s no realm).
- **Principal:** `providerId` da claim `provider_id` (mapper hardcoded por client de provedor,
  desacoplado do `client_id`); papéis de `realm_access.roles` ∪
  `resource_access[OIDC_ROLES_CLIENT].roles`.
- **Permissões:**
  - `wallet-internal` (client `internal-service`): `POST /wallets`, `GET /wallets/{id}`,
    `GET /wallets/{id}/ledger`, `POST /wallets/{id}/reconciliation` e `/metrics` (com
    `METRICS_ACCESS=internal`, o padrão);
  - `wager-provider` **e** `provider_id` (clients `provider-a`, `provider-b`): as três rotas de
    transação, sempre com o `providerId` **da identidade**. O do corpo e o do path precisam
    ser iguais byte a byte, senão 403 `PROVIDER_MISMATCH`. Por id, a transação de outro
    provedor (ou interna) é 404 com corpo idêntico ao de um id inexistente. Replays de outro
    provedor nunca devolvem dados;
  - sem papel → 403 `FORBIDDEN`; o serviço interno não acessa transações e provedores não
    acessam carteiras;
  - público só `/health/live` e `/health/ready` (comparação exata do path).
- **Acessos negados** não abrem transação e não produzem ledger, outbox nem transação
  (testado contra o Keycloak real, ADR-029).
- **Mensageria:** sem token; controle por credenciais/políticas do broker (seção 12).

## 15. Uber Fx: composição, ciclo de vida e shutdown

Detalhes: ADR-007, ADR-021, ADR-022, ADR-023, ADR-024, ADR-025, ADR-026.

- **Composição** (`cmd/server.Options`): `fx.Supply(cfg)` + `logging`, `metrics`, `postgres`,
  `app`, `auth`, `outboxpublisher`, `pendingworker`, `sqs` (cliente + consumidor),
  `httpadapter`, `httpserver` e as ligações cruzadas (recorder de reconciliação, base do
  backoff de pendências, `METRICS_ACCESS`, readiness `postgres`). Cada pacote expõe um
  `fx.Module` com `fx.Provide`/`fx.Invoke`; o domínio e os casos de uso não importam Fx.
- **Start:** a config é carregada e validada **antes** do `fx.New` (config inválida → exit 1
  sem abrir conexões). O pool faz ping com backoff; o consumidor valida fila, `RedrivePolicy` e
  `SQS_MAX_ATTEMPTS`; o servidor faz listen síncrono (porta ocupada falha o start).
  `StartTimeout` = orçamento (`DATABASE_CONNECT_TIMEOUT` + 15s = 45s) + reserva de rollback
  (= `StopTimeout`, 75s) = **120s** com os defaults; os OnStart que repetem desistem antes da
  reserva para que o rollback consiga parar o que já subiu.
- **Ordem de parada** (inversa do start, sequencial, verificada em teste):

  | # | Componente | O que faz |
  | --- | --- | --- |
  | 1 | servidor HTTP | readiness DOWN, `HTTP_DRAIN_DELAY`, `Shutdown` (recusa conexões novas, drena as em voo), `Close` forçado no prazo |
  | 2 | transporte do JWKS | fecha conexões ociosas com o IdP |
  | 3 | consumidor SQS | para de buscar, espera o último long polling, conclui as em voo até `SQS_DRAIN_TIMEOUT`, libera o resto com visibilidade 0 |
  | 4 | worker de pendências | para de reivindicar; itens em voo até `PENDING_WORKER_DRAIN_TIMEOUT`; os não iniciados voltam pelo lease |
  | 5 | publisher da outbox | para de reivindicar, devolve (`Release`) os não iniciados, conclui envios até `OUTBOX_DRAIN_TIMEOUT` |
  | 6 | cliente SQS | fecha conexões ociosas |
  | 7 | pool pgx | `Close`, por último |

  Dependências (pool, clientes) são construídas antes e paradas depois de quem as usa. O
  worker para antes do publisher para que os eventos gravados na drenagem ainda saiam por
  esta instância.
- **`StopTimeout`** = soma dos orçamentos (os OnStop são sequenciais): `SHUTDOWN_TIMEOUT` 15s
  + `sqs.StopBudget` = max(`SQS_DRAIN_TIMEOUT` 10s, `SQS_WAIT_TIME` 20s + 5s) = 25s +
  `PENDING_WORKER_DRAIN_TIMEOUT` 10s + `OUTBOX_DRAIN_TIMEOUT` 10s + folga 15s = **75s**. No
  Compose, `stop_grace_period: 90s`.
- **Trabalho interrompido no prazo:** contexto cancelado → rollback da transação; a mensagem
  SQS volta com visibilidade 0, o item do worker e o evento da outbox voltam quando o lease
  expira. Nenhum caminho perde trabalho confirmado; no pior caso há reprocessamento,
  absorvido pela idempotência.
- **Sinais:** SIGINT/SIGTERM → parada limpa, exit 0; falha de start/stop ou
  `fx.Shutdowner` com código (ex.: o `Serve` do HTTP morreu) → exit 1.

## 16. Contrato HTTP: códigos e corpos

Detalhes: ADR-019, ADR-020. Erros usam `{code, message, correlationId}` com mensagem
genérica; toda resposta JSON leva `Cache-Control: no-store` e o `X-Correlation-ID`.

| Situação | HTTP | Corpo / `code` |
| --- | --- | --- |
| Carteira aberta | 201 + `Location` | `{id, playerId, balance, version, createdAt, updatedAt}` |
| Operação `PROCESSED` (nova ou replay) | 200 | `{transactionId, status, balance, idempotentReplay}` (+ `referenceTransactionId` em operações com referência) |
| Processamento pendente `PENDING_REFERENCE` | 202 + `Location: /wagering/transactions/{id}` | `{transactionId, status, idempotentReplay}` |
| Rejeição de negócio `REJECTED` (ou `FAILED`) | 422 | `{transactionId, status, failureCode, failureCategory, balance?, idempotentReplay}` |
| Entrada inválida | 400 | `INVALID_REQUEST`, `INVALID_AMOUNT`, `INVALID_CURRENCY`, `INVALID_KIND`, `REFERENCE_REQUIRED`, `REFERENCE_NOT_ALLOWED`, `IDEMPOTENCY_KEY_REQUIRED`, `INVALID_IDEMPOTENCY_KEY`, `INVALID_PLAYER_ID`, `INVALID_INITIAL_BALANCE`, `INVALID_WALLET_ID`, `INVALID_TRANSACTION_ID`, `INVALID_PROVIDER_ID`, `INVALID_EXTERNAL_TRANSACTION_ID`, `INVALID_PAGE_LIMIT`, `INVALID_CURSOR`, `INVALID_CORRELATION_ID` |
| Sem token, token inválido/expirado/de outro issuer ou audiência | 401 + `WWW-Authenticate: Bearer realm="wallet"` | `UNAUTHENTICATED` |
| Sem papel / provedor divergente | 403 | `FORBIDDEN` / `PROVIDER_MISMATCH` |
| Inexistente (ou de outro provedor) | 404 | `WALLET_NOT_FOUND`, `TRANSACTION_NOT_FOUND`, `ROUTE_NOT_FOUND` |
| Método não suportado | 405 + `Allow` | `METHOD_NOT_ALLOWED` |
| Conflito | 409 | `IDEMPOTENCY_KEY_CONFLICT`, `EXTERNAL_TRANSACTION_CONFLICT`, `WALLET_ALREADY_EXISTS` |
| Corpo grande demais (> 64 KiB) | 413 | `PAYLOAD_TOO_LARGE` |
| `Content-Type` diferente de JSON | 415 | `UNSUPPORTED_MEDIA_TYPE` |
| Indisponibilidade transitória (banco, commit ambíguo, prazo, IdP) | 503 + `Retry-After` | `TEMPORARILY_UNAVAILABLE` |
| Erro inesperado / panic | 500 | `INTERNAL_ERROR` |

As cinco situações exigidas são distinguíveis pelo status: entrada inválida (400), conflito
(409), rejeição de negócio (422, com `failureCode`), pendente (202) e indisponibilidade
transitória (503 + `Retry-After`; repetir com a mesma chave é seguro). Em `WALLET_NOT_FOUND`,
`WALLET_PLAYER_MISMATCH` e `CURRENCY_MISMATCH` o 422 não leva `balance`. Health:
`/health/live` 200 `{"status":"UP"}`; `/health/ready` 200 ou 503 com
`{"status":"DOWN","checks":{"postgres":...,"sqs":...,"http-server":...}}`.

## 17. Observabilidade

Detalhes: ADR-007 e as seções de logs/métricas de cada ADR.

- **Logs** JSON (`log/slog`) com `correlationId`, `messageId`, `transactionId`, `walletId` e
  `providerId` na raiz; redação de chaves e padrões sensíveis (tokens, segredos, DSN, payloads)
  e `config.Secret` para DSN e chave AWS. Nenhum corpo de requisição/mensagem é logado.
- **Métricas** (namespace `wallet`, registry próprio, rótulos fechados):
  `wager_transactions_total{source,type,status}`, `duplicates_total`, `retries_total`,
  `dlq_messages_total`, `dlq_depth_messages`, `concurrency_conflicts_total`,
  `outbox_pending_events`, `outbox_oldest_pending_age_seconds`,
  `outbox_publish_delay_seconds`, `processing_duration_seconds`,
  `http_request_duration_seconds`, `reconciliations_total` e
  `reconciliation_divergences_total`. Sem valores monetários e sem IDs em rótulos.
- **Itens envenenados do worker de pendências** (ADR-031): cada falha contada sai em WARN
  (`resumeFailures`, `heldUntil`) e em `processing_duration_seconds{source="worker",
  outcome="error"}`; o encerramento sai em ERROR e em
  `wager_transactions_total{source="worker",status="FAILED"}` (alerta recomendado: qualquer
  incremento). `resume_failures` fica na linha para auditoria.
- **Reconciliação:** `POST /wallets/{id}/reconciliation` em `REPEATABLE READ READ ONLY`;
  `difference = stored − calculated`; divergência → resposta, log WARN e métrica; nada é
  corrigido.
- **Health:** liveness sem dependências; readiness com `postgres` (ping), `sqs`
  (`GetQueueAttributes`) e `http-server` (DOWN durante a parada).

## 18. Interpretações adotadas, limitações, trade-offs e trabalho não concluído

**Interpretações adotadas**

- **Aceite síncrono.** Toda operação é concluída numa transação (`PROCESSED`, `REJECTED` ou
  `PENDING_REFERENCE`), sem commit intermediário de `PENDING` (README §6.3 permite). O item 8
  dos testes ("interromper após `PENDING`") é exercitado pelo equivalente: processo morto com o
  lease de pendências gravado, retomado por outra instância. A retomada de `PENDING` órfão
  existe e é testada, mas só seria alcançada por um aceite assíncrono futuro.
- **Uma reversão bem-sucedida por referência, de qualquer tipo** (seção 9).
- **`REFERENCE_NOT_FOUND`** também cobre a referência que existe mas não terminou dentro do
  prazo (seção 8).
- **`WIN` com referência** credita o próprio valor; a `BET` serve só de vínculo de rodada.
- **Rejeições `INPUT` que dependem do banco** (`WALLET_NOT_FOUND`, `*_MISMATCH`) são
  persistidas como `REJECTED` (com evento), para que o replay seja estável; por isso
  `wager_transactions.wallet_id` não tem FK para `wallets`.
- **422 para `REJECTED` e `FAILED`**: ambos são resultados definitivos e persistidos; `status`
  e `failureCategory` os distinguem.
- **Isolamento entre provedores:** a mesma `externalTransactionId`/chave em outro provedor é
  outra operação (únicos escopados por provedor); transação alheia por id → 404, provedor
  divergente no corpo/path → 403.
- **Grupo dos eventos = `aggregateId`** (não `walletId`), com ordem entre grupos dada por
  `walletVersion` (seção 13).
- **Outbox sem DLQ**: retry indefinido com backoff, por não perda (seção 11).

**Limitações e trade-offs**

- **`FAILED` só no worker de pendências** (ADR-031). É o único lugar onde existe uma linha
  confirmada e não terminal que pode falhar para sempre. Transitórios esgotados no SQS
  continuam indo à DLQ sem tocar no banco, e no HTTP viram 503 (o aceite é síncrono: não há
  linha a encerrar); a auditoria deles fica em logs, métricas e na DLQ. `PROCESSING_FAILED`
  continua reservado.
- **Erro não classificado conta como permanente.** A fronteira transitório × permanente é a
  de `ErrUnavailable` (ADR-013). Um erro transitório que o adapter não reconheça seria contado;
  com 5 falhas seguidas e backoff (~30s), uma falha curta e desconhecida não esgota o limite,
  mas uma longa poderia. A contrapartida é a garantia de término de todo item envenenado.
  Por isso disco/memória esgotados (`53000`, `53100`, `53200`) e escrita em servidor somente
  leitura (`25006`, failover) passaram a ser transitórios: são condições do ambiente, não do
  item (`53400`, limite de configuração, continua permanente).
- **Contagem e encerramento em commits separados** (ADR-031). A contagem é confirmada antes
  de tentar gravar `FAILED`, para que nenhum erro do encerramento desfaça o incremento (a causa
  do loop infinito achado na avaliação da T30). Limite residual: se nem o `UPDATE` sem agregado
  conseguir gravar `FAILED` (ex.: trigger ou CHECK recusando a linha), o item não termina
  sozinho, mas não gira livre: volta no máximo a cada `PENDING_REFERENCE_MAX_DELAY`, sempre
  contando, com log ERROR por rodada, sem mover saldo. Exige intervenção manual.
- **Falha depois do TTL encerra na hora:** uma pendência que já passou de
  `PENDING_REFERENCE_TTL` e falha por erro não transitório vira `FAILED` na primeira falha (o
  prazo da operação já acabou; sem a falha ela teria virado `REJECTED REFERENCE_NOT_FOUND`).
- **`FAILED` sem evento** (seção 13): consumidores da fila de eventos não ficam sabendo do
  encerramento; operação acompanha pelo log ERROR e pela métrica.
- **Wake-up de pendências** (T31, ADR-032; resolvia a lacuna "pendências não são acordadas"):
  a chegada da referência antecipa `next_attempt_at` no mesmo commit, e a resolução sai no
  próximo poll do worker (até `PENDING_WORKER_POLL_INTERVAL`, 1s; não há notificação
  in-process — o wake-up é por linha no banco, então vale para qualquer réplica). Custos e
  limites: um `UPDATE` a mais (sonda de índice, normalmente 0 linhas) por operação terminal,
  medido em ~0,6 ms por operação sequencial no Docker local (essencialmente um round-trip);
  uma dependente travada por outra transação no instante do commit é pulada e, no pior caso,
  espera o backoff que já tinha (na mesma carteira isso não acontece: o lock da carteira
  serializa o worker e a referência); o `FAILED` gravado pelo caminho sem agregado (linha que
  não reidrata, ADR-031) não acorda dependentes — elas veem o `FAILED` no próprio backoff.
- **`Money.UnmarshalJSON` estrito** (T32, ADR-033; resolvia a lacuna "`UnmarshalJSON`
  permissivo"): rejeita chave com outra caixa, repetida ou desconhecida, campo ausente,
  `null` e tipos errados, como o `strictjson`. Trade-off: as regras de objeto existem em dois
  lugares (domínio e adapter), fixadas por casos espelhados nas duas suítes (ADR-033). O método
  aceita negativos (round-trip de diferenças); só o `strictjson` exige não negativo.
- **Revogação de token** só vale na expiração (validação local por JWKS).
- **`POST /wallets` sem chave de idempotência:** um commit ambíguo (503) seguido de nova
  tentativa devolve 409 `WALLET_ALREADY_EXISTS`; o cliente consulta a carteira. Sem risco
  financeiro (UNIQUE da carteira e do OPENING).
- **Outbox:** um evento que falha sempre bloqueia o seu agregado indefinidamente (visível em
  métricas). Um agregado com fila longa avança um evento por ciclo (head-of-line).
- **DLQ do SQS:** fora da janela de 5 min de deduplicação, uma falha de `DeleteMessage` depois
  do envio à DLQ pode deixar uma cópia duplicada na DLQ (inofensiva no reprocessamento). Uma
  válida pode chegar à DLQ pelo redrive nativo se acumular `maxReceiveCount` recebimentos sem
  desconto (reinícios repetidos com a mensagem em voo); fica retida, não perdida.
- **Parada de réplica ociosa** leva até `SQS_WAIT_TIME` (20s), porque o consumidor espera o
  último long polling em vez de cancelá-lo (cancelar escondia mensagens liberadas).
- **Desvio de relógio:** leases da outbox usam o relógio do banco; os do worker de pendências e
  `created_at` usam o relógio da instância (um desvio só antecipa/atrasa uma reassunção,
  sem duplicar efeito, por causa do fencing). A paginação do ledger usa `seq`, imune a desvio.
- **Triggers podem ser desligados** pelo dono das tabelas ou superusuário; em produção a
  aplicação deve usar um papel que não seja dono. `down` das migrations destrói dados.
  Migrations não suportam `CREATE INDEX CONCURRENTLY`.
- **LocalStack Community não aplica IAM**: as políticas do broker estão provisionadas, mas só
  seriam aplicadas na AWS.
- **Segredos de desenvolvimento** (clients do Keycloak, admin, senha do banco) são versionados
  de propósito para o ambiente local.
- **Reversão de devolução desfeita:** `REFUND` + `ROLLBACK(REFUND)` impede nova devolução da
  mesma `BET` (seção 9).
- **Testes multi-instância** usam um IdP de teste; o Keycloak real é coberto nos testes E2E
  de autenticação, com uma instância (a validação é a mesma em todas).

**Pendências abertas no código (não resolvidas nesta entrega)**

- **Índice sem uso** `wallet_ledger_entries_wallet_page_idx` (`0003`, em `(wallet_id,
  created_at, id)`): a paginação passou a usar `(wallet_id, seq)` na `0006`. Custo só de
  escrita; remover numa migration futura.
- **`tools.go`** ainda importa com `_` dependências que já são usadas diretamente (`aws-sdk`,
  `uuid`, `pgxpool`). Inofensivo (build tag `tools`, fora do binário), mas é limpeza pendente
  do ADR-001.
- **Fora do escopo:** ledger de partidas dobradas, tracing (OpenTelemetry), dashboards e
  testes de carga (diferenciais opcionais) não foram implementados. Coletor de estatísticas do
  `pgxpool` não é exportado.

---

# Parte II — Registro de decisões (ADRs)

Histórico incremental. As notas "Para a T23/T24" e "pendente" dentro dos ADRs foram
resolvidas nas tarefas indicadas (ver Parte I e as notas de revisão de cada ADR).

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
  dependência passar a ser usada diretamente. *(Estado na T28: os quatro imports restantes —
  `aws-sdk-go-v2/config`, `service/sqs`, `uuid`, `pgxpool` — já são usados diretamente; a
  remoção ficou pendente, seção 18.)*
- **Dependências acrescentadas depois:** `github.com/go-jose/go-jose/v4` (JWKS próprio,
  ADR-019), `go.uber.org/goleak` e `testcontainers-go` (só testes).

## ADR-002 — Layout de pacotes (hexagonal / ports & adapters) (T01)

```
cmd/server/                 composição Fx (main) — único lugar que conhece todos os módulos
internal/domain/            modelo puro: Money, Wallet, ledger, WagerTransaction, regras
internal/app/               casos de uso + portas (interfaces de repositório, tx, publisher)
internal/adapters/http/     handlers net/http (package httpadapter, evita colisão com net/http)
internal/adapters/postgres/ repositórios pgx, gerenciador de transações
internal/adapters/sqs/      cliente SQS e consumidor de entrada
internal/adapters/outboxpublisher/ publisher da outbox (T22; inicialmente previsto em sqs/)
internal/adapters/auth/     verificação OIDC e autorização
internal/adapters/pendingworker/ worker temporizado de referências pendentes e PENDING órfãos (T21)
internal/platform/config/   configuração por variáveis de ambiente
internal/platform/logging/  slog JSON
internal/platform/metrics/  registro Prometheus
```

*(Árvore completa e atual, com `cmd/migrate`, `migrations`, `http/server`, `http/apierror`,
`strictjson`, `postgres/migrate`, `platform/startbudget`, `platform/failpoint`, `deploy` e
`scripts`: Parte I, seção 1.)*

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

Pacotes `internal/platform/{config,logging,metrics}`, cada um com um `fx.Module` e teste de
`fx.ValidateApp`. Conectados em `cmd/server` na T23 (ADR-025); lá a config é carregada antes
do `fx.New` e injetada com `fx.Supply`, em vez de `config.Module`.

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
  métricas, logs e na consulta da transação. *(Reavaliado e mantido na T30, ADR-031.)*
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
  `IsRetryable`: `40001`, `40P01`, `55P03`, `57014`, `08xxx`, `57P01-03`, `53300` (e, desde
  a T30, `53000`/`53100`/`53200` e `25006`, ADR-031) vindos do servidor (inclusive no
  COMMIT, porque se o servidor respondeu, não confirmou), além de falha de conexão antes do
  COMMIT. `RetryPolicy.RetryIf` amplia isso. Uso previsto na T15: corrida no
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
  Não registra coletor de estatísticas do pgxpool (não foi acrescentado na T23; seção 18).
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
  foi feita na T23 (ADR-025).
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
    outra carteira (pendência T18/T20, respeitada pelo HTTP e pelo SQS).
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
    opcional; a composição provê `fx.Provide(func(m *metrics.Metrics)
    app.ReconciliationRecorder { return m })` desde a T23 (ADR-025; fiação coberta por teste).
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
  (`KC_HOSTNAME`); resolvido na T24 (ADR-026, "Issuer OIDC").
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
  não abre conexão com o IdP. Ligado em `cmd/server` na T23 (ADR-025), que também passou a
  fechar o transporte HTTP do JWKS no OnStop.
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
    `KEYCLOAK_URL` faz `t.Skip` com mensagem explícita. Na T17 não havia Docker/Keycloak;
    desde a T24/T27 ele roda contra o Keycloak do Docker Compose
    (`make test-integration-compose`, ADR-026/029). O realm precisa ter: mapper de audiência `wallet-api` nos clients,
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
  duplicadas", porque o HTTP não usa aquele `UnmarshalJSON` *(o próprio método ficou
  estrito na T32, ADR-033)*. A forma equivalente `"25"`
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
  | Entrada inválida | 400 | `code` = `INVALID_REQUEST`, `INVALID_AMOUNT`, `INVALID_CURRENCY`, `INVALID_KIND`, `REFERENCE_REQUIRED`, `REFERENCE_NOT_ALLOWED`, `IDEMPOTENCY_KEY_REQUIRED`, `INVALID_IDEMPOTENCY_KEY`, `INVALID_PLAYER_ID`, `INVALID_INITIAL_BALANCE`, `INVALID_WALLET_ID`, `INVALID_TRANSACTION_ID`, `INVALID_EXTERNAL_TRANSACTION_ID`, `INVALID_PAGE_LIMIT`, `INVALID_CURSOR`, `INVALID_CORRELATION_ID` |
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
  - As verificações entram pelo grupo Fx `readinessChecks`. Desde a T23 (ADR-025) são
    `postgres` (`db.Ping`), `sqs` (consumidor) e `http-server` (drenagem).
  - **Decisão sobre `/metrics`:** por padrão exige o serviço interno
    (`MetricsInternal`). As métricas não têm valores monetários nem ids (ADR-007), mas
    expõem volume e taxas de negócio, e a API compartilha a porta com o tráfego externo.
    `MetricsAccess = MetricsPublic` torna o path público (exato), para um scraper sem
    OAuth em rede privada. Na composição (T23) a escolha vem de `METRICS_ACCESS`
    (`internal`, padrão, ou `public`).
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
  ctx do OnStop. Quando o ctx do OnStop vence, o Fx devolve `ctx.Err()` por conta própria
  e pode abandonar o hook. Por isso a composição (T23, ADR-025) usa `fx.StopTimeout` = soma
  dos orçamentos de todos os componentes + folga (75s com os defaults).
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
    Na T20 não havia Docker nem LocalStack nesta máquina e os testes de integração do SQS
    só compilavam (pulados sem `SQS_ENDPOINT`). Desde a T24 eles rodam contra o LocalStack
    do Compose (`make test-integration-compose`, ADR-026), o que revelou e corrigiu o
    defeito de shutdown descrito acima.
- **Composição (feita na T23, ADR-025):** `sqs.Module` junto de `postgres.Module` e
  `app.Module`. A parcela do consumidor no `fx.StopTimeout` é `sqs.StopBudget` =
  max(`SQS_DRAIN_TIMEOUT`, `SQS_WAIT_TIME` + 5s), revisada na T24.

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
  - A primeira espera é a do aceite (`WithReferenceRetryDelay`, padrão 2s). Desde a T23 a
    composição passa o mesmo `PENDING_REFERENCE_RETRY_BASE_DELAY` (`app.ReferenceRetryDelay`).
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
    transição para `FAILED`. *(Resolvido na T30, ADR-031: falhas não transitórias
    consecutivas são contadas em `resume_failures` com backoff no lease, e o item vira
    `FAILED RETRIES_EXHAUSTED` ao esgotar `PENDING_WORKER_MAX_FAILURES` ou o TTL.)*
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
- **Composição (feita na T23, ADR-025):** `pendingworker.Module` com `postgres.Module` e
  `app.Module`; `app.ReferenceRetryDelay` vem de `PENDING_REFERENCE_RETRY_BASE_DELAY`; o
  `fx.StopTimeout` inclui `PENDING_WORKER_DRAIN_TIMEOUT`.

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
    `MessageGroupId`, `MessageDeduplicationId`, corpo e atributos conferidos. Na T22 não
    havia LocalStack nesta máquina; desde a T24 roda em `make test-integration-compose`.
- **Composição (feita na T23, ADR-025):** `outboxpublisher.Module` com `postgres.Module` e
  `sqs.ClientModule` (já incluído em `sqs.Module`); o `fx.StopTimeout` inclui
  `OUTBOX_DRAIN_TIMEOUT`.
- **Provisionamento (feito na T24, ADR-026):** `wager-events.fifo` com `FifoQueue=true` e
  `ContentBasedDeduplication=false` (deduplicação por `eventId`); a aplicação tem
  `sqs:SendMessage` nessa fila; consumidores de eventos precisam de
  `ReceiveMessage`/`DeleteMessage`.

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
  `sqs.StopBudget` = max(`SQS_DRAIN_TIMEOUT`, `SQS_WAIT_TIME` + 5s) +
  `PENDING_WORKER_DRAIN_TIMEOUT` + `OUTBOX_DRAIN_TIMEOUT` + folga de 15s. A folga cobre a
  liberação das mensagens SQS depois do estouro da drenagem (5s por chamada, em paralelo), a
  última marcação da outbox (`OUTBOX_PUBLISH_TIMEOUT`) e o fechamento do pool e dos clients.
  Com os defaults: 15 + 25 + 10 + 10 + 15 = **75s**. *(Histórico: na T23 a parcela do SQS
  era só `SQS_DRAIN_TIMEOUT`, total 60s, reserva 60s e `StartTimeout` 105s. Na T24 o Stop do
  consumidor passou a esperar o último long polling (ADR-022, "Shutdown") e os valores foram
  revistos para 75s / 75s / 120s.)* Se o ctx vencesse antes, o
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
  iniciou; default 75s) = `StartTimeout` (default 120s). `cmd/server` fornece a reserva
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
  - `internal-service`: papel `wallet-internal`, sem `provider_id`;
  - `e2e-short-lived` e `e2e-other-audience`: clients só de teste, acrescentados na T27
    (ADR-029). O primeiro não tem papel da aplicação e emite tokens de 5s. O segundo tem
    `wager-provider` e `provider_id`, mas não tem audience mapper.

  Os três clients da aplicação têm um audience mapper com `included.custom.audience=wallet-api`
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
  `TestIntegrationConsumerShutdownReleasesVisibility` (T20) falhava sempre nesta máquina. Na versão da T20, o `Stop` do consumidor cancelava no cliente o `ReceiveMessage` em long
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
  inteira (`-p 1`). Os cenários com várias instâncias independentes e com falhas estão na T26 (ADR-028).

## ADR-028 — Testes multi-instância e recuperação de falhas (T26)

- **Instâncias = processos do SO.** Os cenários do README §8 ("pelo menos três processos
  independentes") e de "Testes de concorrência e recuperação" (itens 4–8) rodam com o
  binário `cmd/server` de verdade. O teste o compila uma vez por execução
  (`go build -tags failpoint`, com `-race` quando o próprio teste roda com `-race`) e inicia
  N processos com `HTTP_ADDR` distintos. Todos usam o mesmo banco (exclusivo do teste,
  `TEST_DATABASE_URL`) e as mesmas filas FIFO do LocalStack (exclusivas do teste). Cada
  processo tem seu pool pgx, seu cliente SQS, seu consumidor, seu publisher da outbox e seu
  worker de pendências. Nada é compartilhado em memória. Grafos Fx no mesmo processo foram
  descartados: não dá para "matar" um deles sem matar o teste, e eles dividiriam o runtime
  (GC, `http.DefaultTransport`, relógio). Os filhos rodam com `GORACE=halt_on_error=1`.
  Uma corrida detectada derruba o processo, e o harness falha o teste se encontrar
  `WARNING: DATA RACE` na saída de qualquer filho. O IdP é o de teste do pacote
  (httptest): os filhos buscam o JWKS nele por HTTP. O Keycloak real é coberto na T27 (ADR-029).
- **Crash.** Encerramento abrupto do processo: `os.Process.Kill` (TerminateProcess no
  Windows, SIGKILL no Linux), sem `OnStop`, sem defers, sem `DeleteMessage`/`MarkPublished`
  pendentes. O Windows não tem SIGTERM, então não há parada graciosa nesses cenários,
  apenas o kill. Para matar o processo num ponto exato, o código de produção chama
  `failpoint.Hit(nome)` (`internal/platform/failpoint`):
  - no build normal, `Hit` é uma função vazia (inlined), o ambiente nem é lido e o binário
    de produção (Dockerfile) não tem como ativar falhas;
  - com a build tag `failpoint`, `WALLET_FAILPOINTS=nome=crash` faz o processo escrever
    `FAILPOINT-HIT nome` no stderr e se matar (`Kill` do próprio pid). `nome=hang` bloqueia
    a goroutine para o teste matar de fora.

  Pontos instrumentados:

  | Ponto | Onde |
  |---|---|
  | `sqs-consumer-after-commit` | consumidor: tratamento confirmado (inbox COMPLETED no commit), antes do `DeleteMessage` |
  | `outbox-after-claim` | publisher: lote reivindicado (lease gravado), nada enviado |
  | `outbox-after-send` | publisher: `SendMessage` aceito, antes do `MarkPublished` |
  | `pending-after-claim` | worker: lote de pendências reivindicado (lease gravado), nenhuma retomada iniciada |
- **Indisponibilidade de dependências.** Um proxy TCP do teste fica entre os processos e o
  PostgreSQL (DSN reescrito) ou o LocalStack (`AWS_ENDPOINT_URL` e URLs das filas
  reescritos). As conexões de verificação do teste vão direto à dependência. Há dois modos:
  - `reset`: derruba as conexões abertas e fecha as novas na hora, como um restart ou uma
    queda;
  - `pause`: congela o tráfego das conexões abertas e segura as novas sem conectar, como
    `docker compose pause`. Nesse modo os bytes retidos são entregues depois da volta,
    mesmo que o cliente já tenha desistido. Isso produz commits ambíguos (o cliente recebe
    erro e o COMMIT acontece), o caso mais difícil para a idempotência.

  Escolhemos o proxy e não `docker pause/stop`: ele afeta só os processos do teste (o
  PostgreSQL e o LocalStack do Compose seguem atendendo a verificação) e não deixa container
  pausado se o teste morrer. Cada teste aplica 3s de `reset`, espera o readiness voltar, 4s
  normais e 3s de `pause`, sob carga contínua.
- **Cenários** (`cmd/server/multiinstance_test.go`):
  - `TestMultiInstanceConcurrency`: 3 processos. A disputa 80/80 em 6 carteiras ao mesmo
    tempo, cada aposta por um processo diferente e uma em cada três pelo SQS (consumida por
    qualquer processo). Resultado: uma PROCESSED e uma `INSUFFICIENT_FUNDS` por carteira,
    saldo 20.00 e um débito. A mesma aposta vai 45× pelo HTTP (nos 3 processos) e 5× pelo
    SQS, e gera um débito. Reenvios cruzados (outro processo, outro transporte) são replays,
    sem lançamento nem outbox novos.
  - `TestFailureConsumerKilledAfterCommitBeforeDelete`: o processo A morre em
    `sqs-consumer-after-commit` com duas apostas de 80.00 do mesmo grupo na fila. A
    primeira fica PROCESSED, com inbox COMPLETED e a mensagem ainda na fila; a segunda não
    é processada (o grupo FIFO fica retido). Quando a visibilidade (4s) vence, B ou C
    recebe a primeira de novo, devolve o replay (log `idempotentReplay:true` com o
    `messageId`) e só então a apaga. A segunda vira `INSUFFICIENT_FUNDS`. Resultado: um
    débito e duas linhas na inbox.
  - `TestFailureOutboxPublishersCrash`: a outbox é preenchida (64 eventos) por um processo
    cujo publisher fica ocioso (`OUTBOX_POLL_INTERVAL=50m`). Dois publishers disputam a
    outbox e morrem: P1 em `outbox-after-send` (publicado sem confirmação) e P2 em
    `outbox-after-claim` (commit sem publicação). Os leases deles ficam gravados. P3 e P4,
    também em disputa, os reassumem quando expiram (`attempts >= 2`) e publicam tudo.
  - `TestFailureReversalBeforeReference`: ROLLBACK/REFUND chegam antes das referências,
    pelo HTTP de processos diferentes e pelo SQS, e ficam `PENDING_REFERENCE` (202). Duas
    referências chegam depois, por outro processo e pelo SQS, e as reversões são resolvidas
    pelo worker de qualquer instância. As outras duas expiram com `REFERENCE_NOT_FOUND` no
    TTL (8s). A referência que chega depois da expiração é processada sem reabrir a
    reversão. Replays em outro processo devolvem o estado final (200 ou 422). Há um
    `WagerTransactionPendingReference` por pendência.
  - `TestFailureRestartPreservesIdempotencyAndPending`: os três processos morrem (kill) com
    operações concluídas, uma mensagem consumida e duas reversões pendentes (`attempts >= 2`).
    O processo D reivindica as pendências e morre em `pending-after-claim`, deixando o lease
    gravado. Depois sobem E, F e G:
    - os replays HTTP devolvem o resultado original (transação, status, código e saldo);
    - a reentrega SQS do mesmo `messageId` é replay pela inbox;
    - não surge nenhuma transação ou lançamento novo;
    - a pendência cuja referência chega é resolvida (o lease de D é reassumido quando
      expira);
    - a outra expira no TTL, sem regressão de `attempts`.
  - `TestFailurePostgresUnavailable`: 3 processos com apostas contínuas em 6 carteiras
    (3/4 pelo HTTP, 1/4 pelo SQS) atravessam `reset` e `pause` do PostgreSQL. O readiness
    fica 503 com `postgres` DOWN e volta a 200. O provedor reenvia a mesma operação em
    503/erro de transporte, e o teste exige que algum reenvio tenha acontecido. As
    mensagens voltam com o backoff do consumidor. No fim, cada operação foi processada
    exatamente uma vez, a inbox está toda concluída e a DLQ vazia.
  - `TestFailureSQSUnavailable`: o mesmo, com o LocalStack. O readiness fica 503 com `sqs`
    DOWN. O consumidor volta a receber e o publisher registra a falha (`MarkFailed`, backoff
    persistido; o teste exige `last_error`/`attempts >= 2` em algum evento) e publica tudo
    quando o SQS volta.
- **Verificação final de todo cenário.**
  - A reconciliação do ADR-027 no banco inteiro: saldo == créditos − débitos do ledger,
    cadeia `balance_before`/`balance_after`, sem saldo negativo, sem lançamento duplicado ou
    de transação não PROCESSED, sem operação externa duplicada, outbox coerente com o status
    de cada transação e `POST /wallets/{id}/reconciliation` consistente.
  - Nenhuma transação `PENDING`/`PENDING_REFERENCE` sobra.
  - A fila de entrada e a DLQ terminam vazias, com a inbox concluída na contagem esperada.
  - **Entrega da outbox sem perda.** Toda a outbox tem `published_at`. A fila de eventos é
    drenada e contém exatamente os `eventId` da outbox, com o corpo byte a byte igual ao
    snapshot gravado. A primeira entrega de cada agregado segue a ordem de `seq`. Uma cópia
    repetida (at-least-once) só é aceita com o mesmo `eventId` e o mesmo corpo. Nas
    execuções, o FIFO do LocalStack deduplicou as republicações (0 cópias): P1 publicou e
    morreu, P3/P4 republicaram com o mesmo `MessageDeduplicationId` (`eventId`).
- **Parâmetros dos processos de teste.**
  - Prazos curtos: visibilidade 4s, `SQS_WAIT_TIME` 1s, leases de outbox/worker de 3s,
    backoff de referência de 200ms a 1s.
  - `SQS_MAX_ATTEMPTS=8` com backoff de 1s a 2s (≈13s de orçamento) e filas com
    `maxReceiveCount=10`. Assim uma mensagem atravessa as duas janelas de indisponibilidade
    sem ir à DLQ. Indisponibilidade mais longa que o orçamento leva a mensagem à DLQ, por
    contrato (ADR-022). Isso é retenção, não perda, mas não é o que esses testes medem.
- **Execução** (build tags `integration` + `multiinstance`; a tag separa esses testes de
  `go test -tags=integration ./...`, porque eles compilam o binário e levam cerca de 3
  minutos):

  ```sh
  docker compose up -d --wait postgres localstack
  make test-multiinstance   # = TEST_DATABASE_URL=... SQS_ENDPOINT=http://localhost:4566 \
                            #   CGO_ENABLED=1 go test -race -tags=integration,multiinstance \
                            #   -p 1 -count=1 -run 'TestMultiInstance|TestFailure' ./cmd/server
  ```

  Sem `SQS_ENDPOINT`, o teste falha em vez de ser pulado: quem passa a tag pediu
  explicitamente esses cenários. Nenhum container é pausado ou parado. O proxy é do próprio
  teste.
- **Resultado.** Nenhum defeito encontrado. A única mudança no código de produção são os
  quatro `failpoint.Hit`, no-op no build normal. Os 7 cenários passam com `-race`
  (processos filhos também com `-race`) em execuções repetidas.
- **Limitações.**
  - O fluxo síncrono não confirma `PENDING` (ADR-017), então o item 8 ("interromper depois
    de confirmar `PENDING`") não tem ponto de aceite assíncrono a interromper. O equivalente
    exercitado é o processo que morre com o lease de pendências gravado.
  - O IdP dos processos é o de teste. A autenticação contra o Keycloak real fica para a
    T27.
  - O proxy simula a falha na rede entre aplicação e dependência, não o crash do servidor
    PostgreSQL (WAL/recovery), que é responsabilidade do próprio PostgreSQL.

## ADR-029 — Testes E2E de autenticação/autorização contra o Keycloak real (T27)

- **Escopo.** `cmd/server/auth_e2e_integration_test.go` (build tag `integration`) sobe a
  aplicação completa (`newApp`, mesmo grafo Fx de produção) com a configuração OIDC de
  produção apontando para o realm real do Compose: `OIDC_ISSUER_URL =
  {KEYCLOAK_URL}/realms/junglegaming`, `OIDC_JWKS_URL` vazio (discovery + JWKS reais) e
  audiência `wallet-api`. Nenhum token é assinado pelo teste. Todos vêm do Keycloak por
  `client_credentials` (`provider-a`, `provider-b`, `internal-service` e os dois clients
  de teste abaixo) ou por `password` no `admin-cli` do realm `master`, que é o "outro
  issuer". O SQS é a `sqs.API` em memória do pacote, porque a entrada exercitada aqui é o
  HTTP; a autorização do consumidor SQS não depende de token. O PostgreSQL é real, com um
  banco por teste. Sem `KEYCLOAK_URL`, `t.Skip`.
- **Token expirado com token real.** Não dá para alterar o `exp` de um token real sem
  quebrar a assinatura. Esse caso é o "payload adulterado", testado à parte. Por isso o
  realm ganhou o client **`e2e-short-lived`**, com atributo `access.token.lifespan=5`
  (5s, só nele; o realm continua com 300s), audience mapper `wallet-api` e **nenhum papel
  da aplicação**. O teste:
  1. confere que `exp - iat ≤ 10s` (se o Keycloak foi criado com um realm antigo, a
     mensagem pede `--force-recreate keycloak`);
  2. usa o token ainda válido e recebe 403 `FORBIDDEN` em todas as rotas, o que prova que
     o token é autêntico e que o 401 seguinte vem só da expiração;
  3. espera o `exp` real + `OIDC_CLOCK_SKEW` (2s no teste) + 1s e recebe 401 com
     `error_description="token expired"`;
  4. pede um token novo e recebe 403 de novo.

  A falta de papel desse client também cobre o caso "papel ausente → 403". Alternativas
  descartadas:
  - um IdP de teste, porque a tarefa exige o Keycloak real;
  - mudar o relógio, que não é viável no Windows/Docker Desktop;
  - alterar o lifespan pela Admin API durante o teste, porque mudaria o estado
    compartilhado do Keycloak e exigiria admin no caminho principal.
- **Audiência errada com token real.** Client **`e2e-other-audience`**, com papel
  `wager-provider` e `provider_id=provider-a`, mas **sem** audience mapper (o Keycloak põe
  `aud=account`). Tudo nele está certo, exceto a audiência, e a API o recusa com 401.
  Os dois clients são confidenciais, só com client_credentials, e os segredos são de
  desenvolvimento (`e2e-*-dev-secret`), como os demais. Nenhum consegue operar a API:
  um não tem papel, o outro não tem audiência. O smoke test (`scripts/compose-smoke.sh`)
  continua checando só os três clients da aplicação.
- **Matriz coberta.**
  - **401 `UNAUTHENTICATED` + `WWW-Authenticate: Bearer realm="wallet"`** em todas as 8
    rotas protegidas (wallets ×4, transações ×3, `/metrics`), com corpos válidos para que
    um processamento antes da autenticação aparecesse no banco. Casos:
    - sem `Authorization` ou com ele vazio, esquema `Basic`, `Bearer` sem token ou token
      sem esquema;
    - não-JWT, token com espaço, dois cabeçalhos `Authorization`;
    - assinatura adulterada;
    - payload adulterado com a assinatura original (`provider_id` trocado, papel
      `wallet-internal` injetado, `exp` estendido);
    - `alg=none`;
    - token do realm `master` (outro issuer e outras chaves);
    - audiência errada;
    - token expirado.
  - **403 `FORBIDDEN`** (`error="insufficient_scope"`):
    - provider-a e provider-b em `POST /wallets`, `GET /wallets/{id}`, ledger,
      reconciliação e `/metrics`;
    - `internal-service` nas três rotas de provedor;
    - client sem papel em todas.

    `internal-service` recebe 201/200 nas rotas de carteira e em `/metrics`.
  - **Isolamento entre provedores** (contrato do ADR-019/ADR-018):
    - provider-b consultando por id a transação de provider-a recebe 404
      `TRANSACTION_NOT_FOUND`, com corpo igual ao de um id inexistente (não revela
      existência);
    - consulta no path `/providers/provider-a/...` recebe 403 `PROVIDER_MISMATCH`;
    - a mesma `externalTransactionId` no próprio path de B recebe 404;
    - replay de B com o corpo e a `Idempotency-Key` exatos de A recebe 403
      `PROVIDER_MISMATCH`, e com corpo divergente também (403, não 409, e nada da
      transação de A volta);
    - a mesma `externalTransactionId` e a mesma chave literal enviadas por B como
      provider-b são **outra operação**, porque a idempotência é por provedor (`UNIQUE
      (provider_id, …)`). B recebe outra transação e a linha de A não muda (hash da linha).
      Cada um consulta só a sua, e os replays legítimos devolvem a de cada um;
    - REFUND de B que referencia uma `externalTransactionId` que só A tem fica
      `PENDING_REFERENCE` (202), sem `reference_transaction_id`. Nunca resolve para a
      transação de A.
- **"Nenhum efeito de acesso negado".** Antes e depois de cada lote de requisições
  negadas, com a outbox já publicada para não haver corrida, o teste compara:
  - as contagens de `wallets`, `wager_transactions`, `wallet_ledger_entries`, `inbox` e
    `outbox`;
  - um md5 de todas as linhas de `wallets` e `wager_transactions`, que pega também
    UPDATEs.

  Ao fim de cada teste roda a reconciliação completa (`assertConsistent`, ADR-027). Nenhum
  token nem segredo de client aparece nos logs da aplicação.
- **Resultado.** Nenhum defeito encontrado, e o código de produção não mudou. Mudaram o
  realm (dois clients de teste) e os testes.
- **Execução:**

  ```sh
  docker compose up -d --wait postgres localstack keycloak
  TEST_DATABASE_URL=postgres://wallet:wallet-dev-pw@localhost:5433/wallet?sslmode=disable \
  SQS_ENDPOINT=http://localhost:4566 KEYCLOAK_URL=http://localhost:8081 \
  KEYCLOAK_CLIENT_ID=internal-service KEYCLOAK_CLIENT_SECRET=internal-service-dev-secret \
  CGO_ENABLED=1 go test -race -tags=integration -run TestIntegrationAuthE2E ./cmd/server
  ```

  Segredos dos demais clients: `PROVIDER_A_SECRET`, `PROVIDER_B_SECRET`,
  `E2E_SHORT_LIVED_SECRET`, `E2E_OTHER_AUDIENCE_SECRET`, `KEYCLOAK_ADMIN_USER` e
  `KEYCLOAK_ADMIN_PASSWORD`. Os defaults são os valores de dev do realm e do Compose. O
  Keycloak importa o realm só na criação do container, então, depois de mudar o JSON, é
  preciso rodar `docker compose up -d --wait --force-recreate keycloak`.
- **Limitações.** Os processos multi-instância (ADR-028) continuam com o IdP de teste. A
  validação contra o Keycloak real é a mesma em todas as instâncias (o código é o mesmo, e
  o JWKS fica em cache por processo). A revogação antes do `exp` não é testada, porque não
  existe por desenho (validação local por JWKS, ADR-019).

## ADR-030 — README da solução e verificação final (T29)

- **Onde fica o README da solução.** O enunciado (docs/DESAFIO.md §15) exige que "o
  `README.md` da solução" explique pré-requisitos, variáveis, filas, migrations, execução,
  exemplos e testes. Até a T28 o `README.md` era o próprio enunciado. Ele foi movido sem
  alteração (`git mv README.md docs/DESAFIO.md`, blob idêntico) e o `README.md` passou a ser o
  da solução, com link para o enunciado. Os "README §N" já espalhados pelo ARCHITECTURE.md, pelo
  `task_plan.md` e por comentários do código continuam valendo como referências ao enunciado;
  em vez de reescrever dezenas de comentários, a convenção ficou registrada no topo deste
  documento e do `task_plan.md`.
- **Conteúdo.** Pré-requisitos (Go 1.25+, Docker Compose, gcc para `-race`, make opcional),
  variáveis (o `.env.example` já cobria todas as lidas por `internal/platform/config`; o
  README só agrupa e aponta para ele), filas e contratos de produtor (grupo `walletId`, dedup
  `messageId`, envelope) e de consumidor de eventos (pendência da T22: grupo `aggregateId`,
  dedup `eventId`, atributos, at-least-once, sem ordem entre grupos, `walletVersion`),
  migrations up/down, Compose com 3 réplicas, identidades do realm, exemplos curl com tokens
  `client_credentials` (carteira, aposta, replay, ganho com referência, consultas, ledger,
  reconciliação, 401/403, envio pela fila e leitura dos eventos) e comandos de teste por build
  tag. Todos os comandos do README foram executados contra a stack antes de entrar no texto.
- **LF para Go/SQL.** Com `core.autocrlf=true` (padrão do Git no Windows) um checkout limpo
  grava os `.go` em CRLF e `gofmt -l .` lista todos os arquivos, embora o conteúdo versionado
  esteja formatado. O `.gitattributes` passou a fixar `eol=lf` para `*.go`, `*.sql`, `go.mod` e
  `go.sum` (o índice já estava todo em LF; nenhuma renormalização foi necessária).
- **Verificação final** (clone local do repositório com as mudanças da T29 aplicadas, stack do
  Compose recriada do zero): `docker compose up --build -d --wait` saudável; `--scale app=3`
  saudável; `scripts/compose-smoke.sh` → `SMOKE OK`; exemplos do README §6 executados
  literalmente; `gofmt -l .` vazio; `go vet ./...` (também com `-tags` `integration`,
  `integration,multiinstance` e `failpoint`); `go build ./...`; `go test -count=1 ./...`;
  `CGO_ENABLED=1 go test -race -count=1 ./...`; integração completa contra o Compose
  (`-race -tags=integration -p 1 -count=1 ./...`, nenhum teste pulado) e multi-instância/falhas
  (`-tags=integration,multiinstance`, ~2 min) passando. Nenhuma mudança de código Go.

## ADR-031 — FAILED por esgotamento no worker de pendências (T30)

- **Contexto.** A revisão final (lacunas L1+L2) apontou que `WagerTransaction.Fail` não tinha
  chamador em produção e que um item do worker de pendências que falha sempre (ex.: linha que
  não reidrata) voltava a cada `PENDING_WORKER_LEASE` para sempre: TTL e máximo de tentativas
  só eram avaliados quando `Resume` chegava à decisão, e um erro antes disso só gerava log e
  métrica.
- **Decisão: o que conta como falha.** `Resume` classifica o erro (`classify`); não contam
  `ErrUnavailable` (todas as falhas transitórias do ADR-013: conexão, `57P0x`, `53300`,
  serialização, deadlock, lock/statement timeout, commit ambíguo, prazo) e o cancelamento do
  ctx do próprio worker (shutdown ou `PENDING_WORKER_PROCESS_TIMEOUT`). Esses erros não dizem
  nada sobre o item; contá-los faria uma queda do PostgreSQL levar itens bons a `FAILED`. Eles
  continuam como antes: rollback, retry interno limitado e retorno pelo lease. Todo o resto
  conta: `ErrCorrupted*` (linha, carteira ou referência), violação de constraint, erro de
  domínio inesperado em `DecideWager`/`ApplyWagerDecision`, erro interno. Alternativa
  descartada: contar tudo e confiar num limite alto. Ela não distingue "o banco caiu" de "o
  item é ruim". Outra alternativa descartada: contar só reidratação. Ela deixaria em loop os
  outros erros permanentes.
- **Decisão: onde fica o contador.** Coluna nova `wager_transactions.resume_failures INTEGER NOT
  NULL DEFAULT 0 CHECK (>= 0)` (migration `0009`, sem editar as antigas; ADD COLUMN com default
  constante não reescreve a tabela). `attempts` não foi reaproveitado: ele conta **buscas de
  referência** (ADR-008), aparece no evento `WagerTransactionPendingReference` e tem CHECKs
  próprios (`attempts > 0` exige referência e proíbe `PENDING`). Misturar os dois mudaria a
  política de `REFERENCE_NOT_FOUND` e quebraria o PENDING órfão. Como `claimed_by`/
  `claimed_until` (0007), `resume_failures` é dado operacional do worker, fora do agregado: o
  domínio não o lê nem o valida.
- **Decisão: contagem e backoff.** Depois do rollback da retomada, roda uma transação curta e
  própria só para contar (`handleFailure` → `countFailure`, `Retry: true`, reexecutável), e
  ela é **confirmada antes** de qualquer tentativa de encerramento (passo 3):
  1. `LockClaimedForFailure`: `SELECT status, created_at, resume_failures ... WHERE id = $1 AND
     claimed_by = $token FOR NO KEY UPDATE`. É o mesmo fencing do `GetClaimedForUpdate`, mas
     **sem reidratar**, para funcionar justamente na linha corrompida. Se o lease foi perdido,
     a falha não é registrada: o dono atual decide.
  2. `RecordResumeFailure`: `resume_failures = n`, `claimed_until = agora + Delay(n)`
     (`PENDING_REFERENCE_RETRY_BASE_DELAY · 2^(n−1)`, teto `..._MAX_DELAY`). **O backoff é o
     próprio lease**: o `ClaimDue` só devolve a linha depois dele, o token continua valendo e
     nenhuma coluna do agregado muda (`status`, `attempts`, `next_attempt_at`, `updated_at`).
     Alternativa descartada: reagendar `next_attempt_at`. Ele é do domínio, está limitado ao
     TTL e não existe em `PENDING`.
  3. Esgotado (`n >= PENDING_WORKER_MAX_FAILURES` ou `agora >= created_at + TTL`),
     `failExhausted` grava `FAILED` numa **segunda** transação curta (com o mesmo token; o
     lease foi estendido no passo 2). Se a linha reidrata: `GetClaimedForUpdate` →
     `WagerTransaction.Fail(RETRIES_EXHAUSTED)` → `Update`. Se a própria linha não reidrata
     por **qualquer** erro não transitório (não só `ErrCorruptedWagerTransaction`), ou se
     `Fail`/`Update` falham de forma não transitória, o encerramento é refeito numa transação
     **nova** (a anterior pode ter sido abortada pelo servidor): `LockClaimedForFailure` relê
     o estado e a aresta é validada por
     `domain.ValidateFailTransition(status, código)`. É uma função nova do domínio, com a
     mesma regra de `Fail`: estado conhecido, não terminal, com aresta para `FAILED`, e código
     `INFRASTRUCTURE`. Depois, `FailClaimedUnreadable` roda um `UPDATE ... WHERE id AND
     claimed_by = token AND status = $from`, que grava só as colunas de resultado no formato
     exigido pelos CHECKs de `FAILED`: `failure_code`, `balance_after`/`next_attempt_at` NULL
     e `completed_at = updated_at = max(updated_at, agora)`. Erros transitórios do
     encerramento seguem o retry do `UnitOfWork` e, esgotado, a próxima rodada.
  - **Por que dois commits** (correção na avaliação da T30). Na primeira versão, contagem e
    `FAILED` estavam na mesma transação, e só `ErrCorruptedWagerTransaction` levava ao caminho
    sem agregado. Uma linha com `currency = 'XYZ'` (passa no CHECK `^[A-Z]{3}$`, mas o domínio
    não suporta) falhava em `moneyFromColumns`, cujo erro o `scanWager` não marcava como
    corrompido. No esgotamento, `GetClaimedForUpdate` devolvia esse erro, a transação inteira
    fazia rollback (inclusive o incremento), e o contador ficava em `MAX−1` para sempre (loop
    infinito). Correções: (a) `scanWager` marca **toda** falha de reidratação, inclusive a do
    `Money`, com `ErrCorruptedWagerTransaction` (isso também impede que o replay de uma linha
    assim vire `400 INVALID_CURRENCY`, como se fosse erro de entrada); (b) o caminho sem
    agregado cobre qualquer erro não transitório; (c) a contagem é confirmada antes do
    encerramento. A garantia passa a ser: **toda rodada com erro não transitório avança o
    contador de forma durável**, e a rodada de esgotamento termina em `FAILED`. Limite
    residual, documentado na seção 18: se nem o `UPDATE` sem agregado puder gravar `FAILED`
    (ex.: trigger/CHECK recusando a linha), o item continua `PENDING*`, volta no máximo a cada
    `PENDING_REFERENCE_MAX_DELAY`, conta de novo e loga ERROR. Não move saldo e exige
    intervenção manual. Alternativa descartada: `SAVEPOINT` dentro da mesma transação. Ela
    exigiria ampliar a porta `Tx` só para isso, e um commit separado dá a mesma durabilidade.
    O custo é uma janela em que a linha já tem `resume_failures >= MAX` mas ainda não é
    `FAILED`: se o processo morrer ali, a próxima rodada conta de novo e encerra.
  - **Decisão: SQLSTATEs de recurso e somente leitura** (observação da avaliação). `53000`,
    `53100` (disco cheio), `53200` (sem memória) e `25006` (escrita em servidor somente
    leitura, típico de failover) passaram a ser transitórios em `postgres.TransientReasonOf`
    (razões `insufficient_resources` e `read_only`). Antes, contavam como falha permanente: um
    disco cheio por ~30s levaria itens bons a `FAILED`. Agora fazem rollback, entram no retry
    limitado (nada foi confirmado) e, no HTTP/SQS, viram 503/reentrega. `53400`
    (`configuration_limit_exceeded`) continua permanente.
  - `Update` zera `resume_failures` em toda transição gravada, exceto em `FAILED`, onde o valor
    fica para auditoria. Por isso o limite conta falhas **consecutivas**.
  - **Lock e fencing:** a transação de falha trava só a própria linha (`FOR NO KEY UPDATE`, que
    não conflita com o `KEY SHARE` da FK de quem insere uma reversão citando a linha). Não
    trava a carteira, porque `FAILED` não movimenta saldo. Com uma única trava não há ciclo de
    espera. `Update` e `FailClaimedUnreadable` só gravam a linha travada por este token, como
    no contrato dos ADR-014/023.
- **Decisão: TTL.** Usa o mesmo `PENDING_REFERENCE_TTL` desde `created_at` (vale também para
  `PENDING` órfão). Uma falha depois dele já encerra, porque o prazo da operação acabou.
- **Decisão: código e evento.** O código é sempre `RETRIES_EXHAUSTED`: a retomada foi tentada e
  as tentativas esgotaram. `PROCESSING_FAILED` continua reservado. **Sem evento na outbox:** o
  enunciado §11 lista os eventos exigidos (processado, rejeição de negócio, saldo, espera por
  referência), e `FAILED` ("falha permanente de infraestrutura registrada para auditoria",
  §6.3) não é nenhum deles. Emitir `WagerTransactionRejected` para `FAILED` mentiria sobre a
  natureza da falha. Um tipo novo (`WagerTransactionFailed`) ampliaria o contrato da fila sem
  um consumidor que o exija. A escolha mantém a coerência com o ADR-012 e a seção 13. A
  visibilidade vem de log ERROR, `wager_transactions_total{source="worker",status="FAILED"}`,
  consulta e replay.
- **Replay.** Nada mudou nos adapters: `ProcessWagerTransaction` devolve o estado persistido
  (`IdempotentReplay`); o HTTP já mapeava `FAILED` para 422 (`failureCategory:
  INFRASTRUCTURE`), e o consumidor SQS trata `FAILED` como terminal (conclui a inbox e apaga a
  mensagem). Reversões que citam uma referência `FAILED` recebem `REFERENCE_NOT_PROCESSED`
  (ADR-009).
- **Resultado de `Resume`.** Há dois desfechos novos. `ResumeDeferred` vem **com** o erro
  original e traz `ResumeFailures`/`HeldUntil`. `ResumeFailed` vem com erro nil (estado
  terminal, como `ResumeExpired`) e leva a causa em `Cause`. O worker loga WARN/ERROR e
  registra as métricas.
- **Configuração.** `PENDING_WORKER_MAX_FAILURES` (padrão 5, faixa 1..1000) chega a
  `app.PendingWagerPolicy.MaxResumeFailures` via `config.PendingWorkerConfig.MaxFailures` e
  `pendingworker.PolicyFrom`. Ela está documentada no `.env.example` e no README §2. Com os
  defaults, um item envenenado termina em ~30s (2s+4s+8s+16s de backoff).
- **Testes.**
  - Domínio: `TestValidateFailTransition_AgreesWithFail` (para todo estado × código,
    `ValidateFailTransition` e `Fail` concordam).
  - Unitários (`internal/app/resume_wager_failure_test.go`, UnitOfWork em memória com linhas
    "envenenadas"):
    - linha ilegível → backoff 2s/4s (não reivindicada antes do prazo) → `FAILED` pelo caminho
      sem agregado, sem efeitos, nunca mais reivindicada, e o replay devolve `FAILED`;
    - erro permanente com linha legível → `FAILED` pelo domínio;
    - transitórios e cancelamento não contam, e o item se resolve quando a infraestrutura volta;
    - uma retomada bem-sucedida zera o contador;
    - uma falha depois do TTL encerra na hora;
    - o dono antigo do lease não registra a falha;
    - linha ilegível por erro **não marcado** como corrompido → `FAILED` pelo caminho sem
      agregado, contando 1, 2, 3 de forma durável (`TestResumeWager_AnyUnreadableErrorEndsFailed`);
    - `Update` de `FAILED` recusado (erro permanente) → `FAILED` pelo caminho sem agregado
      (`TestResumeWager_AggregateFailWriteErrorFallsBack`);
    - nem o caminho sem agregado grava (erro permanente e depois transitório) → o contador
      avança a cada rodada (2, 3), o item fica retido pelo backoff e encerra (4) quando a
      gravação volta (`TestResumeWager_FailWriteErrorKeepsCountDurable`).
  - Adapter: `TestScanWager_RehydrationErrorsAreCorrupted` (moeda não suportada em `amount` e
    em `balance_after`, e caractere de controle, saem com `ErrCorruptedWagerTransaction` e não
    são mapeados por `ValidationFailureCode`); `TestTransientClassification` cobre
    `53000`/`53100`/`53200`/`25006` (transitórios) e `53400` (permanente).
  - Worker: `TestWorker_DeferredAndFailedOutcomes` (métrica `FAILED`; o ciclo não para).
  - Integração (`internal/adapters/pendingworker/worker_failure_integration_test.go`, PostgreSQL
    real, worker rodando):
    - linha inserida com `round_id` contendo caractere de controle, ou com `currency = 'XYZ'`
      (passam nos CHECKs, não reidratam; subtestes) → `FAILED RETRIES_EXHAUSTED`, `resume_failures = 3`, sem lease, sem ledger e
      sem outbox, inalterada depois. Um `ROLLBACK` saudável do mesmo ciclo é resolvido, e
      `assertReconciled` e `ReconcileWallet` confirmam a consistência;
    - `REFUND` aceito normalmente cuja referência chega corrompida → `FAILED` pelo domínio
      (`attempts` preservado, só o evento `PendingReference` original), e o replay devolve
      `FAILED`;
    - fencing por token alheio, retenção pelo backoff no `ClaimDue` e contador zerado depois
      de uma retomada bem-sucedida.
  - Schema: o CHECK `wager_transactions_resume_failures_non_negative` recusa valor negativo, e
    `FAILED` com contador é aceito. `TestDownSteps` passa a reverter 6 migrations (0009..0004)
    e o `migrate_test` conhece a `0009`.
- **Fora do escopo.** O wake-up de pendências quando a referência chega fica na T31. Um
  operador que corrige um dado corrompido antes do esgotamento só precisa esperar o backoff:
  a próxima retomada bem-sucedida zera o contador.
- **Revisão (T31).** O wake-up está no ADR-032 e não altera nada do contador de falhas:
  `WakePendingReferences` não toca em `resume_failures` nem no lease, então um item retido pelo
  backoff de falhas continua retido. O `FAILED` gravado pelo agregado (`failExhausted`) também
  acorda as dependentes, que recebem `REFERENCE_NOT_PROCESSED`.

## ADR-032 — Wake-up de pendências quando a referência chega (T31)

- **Contexto.** Lacuna L3 da revisão final: uma reversão (ou `WIN`) aceita antes da sua
  referência fica `PENDING_REFERENCE` com `next_attempt_at` no backoff exponencial (2s, 4s, …,
  até 1m). Quando a referência chegava, nada avisava a pendência: a resolução esperava o
  próximo `next_attempt_at`, até `PENDING_REFERENCE_RETRY_MAX_DELAY` depois.
- **Decisão: antecipar o agendamento na mesma transação.** `persistWagerOutcome` (usado pelo
  fluxo síncrono e pelo worker), depois da outbox, chama `wakeDependents`: se a operação é
  `EXTERNAL` e ficou **terminal**, roda `WagerTransactionStore.WakePendingReferences(provider,
  externalId, t.UpdatedAt())`. `failExhausted` (ADR-031) faz o mesmo ao gravar `FAILED` pelo
  agregado. O SQL (`postgres.wakePendingReferencesSQL`):

  ```sql
  UPDATE wager_transactions w
  SET next_attempt_at = GREATEST($3, w.created_at)
  FROM (SELECT id FROM wager_transactions
        WHERE provider_id = $1 AND reference_external_transaction_id = $2
          AND status = 'PENDING_REFERENCE' AND next_attempt_at > GREATEST($3, created_at)
        FOR NO KEY UPDATE SKIP LOCKED) due
  WHERE w.id = due.id AND w.status = 'PENDING_REFERENCE'
  ```

  - **Por que só estados terminais.** Qualquer terminal resolve a dependente: `PROCESSED`
    aplica ou rejeita pelas regras estáticas (`*_MISMATCH`, `REFERENCE_NOT_REVERSIBLE`,
    `ALREADY_REVERSED`…), e `REJECTED`/`FAILED` levam a `REFERENCE_NOT_PROCESSED` (ADR-009).
    Uma referência `PENDING`/`PENDING_REFERENCE` mantém a dependente aguardando; acordá-la só
    gastaria uma busca (`attempts`) à toa. Quando essa referência é resolvida pelo worker, o
    mesmo `persistWagerOutcome` acorda as dependentes dela (cadeia `ROLLBACK → WIN → BET`).
    Todos os tipos são considerados (um `LOSS` citado também resolve, com rejeição).
  - **Instante.** `t.UpdatedAt()` é o relógio da aplicação que gravou a transição, o mesmo
    que agenda `next_attempt_at` e que o `ClaimDue` compara. `GREATEST(now, created_at)`
    respeita o CHECK `next_attempt_at >= created_at` mesmo com relógios de réplicas
    desalinhados; `next_attempt_at > GREATEST(...)` faz o comando só **antecipar** (nunca
    adiar) e ser idempotente.
  - **O que muda na linha.** Só `next_attempt_at`. `attempts` (buscas, ADR-008), `updated_at`,
    `claimed_by`/`claimed_until` e `resume_failures` (ADR-031) ficam intactos: um item
    reivindicado continua do seu worker (fencing), um item retido pelo backoff de falhas
    continua retido (o `ClaimDue` exige o lease vencido) e o trigger de guarda aceita a
    mudança (coluna de agendamento; `attempts` não diminui). Nenhum evento é emitido
    (reagendamento não gera evento, ADR-012).
  - **Isolamento por provedor.** O filtro é `(provider_id, reference_external_transaction_id)`,
    o mesmo escopo de `loadReference`: a referência de uma operação é sempre do mesmo provedor.
  - **Locks e deadlock.** As dependentes podem ser de outra carteira (são rejeitadas com
    `REFERENCE_MISMATCH` ao resolver), então o comando trava linhas fora do lock da carteira
    em curso. Com `FOR NO KEY UPDATE SKIP LOCKED` ele **nunca espera**: não há aresta de
    espera nova, logo nenhum ciclo com o caminho quente (carteira → linha) nem com o worker
    (carteira → linha com fencing). `NO KEY` não conflita com o `KEY SHARE` da FK
    `reference_transaction_id` de quem insere uma reversão citando a linha. Uma linha pulada
    está travada por quem já a processa (worker no meio da retomada ou `ClaimDue` em curso).
    Na mesma carteira, a retomada segura a carteira antes da linha, então a referência só
    grava depois do commit dela e encontra a linha livre. Em carteira diferente, a dependente
    fica com o backoff que já tinha (latência, não correção). Alternativa descartada: `UPDATE`
    simples, que esperaria a linha travada pelo worker com a carteira do caminho quente travada.
  - **Índice.** `wager_transactions_reference_external_idx (provider_id,
    reference_external_transaction_id) WHERE reference_external_transaction_id IS NOT NULL`
    (`0002`, criado já com esse propósito) serve: a igualdade em
    `reference_external_transaction_id` implica o predicado parcial. Verificado por `EXPLAIN`
    no teste de integração. Não houve migration.
  - **Custo no caminho quente.** Um comando a mais por operação terminal, normalmente uma
    sonda de índice que não encontra linhas. Medição (1000 `BET`s sequenciais, PostgreSQL no
    Docker Desktop local, 2000 pendências de outras referências, duas rodadas): ~7,0–7,2 ms/op
    com o wake-up contra ~6,4–6,5 ms/op sem (~0,6 ms, essencialmente um round-trip).
    Alternativas descartadas: acordar só para tipos "referenciáveis" (`BET`/`WIN`/`REFUND`),
    porque um `LOSS` ou `ROLLBACK` citado também resolve a dependente (com rejeição) e a
    economia seria pequena; trigger no banco, que esconderia regra de domínio (quais estados
    resolvem) no schema; `pgx.Batch` com a outbox, que ganharia um round-trip ao custo de
    quebrar a porta `OutboxStore`.
  - **Atomicidade.** O wake-up está no mesmo commit da referência: rollback desfaz os dois.
    Um erro do wake-up faz a operação falhar e entrar no retry do `UnitOfWork` (transitório)
    como qualquer outro comando. No `failExhausted`, um erro não transitório do wake-up
    desvia para o caminho sem agregado, que não acorda.
- **Decisão: sem notificação in-process.** O worker continua por poll
  (`PENDING_WORKER_POLL_INTERVAL`, 1s). Um canal só acordaria o worker da mesma réplica; o
  wake-up por linha vale para todas. A latência de 1s foi considerada suficiente.
- **Limites.** Dependente travada por outra transação no instante do commit é pulada (acima).
  O `FAILED` pelo caminho sem agregado (`FailClaimedUnreadable`, linha que não reidrata) não
  acorda: não há agregado confiável para ler provedor e `externalId`; as dependentes veem o
  `FAILED` no próprio backoff.
- **Testes.**
  - Unitários (`internal/app/wake_pending_test.go`, UnitOfWork em memória que espelha o
    adapter: `woken` antecipa o vencimento no `ClaimDue`):
    `TestWakePending_ReferenceArrivalWakesSameProviderOnly` (resolução no mesmo instante, sem
    avançar o relógio; provedor alheio intacto; `PENDING_REFERENCE` não chama o wake-up) e
    `TestWakePending_PendingReferenceDoesNotWakeUntilResolved` (cadeia `ROLLBACK → WIN → BET`).
  - Adapter (`internal/adapters/postgres/wake_integration_test.go`):
    `..._WakePendingReferences` (só as futuras do provedor e da referência; já vencidas,
    outro provedor e outra referência intactas; `GREATEST` com `created_at` adiantado;
    `attempts`/`updated_at` intactos; idempotente; exige transação; parâmetros vazios) e
    `..._WakeUsesReferenceIndex` (`EXPLAIN` com o índice).
  - Integração com o worker rodando (`internal/adapters/pendingworker/wake_integration_test.go`,
    backoff de 1h): `TestIntegrationWorker_WakesPendingWhenReferenceArrives` (`ROLLBACK` vira
    `PROCESSED` e `REFUND` vira `REJECTED REFERENCE_NOT_PROCESSED` em menos de 1s após a `BET`
    `PROCESSED`/`REJECTED`, `attempts = 1`, eventos e reconciliação corretos; o `ROLLBACK` de
    outro provedor com o mesmo `externalId` de referência fica intacto),
    `TestIntegrationWake_ClaimedItemUndisturbed` (lease, `resume_failures`, `attempts` e
    `updated_at` preservados; outro worker não reivindica; o dono do lease resolve),
    `TestIntegrationWake_SkipsLockedRowWithoutBlocking` (linha travada por outra transação:
    a `BET` não espera e a linha mantém o agendamento) e
    `TestIntegrationWake_PendingReferenceDoesNotWake`. Sem o wake-up, os dois primeiros
    falham (verificado removendo a chamada).

## ADR-033 — `Money.UnmarshalJSON` estrito (T32)

- **Contexto.** O `UnmarshalJSON` de `Money` decodificava com `encoding/json` sobre uma struct:
  casava chaves sem diferenciar caixa (`{"AMOUNT":...}`), ficava com a última de chaves
  repetidas e o comentário dizia "o decoder é estrito" (a §18 documentava o oposto, lacuna
  L4). Nenhuma entrada externa usa o método (HTTP e SQS usam `strictjson.Money`, ADR-020/022),
  mas ele é o caminho de qualquer `json.Unmarshal` de DTO/evento com `Money` (testes,
  consumidores Go do contrato).
- **Decisão.** O método passou a ler o objeto token a token (`json.Decoder.Token`), com as
  mesmas regras de objeto do `strictjson`, implementadas no próprio domínio (o domínio não
  importa adapters):
  - um único objeto JSON, nada depois dele; `null`, lista, string ou número →
    `ErrMalformedMoneyJSON`;
  - só `amount` e `currency`, caixa exata, cada uma uma vez; desconhecida, outra caixa ou
    repetida → `ErrMalformedMoneyJSON`;
  - `amount`/`currency` obrigatórios e strings JSON (ausente, `null`, número, booleano,
    objeto, lista → `ErrInvalidAmount` / `ErrInvalidCurrency`); amount nunca é número JSON
    (nada passa por `float64`);
  - valores por `Parse`: mesma gramática, escala ≤ 2, overflow, moeda suportada em caixa alta.
    Whitespace JSON entre tokens e escapes `\uXXXX` nas strings são aceitos (é JSON válido;
    o valor decodificado é que passa pela gramática).
  - `ErrMalformedMoneyJSON` embrulha `ErrInvalidAmount`, então `ValidationFailureCode`
    continua classificando como `INVALID_AMOUNT` (como antes para campo desconhecido/null).
  - Em erro, o receptor não é alterado.
- **Negativos: aceitos.** `Money` serializa diferenças internas (`difference` da
  reconciliação, `ReconciliationResponse`) e `Marshal → Unmarshal` precisa fazer round-trip
  de qualquer `Money` válido, inclusive `MinInt64`/`MaxInt64` centavos (testado). A não
  negatividade das entradas financeiras externas é validada no ponto de entrada
  (`strictjson.Money` → `ParseNonNegative`), único caminho de HTTP e SQS.
- **Formato aceito.** O de `MarshalJSON` (`"25.00"`) e as formas equivalentes de `Parse`
  (`"25"`, `"25.5"`), as mesmas aceitas pelo `strictjson`; a saída é sempre canônica.
- **Alternativa descartada: extrair um pacote comum** que o `strictjson` reutilizasse. O
  `strictjson` precisa de caminho de campo (`FieldError`), sentinelas próprias
  (`ErrUnknownField`, `ErrDuplicateField`...) e serve objetos com chaves arbitrárias; o
  domínio só precisa de duas chaves fixas. A duplicação é de ~50 linhas, e a equivalência
  das regras de objeto é fixada por casos espelhados nas duas suítes
  (`TestJSONUnmarshalStrict`/`TestJSONUnmarshalEscapes` no domínio,
  `TestMoneyObjectRules` no `strictjson`): caixa (`AMOUNT`, `Amount`, `Currency`, também via
  escape), repetição (inclusive mesmo valor, `null` seguido de valor e via escape), extras,
  ausentes, tipos, escapes aceitos em chaves e valores e sobra após o objeto. Os sentinelas
  diferem (`ErrMalformedMoneyJSON` no domínio; `ErrUnknownField`/`ErrDuplicateField`/
  `ErrWrongType`/`ErrInvalidJSON` com caminho de campo no adapter), e o adapter rejeita
  negativos (`ParseNonNegative`), que o domínio aceita.
- **Testes** (`internal/domain/money_test.go`): `TestJSONUnmarshalStrict` (aceitos: negativo,
  limites de `int64`, formas equivalentes, whitespace; rejeitados: `AMOUNT`/`Amount`/
  `Currency`, duplicadas — inclusive mesmo valor e `null` seguido de valor —, campos extras,
  `{}`/amount/currency ausentes, `null`/string/número/lista, amount número/booleano/objeto/
  lista, currency `null`/booleano/minúscula/não suportada, espaços, `+`, `Infinity`, `-0.00`,
  overflow negativo, entrada vazia, truncada, vírgula final, sobra; cada erro é classificável
  e o receptor fica intacto), `TestJSONUnmarshalEscapes` (escapes unicode reais, montados
  em tempo de execução com a barra `0x5c` e com guarda que falha se o escape não estiver
  literal na entrada: chave `amount`/`currency` e valores de amount/currency escapados
  aceitos; `AMOUNT`/`Currency` via escape, `amount` repetida via escape, moeda minúscula e
  `+` via escape rejeitados), `TestJSONUnmarshalEmbeddedStrict` (`CURRENCY` dentro de um
  objeto maior), `TestJSONUnmarshal`/`TestJSONMarshal` (contrato e round-trip, já existentes)
  e `FuzzJSONRoundTrip` (todo valor aceito reserializa e volta igual; erro não altera o alvo);
  `internal/adapters/strictjson/strictjson_test.go`: `TestMoneyObjectRules` (os casos
  espelhados acima, com sentinela e caminho de campo do adapter).
