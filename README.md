# Carteira de apostas distribuída (Go + Uber Fx)

Serviço de carteira que processa operações financeiras de provedores de jogos (`BET`, `WIN`,
`LOSS`, `REFUND`, `ROLLBACK`) por **HTTP** e por **SQS FIFO**, com as mesmas garantias nas duas
entradas: dinheiro em inteiros (centavos), ledger imutável, idempotência persistente no
PostgreSQL, lock por carteira, inbox/outbox transacional e autenticação OIDC (Keycloak,
`client_credentials`). Roda em N réplicas idênticas, sem estado em memória que afete o
resultado financeiro.

- Decisões de arquitetura, contratos e limitações: [ARCHITECTURE.md](ARCHITECTURE.md).
- Enunciado original do desafio: [docs/DESAFIO.md](docs/DESAFIO.md). Nos comentários do código
  e no ARCHITECTURE.md, "README §N" é uma seção desse enunciado.

## Sumário

1. [Pré-requisitos](#1-pré-requisitos)
2. [Variáveis de ambiente](#2-variáveis-de-ambiente)
3. [Filas SQS e contratos](#3-filas-sqs-e-contratos)
4. [Migrations](#4-migrations)
5. [Execução com Docker Compose](#5-execução-com-docker-compose)
6. [Exemplos de chamadas (com token)](#6-exemplos-de-chamadas-com-token)
7. [Testes](#7-testes)
8. [Limitações](#8-limitações)

## 1. Pré-requisitos

| Ferramenta | Versão | Para quê |
| --- | --- | --- |
| Go | 1.25+ (`go 1.25.0` no `go.mod`) | build, testes, `cmd/migrate` |
| Docker + Docker Compose v2 | Compose com `--wait` | stack local e testes de integração (testcontainers) |
| gcc (cgo) | qualquer | só para `go test -race` (o detector de corrida exige `CGO_ENABLED=1`). No Windows, MinGW-w64/WinLibs no `PATH`; sem gcc, use `make test-race-docker` |
| bash, curl | — | `scripts/compose-smoke.sh` e exemplos abaixo (no Windows, Git Bash) |
| make | opcional | atalhos do `Makefile`; todos os alvos têm o comando equivalente neste README |
| AWS CLI | opcional | enviar mensagens do host; sem ela, use `awslocal` dentro do container do LocalStack |

Portas usadas no host: **8080** (API via nginx), **8081** (Keycloak), **4566** (LocalStack),
**5433** (PostgreSQL; 5432 fica livre para um PostgreSQL local).

## 2. Variáveis de ambiente

A aplicação lê a configuração só de variáveis de ambiente (`internal/platform/config`), valida
tudo no start e falha com a lista de erros. O arquivo [`.env.example`](.env.example) tem **todas**
as variáveis lidas, com valores locais de exemplo e a explicação de cada uma (sem segredos reais).
No Docker Compose os valores ficam em `docker-compose.yml` (`x-app-env`). Principais grupos:

| Grupo | Variáveis |
| --- | --- |
| Aplicação/HTTP | `APP_ENV`, `HTTP_ADDR`, `LOG_LEVEL`, `SHUTDOWN_TIMEOUT`, `METRICS_ACCESS`, `HTTP_*_TIMEOUT`, `HTTP_DRAIN_DELAY` |
| PostgreSQL | `DATABASE_URL`, `DATABASE_MAX_CONNS`, `DATABASE_CONNECT_TIMEOUT` |
| SQS | `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`, `SQS_INBOUND_QUEUE_URL`, `SQS_INBOUND_DLQ_URL`, `SQS_EVENTS_QUEUE_URL` |
| Consumidor SQS | `SQS_CONSUMER_NAME`, `SQS_CONSUMER_CONCURRENCY`, `SQS_WAIT_TIME`, `SQS_VISIBILITY_TIMEOUT`, `SQS_PROCESS_TIMEOUT`, `SQS_RETRY_BASE_DELAY`, `SQS_RETRY_MAX_DELAY`, `SQS_DRAIN_TIMEOUT`, `SQS_MAX_ATTEMPTS` |
| OIDC | `OIDC_ISSUER_URL`, `OIDC_AUDIENCE`, `OIDC_JWKS_URL`, `OIDC_PROVIDER_CLAIM`, `OIDC_PROVIDER_ROLE`, `OIDC_INTERNAL_ROLE`, `OIDC_ROLES_CLIENT`, `OIDC_CLOCK_SKEW` |
| Outbox | `OUTBOX_POLL_INTERVAL`, `OUTBOX_BATCH_SIZE`, `OUTBOX_LEASE`, `OUTBOX_CONCURRENCY`, `OUTBOX_PUBLISH_TIMEOUT`, `OUTBOX_DRAIN_TIMEOUT`, `OUTBOX_RETRY_BASE_DELAY`, `OUTBOX_RETRY_MAX_DELAY` |
| Pendências | `PENDING_REFERENCE_MAX_ATTEMPTS`, `PENDING_REFERENCE_TTL`, `PENDING_REFERENCE_RETRY_BASE_DELAY`, `PENDING_REFERENCE_RETRY_MAX_DELAY`, `PENDING_WORKER_*`, `PENDING_ORPHAN_GRACE` |
| Migrations (`cmd/migrate`) | `DATABASE_URL`, `MIGRATE_TIMEOUT` (padrão 2m) |
| Testes (não lidas pela aplicação) | `TEST_DATABASE_URL`, `SQS_ENDPOINT`, `KEYCLOAK_URL`, `KEYCLOAK_CLIENT_ID`, `KEYCLOAK_CLIENT_SECRET` (ver [§7](#7-testes)) |

Para rodar o binário fora do Compose (com a stack de dependências de pé), carregue o exemplo e
execute o servidor:

```sh
docker compose up -d --wait postgres localstack keycloak migrate
set -a; . ./.env.example; set +a
HTTP_ADDR=:8090 go run ./cmd/server     # 8080 já é do nginx do Compose
```

## 3. Filas SQS e contratos

As filas são criadas automaticamente pelo init hook do LocalStack
([`deploy/localstack/init-sqs.sh`](deploy/localstack/init-sqs.sh), idempotente); o container só
fica saudável depois disso. Detalhes: ARCHITECTURE §12 e §13.

| Fila | Configuração | Uso |
| --- | --- | --- |
| `wager-transactions.fifo` | FIFO, `ContentBasedDeduplication=false`, visibilidade 30s, `RedrivePolicy` **`maxReceiveCount=5`** → DLQ | entrada (`WagerTransactionRequested`) |
| `wager-transactions-dlq.fifo` | FIFO, retenção 14 dias | inválidas, permanentes e transitórias esgotadas |
| `wager-events.fifo` | FIFO, `ContentBasedDeduplication=false` | eventos de integração publicados pela outbox |

O consumidor envia à DLQ **explicitamente** mensagens inválidas/permanentes (1ª entrega) e
transitórias depois de `SQS_MAX_ATTEMPTS` (3) tentativas com backoff; o redrive nativo (5) é rede
de segurança. A mensagem só é apagada depois do commit.

**Contrato para produtores (`wager-transactions.fifo`):**

- `MessageGroupId = walletId` (ordem por carteira, paralelismo entre carteiras);
- `MessageDeduplicationId = messageId` do envelope;
- corpo (JSON estrito: valores monetários como string, sem campos desconhecidos):

```json
{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-09-08T12:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}
```

`data.referenceExternalTransactionId` é opcional (obrigatório para `REFUND`/`ROLLBACK`). O
`messageId` do envelope é a identidade durável (inbox, com verificação de hash em reentregas); a
chave de idempotência financeira é `data.idempotencyKey`, a mesma do header `Idempotency-Key` no
HTTP. Grupo e dedup id ajudam, mas a correção não depende deles.

**Contrato para consumidores de eventos (`wager-events.fifo`):**

- `MessageGroupId = aggregateId` (id da transação nos eventos `WagerTransactionProcessed`,
  `WagerTransactionRejected` e `WagerTransactionPendingReference`; id da carteira em
  `WalletBalanceChanged`);
- `MessageDeduplicationId = eventId` (UUIDv7; republicações preservam o `eventId`);
- atributos de mensagem: `eventType` (String), `eventVersion` (Number), `eventId`, `correlationId`;
- corpo: envelope `{eventId, eventType, aggregateId, correlationId, causationId?, occurredAt,
  version, data}`, dinheiro como string decimal, datas RFC 3339 UTC;
- entrega **at-least-once**: o FIFO deduplica só numa janela de 5 min; fora dela o consumidor deve
  deduplicar por `eventId`;
- ordem garantida **só dentro do grupo**, nunca entre grupos. Para ordenar saldo de uma carteira
  use `data.walletVersion` de `WalletBalanceChanged` (monotônica por carteira) e relacione com a
  transação por `data.transactionId`.

## 4. Migrations

SQL versionado em [`migrations/`](migrations) (`NNNN_nome.up.sql` / `.down.sql`), embutido no
binário `cmd/migrate`. Execuções simultâneas são seguras (advisory lock) e o checksum de cada
migration aplicada é verificado.

```sh
export DATABASE_URL='postgres://wallet:wallet-dev-pw@localhost:5433/wallet?sslmode=disable'
go run ./cmd/migrate up          # aplica as pendentes          (make migrate-up)
go run ./cmd/migrate status      # lista aplicadas/pendentes    (make migrate-status)
go run ./cmd/migrate down 1      # reverte a última             (make migrate-down)
go run ./cmd/migrate down -all   # reverte todas (destrói dados)
```

No Docker Compose, o serviço `migrate` roda `migrate up` antes das réplicas da aplicação (elas só
sobem depois que ele termina com sucesso). Pelo container:
`docker compose run --rm migrate status` (ou `down 1`, `up`).

## 5. Execução com Docker Compose

```sh
docker compose up --build                  # primeiro plano, 1 réplica
docker compose up --build -d --wait        # segundo plano; espera todos os healthchecks (make compose-up)
docker compose up -d --wait --scale app=3  # 3 réplicas atrás do nginx           (make compose-up-scaled)
bash scripts/compose-smoke.sh              # tokens, API e atributos das filas   (make compose-smoke)
docker compose down -v --remove-orphans    # derruba e apaga os volumes          (make compose-down)
```

Serviços: `postgres` (17), `keycloak` (26, realm `junglegaming` importado de
[`deploy/keycloak/realm-junglegaming.json`](deploy/keycloak/realm-junglegaming.json)),
`localstack` (SQS), `migrate` (job), `app` (N réplicas, healthcheck em `/health/ready`) e `lb`
(nginx, porta 8080, balanceia entre as réplicas). O smoke termina com `SMOKE OK`.

**Identidades de teste** (provisionadas no realm; segredos de DESENVOLVIMENTO, públicos):

| Client (`client_credentials`) | Segredo | Papel | Acesso |
| --- | --- | --- | --- |
| `provider-a` | `provider-a-dev-secret` | `wager-provider`, `provider_id=provider-a` | rotas de transação, só as próprias |
| `provider-b` | `provider-b-dev-secret` | `wager-provider`, `provider_id=provider-b` | idem, isolado de `provider-a` |
| `internal-service` | `internal-service-dev-secret` | `wallet-internal` | carteiras, ledger, reconciliação, `/metrics` |
| `e2e-short-lived`, `e2e-other-audience` | `<nome>-dev-secret` | — | só para os testes E2E de auth |

Admin do Keycloak: http://localhost:8081 (`admin`/`admin`).

## 6. Exemplos de chamadas (com token)

Em bash (Git Bash no Windows), com a stack de pé. Execute os blocos em sequência, na mesma
sessão. Há uma carteira por jogador e moeda, então cada execução gera um `PLAYER_ID` novo; o
sufixo `RUN` evita colisão de chaves de idempotência entre execuções.

```sh
API=http://localhost:8080
TOKEN_URL=http://localhost:8081/realms/junglegaming/protocol/openid-connect/token
RUN=$(date +%s)
PLAYER_ID=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n' |
  sed -E 's/^(.{8})(.{4}).(.{3}).(.{3})(.{12})$/\1-\2-4\3-8\4-\5/')   # UUIDv4 aleatório

# Tokens client_credentials
INTERNAL_TOKEN=$(curl -s -X POST "$TOKEN_URL" -d grant_type=client_credentials \
  -d client_id=internal-service -d client_secret=internal-service-dev-secret \
  | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
PROVIDER_TOKEN=$(curl -s -X POST "$TOKEN_URL" -d grant_type=client_credentials \
  -d client_id=provider-a -d client_secret=provider-a-dev-secret \
  | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')

# Abrir carteira (serviço interno) → 201
WALLET_ID=$(curl -s -X POST "$API/wallets" \
  -H "Authorization: Bearer $INTERNAL_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$PLAYER_ID\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}" \
  | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
curl -s "$API/wallets/$WALLET_ID" -H "Authorization: Bearer $INTERNAL_TOKEN"
```

```sh
# Aposta (provedor; Idempotency-Key obrigatório) → 200 PROCESSED, saldo 75.00.
# Repetir o mesmo comando devolve o mesmo resultado com "idempotentReplay":true.
curl -s -X POST "$API/wagering/transactions" \
  -H "Authorization: Bearer $PROVIDER_TOKEN" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: provider-a:bet-$RUN" \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"bet-$RUN\",\"playerId\":\"$PLAYER_ID\",
       \"walletId\":\"$WALLET_ID\",\"roundId\":\"round-$RUN\",\"gameId\":\"fortune-chimp\",
       \"kind\":\"BET\",\"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"

# Ganho referenciando a aposta → 200 PROCESSED, saldo 115.00
curl -s -X POST "$API/wagering/transactions" \
  -H "Authorization: Bearer $PROVIDER_TOKEN" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: provider-a:win-$RUN" \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"win-$RUN\",\"playerId\":\"$PLAYER_ID\",
       \"walletId\":\"$WALLET_ID\",\"roundId\":\"round-$RUN\",\"gameId\":\"fortune-chimp\",
       \"kind\":\"WIN\",\"money\":{\"amount\":\"40.00\",\"currency\":\"BRL\"},
       \"referenceExternalTransactionId\":\"bet-$RUN\"}"

# Consultar transação: por externalTransactionId ou por transactionId (só o próprio provedor)
curl -s "$API/providers/provider-a/wagering/transactions/bet-$RUN" -H "Authorization: Bearer $PROVIDER_TOKEN"
TX_ID=$(curl -s "$API/providers/provider-a/wagering/transactions/bet-$RUN" \
  -H "Authorization: Bearer $PROVIDER_TOKEN" | sed -n 's/.*"transactionId":"\([^"]*\)".*/\1/p')
curl -s "$API/wagering/transactions/$TX_ID" -H "Authorization: Bearer $PROVIDER_TOKEN"

# Ledger (paginação keyset: ?limit=&cursor=) e reconciliação saldo × ledger (serviço interno)
curl -s "$API/wallets/$WALLET_ID/ledger?limit=10" -H "Authorization: Bearer $INTERNAL_TOKEN"
curl -s -X POST "$API/wallets/$WALLET_ID/reconciliation" -H "Authorization: Bearer $INTERNAL_TOKEN"
# → {"storedBalance":...,"calculatedBalance":...,"difference":{"amount":"0.00",...},"consistent":true,...}

# Autorização: provedor em rota de carteira → 403; sem token → 401
curl -s -o /dev/null -w '%{http_code}\n' "$API/wallets/$WALLET_ID" -H "Authorization: Bearer $PROVIDER_TOKEN"
curl -s -o /dev/null -w '%{http_code}\n' "$API/wallets/$WALLET_ID"
```

```sh
# Mesma operação pela fila (awslocal dentro do container do LocalStack)
docker compose exec -T localstack awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id "$WALLET_ID" --message-deduplication-id "msg-$RUN" \
  --message-body "{\"messageId\":\"msg-$RUN\",\"type\":\"WagerTransactionRequested\",
    \"occurredAt\":\"2026-09-08T12:00:00.000Z\",\"data\":{\"providerId\":\"provider-a\",
    \"externalTransactionId\":\"sqs-bet-$RUN\",\"idempotencyKey\":\"provider-a:sqs-bet-$RUN\",
    \"playerId\":\"$PLAYER_ID\",\"walletId\":\"$WALLET_ID\",\"roundId\":\"round-$RUN\",
    \"gameId\":\"fortune-chimp\",\"kind\":\"BET\",\"money\":{\"amount\":\"10.00\",\"currency\":\"BRL\"}}}"
sleep 2
curl -s "$API/providers/provider-a/wagering/transactions/sqs-bet-$RUN" -H "Authorization: Bearer $PROVIDER_TOKEN"

# Eventos publicados pela outbox (inspeção; visibilidade 0 para não "consumir")
docker compose exec -T localstack awslocal sqs receive-message \
  --queue-url http://localhost:4566/000000000000/wager-events.fifo \
  --max-number-of-messages 10 --message-attribute-names All --visibility-timeout 0
```

Com a AWS CLI no host, o equivalente é `aws --endpoint-url http://localhost:4566 sqs send-message
...` com `AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1`.

Outros endpoints: `GET /health/live`, `GET /health/ready` (públicos) e `GET /metrics`
(Prometheus; token do serviço interno, salvo `METRICS_ACCESS=public`). Códigos HTTP e corpos de
erro: ARCHITECTURE §16 (400 entrada inválida, 401/403, 404, 409 conflito de idempotência, 422
rejeição com `failureCode`, 202 `PENDING_REFERENCE`, 503 + `Retry-After`).

## 7. Testes

| O quê | Comando | Make |
| --- | --- | --- |
| Formatação / análise | `gofmt -l .` e `go vet ./...` | `make fmt-check vet` |
| Unitários (sem Docker) | `go test ./...` | `make test` |
| Unitários com detector de corrida | `CGO_ENABLED=1 go test -race ./...` | `make test-race` (sem gcc: `make test-race-docker`) |
| Integração (PostgreSQL via testcontainers; SQS/Keycloak pulados sem as variáveis) | `CGO_ENABLED=1 go test -race -tags=integration ./...` | `make test-integration` |
| Integração completa contra o Compose (PostgreSQL, LocalStack e Keycloak reais, incluindo concorrência HTTP × SQS e E2E de autenticação/autorização) | ver abaixo | `make test-integration-compose` |
| Multi-instância e falhas (≥3 processos reais, crashes por failpoint, quedas de PostgreSQL/SQS) | ver abaixo | `make test-multiinstance` |

**Build tags:**

- `integration`: testes com dependências reais (PostgreSQL, LocalStack, Keycloak);
- `integration,multiinstance`: cenários multi-processo e de recuperação de falhas
  (`cmd/server/multiinstance_test.go`);
- `failpoint`: ativa os pontos de crash de `internal/platform/failpoint` no binário (sem a tag
  são no-op). O teste multi-instância compila o `cmd/server` com ela sozinho; não é preciso
  passá-la no `go test`.

**Preparar as dependências** (a stack do Compose serve para tudo; os valores são os de
desenvolvimento):

```sh
docker compose up --build -d --wait
export TEST_DATABASE_URL='postgres://wallet:wallet-dev-pw@localhost:5433/wallet?sslmode=disable'
export SQS_ENDPOINT=http://localhost:4566
export KEYCLOAK_URL=http://localhost:8081
export KEYCLOAK_CLIENT_ID=internal-service
export KEYCLOAK_CLIENT_SECRET=internal-service-dev-secret
```

`TEST_DATABASE_URL` precisa de um usuário com `CREATE/DROP DATABASE` (cada teste cria o próprio
banco); sem ela, os pacotes sobem um PostgreSQL via testcontainers. Sem `SQS_ENDPOINT` ou
`KEYCLOAK_URL`, os testes que dependem deles são pulados (`t.Skip`), exceto os multi-instância,
que exigem LocalStack.

```sh
# Integração completa (-p 1: os pacotes compartilham o mesmo LocalStack)
CGO_ENABLED=1 go test -race -tags=integration -p 1 -count=1 ./...

# Concorrência obrigatória e E2E de auth, isoladamente
CGO_ENABLED=1 go test -race -tags=integration -count=1 -run 'TestIntegrationConcurrency' ./cmd/server
CGO_ENABLED=1 go test -race -tags=integration -count=1 -run 'TestIntegrationAuthE2E' ./cmd/server

# Multi-instância e simulações de falha (demora alguns minutos)
CGO_ENABLED=1 go test -race -tags=integration,multiinstance -p 1 -count=1 -timeout 30m \
  -run 'TestMultiInstance|TestFailure' ./cmd/server

# vet com as tags
go vet -tags=integration ./... && go vet -tags=integration,multiinstance ./... && go vet -tags=failpoint ./...
```

## 8. Limitações

Interpretações adotadas, trade-offs, limitações conhecidas e trabalho não concluído estão em
[ARCHITECTURE §18](ARCHITECTURE.md#18-interpretações-adotadas-limitações-trade-offs-e-trabalho-não-concluído).
Testes de carga (diferencial opcional) não foram feitos.
