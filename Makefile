GO      ?= go
BIN_DIR ?= bin
PKGS    ?= ./...
GO_IMAGE ?= golang:1.25

.PHONY: all build run test test-race test-race-docker test-integration vet fmt fmt-check tidy check clean migrate-up migrate-down migrate-status \
	compose-up compose-up-scaled compose-down compose-ps compose-logs compose-smoke test-integration-compose \
	test-multiinstance

all: check build

build:
	$(GO) build -o $(BIN_DIR)/server ./cmd/server

run:
	$(GO) run ./cmd/server

test:
	$(GO) test $(PKGS)

test-race:
	CGO_ENABLED=1 $(GO) test -race $(PKGS)

test-race-docker: ## -race exige cgo; usa container quando nao ha gcc local
	docker run --rm -v "$(CURDIR):/src" -w /src $(GO_IMAGE) go test -race $(PKGS)

test-integration:
	CGO_ENABLED=1 $(GO) test -race -tags=integration $(PKGS)

vet:
	$(GO) vet $(PKGS)

fmt:
	gofmt -w .

fmt-check:
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

tidy:
	$(GO) mod tidy

check: fmt-check vet test-race

clean:
	rm -rf $(BIN_DIR) coverage.out

# Migrations (ADR-011). Exigem DATABASE_URL no ambiente; seguras com várias
# execuções simultâneas (advisory lock). migrate-down reverte só a última.
migrate-up:
	$(GO) run ./cmd/migrate up

migrate-down:
	$(GO) run ./cmd/migrate down 1

migrate-status:
	$(GO) run ./cmd/migrate status

# Docker Compose (ADR-026). --wait espera todos os healthchecks (o job de
# migrations precisa terminar com 0). Portas no host: API 8080 (nginx),
# Keycloak 8081, LocalStack 4566, PostgreSQL 5433.
COMPOSE  ?= docker compose
REPLICAS ?= 3

compose-up:
	$(COMPOSE) up --build -d --wait

compose-up-scaled:
	$(COMPOSE) up --build -d --wait --scale app=$(REPLICAS)

compose-down:
	$(COMPOSE) down -v --remove-orphans

compose-ps:
	$(COMPOSE) ps -a

compose-logs:
	$(COMPOSE) logs -f --tail=100

# Token client_credentials (provider-a, provider-b, internal-service),
# chamadas autenticadas à API e atributos das filas (FIFO, redrive, dedup).
compose-smoke:
	COMPOSE="$(COMPOSE)" bash scripts/compose-smoke.sh

# Integração contra a stack do Compose (valores de DESENVOLVIMENTO). -p 1:
# os pacotes compartilham o mesmo LocalStack/PostgreSQL e rodam em série
# para não disputar o mesmo LocalStack (ADR-026).
COMPOSE_TEST_ENV = \
	TEST_DATABASE_URL='postgres://wallet:wallet-dev-pw@localhost:5433/wallet?sslmode=disable' \
	SQS_ENDPOINT=http://localhost:4566 \
	KEYCLOAK_URL=http://localhost:8081 \
	KEYCLOAK_CLIENT_ID=internal-service \
	KEYCLOAK_CLIENT_SECRET=internal-service-dev-secret

test-integration-compose:
	$(COMPOSE_TEST_ENV) CGO_ENABLED=1 $(GO) test -race -tags=integration -p 1 -count=1 $(PKGS)

# Multi-instância e recuperação de falhas (T26, ADR-028): o teste compila o
# cmd/server com a tag failpoint (e -race) e sobe processos reais contra o
# PostgreSQL e o LocalStack do Compose (docker compose up -d --wait postgres
# localstack). Build tag própria (multiinstance) para não rodar junto com
# test-integration; nenhum container é pausado (as quedas são simuladas por
# um proxy TCP do próprio teste).
test-multiinstance:
	$(COMPOSE_TEST_ENV) CGO_ENABLED=1 $(GO) test -race -tags=integration,multiinstance -p 1 -count=1 -timeout 30m \
		-run 'TestMultiInstance|TestFailure' ./cmd/server
