GO      ?= go
BIN_DIR ?= bin
PKGS    ?= ./...
GO_IMAGE ?= golang:1.25

.PHONY: all build run test test-race test-race-docker test-integration vet fmt fmt-check tidy check clean migrate-up migrate-down migrate-status

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
