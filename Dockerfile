# syntax=docker/dockerfile:1
#
# Imagem da carteira (ADR-026): um único artefato com os dois binários,
# `server` (entrypoint) e `migrate` (job de migrations do Compose).
#
# GO_VERSION acompanha a diretiva `go` do go.mod (1.25.x). GOTOOLCHAIN=local
# impede o download silencioso de outro toolchain: se o go.mod subir de
# versão, o build falha até a imagem base ser atualizada aqui.
ARG GO_VERSION=1.25
ARG ALPINE_VERSION=3.22

# --- build -------------------------------------------------------------------
FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 \
    GOTOOLCHAIN=local \
    GOFLAGS=-trimpath

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY cmd ./cmd
COPY internal ./internal
COPY migrations ./migrations
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags="-s -w" -o /out/server ./cmd/server && \
    go build -ldflags="-s -w" -o /out/migrate ./cmd/migrate

# --- runtime -----------------------------------------------------------------
# Alpine (e não distroless) para ter o `wget` do busybox no healthcheck do
# Compose sem acrescentar um binário só para isso.
FROM alpine:${ALPINE_VERSION}
RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -H -u 10001 -s /sbin/nologin wallet
COPY --from=build /out/server /out/migrate /usr/local/bin/
USER 10001:10001
EXPOSE 8080
# SIGTERM inicia a parada graciosa (ADR-021/ADR-025); o orquestrador precisa
# esperar pelo menos o fx.StopTimeout (stop_grace_period no Compose).
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/server"]
