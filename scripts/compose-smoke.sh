#!/usr/bin/env bash
# Verificação do ambiente do Docker Compose (ADR-026), com a stack já de pé
# (`make compose-up` ou `make compose-up-scaled`):
#
#   1. tokens client_credentials do host (localhost:8081) para provider-a,
#      provider-b e internal-service, com iss/aud/provider_id/papéis esperados;
#   2. API via balanceador (localhost:8080): 401 sem token, 201 abrindo uma
#      carteira com o serviço interno, 200 consultando-a, 403 para um
#      provedor na rota de carteira, 404 (autenticado) para um provedor
#      consultando transação inexistente;
#   3. filas no LocalStack: FIFO, RedrivePolicy (maxReceiveCount 5 → DLQ) e
#      ContentBasedDeduplication=false em wager-events.fifo.
#
# Requer bash, curl, docker. Segredos de client são os de DESENVOLVIMENTO do
# realm importado (deploy/keycloak/realm-junglegaming.json).
set -euo pipefail

KC="${KEYCLOAK_URL:-http://localhost:8081}"
REALM="${KEYCLOAK_REALM:-junglegaming}"
API="${API_URL:-http://localhost:8080}"
ISSUER="$KC/realms/$REALM"
COMPOSE="${COMPOSE:-docker compose}"

fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "ok   $*"; }

token() { # client secret
  local body
  body="$(curl -sS -f -X POST "$ISSUER/protocol/openid-connect/token" \
    -d grant_type=client_credentials -d "client_id=$1" -d "client_secret=$2")" ||
    fail "token client_credentials de $1"
  sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p' <<<"$body"
}

claims() { # jwt → payload JSON
  local p
  p="$(cut -d. -f2 <<<"$1" | tr '_-' '/+')"
  while [ $(( ${#p} % 4 )) -ne 0 ]; do p="$p="; done
  base64 -d <<<"$p" 2>/dev/null
}

expect_claim() { # payload fragmento descrição
  grep -q -- "$2" <<<"$1" || fail "$3: claim ausente ($2)"
}

status() { # método url token [corpo]
  local auth=()
  [ -n "$3" ] && auth=(-H "Authorization: Bearer $3")
  if [ $# -ge 4 ]; then
    curl -sS -o /tmp/smoke-body.$$ -w '%{http_code}' -X "$1" "${auth[@]}" \
      -H 'Content-Type: application/json' --data "$4" "$2"
  else
    curl -sS -o /tmp/smoke-body.$$ -w '%{http_code}' -X "$1" "${auth[@]}" "$2"
  fi
}
trap 'rm -f /tmp/smoke-body.$$' EXIT

uuid4() {
  local h
  h="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
  printf '%s-%s-4%s-8%s-%s\n' "${h:0:8}" "${h:8:4}" "${h:13:3}" "${h:17:3}" "${h:20:12}"
}

echo "== Keycloak ($ISSUER)"
TA="$(token provider-a "${PROVIDER_A_SECRET:-provider-a-dev-secret}")"
TB="$(token provider-b "${PROVIDER_B_SECRET:-provider-b-dev-secret}")"
TI="$(token internal-service "${INTERNAL_SERVICE_SECRET:-internal-service-dev-secret}")"
for spec in "provider-a:$TA:wager-provider" "provider-b:$TB:wager-provider" "internal-service:$TI:wallet-internal"; do
  IFS=: read -r cid tok role <<<"$spec"
  c="$(claims "$tok")"
  expect_claim "$c" "\"iss\":\"$ISSUER\"" "$cid"
  expect_claim "$c" '"wallet-api"' "$cid (aud)"
  expect_claim "$c" "\"$role\"" "$cid (papel)"
  case "$cid" in
    provider-*) expect_claim "$c" "\"provider_id\":\"$cid\"" "$cid" ;;
    *) grep -q '"provider_id"' <<<"$c" && fail "$cid não deveria ter provider_id" ;;
  esac
  ok "token client_credentials de $cid (iss, aud=wallet-api, papel $role)"
done

echo "== API ($API)"
[ "$(status GET "$API/health/ready" "")" = 200 ] || fail "/health/ready"
ok "/health/ready 200"
[ "$(status GET "$API/wallets/$(uuid4)" "")" = 401 ] || fail "sem token deveria ser 401"
ok "sem token → 401"
code="$(status POST "$API/wallets" "$TI" "{\"playerId\":\"$(uuid4)\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}")"
[ "$code" = 201 ] || fail "POST /wallets (internal-service) = $code: $(cat /tmp/smoke-body.$$)"
WALLET="$(sed -n 's/.*"walletId":"\([^"]*\)".*/\1/p' /tmp/smoke-body.$$)"
[ -n "$WALLET" ] || WALLET="$(sed -n 's/.*"id":"\([^"]*\)".*/\1/p' /tmp/smoke-body.$$)"
ok "POST /wallets com internal-service → 201 ($WALLET)"
code="$(status GET "$API/wallets/$WALLET" "$TI")"
[ "$code" = 200 ] || fail "GET /wallets/{id} (internal-service) = $code"
ok "GET /wallets/{id} com internal-service → 200"
code="$(status GET "$API/wallets/$WALLET" "$TA")"
[ "$code" = 403 ] || fail "GET /wallets/{id} (provider-a) = $code, esperado 403"
ok "GET /wallets/{id} com provider-a → 403"
code="$(status GET "$API/wagering/transactions/$(uuid4)" "$TA")"
[ "$code" = 404 ] || fail "GET /wagering/transactions/{id} (provider-a) = $code, esperado 404"
ok "GET /wagering/transactions/{inexistente} com provider-a → 404 (autenticado e autorizado)"

echo "== SQS (LocalStack)"
attr() { # fila atributo
  $COMPOSE exec -T localstack awslocal sqs get-queue-attributes \
    --queue-url "http://localhost:4566/000000000000/$1" --attribute-names "$2" \
    --query "Attributes.$2" --output text | tr -d '\r'
}
for q in wager-transactions.fifo wager-transactions-dlq.fifo wager-events.fifo; do
  [ "$(attr "$q" FifoQueue)" = "True" ] || [ "$(attr "$q" FifoQueue)" = "true" ] || fail "$q não é FIFO"
  ok "$q FifoQueue=true"
done
rp="$(attr wager-transactions.fifo RedrivePolicy)"
grep -q 'wager-transactions-dlq.fifo' <<<"$rp" || fail "RedrivePolicy sem a DLQ: $rp"
grep -Eq '"maxReceiveCount": ?"?5"?' <<<"$rp" || fail "maxReceiveCount != 5: $rp"
ok "wager-transactions.fifo RedrivePolicy=$rp"
cbd="$(attr wager-events.fifo ContentBasedDeduplication)"
[ "$cbd" = "False" ] || [ "$cbd" = "false" ] || fail "wager-events.fifo ContentBasedDeduplication=$cbd"
ok "wager-events.fifo ContentBasedDeduplication=false"

echo "SMOKE OK"
