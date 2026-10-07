#!/bin/bash
# Provisionamento do SQS no LocalStack (ADR-026). Roda como init hook
# "ready.d" (depois que o LocalStack aceita requisições) e é idempotente:
# create-queue só com o atributo imutável FifoQueue e, em seguida,
# set-queue-attributes com o resto (reexecutar não falha com
# QueueAlreadyExists por atributos diferentes).
#
# Filas:
#   wager-transactions-dlq.fifo  DLQ da entrada (retenção de 14 dias)
#   wager-transactions.fifo      entrada; RedrivePolicy maxReceiveCount=5 → DLQ
#                                (o consumidor recusa o start sem ela:
#                                ErrNoRedrivePolicy; SQS_MAX_ATTEMPTS=3 < 5)
#   wager-events.fifo            eventos da outbox; dedup explícita por eventId
#                                (ContentBasedDeduplication=false, ADR-024)
#
# Credenciais/políticas: a aplicação usa test/test (conta 000000000000). As
# filas recebem uma Policy de recurso restrita à conta e há um usuário IAM
# "wallet-app" com a política mínima. O LocalStack Community NÃO aplica IAM;
# ficam como documentação executável do que a AWS real exigiria.
set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
ACCOUNT="000000000000"
MARKER="/tmp/sqs-provisioned"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

rm -f "$MARKER"

arn() { echo "arn:aws:sqs:${REGION}:${ACCOUNT}:$1"; }

# Policy de recurso (string JSON dentro do atributo Policy).
queue_policy() {
  local name="$1"
  printf '{\\"Version\\":\\"2012-10-17\\",\\"Statement\\":[{\\"Sid\\":\\"OwnerAccountOnly\\",\\"Effect\\":\\"Allow\\",\\"Principal\\":{\\"AWS\\":\\"arn:aws:iam::%s:root\\"},\\"Action\\":\\"sqs:*\\",\\"Resource\\":\\"%s\\"}]}' \
    "$ACCOUNT" "$(arn "$name")"
}

create_fifo() {
  local name="$1"
  awslocal sqs create-queue --region "$REGION" --queue-name "$name" \
    --attributes FifoQueue=true >/dev/null
  awslocal sqs get-queue-url --region "$REGION" --queue-name "$name" --query QueueUrl --output text
}

# --- DLQ da entrada -----------------------------------------------------------
DLQ="wager-transactions-dlq.fifo"
DLQ_URL="$(create_fifo "$DLQ")"
cat >"$WORK/dlq.json" <<EOF
{
  "ContentBasedDeduplication": "false",
  "MessageRetentionPeriod": "1209600",
  "VisibilityTimeout": "30",
  "RedriveAllowPolicy": "{\"redrivePermission\":\"byQueue\",\"sourceQueueArns\":[\"$(arn wager-transactions.fifo)\"]}",
  "Policy": "$(queue_policy "$DLQ")"
}
EOF
awslocal sqs set-queue-attributes --region "$REGION" --queue-url "$DLQ_URL" --attributes "file://$WORK/dlq.json"

# --- Entrada ------------------------------------------------------------------
IN="wager-transactions.fifo"
IN_URL="$(create_fifo "$IN")"
cat >"$WORK/in.json" <<EOF
{
  "ContentBasedDeduplication": "false",
  "MessageRetentionPeriod": "345600",
  "VisibilityTimeout": "30",
  "ReceiveMessageWaitTimeSeconds": "20",
  "RedrivePolicy": "{\"deadLetterTargetArn\":\"$(arn "$DLQ")\",\"maxReceiveCount\":\"5\"}",
  "Policy": "$(queue_policy "$IN")"
}
EOF
awslocal sqs set-queue-attributes --region "$REGION" --queue-url "$IN_URL" --attributes "file://$WORK/in.json"

# --- Eventos (outbox) ---------------------------------------------------------
EV="wager-events.fifo"
EV_URL="$(create_fifo "$EV")"
cat >"$WORK/ev.json" <<EOF
{
  "ContentBasedDeduplication": "false",
  "MessageRetentionPeriod": "345600",
  "Policy": "$(queue_policy "$EV")"
}
EOF
awslocal sqs set-queue-attributes --region "$REGION" --queue-url "$EV_URL" --attributes "file://$WORK/ev.json"

# --- IAM (não aplicado pelo LocalStack Community) ------------------------------
cat >"$WORK/app-policy.json" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ConsumeInbound",
      "Effect": "Allow",
      "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"],
      "Resource": "$(arn "$IN")"
    },
    {
      "Sid": "DeadLetter",
      "Effect": "Allow",
      "Action": ["sqs:SendMessage", "sqs:GetQueueUrl", "sqs:GetQueueAttributes"],
      "Resource": "$(arn "$DLQ")"
    },
    {
      "Sid": "PublishEvents",
      "Effect": "Allow",
      "Action": ["sqs:SendMessage", "sqs:GetQueueUrl", "sqs:GetQueueAttributes"],
      "Resource": "$(arn "$EV")"
    }
  ]
}
EOF
awslocal iam create-user --user-name wallet-app >/dev/null 2>&1 || true
awslocal iam put-user-policy --user-name wallet-app --policy-name wallet-sqs \
  --policy-document "file://$WORK/app-policy.json"

echo "SQS provisionado:"
awslocal sqs list-queues --region "$REGION" --query QueueUrls --output text | tr '\t' '\n'
touch "$MARKER"
