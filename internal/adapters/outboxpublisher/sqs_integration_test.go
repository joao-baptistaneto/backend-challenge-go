//go:build integration

package outboxpublisher

// Publisher contra o SQS real (LocalStack) e PostgreSQL real. Pulado sem
// SQS_ENDPOINT/AWS_ENDPOINT_URL (ver main_integration_test.go).
//
// Cria uma fila FIFO exclusiva (como wager-events.fifo, sem deduplicação
// por conteúdo), publica dois eventos do mesmo agregado com um crash entre
// a publicação e a marcação do primeiro, e confere na fila: dois eventos,
// em ordem, com MessageGroupId = aggregateId, MessageDeduplicationId =
// eventId, corpo = snapshot da outbox e atributos de roteamento. A
// republicação do primeiro foi descartada pela deduplicação FIFO.

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/sqs"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func realSQSClient(t *testing.T) *sqs.Client {
	t.Helper()
	if sqsEndpoint == "" {
		t.Skip("SQS_ENDPOINT/AWS_ENDPOINT_URL ausente: LocalStack indisponível")
	}
	c, err := sqs.NewClient(t.Context(), config.AWSConfig{
		Region:          envOr("AWS_REGION", "us-east-1"),
		AccessKeyID:     envOr("AWS_ACCESS_KEY_ID", "test"),
		SecretAccessKey: config.Secret(envOr("AWS_SECRET_ACCESS_KEY", "test")),
		EndpointURL:     sqsEndpoint,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

func createEventsQueue(t *testing.T, c *sqs.Client) string {
	t.Helper()
	out, err := c.SQS().CreateQueue(t.Context(), &awssqs.CreateQueueInput{
		QueueName: aws.String("it-events-" + randomHex(t, 6) + ".fifo"),
		Attributes: map[string]string{
			"FifoQueue": "true", "ContentBasedDeduplication": "false", "VisibilityTimeout": "30",
		},
	})
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = c.SQS().DeleteQueue(ctx, &awssqs.DeleteQueueInput{QueueUrl: out.QueueUrl})
	})
	return aws.ToString(out.QueueUrl)
}

// receiveAll lê e apaga mensagens até ficar idle seconds sem receber nada.
func receiveAll(t *testing.T, c *sqs.Client, queueURL string, want int) []types.Message {
	t.Helper()
	var got []types.Message
	deadline := time.Now().Add(30 * time.Second)
	emptyAfterWant := 0
	for time.Now().Before(deadline) {
		out, err := c.SQS().ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{
			QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 1,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameMessageGroupId,
				types.MessageSystemAttributeNameMessageDeduplicationId,
			},
			MessageAttributeNames: []string{"All"},
		})
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		for _, m := range out.Messages {
			got = append(got, m)
			if _, err := c.SQS().DeleteMessage(t.Context(), &awssqs.DeleteMessageInput{
				QueueUrl: aws.String(queueURL), ReceiptHandle: m.ReceiptHandle,
			}); err != nil {
				t.Fatalf("delete: %v", err)
			}
		}
		if len(got) >= want && len(out.Messages) == 0 {
			emptyAfterWant++
			if emptyAfterWant >= 2 {
				break
			}
		}
	}
	return got
}

func TestIntegrationRealSQSPublishDedupAndRouting(t *testing.T) {
	client := realSQSClient(t)
	queueURL := createEventsQueue(t, client)
	dsn := migratedDSN(t)
	dbA, dbB := startDB(t, dsn), startDB(t, dsn)
	evs := walletEvents(t, 2)
	enqueue(t, dbA, evs...)

	// A publica o 1º evento no SQS real e morre antes de marcar.
	cfgA := integrationConfig("pub-a")
	cfgA.QueueURL = queueURL
	cfgA.Lease, cfgA.PublishTimeout = 1500*time.Millisecond, 300*time.Millisecond
	a, err := New(cfgA, deadStore{newStore(t, dbA, fastBackoff)}, client.SQS())
	if err != nil {
		t.Fatal(err)
	}
	if n, err := a.RunOnce(t.Context()); n != 1 || err != nil {
		t.Fatalf("A.RunOnce = %d, %v", n, err)
	}

	// B republica o 1º (mesmo eventId) depois do lease e publica o 2º.
	cfgB := integrationConfig("pub-b")
	cfgB.QueueURL = queueURL
	b, err := New(cfgB, newStore(t, dbB, fastBackoff), client.SQS())
	if err != nil {
		t.Fatal(err)
	}
	waitForLong(t, 20*time.Second, func() bool {
		if _, err := b.RunOnce(t.Context()); err != nil {
			t.Fatal(err)
		}
		return unpublished(t, dbB) == 0
	})

	msgs := receiveAll(t, client, queueURL, 2)
	if len(msgs) != 2 {
		t.Fatalf("mensagens na fila = %d, want 2 (a republicação deveria ser deduplicada)", len(msgs))
	}
	for i, m := range msgs {
		e := evs[i]
		attrs := m.Attributes
		if aws.ToString(m.Body) != string(e.Payload()) ||
			attrs[string(types.MessageSystemAttributeNameMessageGroupId)] != e.AggregateID().String() ||
			attrs[string(types.MessageSystemAttributeNameMessageDeduplicationId)] != e.ID().String() {
			t.Fatalf("mensagem %d = body %s attrs %v; want evento %s", i, aws.ToString(m.Body), attrs, e.ID())
		}
		if aws.ToString(m.MessageAttributes[AttrEventType].StringValue) != e.Type().String() ||
			aws.ToString(m.MessageAttributes[AttrEventID].StringValue) != e.ID().String() {
			t.Fatalf("atributos da mensagem %d = %v", i, m.MessageAttributes)
		}
	}
}
