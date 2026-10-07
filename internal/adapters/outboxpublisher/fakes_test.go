package outboxpublisher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
)

// fakeQueue é uma fila SQS FIFO em memória com a semântica que o publisher
// usa: exige MessageGroupId e MessageDeduplicationId e descarta um envio
// cuja deduplicação já foi aceita (a janela de 5 minutos do SQS cobre toda a
// duração de um teste). Segura para uso concorrente.
type fakeQueue struct {
	mu       sync.Mutex
	seq      int
	calls    []sentMessage // todo SendMessage aceito, inclusive duplicatas
	accepted []sentMessage // conteúdo da fila: um por MessageDeduplicationId
	byDedup  map[string]string
	// fail, se não nil, é consultado antes de aceitar cada envio.
	fail func(in *awssqs.SendMessageInput) error
}

type sentMessage struct {
	messageID string
	group     string
	dedup     string
	body      string
	attrs     map[string]string
	duplicate bool
}

func newFakeQueue() *fakeQueue { return &fakeQueue{byDedup: map[string]string{}} }

var errFakeSend = errors.New("fake sqs: falha injetada")

func (f *fakeQueue) SendMessage(ctx context.Context, in *awssqs.SendMessageInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if aws.ToString(in.QueueUrl) == "" || aws.ToString(in.MessageGroupId) == "" ||
		aws.ToString(in.MessageDeduplicationId) == "" || aws.ToString(in.MessageBody) == "" {
		return nil, fmt.Errorf("fake sqs: entrada FIFO incompleta: %+v", in)
	}
	f.mu.Lock()
	fail := f.fail
	f.mu.Unlock()
	if fail != nil {
		if err := fail(in); err != nil {
			return nil, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	attrs := map[string]string{}
	for k, v := range in.MessageAttributes {
		attrs[k] = aws.ToString(v.DataType) + ":" + aws.ToString(v.StringValue)
	}
	m := sentMessage{
		group: aws.ToString(in.MessageGroupId), dedup: aws.ToString(in.MessageDeduplicationId),
		body: aws.ToString(in.MessageBody), attrs: attrs,
	}
	if id, ok := f.byDedup[m.dedup]; ok {
		m.messageID, m.duplicate = id, true
		f.calls = append(f.calls, m)
		return &awssqs.SendMessageOutput{MessageId: aws.String(id)}, nil
	}
	f.seq++
	m.messageID = fmt.Sprintf("msg-%d", f.seq)
	f.byDedup[m.dedup] = m.messageID
	f.calls = append(f.calls, m)
	f.accepted = append(f.accepted, m)
	return &awssqs.SendMessageOutput{MessageId: aws.String(m.messageID)}, nil
}

func (f *fakeQueue) setFail(fn func(in *awssqs.SendMessageInput) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = fn
}

func (f *fakeQueue) snapshot() (calls, accepted []sentMessage) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]sentMessage(nil), f.calls...), append([]sentMessage(nil), f.accepted...)
}

// groupOrder devolve, por MessageGroupId, a ordem dos MessageDeduplicationId
// aceitos na fila.
func (f *fakeQueue) groupOrder() map[string][]string {
	_, accepted := f.snapshot()
	out := map[string][]string{}
	for _, m := range accepted {
		out[m.group] = append(out[m.group], m.dedup)
	}
	return out
}

// memStore é um Store roteirizado para os testes unitários: cada Claim
// devolve o próximo lote de batches; as marcações são registradas.
type memStore struct {
	mu        sync.Mutex
	batches   [][]postgres.OutboxMessage
	claimErr  error
	markErr   error
	claims    int
	published []postgres.OutboxLease
	failed    []failedMark
	released  []postgres.OutboxLease
	backlog   postgres.OutboxBacklog
}

type failedMark struct {
	lease postgres.OutboxLease
	cause string
}

func (s *memStore) push(batch ...postgres.OutboxMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = append(s.batches, batch)
}

func (s *memStore) Claim(ctx context.Context, owner string, _ time.Duration, limit int) ([]postgres.OutboxMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claims++
	if s.claimErr != nil {
		return nil, s.claimErr
	}
	if len(s.batches) == 0 {
		return nil, nil
	}
	b := s.batches[0]
	s.batches = s.batches[1:]
	if len(b) > limit {
		panic("lote maior que o limite")
	}
	for i := range b {
		b[i].Lease.Owner = owner
	}
	return b, nil
}

func (s *memStore) MarkPublished(ctx context.Context, lease postgres.OutboxLease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.markErr != nil {
		return s.markErr
	}
	s.published = append(s.published, lease)
	return nil
}

func (s *memStore) MarkFailed(ctx context.Context, lease postgres.OutboxLease, cause string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed = append(s.failed, failedMark{lease, cause})
	return nil
}

func (s *memStore) Release(ctx context.Context, lease postgres.OutboxLease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released = append(s.released, lease)
	return nil
}

func (s *memStore) Backlog(ctx context.Context) (postgres.OutboxBacklog, error) {
	if err := ctx.Err(); err != nil {
		return postgres.OutboxBacklog{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.backlog, nil
}

func (s *memStore) counts() (published, failed, released int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.published), len(s.failed), len(s.released)
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condição não atingida em 10s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// blockingSender bloqueia cada envio até release ser fechado (ou, se
// honorCtx, até o contexto da chamada acabar).
type blockingSender struct {
	inner   Sender
	release chan struct{}
	honor   bool
	started chan string
}

func newBlockingSender(inner Sender, honorCtx bool) *blockingSender {
	return &blockingSender{inner: inner, release: make(chan struct{}), honor: honorCtx, started: make(chan string, 100)}
}

func (b *blockingSender) SendMessage(ctx context.Context, in *awssqs.SendMessageInput, opts ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	b.started <- aws.ToString(in.MessageDeduplicationId)
	if b.honor {
		select {
		case <-b.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return b.inner.SendMessage(ctx, in, opts...)
	}
	<-b.release
	// Ignora o contexto: simula uma chamada que "chegou" depois do prazo.
	return b.inner.SendMessage(context.WithoutCancel(ctx), in, opts...)
}
