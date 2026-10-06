package sqs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// fakeSQS é uma fila FIFO em memória com a semântica que o consumidor usa:
//
//   - entrega em ordem por MessageGroupId; um grupo com mensagem em voo não
//     entrega as seguintes;
//   - visibilidade (1 "segundo" da API = unit de tempo real, para os testes
//     serem rápidos) e ApproximateReceiveCount;
//   - redrive: no recebimento em que a contagem passaria de maxReceive, a
//     mensagem vai para a DLQ em vez de ser entregue;
//   - receipt handle novo a cada recebimento; handle antigo é recusado;
//   - GetQueueUrl (fila de entrada e DLQ) e SendMessage na DLQ FIFO, com
//     deduplicação por MessageDeduplicationId.
type fakeSQS struct {
	mu         sync.Mutex
	unit       time.Duration
	maxReceive int32 // 0 = sem RedrivePolicy
	queueURL   string
	dlqURL     string

	seq  int
	msgs []*fakeMsg
	dlq  []*fakeMsg

	receiveCalls int
	deleted      []string // ids
	visChanges   []visChange
	failReceive  int            // próximas N chamadas de Receive falham
	failDelete   map[string]int // id → falhas restantes
	attrsErr     error          // erro de GetQueueAttributes
	failSend     int            // próximos N SendMessage falham
	getURLErr    int            // próximos N GetQueueUrl falham
	sendCalls    int
	getURLCalls  int
}

const fakeAccount = "000000000000"

type fakeMsg struct {
	id, group, body string
	visibleAt       time.Time
	receiveCount    int32
	handle          string
	dedup           string            // só na DLQ (envio explícito)
	attrs           map[string]string // só na DLQ (envio explícito)
}

type visChange struct {
	id      string
	seconds int32
}

func newFakeSQS(maxReceive int32) *fakeSQS {
	return &fakeSQS{
		unit: 10 * time.Millisecond, maxReceive: maxReceive,
		queueURL: "http://fake/000000000000/in.fifo", dlqURL: "http://fake/000000000000/dlq.fifo",
		failDelete: map[string]int{},
	}
}

// send enfileira uma mensagem e devolve seu id.
func (f *fakeSQS) send(group, body string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	id := fmt.Sprintf("m%d", f.seq)
	f.msgs = append(f.msgs, &fakeMsg{id: id, group: group, body: body})
	return id
}

func (f *fakeSQS) ReceiveMessage(ctx context.Context, in *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	f.receiveCalls++
	fail := f.failReceive > 0
	if fail {
		f.failReceive--
	}
	f.mu.Unlock()
	if fail {
		return nil, errors.New("fake: receive failed")
	}
	deadline := time.Now().Add(time.Duration(in.WaitTimeSeconds) * f.unit)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if out := f.take(in); len(out) > 0 || !time.Now().Before(deadline) {
			return &awssqs.ReceiveMessageOutput{Messages: out}, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func (f *fakeSQS) take(in *awssqs.ReceiveMessageInput) []types.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	max := int(in.MaxNumberOfMessages)
	if max < 1 {
		max = 1
	}
	vis := time.Duration(in.VisibilityTimeout) * f.unit
	if in.VisibilityTimeout == 0 {
		vis = 30 * f.unit
	}
	blocked := map[string]bool{}
	var out []types.Message
	kept := f.msgs[:0:0]
	for _, m := range f.msgs {
		if len(out) >= max || blocked[m.group] || m.visibleAt.After(now) {
			blocked[m.group] = true
			kept = append(kept, m)
			continue
		}
		if f.maxReceive > 0 && m.receiveCount >= f.maxReceive {
			f.dlq = append(f.dlq, m) // redrive
			continue
		}
		m.receiveCount++
		f.seq++
		m.handle = fmt.Sprintf("h%d-%s", f.seq, m.id)
		m.visibleAt = now.Add(vis)
		kept = append(kept, m)
		out = append(out, types.Message{
			MessageId: aws.String(m.id), Body: aws.String(m.body), ReceiptHandle: aws.String(m.handle),
			Attributes: map[string]string{
				attrReceiveCount:   strconv.Itoa(int(m.receiveCount)),
				attrMessageGroupID: m.group,
			},
		})
	}
	f.msgs = kept
	return out
}

func (f *fakeSQS) find(handle string) (*fakeMsg, int) {
	for i, m := range f.msgs {
		if m.handle == handle && m.visibleAt.After(time.Now()) {
			return m, i
		}
	}
	return nil, -1
}

func (f *fakeSQS) DeleteMessage(_ context.Context, in *awssqs.DeleteMessageInput, _ ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, i := f.find(aws.ToString(in.ReceiptHandle))
	if m == nil {
		return nil, errors.New("fake: ReceiptHandleIsInvalid")
	}
	if f.failDelete[m.id] > 0 {
		f.failDelete[m.id]--
		return nil, errors.New("fake: delete failed")
	}
	f.msgs = append(f.msgs[:i], f.msgs[i+1:]...)
	f.deleted = append(f.deleted, m.id)
	return &awssqs.DeleteMessageOutput{}, nil
}

func (f *fakeSQS) ChangeMessageVisibility(_ context.Context, in *awssqs.ChangeMessageVisibilityInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, _ := f.find(aws.ToString(in.ReceiptHandle))
	if m == nil {
		return nil, errors.New("fake: ReceiptHandleIsInvalid")
	}
	m.visibleAt = time.Now().Add(time.Duration(in.VisibilityTimeout) * f.unit)
	f.visChanges = append(f.visChanges, visChange{id: m.id, seconds: in.VisibilityTimeout})
	return &awssqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeSQS) GetQueueAttributes(_ context.Context, in *awssqs.GetQueueAttributesInput, _ ...func(*awssqs.Options)) (*awssqs.GetQueueAttributesOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.attrsErr != nil {
		return nil, f.attrsErr
	}
	attrs := map[string]string{}
	switch aws.ToString(in.QueueUrl) {
	case f.queueURL:
		if f.maxReceive > 0 {
			attrs["RedrivePolicy"] = fmt.Sprintf(`{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq.fifo","maxReceiveCount":%d}`, f.maxReceive)
		}
		attrs["QueueArn"] = "arn:aws:sqs:us-east-1:000000000000:in.fifo"
		attrs["ApproximateNumberOfMessages"] = strconv.Itoa(len(f.msgs))
	case f.dlqURL:
		attrs["ApproximateNumberOfMessages"] = strconv.Itoa(len(f.dlq))
	default:
		return nil, errors.New("fake: queue does not exist")
	}
	return &awssqs.GetQueueAttributesOutput{Attributes: attrs}, nil
}

// snapshot devolve cópias do estado observável.
type fakeState struct {
	pending, dlq, deleted []string
	visChanges            []visChange
	receiveCalls          int
}

func (f *fakeSQS) state() fakeState {
	f.mu.Lock()
	defer f.mu.Unlock()
	var s fakeState
	for _, m := range f.msgs {
		s.pending = append(s.pending, m.id)
	}
	for _, m := range f.dlq {
		s.dlq = append(s.dlq, m.id)
	}
	s.deleted = append(s.deleted, f.deleted...)
	s.visChanges = append(s.visChanges, f.visChanges...)
	s.receiveCalls = f.receiveCalls
	return s
}

// drained informa se a fila principal está vazia.
func (f *fakeSQS) drained() bool {
	s := f.state()
	return len(s.pending) == 0
}

func (f *fakeSQS) GetQueueUrl(_ context.Context, in *awssqs.GetQueueUrlInput, _ ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getURLCalls++
	if f.getURLErr > 0 {
		f.getURLErr--
		return nil, errors.New("fake: GetQueueUrl failed")
	}
	if acc := aws.ToString(in.QueueOwnerAWSAccountId); acc != "" && acc != fakeAccount {
		return nil, &types.QueueDoesNotExist{Message: aws.String("wrong account")}
	}
	for _, u := range []string{f.queueURL, f.dlqURL} {
		if queueName(u) == aws.ToString(in.QueueName) {
			return &awssqs.GetQueueUrlOutput{QueueUrl: aws.String(u)}, nil
		}
	}
	return nil, &types.QueueDoesNotExist{Message: aws.String("no such queue")}
}

// SendMessage só aceita a DLQ (FIFO): exige MessageGroupId e
// MessageDeduplicationId, e deduplica pelo id (como a janela de 5 min).
func (f *fakeSQS) SendMessage(_ context.Context, in *awssqs.SendMessageInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sendCalls++
	if f.failSend > 0 {
		f.failSend--
		return nil, errors.New("fake: send failed")
	}
	if aws.ToString(in.QueueUrl) != f.dlqURL {
		return nil, errors.New("fake: SendMessage só na DLQ")
	}
	if aws.ToString(in.MessageBody) == "" || aws.ToString(in.MessageGroupId) == "" || aws.ToString(in.MessageDeduplicationId) == "" {
		return nil, errors.New("fake: MissingParameter (FIFO exige corpo, MessageGroupId e MessageDeduplicationId)")
	}
	dedup := aws.ToString(in.MessageDeduplicationId)
	for _, m := range f.dlq {
		if m.dedup == dedup {
			return &awssqs.SendMessageOutput{MessageId: aws.String(m.id)}, nil
		}
	}
	attrs := map[string]string{}
	for k, v := range in.MessageAttributes {
		attrs[k] = aws.ToString(v.StringValue)
	}
	f.dlq = append(f.dlq, &fakeMsg{
		id: attrs[DLQAttrSourceMessageID], group: aws.ToString(in.MessageGroupId), body: aws.ToString(in.MessageBody),
		dedup: dedup, attrs: attrs,
	})
	return &awssqs.SendMessageOutput{MessageId: aws.String(dedup)}, nil
}

// dlqEntry é uma mensagem na DLQ falsa: explicit = enviada pelo consumidor
// (com atributos); senão, movida pelo redrive nativo.
type dlqEntry struct {
	id, group, body string
	attrs           map[string]string
	explicit        bool
}

func (f *fakeSQS) dlqEntries() []dlqEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]dlqEntry, 0, len(f.dlq))
	for _, m := range f.dlq {
		out = append(out, dlqEntry{id: m.id, group: m.group, body: m.body, attrs: m.attrs, explicit: m.attrs != nil})
	}
	return out
}
