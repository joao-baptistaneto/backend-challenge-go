package outboxpublisher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/adapters/sqs"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
)

func fxConfig() config.Config {
	var c config.Config
	c.Database = config.DatabaseConfig{URL: "postgres://u:p@127.0.0.1:1/db", MaxConns: 2, ConnectTimeout: time.Second}
	c.AWS = config.AWSConfig{Region: "us-east-1", AccessKeyID: "test", SecretAccessKey: "test", EndpointURL: "http://127.0.0.1:1"}
	c.SQS.EventsQueueURL = testQueueURL
	c.Workers.OutboxPollInterval = 10 * time.Millisecond
	c.Workers.OutboxBatchSize = 7
	c.Workers.OutboxLease = time.Minute
	c.Workers.OutboxConcurrency = 2
	c.Workers.OutboxPublishTimeout = time.Second
	c.Workers.OutboxDrainTimeout = time.Second
	c.Workers.OutboxRetryBaseDelay = 100 * time.Millisecond
	c.Workers.OutboxRetryMaxDelay = time.Second
	return c
}

func TestModuleValidateApp(t *testing.T) {
	if err := fx.ValidateApp(fx.Supply(fxConfig()), postgres.Module, sqs.ClientModule, Module); err != nil {
		t.Fatal(err)
	}
}

func TestConfigAndBackoffFrom(t *testing.T) {
	c := fxConfig()
	pc := ConfigFrom(c)
	want := Config{
		QueueURL: testQueueURL, PollInterval: 10 * time.Millisecond, BatchSize: 7, Concurrency: 2,
		Lease: time.Minute, PublishTimeout: time.Second, DrainTimeout: time.Second,
	}
	if pc != want {
		t.Fatalf("config = %+v", pc)
	}
	if b := BackoffFrom(c); b.Base != 100*time.Millisecond || b.Max != time.Second {
		t.Fatalf("backoff = %+v", b)
	}
}

func TestNewPostgresStoreValidates(t *testing.T) {
	if _, err := NewPostgresStore(nil, postgres.NewOutboxRepository(), postgres.DefaultOutboxBackoff()); err == nil {
		t.Error("querier nil deveria ser recusado")
	}
	var q postgres.Querier = noopQuerier{}
	if _, err := NewPostgresStore(q, nil, postgres.DefaultOutboxBackoff()); err == nil {
		t.Error("repo nil deveria ser recusado")
	}
	if _, err := NewPostgresStore(q, postgres.NewOutboxRepository(), postgres.OutboxBackoff{}); err == nil {
		t.Error("backoff inválido deveria ser recusado")
	}
}

// Ciclo Fx do publisher com Store e Sender falsos: OnStart inicia o laço, os
// eventos são publicados e o OnStop encerra as goroutines (goleak no
// TestMain).
func TestPublisherModuleLifecycle(t *testing.T) {
	s := &memStore{}
	q := newFakeQueue()
	for _, g := range []string{"a", "b", "c"} {
		s.push(message(g))
	}
	var p *Publisher
	app := fxtest.New(t,
		fx.Supply(fxConfig()),
		fx.Provide(func() Store { return s }, func() Sender { return q }, NewPublisherFx),
		fx.Populate(&p),
	)
	app.RequireStart()
	waitFor(t, func() bool { pub, _, _ := s.counts(); return pub == 3 })
	app.RequireStop()
	if p == nil || p.Owner() == "" {
		t.Fatal("publisher não construído")
	}
}

// noopQuerier satisfaz postgres.Querier sem banco (só validação).
type noopQuerier struct{}

func (noopQuerier) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("noop")
}

func (noopQuerier) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("noop")
}

func (noopQuerier) QueryRow(context.Context, string, ...any) pgx.Row { return nil }
