package postgres

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

func TestExponentialDelay(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		base, max time.Duration
		attempt   int
		want      time.Duration
	}{
		{"zero base", 0, time.Second, 5, 0},
		{"negative base", -time.Second, time.Second, 5, 0},
		{"attempt 0 as 1", time.Second, time.Minute, 0, time.Second},
		{"attempt negative as 1", time.Second, time.Minute, -3, time.Second},
		{"first", time.Second, time.Minute, 1, time.Second},
		{"second", time.Second, time.Minute, 2, 2 * time.Second},
		{"sixth", time.Second, time.Minute, 6, 32 * time.Second},
		{"capped", time.Second, time.Minute, 7, time.Minute},
		{"capped far", time.Second, time.Minute, 1_000_000, time.Minute},
		{"capped max int", time.Second, time.Minute, math.MaxInt, time.Minute},
		{"base above max", time.Hour, time.Minute, 1, time.Minute},
		{"no cap 41 saturates", time.Second, 0, 41, math.MaxInt64},
		{"no cap 64", time.Nanosecond, 0, 64, math.MaxInt64},
		{"no cap max int", time.Millisecond, 0, math.MaxInt, math.MaxInt64},
		{"no cap exact", time.Nanosecond, 0, 63, 1 << 62},
		{"max delay MaxInt64", time.Second, math.MaxInt64, 200, math.MaxInt64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := exponentialDelay(tt.base, tt.max, tt.attempt); got != tt.want {
				t.Fatalf("exponentialDelay(%v, %v, %d) = %v, want %v", tt.base, tt.max, tt.attempt, got, tt.want)
			}
		})
	}
}

// Pendência da T11: sem MaxDelay o backoff estourava int64 por volta da 41ª
// tentativa e rand.Int64N entrava em panic.
func TestRetryBackoffWithoutMaxDelayDoesNotOverflow(t *testing.T) {
	t.Parallel()
	p := RetryPolicy{BaseDelay: 10 * time.Millisecond}
	for _, attempt := range []int{1, 2, 40, 41, 42, 63, 64, 100, 1 << 20, math.MaxInt} {
		d := p.backoff(attempt)
		if d <= 0 {
			t.Fatalf("attempt %d: backoff %v must be positive", attempt, d)
		}
	}
	// Saturado: equal jitter entre MaxInt64/2 e MaxInt64.
	if d := p.backoff(100); d < math.MaxInt64/2 {
		t.Fatalf("saturated backoff %v below half of MaxInt64", d)
	}
	if d := p.backoff(2); d < 10*time.Millisecond || d > 20*time.Millisecond {
		t.Fatalf("attempt 2: %v outside [10ms,20ms]", d)
	}
}

func TestOutboxBackoff(t *testing.T) {
	t.Parallel()
	b := OutboxBackoff{Base: 500 * time.Millisecond, Max: 10 * time.Second}
	if err := b.Validate(); err != nil {
		t.Fatal(err)
	}
	want := map[int]time.Duration{
		1: 500 * time.Millisecond, 2: time.Second, 3: 2 * time.Second, 4: 4 * time.Second,
		5: 8 * time.Second, 6: 10 * time.Second, 50: 10 * time.Second, math.MaxInt32: 10 * time.Second,
	}
	for attempt, w := range want {
		if got := b.Delay(attempt); got != w {
			t.Fatalf("Delay(%d) = %v, want %v", attempt, got, w)
		}
	}
	d := DefaultOutboxBackoff()
	if err := d.Validate(); err != nil || d.Delay(1) != time.Second || d.Delay(100) != 5*time.Minute {
		t.Fatalf("default backoff: %+v %v", d, err)
	}
	for _, bad := range []OutboxBackoff{{}, {Base: time.Second}, {Max: time.Second}, {Base: 2 * time.Second, Max: time.Second}, {Base: -1, Max: time.Second}} {
		if err := bad.Validate(); !errors.Is(err, ErrInvalidBackoff) {
			t.Fatalf("Validate(%+v) = %v, want ErrInvalidBackoff", bad, err)
		}
	}
}

func TestTruncateUTF8(t *testing.T) {
	t.Parallel()
	if got := truncateUTF8("abc", 10); got != "abc" {
		t.Fatalf("short = %q", got)
	}
	// "é" tem 2 bytes: cortar em 3 bytes não pode partir o segundo "é".
	if got := truncateUTF8("éé", 3); got != "é" {
		t.Fatalf("cut = %q", got)
	}
	if got := truncateUTF8("a\x00b", 10); got != "ab" {
		t.Fatalf("nul = %q", got)
	}
	if got := truncateUTF8("a\xffb", 10); !utf8.ValidString(got) || got != "a�b" {
		t.Fatalf("invalid utf8 = %q", got)
	}
	long := truncateUTF8(strings.Repeat("ã", 3000), maxOutboxErrorLength)
	if len(long) > maxOutboxErrorLength || !utf8.ValidString(long) {
		t.Fatalf("long: %d bytes, valid %v", len(long), utf8.ValidString(long))
	}
}

func TestInboxStatusString(t *testing.T) {
	t.Parallel()
	for s, w := range map[InboxStatus]string{InboxNew: "NEW", InboxInProgress: "IN_PROGRESS", InboxCompleted: "COMPLETED", 0: "InboxStatus(0)"} {
		if s.String() != w {
			t.Fatalf("%d.String() = %q", int(s), s.String())
		}
	}
}

// fakeTxQuerier é um pgx.Tx cujos métodos de consulta falham o teste (via
// noDBQuerier): valida que a checagem acontece antes do SQL mesmo com Tx.
type fakeTxQuerier struct {
	*fakeTx
	noDBQuerier
}

func TestInboxOutboxValidationBeforeSQL(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	q := noDBQuerier{t}
	txq := fakeTxQuerier{fakeTx: &fakeTx{}, noDBQuerier: q}
	inbox := NewInboxRepository()
	outbox := NewOutboxRepository()
	hash := strings.Repeat("a", 64)
	now := unitT0
	lease := OutboxLease{Seq: 1, EventID: unitWalletID, Owner: "pub-1", Attempt: 1}

	cases := map[string]struct {
		err  error
		want error
	}{
		"register outside tx": {func() error { _, err := inbox.Register(ctx, q, "c", "m", hash, now); return err }(), ErrTxRequired},
		"complete outside tx": {inbox.Complete(ctx, q, "c", "m", hash, uuid.Nil, now), ErrTxRequired},
		"empty consumer":      {func() error { _, err := inbox.Register(ctx, txq, "", "m", hash, now); return err }(), ErrInvalidInboxMessage},
		"long message id":     {func() error { _, err := inbox.Register(ctx, txq, "c", strings.Repeat("m", 256), hash, now); return err }(), ErrInvalidInboxMessage},
		"uppercase hash":      {func() error { _, err := inbox.Register(ctx, txq, "c", "m", strings.Repeat("A", 64), now); return err }(), ErrInvalidInboxMessage},
		"short hash":          {inbox.Complete(ctx, txq, "c", "m", "abc", uuid.Nil, now), ErrInvalidInboxMessage},
		"zero time":           {inbox.Complete(ctx, txq, "c", "m", hash, uuid.Nil, time.Time{}), ErrInvalidInboxMessage},
		"get empty key":       {func() error { _, err := inbox.Get(ctx, q, "c", ""); return err }(), ErrInvalidInboxMessage},
		"enqueue zero event":  {outbox.Enqueue(ctx, q, domain.IntegrationEvent{}), ErrInvalidOutboxEvent},
		"claim empty owner":   {func() error { _, err := outbox.Claim(ctx, q, "", time.Minute, 10); return err }(), ErrInvalidLease},
		"claim long owner":    {func() error { _, err := outbox.Claim(ctx, q, strings.Repeat("o", 256), time.Minute, 10); return err }(), ErrInvalidLease},
		"claim zero lease":    {func() error { _, err := outbox.Claim(ctx, q, "o", 0, 10); return err }(), ErrInvalidLease},
		"claim huge lease":    {func() error { _, err := outbox.Claim(ctx, q, "o", 2*time.Hour, 10); return err }(), ErrInvalidLease},
		"claim zero batch":    {func() error { _, err := outbox.Claim(ctx, q, "o", time.Minute, 0); return err }(), ErrInvalidLease},
		"claim huge batch":    {func() error { _, err := outbox.Claim(ctx, q, "o", time.Minute, MaxOutboxClaimBatch+1); return err }(), ErrInvalidLease},
		"published zero":      {outbox.MarkPublished(ctx, q, OutboxLease{}), ErrInvalidLease},
		"failed zero lease":   {outbox.MarkFailed(ctx, q, OutboxLease{}, "x", DefaultOutboxBackoff()), ErrInvalidLease},
		"failed bad backoff":  {outbox.MarkFailed(ctx, q, lease, "x", OutboxBackoff{}), ErrInvalidBackoff},
	}
	for name, c := range cases {
		if !errors.Is(c.err, c.want) {
			t.Errorf("%s: err = %v, want %v", name, c.err, c.want)
		}
	}
	// Lista vazia não executa SQL.
	if err := outbox.Enqueue(ctx, q); err != nil {
		t.Fatal(err)
	}
}

func TestOutboxConstraintMapping(t *testing.T) {
	t.Parallel()
	for constraint, want := range map[string]error{
		"outbox_event_id_key": ErrDuplicateEvent,
		"outbox_pkey":         ErrDuplicateKey,
	} {
		err := mapWriteError("op", &pgconn.PgError{Code: CodeUniqueViolation, ConstraintName: constraint})
		if !errors.Is(err, want) || !errors.Is(err, ErrUniqueViolation) {
			t.Fatalf("%s: %v", constraint, err)
		}
	}
}
