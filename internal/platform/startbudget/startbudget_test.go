package startbudget

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestContextSubtractsReserveFromDeadline(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	pd, _ := parent.Deadline()

	ctx, cancelCtx := Context(parent, Reserve(10*time.Minute))
	defer cancelCtx()
	d, ok := ctx.Deadline()
	if !ok || !d.Equal(pd.Add(-10*time.Minute)) {
		t.Fatalf("deadline = %v (ok=%v), quer %v", d, ok, pd.Add(-10*time.Minute))
	}
}

func TestContextWithoutDeadlineOrReserveKeepsParentDeadline(t *testing.T) {
	ctx, cancel := Context(context.Background(), Reserve(time.Minute))
	defer cancel()
	if _, ok := ctx.Deadline(); ok {
		t.Fatal("ctx sem prazo não deveria ganhar prazo")
	}

	parent, cancelParent := context.WithTimeout(context.Background(), time.Hour)
	defer cancelParent()
	pd, _ := parent.Deadline()
	ctx, cancel = Context(parent, 0)
	defer cancel()
	if d, _ := ctx.Deadline(); !d.Equal(pd) {
		t.Fatalf("reserva zero mudou o prazo: %v, quer %v", d, pd)
	}
}

func TestContextExpiredWhenReserveExceedsDeadline(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx, cancelCtx := Context(parent, Reserve(time.Minute))
	defer cancelCtx()
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("ctx.Err() = %v, quer DeadlineExceeded", ctx.Err())
	}
	if parent.Err() != nil {
		t.Fatal("o pai não deve ser afetado")
	}
}

func TestContextCancelledWithParent(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	ctx, cancelCtx := Context(parent, Reserve(time.Minute))
	defer cancelCtx()
	cancel()
	<-ctx.Done()
}
