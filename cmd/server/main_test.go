package main

import (
	"context"
	"testing"
	"time"

	"go.uber.org/fx"
)

func TestAppGraphIsValid(t *testing.T) {
	if err := fx.ValidateApp(appOptions()...); err != nil {
		t.Fatalf("fx graph inválido: %v", err)
	}
}

func TestAppStartsAndStops(t *testing.T) {
	app := newApp(fx.NopLogger)
	if err := app.Err(); err != nil {
		t.Fatalf("erro ao construir app: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := app.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := app.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
}
