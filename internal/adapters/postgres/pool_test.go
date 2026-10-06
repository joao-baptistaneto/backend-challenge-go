package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/fx"

	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
	"github.com/junglegaming/backend-challenge-go/internal/platform/logging"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

const testPassword = "s3cr3t-pa55"

func TestPoolConfig(t *testing.T) {
	t.Parallel()
	pc, err := PoolConfig(config.DatabaseConfig{
		URL:            config.Secret("postgres://wallet:" + testPassword + "@db.internal:5432/wallet?sslmode=disable"),
		MaxConns:       7,
		ConnectTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if pc.MaxConns != 7 || pc.ConnConfig.ConnectTimeout != 3*time.Second {
		t.Fatalf("MaxConns=%d ConnectTimeout=%v", pc.MaxConns, pc.ConnConfig.ConnectTimeout)
	}
	if pc.ConnConfig.Host != "db.internal" || pc.ConnConfig.Database != "wallet" {
		t.Fatalf("host/db = %s/%s", pc.ConnConfig.Host, pc.ConnConfig.Database)
	}
	if pc.ConnConfig.RuntimeParams["application_name"] != applicationName {
		t.Fatalf("application_name = %q", pc.ConnConfig.RuntimeParams["application_name"])
	}

	custom, err := PoolConfig(config.DatabaseConfig{URL: "postgres://u@h/db?application_name=custom"})
	if err != nil {
		t.Fatal(err)
	}
	if custom.ConnConfig.RuntimeParams["application_name"] != "custom" {
		t.Fatal("explicit application_name overwritten")
	}
}

func TestPoolConfigErrorDoesNotLeakDSN(t *testing.T) {
	t.Parallel()
	_, err := PoolConfig(config.DatabaseConfig{URL: config.Secret("postgres://u:" + testPassword + "@h/db?sslmode=bogus")})
	if !errors.Is(err, ErrPoolConfig) {
		t.Fatalf("err = %v", err)
	}
	if strings.Contains(err.Error(), testPassword) || strings.Contains(fmt.Sprintf("%+v", err), testPassword) {
		t.Fatalf("error leaked password: %v", err)
	}
}

// closedPort devolve um endereço local sem ninguém escutando.
func closedPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func TestStartRetriesUntilContextAndDoesNotLeakDSN(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	logger := logging.New(&lockedWriter{w: &logs}, logging.Options{Level: slog.LevelDebug})
	db, err := NewDB(config.DatabaseConfig{
		URL:            config.Secret("postgres://wallet:" + testPassword + "@" + closedPort(t) + "/wallet?sslmode=disable"),
		MaxConns:       2,
		ConnectTimeout: time.Second,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.pool.Close)

	ctx, cancel := context.WithCancel(t.Context())
	attempts := 0
	db.sleep = func(ctx context.Context, d time.Duration) error {
		attempts++
		if attempts == 3 {
			cancel()
		}
		return ctx.Err()
	}
	err = db.Start(ctx)
	if err == nil {
		t.Fatal("Start succeeded against closed port")
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3 (retry until ctx done)", attempts)
	}
	if strings.Contains(err.Error(), testPassword) || strings.Contains(logs.String(), testPassword) {
		t.Fatalf("password leaked: err=%v logs=%s", err, logs.String())
	}
	if !strings.Contains(logs.String(), "PostgreSQL indisponível") {
		t.Fatalf("missing retry log: %s", logs.String())
	}
}

func TestStopHonorsContext(t *testing.T) {
	t.Parallel()
	db, err := NewDB(config.DatabaseConfig{URL: config.Secret("postgres://u@" + closedPort(t) + "/db")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Close é idempotente.
	if err := db.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestModuleValidate(t *testing.T) {
	t.Parallel()
	cfg := config.Config{Database: config.DatabaseConfig{URL: "postgres://u@localhost:1/db", MaxConns: 2}}
	if err := fx.ValidateApp(
		fx.NopLogger,
		fx.Supply(cfg),
		metrics.Module,
		Module,
		fx.Invoke(func(Transactor, *TxManager, *DB) {}),
	); err != nil {
		t.Fatal(err)
	}
	// Logger e métricas são opcionais.
	if err := fx.ValidateApp(fx.NopLogger, fx.Supply(cfg), Module, fx.Invoke(func(Transactor) {})); err != nil {
		t.Fatal(err)
	}
}

type lockedWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}
