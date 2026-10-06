//go:build integration

package app_test

// Infraestrutura dos testes de integração dos casos de uso. Mesmo padrão de
// internal/adapters/postgres: TEST_DATABASE_URL (precisa de CREATE/DROP
// DATABASE) ou, na ausência, PostgreSQL real via testcontainers; um banco novo
// e exclusivo por teste, com todas as migrations aplicadas.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres/migrate"
	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
	"github.com/junglegaming/backend-challenge-go/migrations"
)

const postgresImage = "postgres:17-alpine"

var adminDSN string

func TestMain(m *testing.M) { os.Exit(runTests(m)) }

func runTests(m *testing.M) int {
	if dsn := os.Getenv("TEST_DATABASE_URL"); dsn != "" {
		adminDSN = dsn
		return m.Run()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ctr, err := tcpostgres.Run(ctx, postgresImage,
		tcpostgres.WithDatabase("pg_admin"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container (or set TEST_DATABASE_URL): %v\n", err)
		return 1
	}
	defer func() {
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}()
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err)
		return 1
	}
	adminDSN = dsn
	return m.Run()
}

func randomHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// migratedDSN cria um banco exclusivo com todas as migrations aplicadas e
// devolve seu DSN. O banco é removido no cleanup.
func migratedDSN(t *testing.T) string {
	t.Helper()
	ctx := t.Context()
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	defer admin.Close(context.Background())

	name := "apptest_" + randomHex(t, 8)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		a, err := pgx.Connect(ctx, adminDSN)
		if err != nil {
			t.Errorf("cleanup connect: %v", err)
			return
		}
		defer a.Close(ctx)
		if _, err := a.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Errorf("drop database %s: %v", name, err)
		}
	})

	u, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatalf("TEST_DATABASE_URL must be a URL: %v", err)
	}
	u.Path = "/" + name
	dsn := u.String()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect %s: %v", name, err)
	}
	defer conn.Close(context.Background())
	mig, err := migrate.New(migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mig.Up(ctx, conn); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	return dsn
}

func dbConfig(dsn string) config.DatabaseConfig {
	return config.DatabaseConfig{URL: config.Secret(dsn), MaxConns: 16, ConnectTimeout: 5 * time.Second}
}

// startedDB devolve um *postgres.DB iniciado sobre um banco migrado; fechado
// no cleanup.
func startedDB(t *testing.T) *postgres.DB {
	t.Helper()
	db, err := postgres.NewDB(dbConfig(migratedDSN(t)), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := db.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = db.Stop(context.Background()) })
	return db
}
