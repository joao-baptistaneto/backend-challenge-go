//go:build integration

package migrate_test

// Infraestrutura dos testes de integração do schema (T10).
//
// Banco: se TEST_DATABASE_URL estiver definido, usa esse servidor (precisa de
// permissão para CREATE/DROP DATABASE); caso contrário sobe um PostgreSQL real
// via testcontainers-go. Cada teste recebe um banco novo e exclusivo, então os
// testes podem rodar em paralelo sem interferência e sem TRUNCATE (que o ledger
// bloqueia de propósito).

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres/migrate"
	"github.com/junglegaming/backend-challenge-go/migrations"
)

// postgresImage é a imagem usada quando TEST_DATABASE_URL não está definido.
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
		tcpostgres.WithDatabase("migrate_admin"),
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

// freshDB cria um banco exclusivo para o teste e devolve uma conexão a ele. O
// banco é removido no cleanup.
func freshDB(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx := t.Context()
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	defer admin.Close(context.Background())

	name := "mig_" + randomHex(t, 8)
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
	return connectTo(t, name)
}

// connectTo abre mais uma conexão ao banco do teste (sessão independente).
func connectTo(t *testing.T, dbName string) *pgx.Conn {
	t.Helper()
	cfg, err := pgx.ParseConfig(adminDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.Database = dbName
	conn, err := pgx.ConnectConfig(t.Context(), cfg)
	if err != nil {
		t.Fatalf("connect %s: %v", dbName, err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func newMigrator(t *testing.T) *migrate.Migrator {
	t.Helper()
	m, err := migrate.New(migrations.FS)
	if err != nil {
		t.Fatalf("migrate.New: %v", err)
	}
	return m
}

// migratedDB devolve um banco novo com todas as migrations aplicadas.
func migratedDB(t *testing.T) *pgx.Conn {
	t.Helper()
	conn := freshDB(t)
	if _, err := newMigrator(t).Up(t.Context(), conn); err != nil {
		t.Fatalf("Up: %v", err)
	}
	return conn
}

func randomHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// SQLSTATEs verificados.
const (
	codeUniqueViolation     = "23505"
	codeCheckViolation      = "23514"
	codeForeignKeyViolation = "23503"
	codeRestrictViolation   = "23001"
	codeInvalidText         = "22P02"
)

// requirePgError exige um *pgconn.PgError com o SQLSTATE dado e, se
// constraints não for vazio, com um desses nomes de constraint.
func requirePgError(t *testing.T, err error, code string, constraints ...string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("expected PgError %s %v, got %v", code, constraints, err)
	}
	if pgErr.Code != code {
		t.Fatalf("SQLSTATE = %s (%s, constraint %q), want %s", pgErr.Code, pgErr.Message, pgErr.ConstraintName, code)
	}
	if len(constraints) == 0 {
		return
	}
	for _, c := range constraints {
		if pgErr.ConstraintName == c {
			return
		}
	}
	t.Fatalf("constraint = %q (%s), want one of %v", pgErr.ConstraintName, pgErr.Message, constraints)
}

// row é uma linha a inserir; valor nil vira NULL.
type row map[string]any

// with devolve uma cópia de r com as sobrescritas aplicadas.
func (r row) with(over row) row {
	out := make(row, len(r)+len(over))
	for k, v := range r {
		out[k] = v
	}
	for k, v := range over {
		out[k] = v
	}
	return out
}

func insert(ctx context.Context, conn *pgx.Conn, table string, r row) error {
	cols := make([]string, 0, len(r))
	for k := range r {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	ph := make([]string, len(cols))
	args := make([]any, len(cols))
	for i, c := range cols {
		ph[i] = fmt.Sprintf("$%d", i+1)
		args[i] = r[c]
	}
	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ", "), strings.Join(ph, ", "))
	_, err := conn.Exec(ctx, sql, args...)
	return err
}

func mustInsert(t *testing.T, conn *pgx.Conn, table string, r row) {
	t.Helper()
	if err := insert(t.Context(), conn, table, r); err != nil {
		t.Fatalf("insert into %s: %v", table, err)
	}
}

func mustExec(t *testing.T, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	if _, err := conn.Exec(t.Context(), sql, args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}
