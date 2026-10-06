// Command migrate aplica, reverte e lista as migrations embutidas (ADR-011).
//
// Uso:
//
//	DATABASE_URL=postgres://... migrate up
//	DATABASE_URL=postgres://... migrate down 1      # reverte a última
//	DATABASE_URL=postgres://... migrate down -all   # reverte todas (destrutivo)
//	DATABASE_URL=postgres://... migrate status
//
// Lê apenas DATABASE_URL (e não platform/config inteiro) para poder rodar como
// job isolado no Docker Compose, antes das réplicas da aplicação. Várias
// execuções simultâneas são seguras: o runner serializa por advisory lock.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres/migrate"
	"github.com/junglegaming/backend-challenge-go/migrations"
)

const usage = `usage: migrate <command>
commands:
  up            apply all pending migrations
  down N        revert the N most recent migrations
  down -all     revert every applied migration (destroys data)
  status        list migrations and whether they are applied
environment:
  DATABASE_URL  PostgreSQL connection string (required)
  MIGRATE_TIMEOUT  overall timeout, Go duration (default 2m)`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Getenv, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	cmd := args[0]
	steps, err := parseCommand(cmd, args[1:])
	if err != nil {
		return err
	}

	dsn := getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}
	timeout := 2 * time.Minute
	if v := getenv("MIGRATE_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return fmt.Errorf("MIGRATE_TIMEOUT: invalid duration %q", v)
		}
		timeout = d
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	logger := slog.New(slog.NewJSONHandler(out, nil)).With(slog.String("service", "migrate"))
	m, err := migrate.New(migrations.FS, migrate.WithLogger(logger))
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		// Não ecoa o DSN: pode conter senha.
		return fmt.Errorf("connect to database: %w", err)
	}
	defer func() {
		cctx, ccancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer ccancel()
		_ = conn.Close(cctx)
	}()

	switch cmd {
	case "up":
		applied, err := m.Up(ctx, conn)
		if err != nil {
			return err
		}
		logger.InfoContext(ctx, "migrations up to date", slog.Int("applied", len(applied)))
	case "down":
		var reverted []migrate.Migration
		if steps < 0 {
			reverted, err = m.DownAll(ctx, conn)
		} else {
			reverted, err = m.Down(ctx, conn, steps)
		}
		if err != nil {
			return err
		}
		logger.InfoContext(ctx, "migrations reverted", slog.Int("reverted", len(reverted)))
	case "status":
		st, err := m.Status(ctx, conn)
		if err != nil {
			return err
		}
		for _, s := range st {
			state := "pending"
			if s.Applied {
				state = "applied " + s.AppliedAt.UTC().Format(time.RFC3339)
			}
			fmt.Fprintf(out, "%04d %-40s %s\n", s.Version, s.Name, state)
		}
	}
	return nil
}

// parseCommand valida argumentos antes de abrir conexão. Para "down", devolve
// o número de passos (-1 = todos).
func parseCommand(cmd string, rest []string) (int, error) {
	switch cmd {
	case "up", "status":
		if len(rest) != 0 {
			return 0, fmt.Errorf("%s takes no arguments\n%s", cmd, usage)
		}
		return 0, nil
	case "down":
		if len(rest) != 1 {
			return 0, fmt.Errorf("down requires N or -all\n%s", usage)
		}
		if rest[0] == "-all" {
			return -1, nil
		}
		n, err := strconv.Atoi(rest[0])
		if err != nil || n < 1 {
			return 0, fmt.Errorf("down: invalid step count %q\n%s", rest[0], usage)
		}
		return n, nil
	default:
		return 0, fmt.Errorf("unknown command %q\n%s", cmd, usage)
	}
}
