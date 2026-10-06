// Package migrate aplica e reverte as migrations SQL versionadas do schema
// PostgreSQL (ADR-011).
//
// Decisões:
//   - Runner próprio e mínimo sobre pgx/v5, sem dependência adicional. O
//     PostgreSQL tem DDL transacional, então cada migration roda numa transação
//     junto com o registro em schema_migrations: ou aplica tudo, ou nada. Não
//     existe estado "dirty" que exija intervenção manual (como em golang-migrate).
//   - Exclusão mútua entre réplicas que sobem juntas: pg_advisory_lock de sessão
//     com chave fixa (AdvisoryLockKey), adquirido ANTES de criar/ler
//     schema_migrations e mantido até o fim da execução. As demais réplicas
//     esperam o lock, releem o estado e encontram tudo aplicado. Se o processo
//     morrer, a sessão cai e o PostgreSQL libera o lock sozinho.
//   - Por isso o runner exige um *pgx.Conn (uma única sessão), e não um pool: com
//     um pool, o lock e as migrations poderiam rodar em conexões diferentes.
//   - Integridade: schema_migrations guarda o SHA-256 do arquivo up. Uma
//     migration já aplicada cujo arquivo mudou (ErrChecksumMismatch), uma versão
//     aplicada desconhecida pelo binário (ErrUnknownVersion) ou um histórico com
//     lacunas (ErrInconsistentHistory) interrompem a execução antes de qualquer
//     DDL.
//   - Limitação: como toda migration roda em transação, comandos que não podem
//     rodar em bloco transacional (ex.: CREATE INDEX CONCURRENTLY) não são
//     suportados. Para este projeto isso é aceitável.
package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// AdvisoryLockKey é a chave do pg_advisory_lock que serializa runners
// concorrentes no mesmo banco. Valor fixo ("jgwallet" em ASCII) para que todas
// as versões do binário disputem o mesmo lock.
const AdvisoryLockKey int64 = 0x6A6777616C6C6574

// TableName é a tabela de controle de versões.
const TableName = "schema_migrations"

// unlockTimeout limita a liberação do lock quando o contexto original já foi
// cancelado.
const unlockTimeout = 5 * time.Second

var (
	// ErrInvalidSource indica arquivos de migration fora da convenção, pares
	// up/down incompletos, versões duplicadas ou fora de sequência.
	ErrInvalidSource = errors.New("migrate: invalid migration source")
	// ErrChecksumMismatch indica que o arquivo up de uma migration já aplicada
	// foi alterado depois da aplicação.
	ErrChecksumMismatch = errors.New("migrate: applied migration checksum mismatch")
	// ErrUnknownVersion indica versão aplicada no banco que o binário não
	// conhece (banco à frente do código).
	ErrUnknownVersion = errors.New("migrate: database has an unknown migration version")
	// ErrInconsistentHistory indica versões aplicadas fora de sequência.
	ErrInconsistentHistory = errors.New("migrate: inconsistent migration history")
	// ErrInvalidSteps indica quantidade de passos de reversão inválida.
	ErrInvalidSteps = errors.New("migrate: steps must be >= 1")
)

// fileNamePattern: NNNN_descricao.(up|down).sql, descrição em minúsculas.
var fileNamePattern = regexp.MustCompile(`^(\d{4,})_([a-z0-9_]+)\.(up|down)\.sql$`)

// Migration é um par up/down carregado da fonte.
type Migration struct {
	Version  int64
	Name     string
	UpSQL    string
	DownSQL  string
	Checksum string // SHA-256 hex do arquivo up.
}

// Status descreve uma migration conhecida e se está aplicada.
type Status struct {
	Version   int64
	Name      string
	Applied   bool
	AppliedAt time.Time
}

// Load lê e valida as migrations de fsys (somente a raiz). Arquivos que não
// terminam em .sql são ignorados; arquivos .sql fora da convenção são erro.
// As versões precisam ser 1..N sem lacunas.
func Load(fsys fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("%w: read dir: %w", ErrInvalidSource, err)
	}
	byVersion := map[int64]*Migration{}
	for _, e := range entries {
		if e.IsDir() || path.Ext(e.Name()) != ".sql" {
			continue
		}
		m := fileNamePattern.FindStringSubmatch(e.Name())
		if m == nil {
			return nil, fmt.Errorf("%w: file %q does not match NNNN_name.(up|down).sql", ErrInvalidSource, e.Name())
		}
		version, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil || version < 1 {
			return nil, fmt.Errorf("%w: file %q has invalid version", ErrInvalidSource, e.Name())
		}
		body, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, fmt.Errorf("%w: read %q: %w", ErrInvalidSource, e.Name(), err)
		}
		if strings.TrimSpace(string(body)) == "" {
			return nil, fmt.Errorf("%w: file %q is empty", ErrInvalidSource, e.Name())
		}
		mig, ok := byVersion[version]
		if !ok {
			mig = &Migration{Version: version, Name: m[2]}
			byVersion[version] = mig
		} else if mig.Name != m[2] {
			return nil, fmt.Errorf("%w: version %d has conflicting names %q and %q", ErrInvalidSource, version, mig.Name, m[2])
		}
		switch m[3] {
		case "up":
			if mig.UpSQL != "" {
				return nil, fmt.Errorf("%w: duplicate up file for version %d", ErrInvalidSource, version)
			}
			mig.UpSQL = string(body)
			sum := sha256.Sum256(body)
			mig.Checksum = hex.EncodeToString(sum[:])
		case "down":
			if mig.DownSQL != "" {
				return nil, fmt.Errorf("%w: duplicate down file for version %d", ErrInvalidSource, version)
			}
			mig.DownSQL = string(body)
		}
	}
	if len(byVersion) == 0 {
		return nil, fmt.Errorf("%w: no migrations found", ErrInvalidSource)
	}
	out := make([]Migration, 0, len(byVersion))
	for _, m := range byVersion {
		if m.UpSQL == "" || m.DownSQL == "" {
			return nil, fmt.Errorf("%w: version %d (%s) must have both up and down files", ErrInvalidSource, m.Version, m.Name)
		}
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	for i, m := range out {
		if m.Version != int64(i+1) {
			return nil, fmt.Errorf("%w: versions must be sequential from 1; expected %d, got %d", ErrInvalidSource, i+1, m.Version)
		}
	}
	return out, nil
}

// Migrator aplica/reverte um conjunto validado de migrations.
type Migrator struct {
	migrations []Migration
	logger     *slog.Logger
}

// Option configura o Migrator.
type Option func(*Migrator)

// WithLogger registra cada migration aplicada/revertida.
func WithLogger(l *slog.Logger) Option {
	return func(m *Migrator) { m.logger = l }
}

// New carrega e valida as migrations de fsys.
func New(fsys fs.FS, opts ...Option) (*Migrator, error) {
	migs, err := Load(fsys)
	if err != nil {
		return nil, err
	}
	m := &Migrator{migrations: migs}
	for _, o := range opts {
		o(m)
	}
	return m, nil
}

// Migrations devolve uma cópia das migrations conhecidas, em ordem.
func (m *Migrator) Migrations() []Migration {
	return append([]Migration(nil), m.migrations...)
}

// Up aplica todas as migrations pendentes, cada uma em sua transação, e devolve
// as aplicadas nesta chamada (vazio se já estava atualizado).
func (m *Migrator) Up(ctx context.Context, conn *pgx.Conn) ([]Migration, error) {
	var applied []Migration
	err := m.withLock(ctx, conn, func(state map[int64]appliedRow) error {
		for _, mig := range m.migrations {
			if _, ok := state[mig.Version]; ok {
				continue
			}
			if err := m.apply(ctx, conn, mig); err != nil {
				return err
			}
			applied = append(applied, mig)
		}
		return nil
	})
	return applied, err
}

// Down reverte as `steps` migrations aplicadas mais recentes, da mais nova para
// a mais antiga, e devolve as revertidas. Reverter mais passos do que existem
// reverte tudo.
func (m *Migrator) Down(ctx context.Context, conn *pgx.Conn, steps int) ([]Migration, error) {
	if steps < 1 {
		return nil, ErrInvalidSteps
	}
	var reverted []Migration
	err := m.withLock(ctx, conn, func(state map[int64]appliedRow) error {
		for i := len(m.migrations) - 1; i >= 0 && len(reverted) < steps; i-- {
			mig := m.migrations[i]
			if _, ok := state[mig.Version]; !ok {
				continue
			}
			if err := m.revert(ctx, conn, mig); err != nil {
				return err
			}
			reverted = append(reverted, mig)
		}
		return nil
	})
	return reverted, err
}

// DownAll reverte todas as migrations aplicadas.
func (m *Migrator) DownAll(ctx context.Context, conn *pgx.Conn) ([]Migration, error) {
	return m.Down(ctx, conn, len(m.migrations))
}

// Status devolve o estado de cada migration conhecida. Não adquire o lock nem
// cria schema_migrations; apenas valida o histórico.
func (m *Migrator) Status(ctx context.Context, conn *pgx.Conn) ([]Status, error) {
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, TableName).Scan(&exists); err != nil {
		return nil, fmt.Errorf("migrate: check %s: %w", TableName, err)
	}
	state := map[int64]appliedRow{}
	if exists {
		var err error
		if state, err = readApplied(ctx, conn); err != nil {
			return nil, err
		}
		if err := m.validate(state); err != nil {
			return nil, err
		}
	}
	out := make([]Status, 0, len(m.migrations))
	for _, mig := range m.migrations {
		s := Status{Version: mig.Version, Name: mig.Name}
		if row, ok := state[mig.Version]; ok {
			s.Applied = true
			s.AppliedAt = row.appliedAt
		}
		out = append(out, s)
	}
	return out, nil
}

type appliedRow struct {
	checksum  string
	appliedAt time.Time
}

// withLock serializa a execução entre processos, garante a tabela de controle,
// lê e valida o histórico e só então chama fn.
func (m *Migrator) withLock(ctx context.Context, conn *pgx.Conn, fn func(map[int64]appliedRow) error) (err error) {
	if conn == nil {
		return errors.New("migrate: nil connection")
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, AdvisoryLockKey); err != nil {
		return fmt.Errorf("migrate: acquire advisory lock: %w", err)
	}
	defer func() {
		// Libera mesmo com ctx cancelado; se a conexão estiver quebrada, o
		// encerramento da sessão libera o lock.
		uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unlockTimeout)
		defer cancel()
		if _, uerr := conn.Exec(uctx, `SELECT pg_advisory_unlock($1)`, AdvisoryLockKey); uerr != nil && err == nil {
			err = fmt.Errorf("migrate: release advisory lock: %w", uerr)
		}
	}()

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+TableName+` (
	version    BIGINT      NOT NULL PRIMARY KEY,
	name       TEXT        NOT NULL,
	checksum   CHAR(64)    NOT NULL,
	applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`); err != nil {
		return fmt.Errorf("migrate: create %s: %w", TableName, err)
	}
	state, err := readApplied(ctx, conn)
	if err != nil {
		return err
	}
	if err := m.validate(state); err != nil {
		return err
	}
	return fn(state)
}

func readApplied(ctx context.Context, conn *pgx.Conn) (map[int64]appliedRow, error) {
	rows, err := conn.Query(ctx, `SELECT version, checksum, applied_at FROM `+TableName+` ORDER BY version`)
	if err != nil {
		return nil, fmt.Errorf("migrate: read %s: %w", TableName, err)
	}
	defer rows.Close()
	state := map[int64]appliedRow{}
	for rows.Next() {
		var (
			v   int64
			row appliedRow
		)
		if err := rows.Scan(&v, &row.checksum, &row.appliedAt); err != nil {
			return nil, fmt.Errorf("migrate: scan %s: %w", TableName, err)
		}
		state[v] = row
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("migrate: read %s: %w", TableName, err)
	}
	return state, nil
}

// validate confere o histórico aplicado contra as migrations conhecidas: toda
// versão aplicada precisa ser conhecida e ter o mesmo checksum, e as versões
// aplicadas precisam formar o prefixo 1..k.
func (m *Migrator) validate(state map[int64]appliedRow) error {
	known := make(map[int64]Migration, len(m.migrations))
	for _, mig := range m.migrations {
		known[mig.Version] = mig
	}
	for v, row := range state {
		mig, ok := known[v]
		if !ok {
			return fmt.Errorf("%w: version %d", ErrUnknownVersion, v)
		}
		if row.checksum != mig.Checksum {
			return fmt.Errorf("%w: version %d (%s)", ErrChecksumMismatch, v, mig.Name)
		}
	}
	for v := int64(1); v <= int64(len(state)); v++ {
		if _, ok := state[v]; !ok {
			return fmt.Errorf("%w: version %d missing while later versions are applied", ErrInconsistentHistory, v)
		}
	}
	return nil
}

func (m *Migrator) apply(ctx context.Context, conn *pgx.Conn, mig Migration) error {
	err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		// Sem argumentos o pgx usa o protocolo simples, que aceita vários
		// comandos num único Exec.
		if _, err := tx.Exec(ctx, mig.UpSQL); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO `+TableName+` (version, name, checksum) VALUES ($1, $2, $3)`,
			mig.Version, mig.Name, mig.Checksum)
		return err
	})
	if err != nil {
		return fmt.Errorf("migrate: apply %04d_%s: %w", mig.Version, mig.Name, err)
	}
	m.log(ctx, "migration applied", mig)
	return nil
}

func (m *Migrator) revert(ctx context.Context, conn *pgx.Conn, mig Migration) error {
	err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, mig.DownSQL); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DELETE FROM `+TableName+` WHERE version = $1`, mig.Version)
		return err
	})
	if err != nil {
		return fmt.Errorf("migrate: revert %04d_%s: %w", mig.Version, mig.Name, err)
	}
	m.log(ctx, "migration reverted", mig)
	return nil
}

func (m *Migrator) log(ctx context.Context, msg string, mig Migration) {
	if m.logger == nil {
		return
	}
	m.logger.InfoContext(ctx, msg, slog.Int64("version", mig.Version), slog.String("name", mig.Name))
}
