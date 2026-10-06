//go:build integration

package migrate_test

import (
	"errors"
	"io/fs"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres/migrate"
	"github.com/junglegaming/backend-challenge-go/migrations"
)

var schemaTables = []string{"wallets", "wager_transactions", "wallet_ledger_entries", "inbox", "outbox"}

var schemaFunctions = []string{
	"wallets_guard_update",
	"wager_transactions_guard_update",
	"wallet_ledger_entries_reject_mutation",
	"outbox_guard",
}

func tableExists(t *testing.T, conn *pgx.Conn, name string) bool {
	t.Helper()
	var ok bool
	if err := conn.QueryRow(t.Context(), `SELECT to_regclass($1) IS NOT NULL`, name).Scan(&ok); err != nil {
		t.Fatal(err)
	}
	return ok
}

func functionExists(t *testing.T, conn *pgx.Conn, name string) bool {
	t.Helper()
	var n int
	if err := conn.QueryRow(t.Context(), `SELECT count(*) FROM pg_proc WHERE proname = $1`, name).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n > 0
}

func appliedVersions(t *testing.T, conn *pgx.Conn) []int64 {
	t.Helper()
	rows, err := conn.Query(t.Context(), `SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatal(err)
	}
	vs, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		t.Fatal(err)
	}
	return vs
}

func TestUpDownUp(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	conn := freshDB(t)
	m := newMigrator(t)
	total := len(m.Migrations())

	st, err := m.Status(ctx, conn)
	if err != nil {
		t.Fatalf("Status on empty db: %v", err)
	}
	for _, s := range st {
		if s.Applied {
			t.Fatalf("migration %d applied on empty db", s.Version)
		}
	}

	applied, err := m.Up(ctx, conn)
	if err != nil {
		t.Fatalf("Up: %v", err)
	}
	if len(applied) != total {
		t.Fatalf("Up applied %d, want %d", len(applied), total)
	}
	for _, tbl := range schemaTables {
		if !tableExists(t, conn, tbl) {
			t.Errorf("table %s missing after Up", tbl)
		}
	}
	for _, fn := range schemaFunctions {
		if !functionExists(t, conn, fn) {
			t.Errorf("function %s missing after Up", fn)
		}
	}

	// Idempotente.
	again, err := m.Up(ctx, conn)
	if err != nil || len(again) != 0 {
		t.Fatalf("second Up: applied %d, err %v", len(again), err)
	}
	st, err = m.Status(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range st {
		if !s.Applied || s.AppliedAt.IsZero() {
			t.Errorf("migration %d not reported as applied", s.Version)
		}
	}

	reverted, err := m.DownAll(ctx, conn)
	if err != nil {
		t.Fatalf("DownAll: %v", err)
	}
	if len(reverted) != total {
		t.Fatalf("DownAll reverted %d, want %d", len(reverted), total)
	}
	for i, mig := range reverted {
		if want := int64(total - i); mig.Version != want {
			t.Errorf("revert order: position %d is version %d, want %d", i, mig.Version, want)
		}
	}
	for _, tbl := range schemaTables {
		if tableExists(t, conn, tbl) {
			t.Errorf("table %s still exists after DownAll", tbl)
		}
	}
	for _, fn := range schemaFunctions {
		if functionExists(t, conn, fn) {
			t.Errorf("function %s still exists after DownAll", fn)
		}
	}
	if vs := appliedVersions(t, conn); len(vs) != 0 {
		t.Fatalf("schema_migrations not empty after DownAll: %v", vs)
	}

	applied, err = m.Up(ctx, conn)
	if err != nil || len(applied) != total {
		t.Fatalf("Up after DownAll: applied %d, err %v", len(applied), err)
	}
}

func TestDownSteps(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	conn := freshDB(t)
	m := newMigrator(t)
	if _, err := m.Up(ctx, conn); err != nil {
		t.Fatal(err)
	}
	reverted, err := m.Down(ctx, conn, 3)
	if err != nil {
		t.Fatalf("Down(3): %v", err)
	}
	if len(reverted) != 3 || reverted[0].Name != "add_wallet_ledger_entries_seq" || reverted[1].Name != "create_outbox" || reverted[2].Name != "create_inbox" {
		t.Fatalf("Down(3) reverted %+v", reverted)
	}
	if got := appliedVersions(t, conn); len(got) != 3 {
		t.Fatalf("applied after Down(3) = %v", got)
	}
	if tableExists(t, conn, "outbox") || tableExists(t, conn, "inbox") || !tableExists(t, conn, "wallet_ledger_entries") {
		t.Fatal("unexpected tables after Down(3)")
	}
	applied, err := m.Up(ctx, conn)
	if err != nil || len(applied) != 3 {
		t.Fatalf("Up after Down(3): applied %d, err %v", len(applied), err)
	}
}

// Down funciona com dados no ledger: DROP TABLE não dispara os triggers de
// imutabilidade (reversão é operação de ambiente descartável).
func TestDownAllWithData(t *testing.T) {
	t.Parallel()
	conn := migratedDB(t)
	f := newFixture(t, conn)
	w := f.wallet(t, 10_000)
	f.openingWithLedger(t, w, 10_000)
	if _, err := newMigrator(t).DownAll(t.Context(), conn); err != nil {
		t.Fatalf("DownAll with data: %v", err)
	}
}

// Três réplicas subindo juntas: o advisory lock serializa, cada migration é
// aplicada exatamente uma vez e ninguém falha.
func TestConcurrentUp(t *testing.T) {
	t.Parallel()
	first := freshDB(t)
	var dbName string
	if err := first.QueryRow(t.Context(), `SELECT current_database()`).Scan(&dbName); err != nil {
		t.Fatal(err)
	}
	const replicas = 3
	conns := []*pgx.Conn{first}
	for i := 1; i < replicas; i++ {
		conns = append(conns, connectTo(t, dbName))
	}
	m := newMigrator(t)

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		applied int
		errs    []error
		start   = make(chan struct{})
	)
	for _, c := range conns {
		wg.Add(1)
		go func(c *pgx.Conn) {
			defer wg.Done()
			<-start
			got, err := m.Up(t.Context(), c)
			mu.Lock()
			defer mu.Unlock()
			applied += len(got)
			if err != nil {
				errs = append(errs, err)
			}
		}(c)
	}
	close(start)
	wg.Wait()
	if len(errs) != 0 {
		t.Fatalf("concurrent Up errors: %v", errs)
	}
	if applied != len(m.Migrations()) {
		t.Fatalf("total applied = %d, want %d", applied, len(m.Migrations()))
	}
	if vs := appliedVersions(t, first); len(vs) != len(m.Migrations()) {
		t.Fatalf("schema_migrations = %v", vs)
	}
}

func copyFS(t *testing.T, src fs.FS) fstest.MapFS {
	t.Helper()
	out := fstest.MapFS{}
	entries, err := fs.ReadDir(src, ".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		b, err := fs.ReadFile(src, e.Name())
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = &fstest.MapFile{Data: b}
	}
	return out
}

func TestUpDetectsTamperedAndUnknownMigrations(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	conn := migratedDB(t)

	tampered := copyFS(t, migrations.FS)
	tampered["0001_create_wallets.up.sql"].Data = append(tampered["0001_create_wallets.up.sql"].Data, []byte("\n-- edited\n")...)
	mt, err := migrate.New(tampered)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mt.Up(ctx, conn); !errors.Is(err, migrate.ErrChecksumMismatch) {
		t.Fatalf("tampered Up: err = %v, want ErrChecksumMismatch", err)
	}
	if _, err := mt.Down(ctx, conn, 1); !errors.Is(err, migrate.ErrChecksumMismatch) {
		t.Fatalf("tampered Down: err = %v, want ErrChecksumMismatch", err)
	}

	older := fstest.MapFS{}
	for name, f := range copyFS(t, migrations.FS) {
		if name[:4] == "0001" || name[:4] == "0002" {
			older[name] = f
		}
	}
	mo, err := migrate.New(older)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mo.Up(ctx, conn); !errors.Is(err, migrate.ErrUnknownVersion) {
		t.Fatalf("older binary Up: err = %v, want ErrUnknownVersion", err)
	}
	if _, err := mo.Status(ctx, conn); !errors.Is(err, migrate.ErrUnknownVersion) {
		t.Fatalf("older binary Status: err = %v, want ErrUnknownVersion", err)
	}
}

// Uma migration que falha no meio não deixa DDL parcial nem registro em
// schema_migrations (DDL transacional; sem estado "dirty").
func TestFailedMigrationIsAtomic(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	conn := freshDB(t)
	m, err := migrate.New(fstest.MapFS{
		"0001_ok.up.sql":     {Data: []byte("CREATE TABLE ok_table (id INT);")},
		"0001_ok.down.sql":   {Data: []byte("DROP TABLE ok_table;")},
		"0002_boom.up.sql":   {Data: []byte("CREATE TABLE partial_table (id INT);\nSELECT 1/0;")},
		"0002_boom.down.sql": {Data: []byte("DROP TABLE partial_table;")},
	})
	if err != nil {
		t.Fatal(err)
	}
	applied, err := m.Up(ctx, conn)
	if err == nil {
		t.Fatal("Up must fail on division by zero")
	}
	if len(applied) != 1 {
		t.Fatalf("applied = %d, want 1", len(applied))
	}
	if !tableExists(t, conn, "ok_table") || tableExists(t, conn, "partial_table") {
		t.Fatal("failed migration left partial DDL")
	}
	if vs := appliedVersions(t, conn); len(vs) != 1 || vs[0] != 1 {
		t.Fatalf("schema_migrations = %v, want [1]", vs)
	}
	// O lock foi liberado: outra sessão (locks de sessão são reentrantes, então
	// testar na mesma sessão não provaria nada) consegue adquiri-lo.
	var dbName string
	if err := conn.QueryRow(ctx, `SELECT current_database()`).Scan(&dbName); err != nil {
		t.Fatal(err)
	}
	other := connectTo(t, dbName)
	var locked bool
	if err := other.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, migrate.AdvisoryLockKey).Scan(&locked); err != nil || !locked {
		t.Fatalf("advisory lock still held after failure: locked=%v err=%v", locked, err)
	}
	mustExec(t, other, `SELECT pg_advisory_unlock($1)`, migrate.AdvisoryLockKey)
}
