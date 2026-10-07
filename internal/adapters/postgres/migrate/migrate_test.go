package migrate

import (
	"errors"
	"regexp"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/migrations"
)

func TestLoadEmbeddedMigrations(t *testing.T) {
	migs, err := Load(migrations.FS)
	if err != nil {
		t.Fatalf("Load(migrations.FS): %v", err)
	}
	wantNames := []string{
		"create_wallets",
		"create_wager_transactions",
		"create_wallet_ledger_entries",
		"create_inbox",
		"create_outbox",
		"add_wallet_ledger_entries_seq",
		"add_wager_transactions_claim",
		"add_outbox_claim_indexes",
	}
	if len(migs) != len(wantNames) {
		t.Fatalf("got %d migrations, want %d", len(migs), len(wantNames))
	}
	hexRe := regexp.MustCompile(`^[0-9a-f]{64}$`)
	for i, m := range migs {
		if m.Version != int64(i+1) {
			t.Errorf("migration %d: version %d", i, m.Version)
		}
		if m.Name != wantNames[i] {
			t.Errorf("migration %d: name %q, want %q", i, m.Name, wantNames[i])
		}
		if strings.TrimSpace(m.UpSQL) == "" || strings.TrimSpace(m.DownSQL) == "" {
			t.Errorf("migration %d: empty up/down", m.Version)
		}
		if !hexRe.MatchString(m.Checksum) {
			t.Errorf("migration %d: checksum %q", m.Version, m.Checksum)
		}
	}
}

// Todo arquivo .sql embutido participa de um par up/down válido.
func TestEmbeddedFilesArePaired(t *testing.T) {
	entries, err := migrations.FS.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	ups, downs := map[string]bool{}, map[string]bool{}
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, ".up.sql"):
			ups[strings.TrimSuffix(name, ".up.sql")] = true
		case strings.HasSuffix(name, ".down.sql"):
			downs[strings.TrimSuffix(name, ".down.sql")] = true
		default:
			t.Errorf("unexpected embedded file %q", name)
		}
	}
	if len(ups) == 0 {
		t.Fatal("no embedded migrations")
	}
	for base := range ups {
		if !downs[base] {
			t.Errorf("%s.up.sql has no down", base)
		}
	}
	for base := range downs {
		if !ups[base] {
			t.Errorf("%s.down.sql has no up", base)
		}
	}
}

func TestLoadRejectsInvalidSources(t *testing.T) {
	file := func(s string) *fstest.MapFile { return &fstest.MapFile{Data: []byte(s)} }
	tests := []struct {
		name string
		fs   fstest.MapFS
	}{
		{"empty", fstest.MapFS{}},
		{"missing down", fstest.MapFS{"0001_a.up.sql": file("SELECT 1;")}},
		{"missing up", fstest.MapFS{"0001_a.down.sql": file("SELECT 1;")}},
		{"bad name", fstest.MapFS{"1_a.up.sql": file("SELECT 1;"), "1_a.down.sql": file("SELECT 1;")}},
		{"uppercase name", fstest.MapFS{"0001_A.up.sql": file("SELECT 1;"), "0001_A.down.sql": file("SELECT 1;")}},
		{"empty file", fstest.MapFS{"0001_a.up.sql": file("  \n"), "0001_a.down.sql": file("SELECT 1;")}},
		{"version zero", fstest.MapFS{"0000_a.up.sql": file("SELECT 1;"), "0000_a.down.sql": file("SELECT 1;")}},
		{"gap", fstest.MapFS{
			"0001_a.up.sql": file("SELECT 1;"), "0001_a.down.sql": file("SELECT 1;"),
			"0003_c.up.sql": file("SELECT 1;"), "0003_c.down.sql": file("SELECT 1;"),
		}},
		{"conflicting names", fstest.MapFS{"0001_a.up.sql": file("SELECT 1;"), "0001_b.down.sql": file("SELECT 1;")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Load(tt.fs); !errors.Is(err, ErrInvalidSource) {
				t.Fatalf("Load: err = %v, want ErrInvalidSource", err)
			}
		})
	}
}

func TestLoadIgnoresNonSQLFilesAndSorts(t *testing.T) {
	fsys := fstest.MapFS{
		"README.md":         {Data: []byte("docs")},
		"embed.go":          {Data: []byte("package migrations")},
		"0002_b.up.sql":     {Data: []byte("SELECT 2;")},
		"0002_b.down.sql":   {Data: []byte("SELECT -2;")},
		"0001_a.up.sql":     {Data: []byte("SELECT 1;")},
		"0001_a.down.sql":   {Data: []byte("SELECT -1;")},
		"sub/0003_c.up.sql": {Data: []byte("SELECT 3;")},
	}
	migs, err := Load(fsys)
	if err != nil {
		t.Fatal(err)
	}
	if len(migs) != 2 || migs[0].Name != "a" || migs[1].Name != "b" {
		t.Fatalf("unexpected migrations: %+v", migs)
	}
	if migs[0].UpSQL != "SELECT 1;" || migs[0].DownSQL != "SELECT -1;" {
		t.Fatalf("wrong bodies: %+v", migs[0])
	}
}

func TestValidateHistory(t *testing.T) {
	m, err := New(migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	all := m.Migrations()
	row := func(v int64) appliedRow { return appliedRow{checksum: all[v-1].Checksum} }

	if err := m.validate(map[int64]appliedRow{}); err != nil {
		t.Errorf("empty history: %v", err)
	}
	if err := m.validate(map[int64]appliedRow{1: row(1), 2: row(2)}); err != nil {
		t.Errorf("prefix history: %v", err)
	}
	if err := m.validate(map[int64]appliedRow{1: row(1), 99: {checksum: "x"}}); !errors.Is(err, ErrUnknownVersion) {
		t.Errorf("unknown version: err = %v", err)
	}
	if err := m.validate(map[int64]appliedRow{1: {checksum: strings.Repeat("0", 64)}}); !errors.Is(err, ErrChecksumMismatch) {
		t.Errorf("checksum mismatch: err = %v", err)
	}
	if err := m.validate(map[int64]appliedRow{1: row(1), 3: row(3)}); !errors.Is(err, ErrInconsistentHistory) {
		t.Errorf("gap: err = %v", err)
	}
}

func TestMigrationsReturnsCopy(t *testing.T) {
	m, err := New(migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	got := m.Migrations()
	got[0].UpSQL = "tampered"
	if m.Migrations()[0].UpSQL == "tampered" {
		t.Fatal("Migrations exposes internal slice")
	}
}

func TestDownRejectsInvalidSteps(t *testing.T) {
	m, err := New(migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	for _, steps := range []int{0, -1} {
		if _, err := m.Down(t.Context(), nil, steps); !errors.Is(err, ErrInvalidSteps) {
			t.Errorf("Down(%d): err = %v", steps, err)
		}
	}
	if _, err := m.Up(t.Context(), nil); err == nil {
		t.Error("Up(nil conn) must fail")
	}
}

// Os CHECKs do schema precisam listar exatamente os valores de enum do domínio;
// este teste detecta divergência quando um lado muda sem o outro.
func TestSchemaEnumsMatchDomain(t *testing.T) {
	m, err := New(migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	var wager, ledger string
	for _, mig := range m.Migrations() {
		switch mig.Name {
		case "create_wager_transactions":
			wager = mig.UpSQL
		case "create_wallet_ledger_entries":
			ledger = mig.UpSQL
		}
	}

	checkList := func(t *testing.T, sql, constraint string, want []string) {
		t.Helper()
		// Captura a lista IN (...) logo após o nome da constraint.
		re := regexp.MustCompile(`(?s)CONSTRAINT ` + constraint + `\b.*?IN \(([^)]*)\)`)
		mm := re.FindStringSubmatch(sql)
		if mm == nil {
			t.Fatalf("constraint %s not found", constraint)
		}
		got := map[string]bool{}
		for _, v := range regexp.MustCompile(`'([A-Z_]+)'`).FindAllStringSubmatch(mm[1], -1) {
			got[v[1]] = true
		}
		if len(got) != len(want) {
			t.Errorf("%s: schema has %d values, domain has %d (%v)", constraint, len(got), len(want), got)
		}
		for _, w := range want {
			if !got[w] {
				t.Errorf("%s: missing %q", constraint, w)
			}
		}
	}

	var codes []string
	for _, c := range domain.AllFailureCodes() {
		codes = append(codes, string(c))
	}
	checkList(t, wager, "wager_transactions_failure_code_valid", codes)
	checkList(t, wager, "wager_transactions_kind_valid", []string{
		string(domain.WagerKindOpening), string(domain.WagerKindBet), string(domain.WagerKindWin),
		string(domain.WagerKindLoss), string(domain.WagerKindRefund), string(domain.WagerKindRollback),
	})
	checkList(t, wager, "wager_transactions_status_valid", []string{
		string(domain.WagerStatusPending), string(domain.WagerStatusPendingReference),
		string(domain.WagerStatusProcessed), string(domain.WagerStatusRejected), string(domain.WagerStatusFailed),
	})
	checkList(t, wager, "wager_transactions_origin_valid", []string{
		string(domain.WagerOriginInternal), string(domain.WagerOriginExternal),
	})
	checkList(t, ledger, "wallet_ledger_entries_direction_valid", []string{
		string(domain.LedgerDirectionDebit), string(domain.LedgerDirectionCredit),
	})

	// Códigos de infraestrutura (só FAILED) listados no CHECK de status.
	var infra []string
	for _, c := range domain.AllFailureCodes() {
		if c.Category() == domain.FailureCategoryInfrastructure {
			infra = append(infra, string(c))
		}
	}
	for _, c := range infra {
		if !strings.Contains(wager, "failure_code IN ('PROCESSING_FAILED', 'RETRIES_EXHAUSTED')") ||
			!strings.Contains(wager, "'"+c+"'") {
			t.Errorf("infrastructure code %s not wired into status x failure_code CHECK", c)
		}
	}
	if len(infra) != 2 {
		t.Errorf("domain has %d infrastructure codes; update wager_transactions_status_failure_code", len(infra))
	}

	// Identificadores: o limite do schema é o do domínio.
	if domain.MaxWagerIdentifierLength != 255 || domain.RequestHashLength != 64 {
		t.Errorf("domain identifier limits changed (%d, %d); update the schema", domain.MaxWagerIdentifierLength, domain.RequestHashLength)
	}
}
