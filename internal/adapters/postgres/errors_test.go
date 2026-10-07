package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func pgErr(code, constraint string) *pgconn.PgError {
	return &pgconn.PgError{
		Severity:       "ERROR",
		Code:           code,
		Message:        "synthetic " + code,
		Detail:         "Key (player_id)=(secret-value) already exists.",
		ConstraintName: constraint,
		TableName:      "wallets",
	}
}

type fakeNetErr struct{ timeout bool }

func (e fakeNetErr) Error() string   { return "fake net error" }
func (e fakeNetErr) Timeout() bool   { return e.timeout }
func (e fakeNetErr) Temporary() bool { return false }

var _ net.Error = fakeNetErr{}

// safeErr imita os erros do pgconn que garantem que nada foi enviado.
type safeErr struct{}

func (safeErr) Error() string     { return "nothing sent" }
func (safeErr) SafeToRetry() bool { return true }

func TestTransientClassification(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		err       error
		transient bool
		retryable bool
		reason    TransientReason
	}{
		{"nil", nil, false, false, ""},
		{"serialization", pgErr(CodeSerializationFailure, ""), true, true, ReasonSerialization},
		{"deadlock wrapped", fmt.Errorf("repo: %w", pgErr(CodeDeadlockDetected, "")), true, true, ReasonDeadlock},
		{"lock timeout", pgErr(CodeLockNotAvailable, ""), true, true, ReasonLockTimeout},
		{"statement timeout", pgErr(CodeQueryCanceled, ""), true, true, ReasonQueryCanceled},
		{"admin shutdown", pgErr(CodeAdminShutdown, ""), true, true, ReasonConnection},
		{"crash shutdown", pgErr(CodeCrashShutdown, ""), true, true, ReasonConnection},
		{"cannot connect now", pgErr(CodeCannotConnectNow, ""), true, true, ReasonConnection},
		{"too many connections", pgErr(CodeTooManyConnections, ""), true, true, ReasonConnection},
		{"class 08", pgErr("08006", ""), true, true, ReasonConnection},
		{"insufficient resources", pgErr(CodeInsufficientResources, ""), true, true, ReasonResources},
		{"disk full", pgErr(CodeDiskFull, ""), true, true, ReasonResources},
		{"out of memory", pgErr(CodeOutOfMemory, ""), true, true, ReasonResources},
		{"read only (failover)", pgErr(CodeReadOnlyTransaction, ""), true, true, ReasonReadOnly},
		{"configuration limit is permanent", pgErr("53400", ""), false, false, ""},
		{"unique is permanent", pgErr(CodeUniqueViolation, "x"), false, false, ""},
		{"check is permanent", pgErr(CodeCheckViolation, "x"), false, false, ""},
		{"restrict is permanent", pgErr(CodeRestrictViolation, "x"), false, false, ""},
		{"auth failure is permanent", pgErr("28P01", ""), false, false, ""},
		{"undefined database is permanent", pgErr("3D000", ""), false, false, ""},
		{"syntax error is permanent", pgErr("42601", ""), false, false, ""},
		{"deadline exceeded", context.DeadlineExceeded, true, true, ReasonTimeout},
		{"deadline wrapped", fmt.Errorf("query: %w", context.DeadlineExceeded), true, true, ReasonTimeout},
		{"canceled", context.Canceled, false, false, ""},
		{"canceled wrapping safe-to-retry", errors.Join(safeErr{}, context.Canceled), false, false, ""},
		{"net timeout", fakeNetErr{timeout: true}, true, true, ReasonTimeout},
		{"net error", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, true, true, ReasonConnection},
		{"unexpected eof", fmt.Errorf("read: %w", io.ErrUnexpectedEOF), true, true, ReasonConnection},
		{"net closed", net.ErrClosed, true, true, ReasonConnection},
		{"safe to retry", safeErr{}, true, true, ReasonConnection},
		{"commit unknown", fmt.Errorf("%w: %w", ErrCommitOutcomeUnknown, io.EOF), true, false, ReasonCommitUnknown},
		{"commit unknown without cause", ErrCommitOutcomeUnknown, true, false, ReasonCommitUnknown},
		{"commit rollback", pgx.ErrTxCommitRollback, false, false, ""},
		{"no rows", pgx.ErrNoRows, false, false, ""},
		{"plain", errors.New("boom"), false, false, ""},
		{"translated transient", &TransientError{Reason: ReasonDeadlock, Err: errors.New("x")}, true, true, ReasonDeadlock},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsTransient(tc.err); got != tc.transient {
				t.Errorf("IsTransient = %v, want %v", got, tc.transient)
			}
			if got := IsRetryable(tc.err); got != tc.retryable {
				t.Errorf("IsRetryable = %v, want %v", got, tc.retryable)
			}
			reason, _ := TransientReasonOf(tc.err)
			if reason != tc.reason {
				t.Errorf("reason = %q, want %q", reason, tc.reason)
			}
		})
	}
}

func TestConstraintClassification(t *testing.T) {
	t.Parallel()
	cases := []struct {
		code     string
		kind     ConstraintKind
		sentinel error
		is       func(error, string) bool
	}{
		{CodeUniqueViolation, ConstraintUnique, ErrUniqueViolation, IsUniqueViolation},
		{CodeCheckViolation, ConstraintCheck, ErrCheckViolation, IsCheckViolation},
		{CodeForeignKeyViolation, ConstraintForeignKey, ErrForeignKeyViolation, IsForeignKeyViolation},
		{CodeRestrictViolation, ConstraintRestrict, ErrRestrictViolation, IsRestrictViolation},
		{CodeNotNullViolation, ConstraintNotNull, ErrNotNullViolation, nil},
		{CodeExclusionViolation, ConstraintExclusion, ErrExclusionViolation, nil},
	}
	all := []error{ErrUniqueViolation, ErrCheckViolation, ErrForeignKeyViolation, ErrRestrictViolation, ErrNotNullViolation, ErrExclusionViolation}
	for _, tc := range cases {
		t.Run(string(tc.kind), func(t *testing.T) {
			t.Parallel()
			raw := fmt.Errorf("repo insert: %w", pgErr(tc.code, "the_constraint"))

			cv, ok := AsConstraintViolation(raw)
			if !ok {
				t.Fatal("AsConstraintViolation = false")
			}
			if cv.Kind != tc.kind || cv.Constraint != "the_constraint" || cv.Table != "wallets" || cv.SQLState != tc.code {
				t.Fatalf("violation = %+v", cv)
			}
			if tc.is != nil {
				if !tc.is(raw, "the_constraint") || !tc.is(raw, "") {
					t.Error("Is* by name / any = false")
				}
				if tc.is(raw, "other_constraint") {
					t.Error("Is* matched wrong constraint name")
				}
			}

			tr := Translate(raw)
			if !errors.Is(tr, tc.sentinel) || !errors.Is(tr, ErrConstraintViolation) {
				t.Errorf("translated %v does not match sentinels", tr)
			}
			for _, other := range all {
				if other != tc.sentinel && errors.Is(tr, other) {
					t.Errorf("translated matches unrelated sentinel %v", other)
				}
			}
			var pe *pgconn.PgError
			if !errors.As(tr, &pe) || pe.Code != tc.code {
				t.Error("translated error lost *pgconn.PgError")
			}
			var got *ConstraintViolation
			if !errors.As(tr, &got) || got.Constraint != "the_constraint" {
				t.Error("errors.As(*ConstraintViolation) failed")
			}
			if !strings.Contains(tr.Error(), "repo insert") {
				t.Errorf("translated message lost wrapping context: %q", tr.Error())
			}
			if strings.Contains(cv.Error(), "secret-value") {
				t.Errorf("violation message leaked Detail: %q", cv.Error())
			}
			if Translate(tr) != tr {
				t.Error("Translate is not idempotent")
			}
			if IsTransient(tr) {
				t.Error("constraint violation classified as transient")
			}
		})
	}
}

func TestIsViolationCrossKind(t *testing.T) {
	t.Parallel()
	u := pgErr(CodeUniqueViolation, "wager_transactions_provider_idempotency_key")
	if !IsUniqueViolation(u, "wager_transactions_provider_idempotency_key") {
		t.Fatal("unique by name")
	}
	if IsCheckViolation(u, "") || IsRestrictViolation(u, "") || IsForeignKeyViolation(u, "") {
		t.Fatal("unique matched another kind")
	}
	for _, err := range []error{nil, errors.New("x"), pgErr(CodeSerializationFailure, ""), context.Canceled} {
		if _, ok := AsConstraintViolation(err); ok {
			t.Errorf("AsConstraintViolation(%v) = true", err)
		}
		if IsUniqueViolation(err, "") {
			t.Errorf("IsUniqueViolation(%v) = true", err)
		}
	}
}

func TestTranslate(t *testing.T) {
	t.Parallel()
	if Translate(nil) != nil {
		t.Fatal("Translate(nil) != nil")
	}
	plain := errors.New("plain")
	if Translate(plain) != plain {
		t.Fatal("plain error changed")
	}

	direct := pgErr(CodeUniqueViolation, "c")
	if _, ok := Translate(direct).(*ConstraintViolation); !ok {
		t.Fatalf("direct PgError should become *ConstraintViolation, got %T", Translate(direct))
	}

	ser := fmt.Errorf("x: %w", pgErr(CodeSerializationFailure, ""))
	tr := Translate(ser)
	var te *TransientError
	if !errors.As(tr, &te) || te.Reason != ReasonSerialization {
		t.Fatalf("serialization not translated: %v", tr)
	}
	if !errors.Is(tr, ErrTransient) || SQLState(tr) != CodeSerializationFailure {
		t.Fatal("translated transient lost sentinel or SQLSTATE")
	}
	if Translate(tr) != tr {
		t.Fatal("Translate not idempotent for transient")
	}

	if got := Translate(context.Canceled); got != context.Canceled {
		t.Fatalf("canceled translated to %v", got)
	}
	if SQLState(plain) != "" {
		t.Fatal("SQLState of non-pg error")
	}
}
