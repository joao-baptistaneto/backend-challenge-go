package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/fx"

	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// fakeUoW registra as chamadas feitas pelo caso de uso. Um "commit" só
// acontece se fn devolver nil; em erro os registros da tentativa são
// descartados (como um rollback).
type fakeUoW struct {
	opts     []app.TxOptions
	calls    []string // ordem das escritas confirmadas
	wallets  []*domain.Wallet
	wagers   []*domain.WagerTransaction
	entries  []domain.WalletLedgerEntry
	events   []domain.IntegrationEvent
	failOn   string // "wallet", "wager", "ledger", "outbox"
	failErr  error
	txErr    error // devolvido por WithinTx sem chamar fn
	attempts int
}

type fakeTx struct {
	u   *fakeUoW
	buf *fakeUoW // escritas da tentativa
}

func (u *fakeUoW) WithinTx(ctx context.Context, opts app.TxOptions, fn app.TxFunc) error {
	u.opts = append(u.opts, opts)
	if u.txErr != nil {
		return u.txErr
	}
	u.attempts++
	buf := &fakeUoW{}
	if err := fn(ctx, fakeTx{u: u, buf: buf}); err != nil {
		return err
	}
	u.calls = append(u.calls, buf.calls...)
	u.wallets = append(u.wallets, buf.wallets...)
	u.wagers = append(u.wagers, buf.wagers...)
	u.entries = append(u.entries, buf.entries...)
	u.events = append(u.events, buf.events...)
	return nil
}

func (t fakeTx) fail(step string) error {
	if t.u.failOn == step {
		return t.u.failErr
	}
	return nil
}

func (t fakeTx) Wallets() app.WalletStore                { return fakeWallets(t) }
func (t fakeTx) Transactions() app.WagerTransactionStore { return fakeWagers(t) }
func (t fakeTx) Ledger() app.LedgerStore                 { return fakeLedger(t) }
func (t fakeTx) Outbox() app.OutboxStore                 { return fakeOutbox(t) }

type (
	fakeWallets fakeTx
	fakeWagers  fakeTx
	fakeLedger  fakeTx
	fakeOutbox  fakeTx
)

func (s fakeWallets) Insert(_ context.Context, w *domain.Wallet) error {
	if err := fakeTx(s).fail("wallet"); err != nil {
		return err
	}
	s.buf.calls = append(s.buf.calls, "wallet")
	s.buf.wallets = append(s.buf.wallets, w)
	return nil
}

func (s fakeWagers) Insert(_ context.Context, w *domain.WagerTransaction) error {
	if err := fakeTx(s).fail("wager"); err != nil {
		return err
	}
	s.buf.calls = append(s.buf.calls, "wager")
	s.buf.wagers = append(s.buf.wagers, w)
	return nil
}

func (s fakeLedger) Append(_ context.Context, e domain.WalletLedgerEntry) error {
	if err := fakeTx(s).fail("ledger"); err != nil {
		return err
	}
	// Contrato: a carteira do lançamento precisa ter sido inserida (ou
	// travada) nesta mesma transação antes do Append.
	locked := false
	for _, w := range s.buf.wallets {
		locked = locked || w.ID() == e.WalletID()
	}
	if !locked {
		return fmt.Errorf("ledger append without wallet %s locked in this tx", e.WalletID())
	}
	s.buf.calls = append(s.buf.calls, "ledger")
	s.buf.entries = append(s.buf.entries, e)
	return nil
}

func (s fakeOutbox) Enqueue(_ context.Context, events ...domain.IntegrationEvent) error {
	if err := fakeTx(s).fail("outbox"); err != nil {
		return err
	}
	s.buf.calls = append(s.buf.calls, "outbox")
	s.buf.events = append(s.buf.events, events...)
	return nil
}

var fixedNow = time.Date(2026, 9, 8, 12, 0, 0, 123456000, time.UTC)

func brl(t *testing.T, amount string) domain.Money {
	t.Helper()
	m, err := domain.ParseNonNegative(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func newUseCase(t *testing.T, uow app.UnitOfWork, opts ...app.Option) *app.OpenWallet {
	t.Helper()
	opts = append([]app.Option{app.WithClock(func() time.Time { return fixedNow })}, opts...)
	uc, err := app.NewOpenWallet(uow, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return uc
}

func command(t *testing.T, amount string) app.OpenWalletCommand {
	return app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: brl(t, amount), CorrelationID: "corr-1"}
}

func TestOpenWallet_PositiveBalance(t *testing.T) {
	u := &fakeUoW{}
	cmd := command(t, "1000.00")
	cmd.CausationID = "cause-1"
	res, err := newUseCase(t, u).Execute(t.Context(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fmt.Sprint(u.calls), "[wallet wager ledger outbox]"; got != want {
		t.Fatalf("calls = %s, want %s (wallet must be inserted before ledger append)", got, want)
	}
	if len(u.opts) != 1 || !u.opts[0].Retry || u.opts[0].Operation != app.OperationOpenWallet {
		t.Fatalf("tx options = %+v", u.opts)
	}
	w := u.wallets[0]
	if res.Wallet.ID != w.ID() || res.Wallet.PlayerID != cmd.PlayerID || res.Wallet.Version != 1 ||
		res.Wallet.Balance != cmd.InitialBalance || !res.Wallet.CreatedAt.Equal(fixedNow) {
		t.Fatalf("result = %+v", res)
	}
	if w.Version() != 1 || w.PersistedVersion() != 0 {
		t.Fatalf("wallet version %d persisted %d", w.Version(), w.PersistedVersion())
	}
	op := u.wagers[0]
	if op.ID() != res.OpeningTransactionID || op.Kind() != domain.WagerKindOpening ||
		op.Status() != domain.WagerStatusProcessed || op.Origin() != domain.WagerOriginInternal ||
		op.WalletID() != w.ID() || op.Money() != cmd.InitialBalance || op.BalanceAfter() != cmd.InitialBalance ||
		op.ProviderID() != "" {
		t.Fatalf("opening = %+v", op)
	}
	e := u.entries[0]
	if e.Direction() != domain.LedgerDirectionCredit || e.TransactionID() != op.ID() || e.WalletID() != w.ID() ||
		!e.BalanceBefore().IsZero() || e.BalanceAfter() != cmd.InitialBalance || e.Amount() != cmd.InitialBalance {
		t.Fatalf("entry = %+v", e)
	}
	if len(u.events) != 2 {
		t.Fatalf("events = %d", len(u.events))
	}
	if u.events[0].Type() != domain.EventTypeWagerTransactionProcessed || u.events[0].AggregateID() != op.ID() {
		t.Fatalf("event 0 = %s %s", u.events[0].Type(), u.events[0].AggregateID())
	}
	if u.events[1].Type() != domain.EventTypeWalletBalanceChanged || u.events[1].AggregateID() != w.ID() {
		t.Fatalf("event 1 = %s %s", u.events[1].Type(), u.events[1].AggregateID())
	}
	for _, ev := range u.events {
		if ev.CorrelationID() != "corr-1" || ev.CausationID() != "cause-1" {
			t.Fatalf("metadata = %q %q", ev.CorrelationID(), ev.CausationID())
		}
	}
	var changed struct {
		Data struct {
			WalletVersion int64  `json:"walletVersion"`
			Direction     string `json:"direction"`
		} `json:"data"`
	}
	if err := json.Unmarshal(u.events[1].Payload(), &changed); err != nil {
		t.Fatal(err)
	}
	if changed.Data.WalletVersion != 1 || changed.Data.Direction != "CREDIT" {
		t.Fatalf("balance changed = %+v", changed.Data)
	}
}

func TestOpenWallet_ZeroBalanceOnlyWallet(t *testing.T) {
	u := &fakeUoW{}
	res, err := newUseCase(t, u).Execute(t.Context(), command(t, "0.00"))
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(u.calls); got != "[wallet]" {
		t.Fatalf("calls = %s, want only wallet", got)
	}
	if res.OpeningTransactionID != uuid.Nil || res.Wallet.Version != 1 || !res.Wallet.Balance.IsZero() {
		t.Fatalf("result = %+v", res)
	}
}

func TestOpenWallet_Conflict(t *testing.T) {
	u := &fakeUoW{failOn: "wallet", failErr: fmt.Errorf("adapter: %w", app.ErrWalletAlreadyExists)}
	_, err := newUseCase(t, u).Execute(t.Context(), command(t, "10.00"))
	if !errors.Is(err, app.ErrConflict) || !errors.Is(err, app.ErrWalletAlreadyExists) {
		t.Fatalf("err = %v, want conflict", err)
	}
	ae, ok := app.AsError(err)
	if !ok || ae.Kind != app.KindConflict || ae.Code != app.CodeWalletAlreadyExists {
		t.Fatalf("app error = %+v", ae)
	}
	if errors.Is(err, app.ErrInvalidInput) || errors.Is(err, app.ErrUnavailable) {
		t.Fatal("conflict matched another kind")
	}
	if len(u.calls) != 0 {
		t.Fatalf("writes committed: %v", u.calls)
	}
}

func TestOpenWallet_StepFailureCommitsNothing(t *testing.T) {
	boom := errors.New("boom")
	for _, step := range []string{"wallet", "wager", "ledger", "outbox"} {
		t.Run(step, func(t *testing.T) {
			u := &fakeUoW{failOn: step, failErr: boom}
			_, err := newUseCase(t, u).Execute(t.Context(), command(t, "10.00"))
			if !errors.Is(err, boom) {
				t.Fatalf("err = %v", err)
			}
			if _, ok := app.AsError(err); ok {
				t.Fatalf("unclassified failure became app error: %v", err)
			}
			if len(u.calls) != 0 {
				t.Fatalf("writes committed: %v", u.calls)
			}
		})
	}
}

func TestOpenWallet_Unavailable(t *testing.T) {
	u := &fakeUoW{txErr: fmt.Errorf("%w: connection reset", app.ErrUnavailable)}
	_, err := newUseCase(t, u).Execute(t.Context(), command(t, "10.00"))
	ae, ok := app.AsError(err)
	if !ok || ae.Kind != app.KindUnavailable || !errors.Is(err, app.ErrUnavailable) {
		t.Fatalf("err = %v", err)
	}
}

func TestOpenWallet_InvalidInput(t *testing.T) {
	negative, err := domain.FromMinorUnits(-1, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		cmd  app.OpenWalletCommand
		code string
	}{
		{"nil player", app.OpenWalletCommand{InitialBalance: brl(t, "1.00"), CorrelationID: "c"}, app.CodeInvalidPlayerID},
		{"uninitialized money", app.OpenWalletCommand{PlayerID: uuid.New(), CorrelationID: "c"}, app.CodeInvalidInitialBalance},
		{"negative", app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: negative, CorrelationID: "c"}, app.CodeInvalidInitialBalance},
		{"no correlation", app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: brl(t, "1.00")}, app.CodeInvalidCorrelationID},
		{"bad correlation", app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: brl(t, "1.00"), CorrelationID: " x"}, app.CodeInvalidCorrelationID},
		// Pendência da T14 resolvida na T15: o formato é validado mesmo sem
		// eventos (saldo zero), e causationId tem código próprio.
		{"bad correlation zero balance", app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: brl(t, "0.00"), CorrelationID: "c\n"}, app.CodeInvalidCorrelationID},
		{"long correlation zero balance", app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: brl(t, "0.00"), CorrelationID: strings.Repeat("a", 256)}, app.CodeInvalidCorrelationID},
		{"bad causation", app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: brl(t, "1.00"), CorrelationID: "c", CausationID: "m "}, app.CodeInvalidCausationID},
		{"bad causation zero balance", app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: brl(t, "0.00"), CorrelationID: "c", CausationID: "\x00"}, app.CodeInvalidCausationID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := &fakeUoW{}
			_, err := newUseCase(t, u).Execute(t.Context(), tc.cmd)
			ae, ok := app.AsError(err)
			if !ok || ae.Kind != app.KindInvalidInput || ae.Code != tc.code || !errors.Is(err, app.ErrInvalidInput) {
				t.Fatalf("err = %v", err)
			}
			if len(u.opts) != 0 {
				t.Fatal("transaction opened for invalid input")
			}
		})
	}
}

func TestOpenWallet_CanceledContextOpensNoTx(t *testing.T) {
	u := &fakeUoW{}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := newUseCase(t, u).Execute(ctx, command(t, "1.00")); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if len(u.opts) != 0 {
		t.Fatal("transaction opened with canceled context")
	}
}

func TestOpenWallet_IDGeneratorFailure(t *testing.T) {
	boom := errors.New("entropy")
	u := &fakeUoW{}
	uc := newUseCase(t, u, app.WithIDGenerator(func() (uuid.UUID, error) { return uuid.Nil, boom }))
	if _, err := uc.Execute(t.Context(), command(t, "1.00")); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if len(u.opts) != 0 {
		t.Fatal("transaction opened")
	}
}

// O corpo da transação é reexecutável: uma tentativa que falha seguida de
// outra que confirma grava exatamente um conjunto.
func TestOpenWallet_TxBodyIsReexecutable(t *testing.T) {
	u := &retryUoW{fakeUoW: &fakeUoW{}}
	if _, err := newUseCase(t, u).Execute(t.Context(), command(t, "5.00")); err != nil {
		t.Fatal(err)
	}
	if u.attempts != 2 || fmt.Sprint(u.calls) != "[wallet wager ledger outbox]" {
		t.Fatalf("attempts %d calls %v", u.attempts, u.calls)
	}
}

// retryUoW simula o retry do adapter: a 1ª tentativa executa fn inteira e
// "falha no commit"; a 2ª confirma.
type retryUoW struct{ *fakeUoW }

func (r *retryUoW) WithinTx(ctx context.Context, opts app.TxOptions, fn app.TxFunc) error {
	if !opts.Retry {
		return errors.New("retry not enabled")
	}
	r.attempts++
	if err := fn(ctx, fakeTx{u: r.fakeUoW, buf: &fakeUoW{}}); err != nil {
		return err
	}
	return r.fakeUoW.WithinTx(ctx, opts, fn)
}

func TestNewOpenWallet_NilUoW(t *testing.T) {
	if _, err := app.NewOpenWallet(nil); err == nil {
		t.Fatal("want error")
	}
}

func TestErrorKinds(t *testing.T) {
	e := &app.Error{Kind: app.KindNotFound, Code: "X", Op: "op"}
	if !errors.Is(e, app.ErrNotFound) || errors.Is(e, app.ErrConflict) {
		t.Fatal("kind matching")
	}
	if !errors.Is(app.ErrWalletAlreadyExists, app.ErrConflict) {
		t.Fatal("ErrWalletAlreadyExists must be a conflict")
	}
	if (&app.Error{Kind: "BOGUS"}).Is(nil) {
		t.Fatal("unknown kind matched nil")
	}
}

func TestModule_Validate(t *testing.T) {
	err := fx.ValidateApp(
		fx.Supply(fx.Annotate(&fakeUoW{}, fx.As(new(app.UnitOfWork)))),
		app.Module,
		fx.Invoke(func(*app.OpenWallet) {}),
	)
	if err != nil {
		t.Fatal(err)
	}
}

// Métodos das portas que OpenWallet não usa (acrescentados na T15): o fake
// falha alto se forem chamados.
var errUnexpectedCall = errors.New("fake: unexpected call")

func (t fakeTx) Inbox() app.InboxStore { return fakeInbox(t) }

type fakeInbox fakeTx

func (fakeInbox) Register(context.Context, string, string, string, time.Time) (app.InboxRecord, error) {
	return app.InboxRecord{}, errUnexpectedCall
}

func (fakeInbox) Complete(context.Context, string, string, string, uuid.UUID, time.Time) error {
	return errUnexpectedCall
}

func (fakeWallets) GetForUpdate(context.Context, uuid.UUID) (*domain.Wallet, error) {
	return nil, errUnexpectedCall
}

func (fakeWallets) Update(context.Context, *domain.Wallet) error { return errUnexpectedCall }

func (fakeWagers) Update(context.Context, *domain.WagerTransaction) error { return errUnexpectedCall }

func (fakeWagers) GetByID(context.Context, uuid.UUID) (*domain.WagerTransaction, error) {
	return nil, errUnexpectedCall
}

func (fakeWagers) GetByProviderIdempotencyKey(context.Context, string, string) (*domain.WagerTransaction, error) {
	return nil, errUnexpectedCall
}

func (fakeWagers) GetByProviderExternalID(context.Context, string, string) (*domain.WagerTransaction, error) {
	return nil, errUnexpectedCall
}

func (fakeWagers) ListByProviderReference(context.Context, string, string) ([]*domain.WagerTransaction, error) {
	return nil, errUnexpectedCall
}

// Leituras acrescentadas na T16 (não usadas por OpenWallet).

func (fakeWallets) Get(context.Context, uuid.UUID) (*domain.Wallet, error) {
	return nil, errUnexpectedCall
}

func (fakeLedger) List(context.Context, uuid.UUID, string, int) (app.LedgerPage, error) {
	return app.LedgerPage{}, errUnexpectedCall
}

func (fakeLedger) Summarize(context.Context, uuid.UUID) (app.LedgerSummary, error) {
	return app.LedgerSummary{}, errUnexpectedCall
}
