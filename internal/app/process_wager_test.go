package app_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/fx"

	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// memUoW é uma UnitOfWork em memória para os testes unitários de
// ProcessWagerTransaction: cada transação trabalha numa cópia do estado e só
// a publica se fn devolver nil (rollback em erro). As transações são
// serializadas por um mutex (equivale a um lock global, suficiente para
// testar a lógica; a concorrência real é testada na integração). Respeita
// Retry/RetryIf como o adapter.
type memUoW struct {
	mu       sync.Mutex
	state    memState
	opts     []app.TxOptions
	attempts int
	// failInsertOnce devolve este erro no primeiro Insert de transação.
	failInsertOnce error
	txErr          error
}

type memInbox struct {
	hash      string
	txID      uuid.UUID
	completed bool
}

type memState struct {
	wallets map[uuid.UUID]*domain.Wallet
	wagers  []*domain.WagerTransaction
	ledger  []domain.WalletLedgerEntry
	events  []domain.IntegrationEvent
	inbox   map[string]memInbox
}

func cloneWallet(w *domain.Wallet) *domain.Wallet {
	c, err := domain.RehydrateWallet(domain.RehydrateWalletParams{
		ID: w.ID(), PlayerID: w.PlayerID(), Balance: w.Balance(), Version: w.Version(),
		CreatedAt: w.CreatedAt(), UpdatedAt: w.UpdatedAt(),
	})
	if err != nil {
		panic(err)
	}
	return c
}

func cloneWager(t *domain.WagerTransaction) *domain.WagerTransaction {
	c := *t
	return &c
}

func (s memState) clone() memState {
	c := memState{wallets: map[uuid.UUID]*domain.Wallet{}, inbox: map[string]memInbox{}}
	for k, v := range s.wallets {
		c.wallets[k] = cloneWallet(v)
	}
	for k, v := range s.inbox {
		c.inbox[k] = v
	}
	for _, w := range s.wagers {
		c.wagers = append(c.wagers, cloneWager(w))
	}
	c.ledger = append(c.ledger, s.ledger...)
	c.events = append(c.events, s.events...)
	return c
}

func newMemUoW() *memUoW {
	return &memUoW{state: memState{wallets: map[uuid.UUID]*domain.Wallet{}, inbox: map[string]memInbox{}}}
}

func (u *memUoW) WithinTx(ctx context.Context, opts app.TxOptions, fn app.TxFunc) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.opts = append(u.opts, opts)
	if u.txErr != nil {
		return u.txErr
	}
	max := 1
	if opts.Retry {
		max = 4
	}
	for attempt := 1; ; attempt++ {
		u.attempts++
		work := u.state.clone()
		err := fn(ctx, &memTx{u: u, s: &work})
		if err == nil {
			u.state = work
			return nil
		}
		if attempt >= max || opts.RetryIf == nil || !opts.RetryIf(err) {
			return err
		}
	}
}

type memTx struct {
	u *memUoW
	s *memState
}

func (t *memTx) Wallets() app.WalletStore                { return memWallets{t} }
func (t *memTx) Transactions() app.WagerTransactionStore { return memWagers{t} }
func (t *memTx) Ledger() app.LedgerStore                 { return memLedger{t} }
func (t *memTx) Outbox() app.OutboxStore                 { return memOutbox{t} }
func (t *memTx) Inbox() app.InboxStore                   { return memInboxStore{t} }

type (
	memWallets    struct{ *memTx }
	memWagers     struct{ *memTx }
	memLedger     struct{ *memTx }
	memOutbox     struct{ *memTx }
	memInboxStore struct{ *memTx }
)

func (s memWallets) Insert(_ context.Context, w *domain.Wallet) error {
	s.s.wallets[w.ID()] = cloneWallet(w)
	return nil
}

func (s memWallets) GetForUpdate(_ context.Context, id uuid.UUID) (*domain.Wallet, error) {
	w, ok := s.s.wallets[id]
	if !ok {
		return nil, app.ErrRecordNotFound
	}
	return cloneWallet(w), nil
}

func (s memWallets) Update(_ context.Context, w *domain.Wallet) error {
	cur := s.s.wallets[w.ID()]
	if cur == nil || cur.Version() != w.PersistedVersion() {
		return errors.New("mem: stale wallet")
	}
	if w.Version() == w.PersistedVersion() {
		return nil
	}
	s.s.wallets[w.ID()] = cloneWallet(w)
	return nil
}

func (s memWagers) Insert(_ context.Context, t *domain.WagerTransaction) error {
	if err := s.u.failInsertOnce; err != nil {
		s.u.failInsertOnce = nil
		return err
	}
	for _, w := range s.s.wagers {
		if w.ProviderID() == t.ProviderID() &&
			(w.ExternalTransactionID() == t.ExternalTransactionID() || w.IdempotencyKey() == t.IdempotencyKey()) {
			return fmt.Errorf("mem: %w", app.ErrDuplicateWagerTransaction)
		}
	}
	s.s.wagers = append(s.s.wagers, cloneWager(t))
	return nil
}

func (s memWagers) Update(context.Context, *domain.WagerTransaction) error {
	return errors.New("mem: update not used by ProcessWagerTransaction")
}

func (s memWagers) find(match func(*domain.WagerTransaction) bool) (*domain.WagerTransaction, error) {
	for _, w := range s.s.wagers {
		if match(w) {
			return cloneWager(w), nil
		}
	}
	return nil, app.ErrRecordNotFound
}

func (s memWagers) GetByID(_ context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	return s.find(func(w *domain.WagerTransaction) bool { return w.ID() == id })
}

func (s memWagers) GetByProviderIdempotencyKey(_ context.Context, p, k string) (*domain.WagerTransaction, error) {
	return s.find(func(w *domain.WagerTransaction) bool { return w.ProviderID() == p && w.IdempotencyKey() == k })
}

func (s memWagers) GetByProviderExternalID(_ context.Context, p, e string) (*domain.WagerTransaction, error) {
	return s.find(func(w *domain.WagerTransaction) bool { return w.ProviderID() == p && w.ExternalTransactionID() == e })
}

func (s memWagers) ListByProviderReference(_ context.Context, p, ref string) ([]*domain.WagerTransaction, error) {
	var out []*domain.WagerTransaction
	for _, w := range s.s.wagers {
		if w.ProviderID() == p && w.ReferenceExternalTransactionID() == ref {
			out = append(out, cloneWager(w))
		}
	}
	return out, nil
}

func (s memLedger) Append(_ context.Context, e domain.WalletLedgerEntry) error {
	s.s.ledger = append(s.s.ledger, e)
	return nil
}

func (s memOutbox) Enqueue(_ context.Context, events ...domain.IntegrationEvent) error {
	s.s.events = append(s.s.events, events...)
	return nil
}

func (s memInboxStore) Register(_ context.Context, c, m, h string, _ time.Time) (app.InboxRecord, error) {
	key := c + "/" + m
	cur, ok := s.s.inbox[key]
	if !ok {
		s.s.inbox[key] = memInbox{hash: h}
		return app.InboxRecord{Status: app.InboxNew}, nil
	}
	if cur.hash != h {
		return app.InboxRecord{}, fmt.Errorf("mem: %w", app.ErrInboxHashMismatch)
	}
	if cur.completed {
		return app.InboxRecord{Status: app.InboxCompleted, TransactionID: cur.txID}, nil
	}
	return app.InboxRecord{Status: app.InboxInProgress}, nil
}

func (s memInboxStore) Complete(_ context.Context, c, m, h string, txID uuid.UUID, _ time.Time) error {
	key := c + "/" + m
	cur, ok := s.s.inbox[key]
	if !ok || cur.hash != h || cur.completed {
		return errors.New("mem: bad inbox complete")
	}
	s.s.inbox[key] = memInbox{hash: h, txID: txID, completed: true}
	return nil
}

// --- helpers ---------------------------------------------------------------

var (
	unitPlayer = uuid.MustParse("0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1")
	unitWallet = uuid.MustParse("0192f291-27dd-7d3f-8071-5f8685deef37")
)

func seedWallet(t *testing.T, u *memUoW, balance string) {
	t.Helper()
	w, err := domain.RehydrateWallet(domain.RehydrateWalletParams{
		ID: unitWallet, PlayerID: unitPlayer, Balance: brl(t, balance), Version: 1,
		CreatedAt: fixedNow.Add(-time.Hour), UpdatedAt: fixedNow.Add(-time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	u.state.wallets[unitWallet] = w
}

func newProcess(t *testing.T, u app.UnitOfWork, opts ...app.Option) *app.ProcessWagerTransaction {
	t.Helper()
	opts = append([]app.Option{app.WithClock(func() time.Time { return fixedNow })}, opts...)
	uc, err := app.NewProcessWagerTransaction(u, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return uc
}

func wagerCmd(t *testing.T, kind domain.WagerKind, ext, amount, ref string) app.ProcessWagerCommand {
	t.Helper()
	return app.ProcessWagerCommand{
		ProviderID: "provider-a", ExternalTransactionID: ext, IdempotencyKey: "provider-a:" + ext,
		PlayerID: unitPlayer, WalletID: unitWallet, RoundID: "round-1", GameID: "game-1",
		Kind: kind, Money: brl(t, amount), ReferenceExternalTransactionID: ref,
		CorrelationID: "corr-" + ext,
	}
}

func eventTypes(events []domain.IntegrationEvent) string {
	var s []string
	for _, e := range events {
		s = append(s, e.Type().String())
	}
	return strings.Join(s, ",")
}

func wantAppError(t *testing.T, err error, kind app.ErrorKind, code string) {
	t.Helper()
	ae, ok := app.AsError(err)
	if !ok || ae.Kind != kind || ae.Code != code {
		t.Fatalf("err = %v, want %s/%s", err, kind, code)
	}
}

// --- testes ----------------------------------------------------------------

func TestProcessWager_BetProcessed(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	res, err := newProcess(t, u).Execute(t.Context(), wagerCmd(t, domain.WagerKindBet, "b1", "25.00", ""))
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != domain.WagerStatusProcessed || res.Balance.Amount() != "75.00" || res.IdempotentReplay {
		t.Fatalf("result = %+v", res)
	}
	if got := eventTypes(u.state.events); got != "WagerTransactionProcessed,WalletBalanceChanged" {
		t.Fatalf("events = %s", got)
	}
	if len(u.state.ledger) != 1 || u.state.wallets[unitWallet].Version() != 2 {
		t.Fatalf("ledger %d version %d", len(u.state.ledger), u.state.wallets[unitWallet].Version())
	}
	o := u.opts[0]
	if o.Operation != app.OperationProcessTransaction || !o.Retry || o.RetryIf == nil ||
		!o.RetryIf(fmt.Errorf("x: %w", app.ErrDuplicateWagerTransaction)) || o.RetryIf(errors.New("other")) {
		t.Fatalf("tx options = %+v", o)
	}
	for _, e := range u.state.events {
		if e.CorrelationID() != "corr-b1" {
			t.Fatalf("correlation = %q", e.CorrelationID())
		}
	}
}

func TestProcessWager_ReplayReturnsOriginalBalance(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	uc := newProcess(t, u)
	cmd := wagerCmd(t, domain.WagerKindBet, "b1", "25.00", "")
	first, err := uc.Execute(t.Context(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := uc.Execute(t.Context(), wagerCmd(t, domain.WagerKindWin, "w1", "10.00", "")); err != nil {
		t.Fatal(err)
	}
	ledger, events := len(u.state.ledger), len(u.state.events)
	again, err := uc.Execute(t.Context(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !again.IdempotentReplay || again.TransactionID != first.TransactionID || again.Balance.Amount() != "75.00" {
		t.Fatalf("replay = %+v", again)
	}
	if len(u.state.ledger) != ledger || len(u.state.events) != events || u.state.wallets[unitWallet].Balance().Amount() != "85.00" {
		t.Fatal("replay had side effects")
	}
}

func TestProcessWager_Conflicts(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	uc := newProcess(t, u)
	if _, err := uc.Execute(t.Context(), wagerCmd(t, domain.WagerKindBet, "b1", "25.00", "")); err != nil {
		t.Fatal(err)
	}
	before := u.state.clone()

	// Mesma key, outro conteúdo.
	other := wagerCmd(t, domain.WagerKindBet, "b1", "30.00", "")
	_, err := uc.Execute(t.Context(), other)
	wantAppError(t, err, app.KindConflict, app.CodeIdempotencyKeyConflict)
	if !errors.Is(err, app.ErrConflict) {
		t.Fatal("not ErrConflict")
	}

	// Mesma operação (provider, externalId), outra key.
	otherKey := wagerCmd(t, domain.WagerKindBet, "b1", "25.00", "")
	otherKey.IdempotencyKey = "another-key"
	_, err = uc.Execute(t.Context(), otherKey)
	wantAppError(t, err, app.KindConflict, app.CodeExternalTransactionConflict)

	if len(u.state.wagers) != len(before.wagers) || len(u.state.events) != len(before.events) ||
		u.state.wallets[unitWallet].Balance() != before.wallets[unitWallet].Balance() {
		t.Fatal("conflict had side effects")
	}
}

func TestProcessWager_InvalidInputOpensNoTx(t *testing.T) {
	long := strings.Repeat("m", 256)
	cases := map[string]struct {
		mutate func(*app.ProcessWagerCommand)
		code   string
	}{
		"opening":            {func(c *app.ProcessWagerCommand) { c.Kind = domain.WagerKindOpening }, string(domain.FailureCodeInvalidKind)},
		"unknown kind":       {func(c *app.ProcessWagerCommand) { c.Kind = "bet" }, string(domain.FailureCodeInvalidKind)},
		"uninitialized":      {func(c *app.ProcessWagerCommand) { c.Money = domain.Money{} }, string(domain.FailureCodeInvalidAmount)},
		"zero bet":           {func(c *app.ProcessWagerCommand) { c.Money = brl(t, "0.00") }, string(domain.FailureCodeInvalidAmount)},
		"loss non zero":      {func(c *app.ProcessWagerCommand) { c.Kind = domain.WagerKindLoss }, string(domain.FailureCodeInvalidAmount)},
		"refund without ref": {func(c *app.ProcessWagerCommand) { c.Kind = domain.WagerKindRefund }, string(domain.FailureCodeReferenceRequired)},
		"bet with ref":       {func(c *app.ProcessWagerCommand) { c.ReferenceExternalTransactionID = "x" }, string(domain.FailureCodeReferenceNotAllowed)},
		"empty key":          {func(c *app.ProcessWagerCommand) { c.IdempotencyKey = "" }, string(domain.FailureCodeInvalidRequest)},
		"invalid utf8":       {func(c *app.ProcessWagerCommand) { c.RoundID = "\xff" }, string(domain.FailureCodeInvalidRequest)},
		"no correlation":     {func(c *app.ProcessWagerCommand) { c.CorrelationID = "" }, app.CodeInvalidCorrelationID},
		"bad correlation":    {func(c *app.ProcessWagerCommand) { c.CorrelationID = " c" }, app.CodeInvalidCorrelationID},
		"bad causation":      {func(c *app.ProcessWagerCommand) { c.CausationID = "m\n" }, app.CodeInvalidCausationID},
		"empty message id": {func(c *app.ProcessWagerCommand) {
			c.Inbox = &app.InboxMessageRef{ConsumerName: "sqs"}
		}, app.CodeInvalidMessage},
		"long message id": {func(c *app.ProcessWagerCommand) {
			c.Inbox = &app.InboxMessageRef{ConsumerName: "sqs", MessageID: long}
		}, app.CodeInvalidMessage},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			u := newMemUoW()
			cmd := wagerCmd(t, domain.WagerKindBet, "b1", "25.00", "")
			tc.mutate(&cmd)
			_, err := newProcess(t, u).Execute(t.Context(), cmd)
			wantAppError(t, err, app.KindInvalidInput, tc.code)
			if len(u.opts) != 0 {
				t.Fatal("transaction opened for invalid input")
			}
		})
	}
}

func TestProcessWager_RetriesDuplicateRaceAndReplays(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	uc := newProcess(t, u)
	cmd := wagerCmd(t, domain.WagerKindBet, "b1", "25.00", "")
	first, err := uc.Execute(t.Context(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	// Simula a corrida: a primeira tentativa não vê a linha vencedora e o
	// UNIQUE a barra; a nova tentativa a encontra e devolve o replay.
	u.failInsertOnce = fmt.Errorf("unique: %w", app.ErrDuplicateWagerTransaction)
	cmd2 := wagerCmd(t, domain.WagerKindBet, "b2", "5.00", "")
	u.attempts = 0
	res, err := uc.Execute(t.Context(), cmd2)
	if err != nil {
		t.Fatal(err)
	}
	if u.attempts != 2 || res.Status != domain.WagerStatusProcessed || res.Balance.Amount() != "70.00" {
		t.Fatalf("attempts %d result %+v", u.attempts, res)
	}
	if again, err := uc.Execute(t.Context(), cmd); err != nil || !again.IdempotentReplay || again.TransactionID != first.TransactionID {
		t.Fatalf("replay %+v %v", again, err)
	}
}

func TestProcessWager_UnavailableAndUnclassified(t *testing.T) {
	u := newMemUoW()
	u.txErr = fmt.Errorf("%w: begin", app.ErrUnavailable)
	_, err := newProcess(t, u).Execute(t.Context(), wagerCmd(t, domain.WagerKindBet, "b1", "1.00", ""))
	wantAppError(t, err, app.KindUnavailable, app.CodeTemporarilyUnavailable)

	u.txErr = fmt.Errorf("unique after retries: %w", app.ErrDuplicateWagerTransaction)
	_, err = newProcess(t, u).Execute(t.Context(), wagerCmd(t, domain.WagerKindBet, "b1", "1.00", ""))
	wantAppError(t, err, app.KindUnavailable, app.CodeTemporarilyUnavailable)

	boom := errors.New("permanent")
	u.txErr = boom
	_, err = newProcess(t, u).Execute(t.Context(), wagerCmd(t, domain.WagerKindBet, "b1", "1.00", ""))
	if _, ok := app.AsError(err); ok || !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

func TestProcessWager_ReversalBeforeReferenceIsPending(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	res, err := newProcess(t, u, app.WithReferenceRetryDelay(5*time.Second)).
		Execute(t.Context(), wagerCmd(t, domain.WagerKindRollback, "rb1", "10.00", "b-missing"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != domain.WagerStatusPendingReference || res.Balance.IsValid() {
		t.Fatalf("result = %+v", res)
	}
	if got := eventTypes(u.state.events); got != "WagerTransactionPendingReference" {
		t.Fatalf("events = %s", got)
	}
	w := u.state.wagers[0]
	if w.Attempts() != 1 || !w.NextAttemptAt().Equal(fixedNow.Add(5*time.Second)) {
		t.Fatalf("attempts %d next %v", w.Attempts(), w.NextAttemptAt())
	}
	if len(u.state.ledger) != 0 || u.state.wallets[unitWallet].Version() != 1 {
		t.Fatal("pending reference moved the wallet")
	}
}

func TestProcessWager_RejectionsHideWalletBalance(t *testing.T) {
	u := newMemUoW() // carteira inexistente
	res, err := newProcess(t, u).Execute(t.Context(), wagerCmd(t, domain.WagerKindBet, "b1", "1.00", ""))
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != domain.WagerStatusRejected || res.FailureCode != domain.FailureCodeWalletNotFound || res.Balance.IsValid() {
		t.Fatalf("result = %+v", res)
	}
	if got := eventTypes(u.state.events); got != "WagerTransactionRejected" {
		t.Fatalf("events = %s", got)
	}
}

func TestProcessWager_Inbox(t *testing.T) {
	u := newMemUoW()
	seedWallet(t, u, "100.00")
	uc := newProcess(t, u)
	cmd := wagerCmd(t, domain.WagerKindBet, "b1", "25.00", "")
	cmd.Inbox = &app.InboxMessageRef{ConsumerName: "wager-consumer", MessageID: "msg-1"}
	first, err := uc.Execute(t.Context(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	rec := u.state.inbox["wager-consumer/msg-1"]
	if !rec.completed || rec.txID != first.TransactionID {
		t.Fatalf("inbox = %+v", rec)
	}

	// Reentrega: replay pela inbox, sem efeitos.
	events := len(u.state.events)
	again, err := uc.Execute(t.Context(), cmd)
	if err != nil || !again.IdempotentReplay || again.TransactionID != first.TransactionID || again.Balance.Amount() != "75.00" {
		t.Fatalf("redelivery %+v %v", again, err)
	}
	if len(u.state.events) != events {
		t.Fatal("redelivery had side effects")
	}

	// Mesmo messageId com outro conteúdo.
	changed := wagerCmd(t, domain.WagerKindBet, "b1", "26.00", "")
	changed.Inbox = cmd.Inbox
	_, err = uc.Execute(t.Context(), changed)
	wantAppError(t, err, app.KindConflict, app.CodeMessageConflict)

	// Mesmo conteúdo sob outra key, entregue com o mesmo messageId.
	otherKey := cmd
	otherKey.IdempotencyKey = "other"
	_, err = uc.Execute(t.Context(), otherKey)
	wantAppError(t, err, app.KindConflict, app.CodeExternalTransactionConflict)

	// Nova mensagem para a operação já processada por HTTP: replay e a
	// mensagem também é concluída.
	viaOther := cmd
	viaOther.Inbox = &app.InboxMessageRef{ConsumerName: "wager-consumer", MessageID: "msg-2"}
	res, err := uc.Execute(t.Context(), viaOther)
	if err != nil || !res.IdempotentReplay {
		t.Fatalf("other message %+v %v", res, err)
	}
	if rec := u.state.inbox["wager-consumer/msg-2"]; !rec.completed || rec.txID != first.TransactionID {
		t.Fatalf("inbox msg-2 = %+v", rec)
	}
}

func TestProcessWager_ConstructorAndFx(t *testing.T) {
	if _, err := app.NewProcessWagerTransaction(nil); err == nil {
		t.Fatal("nil uow accepted")
	}
	if _, err := app.NewProcessWagerTransaction(newMemUoW(), app.WithReferenceRetryDelay(-time.Second)); err == nil {
		t.Fatal("negative delay accepted")
	}
	err := fx.ValidateApp(app.Module, fx.Provide(func() app.UnitOfWork { return newMemUoW() }),
		fx.Invoke(func(*app.ProcessWagerTransaction, *app.OpenWallet) {}))
	if err != nil {
		t.Fatal(err)
	}
}
