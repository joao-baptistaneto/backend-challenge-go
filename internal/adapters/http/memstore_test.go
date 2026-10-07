package httpadapter_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// memUoW é uma UnitOfWork em memória: os testes de contrato HTTP rodam os
// casos de uso REAIS (app.OpenWallet, ProcessWagerTransaction, Queries,
// ReconcileWallet) sobre ela, para verificar replay e ausência de efeitos.
// Cada transação trabalha numa cópia do estado e só a publica se fn devolver
// nil. Transações são serializadas por um mutex (a concorrência real é
// testada na integração com PostgreSQL). Transações ReadOnly que tentam
// escrever falham, como o PostgreSQL faria.
type memUoW struct {
	mu    sync.Mutex
	state memState
	// txCount conta as unidades de trabalho abertas (para provar que um
	// acesso negado não chegou ao caso de uso).
	txCount int
}

type memState struct {
	wallets map[uuid.UUID]*domain.Wallet
	wagers  []*domain.WagerTransaction
	ledger  []domain.WalletLedgerEntry
	events  []domain.IntegrationEvent
}

func newMemUoW() *memUoW {
	return &memUoW{state: memState{wallets: map[uuid.UUID]*domain.Wallet{}}}
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
	c := memState{wallets: map[uuid.UUID]*domain.Wallet{}}
	for k, v := range s.wallets {
		c.wallets[k] = cloneWallet(v)
	}
	for _, w := range s.wagers {
		c.wagers = append(c.wagers, cloneWager(w))
	}
	c.ledger = append(c.ledger, s.ledger...)
	c.events = append(c.events, s.events...)
	return c
}

// snapshot devolve as contagens e uma cópia das carteiras (comparação de
// efeitos).
type snapshot struct {
	wallets, wagers, ledger, events int
	balances                        map[uuid.UUID]string
	versions                        map[uuid.UUID]int64
}

func (u *memUoW) snapshot() snapshot {
	u.mu.Lock()
	defer u.mu.Unlock()
	s := snapshot{
		wallets: len(u.state.wallets), wagers: len(u.state.wagers),
		ledger: len(u.state.ledger), events: len(u.state.events),
		balances: map[uuid.UUID]string{}, versions: map[uuid.UUID]int64{},
	}
	for id, w := range u.state.wallets {
		s.balances[id] = w.Balance().String()
		s.versions[id] = w.Version()
	}
	return s
}

func (u *memUoW) transactions() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.txCount
}

func (u *memUoW) WithinTx(ctx context.Context, opts app.TxOptions, fn app.TxFunc) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	u.txCount++
	max := 1
	if opts.Retry {
		max = 4
	}
	for attempt := 1; ; attempt++ {
		work := u.state.clone()
		err := fn(ctx, &memTx{s: &work, readOnly: opts.ReadOnly})
		if err == nil {
			u.state = work
			return nil
		}
		if attempt >= max || opts.RetryIf == nil || !opts.RetryIf(err) {
			return err
		}
	}
}

var errReadOnly = errors.New("mem: write in read-only transaction")

type memTx struct {
	s        *memState
	readOnly bool
}

func (t *memTx) write() error {
	if t.readOnly {
		return errReadOnly
	}
	return nil
}

func (t *memTx) Wallets() app.WalletStore                { return memWallets{t} }
func (t *memTx) Transactions() app.WagerTransactionStore { return memWagers{t} }
func (t *memTx) Ledger() app.LedgerStore                 { return memLedger{t} }
func (t *memTx) Outbox() app.OutboxStore                 { return memOutbox{t} }
func (t *memTx) Inbox() app.InboxStore                   { return memInbox{t} }

type (
	memWallets struct{ *memTx }
	memWagers  struct{ *memTx }
	memLedger  struct{ *memTx }
	memOutbox  struct{ *memTx }
	memInbox   struct{ *memTx }
)

func (s memWallets) Insert(_ context.Context, w *domain.Wallet) error {
	if err := s.write(); err != nil {
		return err
	}
	for _, cur := range s.s.wallets {
		if cur.PlayerID() == w.PlayerID() && cur.Currency() == w.Currency() {
			return fmt.Errorf("mem: %w", app.ErrWalletAlreadyExists)
		}
	}
	s.s.wallets[w.ID()] = cloneWallet(w)
	return nil
}

func (s memWallets) Get(_ context.Context, id uuid.UUID) (*domain.Wallet, error) {
	w, ok := s.s.wallets[id]
	if !ok {
		return nil, app.ErrRecordNotFound
	}
	return cloneWallet(w), nil
}

func (s memWallets) GetForUpdate(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	return s.Get(ctx, id)
}

func (s memWallets) Update(_ context.Context, w *domain.Wallet) error {
	if err := s.write(); err != nil {
		return err
	}
	cur := s.s.wallets[w.ID()]
	if cur == nil || cur.Version() != w.PersistedVersion() {
		return errors.New("mem: stale wallet")
	}
	s.s.wallets[w.ID()] = cloneWallet(w)
	return nil
}

func (s memWagers) Insert(_ context.Context, t *domain.WagerTransaction) error {
	if err := s.write(); err != nil {
		return err
	}
	if t.Origin() == domain.WagerOriginExternal {
		for _, w := range s.s.wagers {
			if w.Origin() == domain.WagerOriginExternal && w.ProviderID() == t.ProviderID() &&
				(w.ExternalTransactionID() == t.ExternalTransactionID() || w.IdempotencyKey() == t.IdempotencyKey()) {
				return fmt.Errorf("mem: %w", app.ErrDuplicateWagerTransaction)
			}
		}
	}
	s.s.wagers = append(s.s.wagers, cloneWager(t))
	return nil
}

func (s memWagers) Update(context.Context, *domain.WagerTransaction) error {
	return errors.New("mem: update not used")
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
	return s.find(func(w *domain.WagerTransaction) bool {
		return w.Origin() == domain.WagerOriginExternal && w.ProviderID() == p && w.IdempotencyKey() == k
	})
}

func (s memWagers) GetByProviderExternalID(_ context.Context, p, e string) (*domain.WagerTransaction, error) {
	return s.find(func(w *domain.WagerTransaction) bool {
		return w.Origin() == domain.WagerOriginExternal && w.ProviderID() == p && w.ExternalTransactionID() == e
	})
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
	if err := s.write(); err != nil {
		return err
	}
	s.s.ledger = append(s.s.ledger, e)
	return nil
}

// List usa como cursor "w:<walletId>:<índice>".
func (s memLedger) List(_ context.Context, walletID uuid.UUID, cursor string, limit int) (app.LedgerPage, error) {
	var entries []domain.WalletLedgerEntry
	for _, e := range s.s.ledger {
		if e.WalletID() == walletID {
			entries = append(entries, e)
		}
	}
	start := 0
	if cursor != "" {
		prefix := "w:" + walletID.String() + ":"
		n, err := strconv.Atoi(strings.TrimPrefix(cursor, prefix))
		if !strings.HasPrefix(cursor, prefix) || err != nil || n < 1 || n > len(entries) {
			return app.LedgerPage{}, app.ErrInvalidCursor
		}
		start = n
	}
	end := min(start+limit, len(entries))
	p := app.LedgerPage{Entries: entries[start:end]}
	if end < len(entries) {
		p.NextCursor = "w:" + walletID.String() + ":" + strconv.Itoa(end)
	}
	return p, nil
}

func (s memLedger) Summarize(_ context.Context, walletID uuid.UUID) (app.LedgerSummary, error) {
	var sum app.LedgerSummary
	for _, e := range s.s.ledger {
		if e.WalletID() != walletID {
			continue
		}
		sum.Entries++
		if e.Direction() == domain.LedgerDirectionCredit {
			sum.NetMinorUnits += e.Amount().MinorUnits()
		} else {
			sum.NetMinorUnits -= e.Amount().MinorUnits()
		}
	}
	return sum, nil
}

func (s memOutbox) Enqueue(_ context.Context, events ...domain.IntegrationEvent) error {
	if err := s.write(); err != nil {
		return err
	}
	s.s.events = append(s.s.events, events...)
	return nil
}

func (memInbox) Register(context.Context, string, string, string, time.Time) (app.InboxRecord, error) {
	return app.InboxRecord{}, errors.New("mem: inbox not used by HTTP")
}

func (memInbox) Complete(context.Context, string, string, string, uuid.UUID, time.Time) error {
	return errors.New("mem: inbox not used by HTTP")
}

// Worker de pendências (T21): não usados pela API HTTP.

func (memWagers) ClaimDue(context.Context, app.ClaimDueParams) ([]app.ClaimedWager, error) {
	return nil, errors.New("mem: claim not used by the HTTP API")
}

func (memWagers) GetClaimedForUpdate(context.Context, uuid.UUID, uuid.UUID) (*domain.WagerTransaction, error) {
	return nil, errors.New("mem: claimed lock not used by the HTTP API")
}

func (memWagers) LockClaimedForFailure(context.Context, uuid.UUID, uuid.UUID) (app.ResumeFailureState, error) {
	return app.ResumeFailureState{}, errors.New("mem: failure lock not used by the HTTP API")
}

func (memWagers) RecordResumeFailure(context.Context, app.RecordResumeFailureParams) error {
	return errors.New("mem: resume failure not used by the HTTP API")
}

func (memWagers) FailClaimedUnreadable(context.Context, app.FailClaimedParams) error {
	return errors.New("mem: fail unreadable not used by the HTTP API")
}

// WakePendingReferences: os testes HTTP não têm worker; o agendamento das
// pendências não é observado aqui (T31 é coberto em app e na integração).
func (s memWagers) WakePendingReferences(context.Context, string, string, time.Time) (int64, error) {
	return 0, nil
}
