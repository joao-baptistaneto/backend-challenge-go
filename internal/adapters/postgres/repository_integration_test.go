//go:build integration

package postgres_test

// Integração dos repositórios de wallet, ledger e transação (T12) contra
// PostgreSQL real (TEST_DATABASE_URL ou testcontainers; banco novo por teste,
// ver main_integration_test.go).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

type repos struct {
	db      *postgres.DB
	tm      *postgres.TxManager
	wallets *postgres.WalletRepository
	ledger  *postgres.LedgerRepository
	wagers  *postgres.WagerTransactionRepository
}

func newRepos(t *testing.T) repos {
	t.Helper()
	db := startedDB(t)
	tm, _ := newTxManager(t, db)
	return repos{
		db: db, tm: tm,
		wallets: postgres.NewWalletRepository(),
		ledger:  postgres.NewLedgerRepository(),
		wagers:  postgres.NewWagerTransactionRepository(),
	}
}

// baseTime é alinhado a microssegundos (precisão do timestamptz), para que o
// round-trip seja exato.
func baseTime() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

func money(t *testing.T, amount string) domain.Money {
	t.Helper()
	m, err := domain.Parse(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func newID(t *testing.T) uuid.UUID {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func hashOf(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// within executa fn numa transação sem retry.
func (r repos) within(t *testing.T, fn func(ctx context.Context, tx pgx.Tx) error) error {
	t.Helper()
	return r.tm.WithinTx(t.Context(), fn)
}

// openWallet abre uma carteira como o caso de uso OpenWallet faria (T14):
// carteira, OPENING e lançamento de abertura no mesmo commit.
func (r repos) openWallet(t *testing.T, balance string, now time.Time) *domain.Wallet {
	t.Helper()
	openingID := newID(t)
	w, entry, err := domain.NewWallet(domain.NewWalletParams{
		ID: newID(t), PlayerID: newID(t), InitialBalance: money(t, balance),
		OpeningTransactionID: openingID, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		if err := r.wallets.Insert(ctx, tx, w); err != nil {
			return err
		}
		if entry == nil {
			return nil
		}
		opening, err := domain.NewOpeningWagerTransaction(domain.NewOpeningWagerTransactionParams{
			ID: openingID, WalletID: w.ID(), PlayerID: w.PlayerID(), Amount: w.Balance(), Now: now,
		})
		if err != nil {
			return err
		}
		if err := r.wagers.Insert(ctx, tx, opening); err != nil {
			return err
		}
		return r.ledger.Append(ctx, tx, *entry)
	})
	if err != nil {
		t.Fatalf("open wallet: %v", err)
	}
	return w
}

type wagerSpec struct {
	provider, external, key string
	kind                    domain.WagerKind
	amount                  string
	reference               string
	now                     time.Time
}

func newWager(t *testing.T, w *domain.Wallet, s wagerSpec) *domain.WagerTransaction {
	t.Helper()
	if s.provider == "" {
		s.provider = "provider-a"
	}
	if s.key == "" {
		s.key = s.provider + ":" + s.external
	}
	tx, err := domain.NewExternalWagerTransaction(domain.NewExternalWagerTransactionParams{
		ID: newID(t), ProviderID: s.provider, ExternalTransactionID: s.external, IdempotencyKey: s.key,
		RequestHash: hashOf(s.provider + s.external + s.amount), WalletID: w.ID(), PlayerID: w.PlayerID(),
		RoundID: "round-1", GameID: "game-1", Kind: s.kind, Money: money(t, s.amount),
		ReferenceExternalTransactionID: s.reference, Now: s.now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

// bet aplica uma BET completa numa transação: lock da carteira, débito,
// transação PROCESSED, UPDATE condicional da carteira e lançamento.
func (r repos) bet(ctx context.Context, tx pgx.Tx, t *testing.T, walletID uuid.UUID, external, amount string, now time.Time) (domain.WalletLedgerEntry, error) {
	w, err := r.wallets.GetForUpdate(ctx, tx, walletID)
	if err != nil {
		return domain.WalletLedgerEntry{}, err
	}
	wager := newWager(t, w, wagerSpec{external: external, kind: domain.WagerKindBet, amount: amount, now: now})
	entry, err := w.Debit(wager.Money(), wager.ID(), now)
	if err != nil {
		return domain.WalletLedgerEntry{}, err
	}
	if err := wager.MarkProcessed(domain.MarkWagerProcessedParams{BalanceAfter: w.Balance(), Now: now}); err != nil {
		return domain.WalletLedgerEntry{}, err
	}
	if err := r.wagers.Insert(ctx, tx, wager); err != nil {
		return domain.WalletLedgerEntry{}, err
	}
	if err := r.wallets.Update(ctx, tx, w); err != nil {
		return domain.WalletLedgerEntry{}, err
	}
	return entry, r.ledger.Append(ctx, tx, entry)
}

func (r repos) mustGetWallet(t *testing.T, id uuid.UUID) *domain.Wallet {
	t.Helper()
	w, err := r.wallets.Get(t.Context(), r.db.Pool(), id)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func (r repos) allEntries(t *testing.T, walletID uuid.UUID) []domain.WalletLedgerEntry {
	t.Helper()
	var all []domain.WalletLedgerEntry
	cursor := ""
	for {
		page, err := r.ledger.ListByWallet(t.Context(), r.db.Pool(), walletID, cursor, postgres.MaxLedgerPageSize)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, page.Entries...)
		if page.NextCursor == "" {
			return all
		}
		cursor = page.NextCursor
	}
}

func assertWalletState(t *testing.T, w *domain.Wallet, balance string, version int64) {
	t.Helper()
	if w.Balance() != money(t, balance) || w.Version() != version || w.PersistedVersion() != version {
		t.Fatalf("wallet balance %s version %d (persisted %d), want %s v%d",
			w.Balance(), w.Version(), w.PersistedVersion(), balance, version)
	}
}

// --- Wallet --------------------------------------------------------------------

func TestIntegrationWalletRepository_CRUD(t *testing.T) {
	t.Parallel()
	r := newRepos(t)
	ctx := t.Context()
	now := baseTime()

	w := r.openWallet(t, "100.00", now)
	got := r.mustGetWallet(t, w.ID())
	assertWalletState(t, got, "100.00", 1)
	if got.ID() != w.ID() || got.PlayerID() != w.PlayerID() || !got.CreatedAt().Equal(now) || !got.UpdatedAt().Equal(now) {
		t.Fatalf("round-trip mismatch: %+v vs %+v", got, w)
	}
	byPlayer, err := r.wallets.GetByPlayerCurrency(ctx, r.db.Pool(), w.PlayerID(), "BRL")
	if err != nil || byPlayer.ID() != w.ID() {
		t.Fatalf("by player: %v %v", byPlayer, err)
	}
	if _, err := r.wallets.GetByPlayerCurrency(ctx, r.db.Pool(), w.PlayerID(), "USD"); !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("other currency err = %v", err)
	}
	if _, err := r.wallets.Get(ctx, r.db.Pool(), newID(t)); !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("missing wallet err = %v", err)
	}

	// Conflito (playerId, currency) e id repetido.
	dupPlayer, _, err := domain.NewWallet(domain.NewWalletParams{ID: newID(t), PlayerID: w.PlayerID(), InitialBalance: money(t, "0.00"), Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.wallets.Insert(ctx, r.db.Pool(), dupPlayer); !errors.Is(err, postgres.ErrWalletAlreadyExists) {
		t.Fatalf("duplicate player/currency err = %v", err)
	}
	dupID, _, err := domain.NewWallet(domain.NewWalletParams{ID: w.ID(), PlayerID: newID(t), InitialBalance: money(t, "0.00"), Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.wallets.Insert(ctx, r.db.Pool(), dupID); !errors.Is(err, postgres.ErrDuplicateKey) {
		t.Fatalf("duplicate id err = %v", err)
	}
	usd, err := domain.Parse("5.00", "USD")
	if err != nil {
		t.Fatal(err)
	}
	other, _, err := domain.NewWallet(domain.NewWalletParams{ID: newID(t), PlayerID: w.PlayerID(), InitialBalance: usd, OpeningTransactionID: newID(t), Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.wallets.Insert(ctx, r.db.Pool(), other); err != nil {
		t.Fatalf("same player, other currency: %v", err)
	}
	if got := r.mustGetWallet(t, other.ID()); got.Balance() != usd {
		t.Fatalf("USD balance = %s", got.Balance())
	}

	// Update: um movimento por vez, versão +1.
	later := now.Add(time.Second)
	if err := r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		_, err := r.bet(ctx, tx, t, w.ID(), "bet-1", "25.00", later)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	got = r.mustGetWallet(t, w.ID())
	assertWalletState(t, got, "75.00", 2)
	if !got.UpdatedAt().Equal(later) {
		t.Fatalf("updatedAt = %v, want %v", got.UpdatedAt(), later)
	}

	// Saldo zero: só a carteira, sem OPENING nem ledger.
	zero := r.openWallet(t, "0.00", now)
	assertWalletState(t, r.mustGetWallet(t, zero.ID()), "0.00", 1)
	if n := len(r.allEntries(t, zero.ID())); n != 0 {
		t.Fatalf("zero-balance wallet has %d entries", n)
	}
}

func TestIntegrationWalletRepository_LostUpdateDetected(t *testing.T) {
	t.Parallel()
	r := newRepos(t)
	ctx := t.Context()
	now := baseTime()
	w := r.openWallet(t, "100.00", now)

	// Dois escritores leem a MESMA versão sem lock.
	a := r.mustGetWallet(t, w.ID())
	b := r.mustGetWallet(t, w.ID())
	if _, err := a.Debit(money(t, "30.00"), newID(t), now); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Credit(money(t, "50.00"), newID(t), now); err != nil {
		t.Fatal(err)
	}

	// A grava e segura o lock da linha (sem commit); B tenta gravar em
	// paralelo, espera o lock e, após o commit de A, reavalia o WHERE version
	// (READ COMMITTED) e afeta 0 linhas.
	txA, err := r.db.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer txA.Rollback(context.Background())
	if err := r.wallets.Update(ctx, txA, a); err != nil {
		t.Fatalf("A update: %v", err)
	}
	errB := make(chan error, 1)
	go func() {
		errB <- r.tm.WithinTx(ctx, func(ctx context.Context, tx pgx.Tx) error { return r.wallets.Update(ctx, tx, b) })
	}()
	select {
	case err := <-errB:
		t.Fatalf("B must block on A's row lock, got %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if err := txA.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-errB; !errors.Is(err, postgres.ErrStaleWallet) {
		t.Fatalf("B err = %v, want ErrStaleWallet", err)
	}
	// O valor confirmado por A não foi sobrescrito.
	assertWalletState(t, r.mustGetWallet(t, w.ID()), "70.00", 2)

	// Sequencial: escrita com versão velha também é detectada.
	if err := r.wallets.Update(ctx, r.db.Pool(), b); !errors.Is(err, postgres.ErrStaleWallet) {
		t.Fatalf("stale sequential update err = %v", err)
	}
	// Carteira inexistente: mesma detecção (0 linhas).
	ghost, err := domain.RehydrateWallet(domain.RehydrateWalletParams{
		ID: newID(t), PlayerID: newID(t), Balance: money(t, "1.00"), Version: 1, CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ghost.Credit(money(t, "1.00"), newID(t), now); err != nil {
		t.Fatal(err)
	}
	if err := r.wallets.Update(ctx, r.db.Pool(), ghost); !errors.Is(err, postgres.ErrStaleWallet) {
		t.Fatalf("ghost update err = %v", err)
	}
}

func TestIntegrationWalletRepository_LockPerWallet(t *testing.T) {
	t.Parallel()
	r := newRepos(t)
	ctx := t.Context()
	now := baseTime()
	w1 := r.openWallet(t, "100.00", now)
	w2 := r.openWallet(t, "100.00", now)

	holder, err := r.db.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback(context.Background())
	if _, err := r.wallets.GetForUpdate(ctx, holder, w1.ID()); err != nil {
		t.Fatal(err)
	}

	// Outra transação não consegue travar a mesma carteira.
	err = r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '150ms'`); err != nil {
			return err
		}
		_, err := r.wallets.GetForUpdate(ctx, tx, w1.ID())
		return err
	})
	if !postgres.IsTransient(err) {
		t.Fatalf("second lock on same wallet err = %v, want lock_not_available", err)
	}
	// Mas outra carteira trava na hora (nada de lock global).
	err = r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '150ms'`); err != nil {
			return err
		}
		_, err := r.bet(ctx, tx, t, w2.ID(), "w2-bet", "10.00", now)
		return err
	})
	if err != nil {
		t.Fatalf("other wallet must not be blocked: %v", err)
	}
	// Leitura sem lock também não bloqueia.
	if _, err := r.wallets.Get(ctx, r.db.Pool(), w1.ID()); err != nil {
		t.Fatal(err)
	}
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	// 20 débitos concorrentes na mesma carteira: serializados pelo lock,
	// nenhum perdido, nenhum ErrStaleWallet.
	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- r.tm.WithinTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
				_, err := r.bet(ctx, tx, t, w1.ID(), fmt.Sprintf("concurrent-%d", i), "1.00", now.Add(time.Duration(i)*time.Millisecond))
				return err
			})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent bet: %v", err)
		}
	}
	assertWalletState(t, r.mustGetWallet(t, w1.ID()), "80.00", n+1)

	// O ledger, na ordem de seq, encadeia: before(i) = after(i-1), e o
	// saldo final bate com o último lançamento.
	entries := r.allEntries(t, w1.ID())
	if len(entries) != n+1 {
		t.Fatalf("entries = %d, want %d", len(entries), n+1)
	}
	for i := 1; i < len(entries); i++ {
		if entries[i].BalanceBefore() != entries[i-1].BalanceAfter() {
			t.Fatalf("ledger chain broken at %d: before %s, previous after %s", i, entries[i].BalanceBefore(), entries[i-1].BalanceAfter())
		}
	}
	if last := entries[len(entries)-1]; last.BalanceAfter() != money(t, "80.00") {
		t.Fatalf("last entry after = %s", last.BalanceAfter())
	}
}

// --- Ledger --------------------------------------------------------------------

func TestIntegrationLedgerRepository_AppendAndRead(t *testing.T) {
	t.Parallel()
	r := newRepos(t)
	ctx := t.Context()
	now := baseTime()
	w := r.openWallet(t, "50.00", now)

	var entry domain.WalletLedgerEntry
	if err := r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		entry, err = r.bet(ctx, tx, t, w.ID(), "bet-1", "20.00", now)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	got, err := r.ledger.GetByTransaction(ctx, r.db.Pool(), w.ID(), entry.TransactionID())
	if err != nil {
		t.Fatal(err)
	}
	if got != entry {
		t.Fatalf("round-trip: %+v != %+v", got, entry)
	}
	if _, err := r.ledger.GetByTransaction(ctx, r.db.Pool(), w.ID(), newID(t)); !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("missing entry err = %v", err)
	}

	// Segundo lançamento para a mesma (carteira, transação): barrado.
	dup, err := domain.NewWalletLedgerEntry(domain.NewWalletLedgerEntryParams{
		ID: newID(t), WalletID: w.ID(), TransactionID: entry.TransactionID(), Direction: domain.LedgerDirectionDebit,
		Amount: money(t, "1.00"), BalanceBefore: money(t, "30.00"), BalanceAfter: money(t, "29.00"), CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ledger.Append(ctx, r.db.Pool(), dup); !errors.Is(err, postgres.ErrLedgerEntryExists) {
		t.Fatalf("duplicate entry err = %v", err)
	}
	// Lançamento sem transação correspondente: FK do schema.
	orphan, err := domain.NewWalletLedgerEntry(domain.NewWalletLedgerEntryParams{
		ID: newID(t), WalletID: w.ID(), TransactionID: newID(t), Direction: domain.LedgerDirectionCredit,
		Amount: money(t, "1.00"), BalanceBefore: money(t, "30.00"), BalanceAfter: money(t, "31.00"), CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ledger.Append(ctx, r.db.Pool(), orphan); !errors.Is(err, postgres.ErrForeignKeyViolation) {
		t.Fatalf("orphan entry err = %v", err)
	}

	// Atomicidade: falha depois do UPDATE da carteira desfaz tudo.
	boom := errors.New("boom")
	err = r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := r.bet(ctx, tx, t, w.ID(), "bet-rolled-back", "5.00", now); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatal(err)
	}
	assertWalletState(t, r.mustGetWallet(t, w.ID()), "30.00", 2)
	if n := len(r.allEntries(t, w.ID())); n != 2 {
		t.Fatalf("entries after rollback = %d, want 2", n)
	}
	if _, err := r.wagers.GetByProviderExternalID(ctx, r.db.Pool(), "provider-a", "bet-rolled-back"); !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("rolled back transaction err = %v", err)
	}
}

func TestIntegrationLedgerRepository_KeysetCursorStable(t *testing.T) {
	t.Parallel()
	r := newRepos(t)
	ctx := t.Context()
	now := baseTime()
	w := r.openWallet(t, "100.00", now)
	other := r.openWallet(t, "100.00", now)

	betAt := func(external string, at time.Time) {
		t.Helper()
		if err := r.within(t, func(ctx context.Context, tx pgx.Tx) error {
			_, err := r.bet(ctx, tx, t, w.ID(), external, "1.00", at)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 6 {
		betAt(fmt.Sprintf("bet-%d", i), now.Add(time.Duration(i+1)*time.Second))
	}
	// 7 lançamentos (abertura + 6). Páginas de 3.
	page1, err := r.ledger.ListByWallet(ctx, r.db.Pool(), w.ID(), "", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.Entries) != 3 || page1.NextCursor == "" {
		t.Fatalf("page1 = %d entries, cursor %q", len(page1.Entries), page1.NextCursor)
	}
	// Reler o mesmo cursor devolve a mesma página.
	page2, err := r.ledger.ListByWallet(ctx, r.db.Pool(), w.ID(), page1.NextCursor, 3)
	if err != nil {
		t.Fatal(err)
	}
	again, err := r.ledger.ListByWallet(ctx, r.db.Pool(), w.ID(), page1.NextCursor, 3)
	if err != nil || !reflect.DeepEqual(again, page2) {
		t.Fatalf("same cursor gave a different page: %v", err)
	}

	// Novos lançamentos chegam entre as leituras, com relógio ATRASADO
	// (instância com clock skew): created_at menor que o de tudo que já foi
	// lido. Com keyset por seq eles aparecem depois, sem pular nem duplicar.
	betAt("late-1", now.Add(-time.Hour))
	betAt("late-2", now.Add(-2*time.Hour))

	var seen []domain.WalletLedgerEntry
	seen = append(seen, page1.Entries...)
	seen = append(seen, page2.Entries...)
	cursor := page2.NextCursor
	pages := 2
	for cursor != "" {
		p, err := r.ledger.ListByWallet(ctx, r.db.Pool(), w.ID(), cursor, 3)
		if err != nil {
			t.Fatal(err)
		}
		seen = append(seen, p.Entries...)
		cursor = p.NextCursor
		pages++
	}
	// 9 lançamentos em páginas de 3: a terceira é exatamente a última e não
	// devolve cursor (limit+1 evita uma página vazia no fim).
	if len(seen) != 9 || pages != 3 {
		t.Fatalf("seen %d entries in %d pages, want 9 in 3", len(seen), pages)
	}
	ids := map[uuid.UUID]bool{}
	for i, e := range seen {
		if ids[e.ID()] {
			t.Fatalf("entry %s seen twice", e.ID())
		}
		ids[e.ID()] = true
		if i > 0 && e.BalanceBefore() != seen[i-1].BalanceAfter() {
			t.Fatalf("order broken at %d", i)
		}
	}
	if seen[0].Direction() != domain.LedgerDirectionCredit || seen[8].BalanceAfter() != money(t, "92.00") {
		t.Fatalf("first %s, last after %s", seen[0].Direction(), seen[8].BalanceAfter())
	}
	if !reflect.DeepEqual(seen, r.allEntries(t, w.ID())) {
		t.Fatal("paged read differs from a single full read")
	}

	// O cursor é da carteira: usado em outra é recusado.
	if _, err := r.ledger.ListByWallet(ctx, r.db.Pool(), other.ID(), page1.NextCursor, 3); !errors.Is(err, postgres.ErrInvalidCursor) {
		t.Fatalf("cross-wallet cursor err = %v", err)
	}
	// Carteira sem lançamentos depois do cursor/inexistente: página vazia.
	empty, err := r.ledger.ListByWallet(ctx, r.db.Pool(), newID(t), "", 3)
	if err != nil || len(empty.Entries) != 0 || empty.NextCursor != "" {
		t.Fatalf("empty wallet page = %+v, %v", empty, err)
	}
}

// --- WagerTransaction ------------------------------------------------------------

func TestIntegrationWagerTransactionRepository_CRUDAndQueries(t *testing.T) {
	t.Parallel()
	r := newRepos(t)
	ctx := t.Context()
	pool := r.db.Pool()
	now := baseTime()
	w := r.openWallet(t, "100.00", now)

	bet := newWager(t, w, wagerSpec{external: "tx-1", kind: domain.WagerKindBet, amount: "25.00", now: now})
	if err := r.wagers.Insert(ctx, pool, bet); err != nil {
		t.Fatal(err)
	}
	for name, get := range map[string]func() (*domain.WagerTransaction, error){
		"id": func() (*domain.WagerTransaction, error) { return r.wagers.GetByID(ctx, pool, bet.ID()) },
		"external id": func() (*domain.WagerTransaction, error) {
			return r.wagers.GetByProviderExternalID(ctx, pool, "provider-a", "tx-1")
		},
		"key": func() (*domain.WagerTransaction, error) {
			return r.wagers.GetByProviderIdempotencyKey(ctx, pool, "provider-a", "provider-a:tx-1")
		},
	} {
		got, err := get()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !reflect.DeepEqual(got, bet) {
			t.Fatalf("%s: round-trip mismatch:\n got %+v\nwant %+v", name, got, bet)
		}
	}
	// Escopo por provedor e inexistentes.
	if _, err := r.wagers.GetByProviderExternalID(ctx, pool, "provider-b", "tx-1"); !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("other provider err = %v", err)
	}
	if _, err := r.wagers.GetByProviderIdempotencyKey(ctx, pool, "provider-b", "provider-a:tx-1"); !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("other provider key err = %v", err)
	}
	if _, err := r.wagers.GetByID(ctx, pool, newID(t)); !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("missing id err = %v", err)
	}

	// Idempotência no schema.
	sameExternal := newWager(t, w, wagerSpec{external: "tx-1", key: "another-key", kind: domain.WagerKindBet, amount: "25.00", now: now})
	if err := r.wagers.Insert(ctx, pool, sameExternal); !errors.Is(err, postgres.ErrDuplicateExternalTransaction) {
		t.Fatalf("same external id err = %v", err)
	}
	sameKey := newWager(t, w, wagerSpec{external: "tx-2", key: "provider-a:tx-1", kind: domain.WagerKindBet, amount: "25.00", now: now})
	if err := r.wagers.Insert(ctx, pool, sameKey); !errors.Is(err, postgres.ErrDuplicateIdempotencyKey) {
		t.Fatalf("same key err = %v", err)
	}
	otherProvider := newWager(t, w, wagerSpec{provider: "provider-b", external: "tx-1", kind: domain.WagerKindBet, amount: "25.00", now: now})
	if err := r.wagers.Insert(ctx, pool, otherProvider); err != nil {
		t.Fatalf("same external id for another provider: %v", err)
	}
	secondOpening, err := domain.NewOpeningWagerTransaction(domain.NewOpeningWagerTransactionParams{
		ID: newID(t), WalletID: w.ID(), PlayerID: w.PlayerID(), Amount: money(t, "1.00"), Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.wagers.Insert(ctx, pool, secondOpening); !errors.Is(err, postgres.ErrOpeningAlreadyExists) {
		t.Fatalf("second opening err = %v", err)
	}

	// Update: PENDING -> PROCESSED; depois disso a linha é terminal.
	later := now.Add(time.Second)
	if err := bet.MarkProcessed(domain.MarkWagerProcessedParams{BalanceAfter: money(t, "75.00"), Now: later}); err != nil {
		t.Fatal(err)
	}
	if err := r.wagers.Update(ctx, pool, bet); err != nil {
		t.Fatal(err)
	}
	got, err := r.wagers.GetByID(ctx, pool, bet.ID())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, bet) || got.Status() != domain.WagerStatusProcessed || got.BalanceAfter() != money(t, "75.00") {
		t.Fatalf("processed round-trip: %+v", got)
	}
	if err := r.wagers.Update(ctx, pool, bet); !errors.Is(err, postgres.ErrTransactionFinalized) {
		t.Fatalf("update of terminal err = %v", err)
	}
	ghost := newWager(t, w, wagerSpec{external: "never-inserted", kind: domain.WagerKindBet, amount: "1.00", now: now})
	if err := r.wagers.Update(ctx, pool, ghost); !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("update of missing err = %v", err)
	}

	// Rejeição persiste failureCode e saldo observado.
	rejected := newWager(t, w, wagerSpec{external: "tx-3", kind: domain.WagerKindBet, amount: "500.00", now: now})
	if err := rejected.Reject(domain.RejectWagerParams{Code: domain.FailureCodeInsufficientFunds, BalanceAfter: money(t, "75.00"), Now: later}); err != nil {
		t.Fatal(err)
	}
	if err := r.wagers.Insert(ctx, pool, rejected); err != nil {
		t.Fatal(err)
	}
	if got, err := r.wagers.GetByID(ctx, pool, rejected.ID()); err != nil || !reflect.DeepEqual(got, rejected) {
		t.Fatalf("rejected round-trip: %+v, %v", got, err)
	}

	// Reversões: listadas por (provider, referência); a segunda PROCESSED da
	// mesma referência é barrada pelo índice único parcial.
	refund := newWager(t, w, wagerSpec{external: "refund-1", kind: domain.WagerKindRefund, amount: "25.00", reference: "tx-1", now: later})
	if err := refund.MarkProcessed(domain.MarkWagerProcessedParams{BalanceAfter: money(t, "100.00"), ReferenceTransactionID: bet.ID(), Now: later}); err != nil {
		t.Fatal(err)
	}
	if err := r.wagers.Insert(ctx, pool, refund); err != nil {
		t.Fatal(err)
	}
	rollback := newWager(t, w, wagerSpec{external: "rollback-1", kind: domain.WagerKindRollback, amount: "25.00", reference: "tx-1", now: later})
	if err := rollback.MarkProcessed(domain.MarkWagerProcessedParams{BalanceAfter: money(t, "125.00"), ReferenceTransactionID: bet.ID(), Now: later}); err != nil {
		t.Fatal(err)
	}
	if err := r.wagers.Insert(ctx, pool, rollback); !errors.Is(err, postgres.ErrReferenceAlreadyReversed) {
		t.Fatalf("second reversal err = %v", err)
	}
	pendingRollback := newWager(t, w, wagerSpec{external: "rollback-2", kind: domain.WagerKindRollback, amount: "25.00", reference: "tx-1", now: later})
	if err := r.wagers.Insert(ctx, pool, pendingRollback); err != nil {
		t.Fatal(err)
	}
	refs, err := r.wagers.ListByProviderReference(ctx, pool, "provider-a", "tx-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 2 || refs[0].ID() != refund.ID() || refs[1].ID() != pendingRollback.ID() {
		t.Fatalf("reversals of tx-1 = %d", len(refs))
	}
	if refs, err := r.wagers.ListByProviderReference(ctx, pool, "provider-b", "tx-1"); err != nil || len(refs) != 0 {
		t.Fatalf("other provider reversals = %d, %v", len(refs), err)
	}

	// Lock da transação exige pgx.Tx e funciona dentro dela.
	if err := r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		got, err := r.wagers.GetByIDForUpdate(ctx, tx, pendingRollback.ID())
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(got, pendingRollback) {
			return fmt.Errorf("locked read mismatch")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// claimState lê as colunas de lease (fora do agregado de domínio).
func claimState(t *testing.T, r repos, id uuid.UUID) (by pgtype.UUID, until pgtype.Timestamptz) {
	t.Helper()
	if err := r.db.Pool().QueryRow(t.Context(),
		`SELECT claimed_by, claimed_until FROM wager_transactions WHERE id = $1`, id).Scan(&by, &until); err != nil {
		t.Fatal(err)
	}
	return by, until
}

func claimedIDs(c []postgres.ClaimedWager) []uuid.UUID {
	out := make([]uuid.UUID, len(c))
	for i, x := range c {
		out[i] = x.ID
	}
	return out
}

func TestIntegrationWagerTransactionRepository_ClaimDue(t *testing.T) {
	t.Parallel()
	r := newRepos(t)
	ctx := t.Context()
	pool := r.db.Pool()
	now := baseTime()
	created := now.Add(-time.Hour)
	w := r.openWallet(t, "100.00", created)
	const lease = time.Minute
	grace := 30 * time.Second

	// 3 vencidas (next_attempt_at no passado, em ordem crescente) e 2 futuras.
	var due, future []*domain.WagerTransaction
	for i := range 5 {
		tx := newWager(t, w, wagerSpec{external: fmt.Sprintf("refund-%d", i), kind: domain.WagerKindRefund,
			amount: "10.00", reference: fmt.Sprintf("missing-bet-%d", i), now: created})
		next := now.Add(-time.Duration(30-i) * time.Minute)
		if i >= 3 {
			next = now.Add(time.Duration(i) * time.Minute)
		}
		if err := tx.MarkPendingReference(domain.MarkWagerPendingReferenceParams{NextAttemptAt: next, Now: created}); err != nil {
			t.Fatal(err)
		}
		if err := r.wagers.Insert(ctx, pool, tx); err != nil {
			t.Fatal(err)
		}
		if i < 3 {
			due = append(due, tx)
		} else {
			future = append(future, tx)
		}
	}
	// PENDING órfão (criado há 1h) e PENDING recente (dentro da carência).
	orphan := newWager(t, w, wagerSpec{external: "orphan-bet", kind: domain.WagerKindBet, amount: "1.00", now: created})
	fresh := newWager(t, w, wagerSpec{external: "fresh-bet", kind: domain.WagerKindBet, amount: "1.00", now: now.Add(-time.Second)})
	// Terminal nunca é elegível.
	done := newWager(t, w, wagerSpec{external: "done-loss", kind: domain.WagerKindLoss, amount: "0.00", now: created})
	if err := done.MarkProcessed(domain.MarkWagerProcessedParams{BalanceAfter: money(t, "100.00"), Now: created}); err != nil {
		t.Fatal(err)
	}
	for _, x := range []*domain.WagerTransaction{orphan, fresh, done} {
		if err := r.wagers.Insert(ctx, pool, x); err != nil {
			t.Fatal(err)
		}
	}

	claim := func(q postgres.Querier, at time.Time, token uuid.UUID, limit int) []postgres.ClaimedWager {
		t.Helper()
		got, err := r.wagers.ClaimDue(ctx, q, postgres.ClaimParams{Now: at, Token: token, Lease: lease, Limit: limit, OrphanGrace: grace})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}

	// Instância A (transação ainda aberta: linhas travadas) pega as 2 mais
	// atrasadas.
	tokenA, tokenB := newID(t), newID(t)
	txA, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer txA.Rollback(context.Background())
	claimedA := claim(txA, now, tokenA, 2)
	if got := claimedIDs(claimedA); !reflect.DeepEqual(got, []uuid.UUID{due[0].ID(), due[1].ID()}) ||
		claimedA[0].Status != domain.WagerStatusPendingReference {
		t.Fatalf("A claimed %v, want the two oldest due", got)
	}

	// Instância B, em paralelo, não espera (lock_timeout curto) nem duplica:
	// pega a referência restante e depois o órfão (referências primeiro).
	var claimedB []postgres.ClaimedWager
	if err := r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '150ms'"); err != nil {
			return err
		}
		claimedB = claim(tx, now, tokenB, 10)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := claimedIDs(claimedB); !reflect.DeepEqual(got, []uuid.UUID{due[2].ID(), orphan.ID()}) ||
		claimedB[1].Status != domain.WagerStatusPending {
		t.Fatalf("B claimed %v, want [third due, orphan]", got)
	}
	by, until := claimState(t, r, orphan.ID())
	if !by.Valid || uuid.UUID(by.Bytes) != tokenB || !until.Time.Equal(now.Add(lease)) {
		t.Fatalf("orphan lease = %v until %v", by, until.Time)
	}

	// A confirma; os leases vigentes impedem nova reivindicação.
	if err := txA.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if again := claim(pool, now.Add(10*time.Second), newID(t), 10); len(again) != 0 {
		t.Fatalf("claimed %v while leases are valid", claimedIDs(again))
	}

	// Fencing: só o dono do lease trava a linha.
	if err := r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := r.wagers.GetClaimedForUpdate(ctx, tx, due[2].ID(), tokenA); !errors.Is(err, postgres.ErrNotFound) {
			return fmt.Errorf("foreign token err = %v", err)
		}
		got, err := r.wagers.GetClaimedForUpdate(ctx, tx, due[2].ID(), tokenB)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(got, due[2]) {
			return fmt.Errorf("claimed read mismatch")
		}
		// Reagenda e solta o lease no mesmo UPDATE.
		if err := got.RescheduleReferenceRetry(domain.MarkWagerPendingReferenceParams{NextAttemptAt: now.Add(time.Hour), Now: now}); err != nil {
			return err
		}
		return r.wagers.Update(ctx, tx, got)
	}); err != nil {
		t.Fatal(err)
	}
	if by, until := claimState(t, r, due[2].ID()); by.Valid || until.Valid {
		t.Fatalf("Update kept the lease: %v %v", by, until)
	}
	if got, err := r.wagers.GetByID(ctx, pool, due[2].ID()); err != nil || got.Attempts() != 2 || !got.NextAttemptAt().Equal(now.Add(time.Hour)) {
		t.Fatalf("rescheduled: %+v, %v", got, err)
	}

	// Lease expirado (worker morto): outra instância reassume, e o dono antigo
	// perde o fencing. O PENDING recente já passou da carência; a reagendada
	// (+1h) e as futuras ainda não venceram.
	tokenC := newID(t)
	reclaimed := claim(pool, now.Add(lease), tokenC, 10)
	if got := claimedIDs(reclaimed); !reflect.DeepEqual(got, []uuid.UUID{due[0].ID(), due[1].ID(), orphan.ID(), fresh.ID()}) {
		t.Fatalf("after lease claimed %v, want [due0, due1, orphan, fresh]", got)
	}
	if err := r.within(t, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := r.wagers.GetClaimedForUpdate(ctx, tx, due[0].ID(), tokenA); !errors.Is(err, postgres.ErrNotFound) {
			return fmt.Errorf("stale owner err = %v", err)
		}
		_, err := r.wagers.GetClaimedForUpdate(ctx, tx, due[0].ID(), tokenC)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// Mais tarde: as futuras vencem e os leases de C expiram (o PENDING recente
	// volta a ser elegível); a terminal nunca entra.
	later := claim(pool, now.Add(2*time.Hour), newID(t), 10)
	got := map[uuid.UUID]bool{}
	for _, c := range later {
		got[c.ID] = true
	}
	for _, want := range []uuid.UUID{future[0].ID(), future[1].ID(), fresh.ID(), due[2].ID()} {
		if !got[want] {
			t.Errorf("claim at +2h missing %s", want)
		}
	}
	if got[done.ID()] {
		t.Fatal("terminal transaction claimed")
	}
}

// Muitos claimers concorrentes: cada linha vencida é reivindicada por
// exatamente um deles.
func TestIntegrationWagerTransactionRepository_ClaimDueConcurrent(t *testing.T) {
	t.Parallel()
	r := newRepos(t)
	ctx := t.Context()
	now := baseTime()
	created := now.Add(-time.Hour)
	w := r.openWallet(t, "100.00", created)
	const n = 60
	for i := range n {
		tx := newWager(t, w, wagerSpec{external: fmt.Sprintf("rb-%d", i), kind: domain.WagerKindRollback,
			amount: "1.00", reference: fmt.Sprintf("missing-%d", i), now: created})
		if err := tx.MarkPendingReference(domain.MarkWagerPendingReferenceParams{NextAttemptAt: created, Now: created}); err != nil {
			t.Fatal(err)
		}
		if err := r.wagers.Insert(ctx, r.db.Pool(), tx); err != nil {
			t.Fatal(err)
		}
	}
	var (
		mu    sync.Mutex
		seen  = map[uuid.UUID]int{}
		wg    sync.WaitGroup
		errCh = make(chan error, 8)
	)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			token := uuid.New()
			for {
				got, err := r.wagers.ClaimDue(ctx, r.db.Pool(), postgres.ClaimParams{
					Now: now, Token: token, Lease: time.Minute, Limit: 3,
				})
				if err != nil {
					errCh <- err
					return
				}
				if len(got) == 0 {
					return
				}
				mu.Lock()
				for _, c := range got {
					seen[c.ID]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	if len(seen) != n {
		t.Fatalf("claimed %d distinct, want %d", len(seen), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("%s claimed %d times", id, c)
		}
	}
}

// O schema (0007) impede lease pela metade e lease em linha terminal.
func TestIntegrationWagerTransactionRepository_ClaimConstraints(t *testing.T) {
	t.Parallel()
	r := newRepos(t)
	ctx := t.Context()
	pool := r.db.Pool()
	now := baseTime()
	w := r.openWallet(t, "100.00", now)
	pending := newWager(t, w, wagerSpec{external: "p-bet", kind: domain.WagerKindBet, amount: "1.00", now: now})
	done := newWager(t, w, wagerSpec{external: "d-loss", kind: domain.WagerKindLoss, amount: "0.00", now: now})
	if err := done.MarkProcessed(domain.MarkWagerProcessedParams{BalanceAfter: money(t, "100.00"), Now: now}); err != nil {
		t.Fatal(err)
	}
	for _, x := range []*domain.WagerTransaction{pending, done} {
		if err := r.wagers.Insert(ctx, pool, x); err != nil {
			t.Fatal(err)
		}
	}
	cases := map[string]struct {
		sql        string
		id         uuid.UUID
		constraint string
	}{
		"half lease (token only)": {"UPDATE wager_transactions SET claimed_by = gen_random_uuid() WHERE id = $1", pending.ID(), "wager_transactions_claim_pair"},
		"half lease (until only)": {"UPDATE wager_transactions SET claimed_until = now() WHERE id = $1", pending.ID(), "wager_transactions_claim_pair"},
	}
	for name, c := range cases {
		_, err := pool.Exec(ctx, c.sql, c.id)
		var cv *postgres.ConstraintViolation
		if !errors.As(postgres.Translate(err), &cv) || cv.Constraint != c.constraint {
			t.Errorf("%s: err = %v, want %s", name, err, c.constraint)
		}
	}
	// Linha terminal: o trigger de guarda barra qualquer UPDATE, e o CHECK
	// barra uma inserção já reivindicada.
	if _, err := pool.Exec(ctx, "UPDATE wager_transactions SET claimed_by = gen_random_uuid(), claimed_until = now() WHERE id = $1", done.ID()); err == nil {
		t.Fatal("terminal transaction accepted a lease")
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE wager_transactions DISABLE TRIGGER wager_transactions_guard_update`); err != nil {
		t.Fatal(err)
	}
	_, err := pool.Exec(ctx, "UPDATE wager_transactions SET claimed_by = gen_random_uuid(), claimed_until = now() WHERE id = $1", done.ID())
	var cv *postgres.ConstraintViolation
	if !errors.As(postgres.Translate(err), &cv) || cv.Constraint != "wager_transactions_terminal_unclaimed" {
		t.Fatalf("terminal lease without trigger: err = %v", err)
	}
}
