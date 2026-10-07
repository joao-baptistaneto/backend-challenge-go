//go:build integration

package app_test

// T25 (pendência da avaliação): reversões concorrentes da mesma referência
// com externalTransactionIds diferentes. A barreira é o lock da carteira
// (GetForUpdate) + a regra ALREADY_REVERSED do domínio, com o índice
// wager_transactions_reversal_once_key como última defesa no banco.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// reversalKinds alterna ROLLBACK e REFUND: a política (ADR-009) admite uma
// única reversão bem-sucedida por referência, de qualquer tipo.
func reversalKind(i int) domain.WagerKind {
	if i%2 == 0 {
		return domain.WagerKindRollback
	}
	return domain.WagerKindRefund
}

// assertOneReversal confere que, entre as reversões de ref, exatamente uma
// ficou PROCESSED e as demais REJECTED ALREADY_REVERSED, cada uma com o seu
// evento terminal (e um único WalletBalanceChanged, o da reversão vencedora);
// quem passou por PENDING_REFERENCE tem antes um único evento de pendência.
func assertOneReversal(t *testing.T, e env, ref string, exts []string) uuid.UUID {
	t.Helper()
	p := e.pool()
	var winner uuid.UUID
	for _, ext := range exts {
		var (
			id      uuid.UUID
			status  string
			failure *string
		)
		if err := p.QueryRow(t.Context(), `SELECT id, status, failure_code FROM wager_transactions
 WHERE provider_id = $1 AND external_transaction_id = $2`, provider, ext).Scan(&id, &status, &failure); err != nil {
			t.Fatalf("%s: %v", ext, err)
		}
		evs := outboxFor(t, p, id)
		// Uma reversão aceita antes da referência emite antes o
		// WagerTransactionPendingReference (uma única vez).
		if len(evs) > 0 && evs[0] == "WagerTransactionPendingReference" {
			evs = evs[1:]
		}
		events := fmt.Sprint(evs)
		switch {
		case status == "PROCESSED":
			if winner != uuid.Nil {
				t.Fatalf("duas reversões PROCESSED de %s: %s e %s", ref, winner, id)
			}
			winner = id
			if events != "[WagerTransactionProcessed WalletBalanceChanged]" {
				t.Fatalf("%s: eventos %s", ext, events)
			}
		case status == "REJECTED" && failure != nil && *failure == string(domain.FailureCodeAlreadyReversed):
			if events != "[WagerTransactionRejected]" {
				t.Fatalf("%s: eventos %s", ext, events)
			}
		default:
			t.Fatalf("%s: status %s failure %v", ext, status, failure)
		}
	}
	if winner == uuid.Nil {
		t.Fatalf("nenhuma reversão de %s foi processada", ref)
	}
	return winner
}

// Referência já processada: N reversões (ROLLBACK/REFUND alternados, externalIds
// distintos) em paralelo → uma PROCESSED, N-1 ALREADY_REVERSED, um crédito.
// Reenvios devolvem o resultado persistido sem efeito.
func TestIntegrationProcessWager_ConcurrentReversalsSameReference(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	uc := e.processor(t, nil)
	w := e.openWallet(t, "100.00")
	expectResult(t, process(t, uc, w.cmd(t, domain.WagerKindBet, "rev-bet", "30.00", "")),
		domain.WagerStatusProcessed, "70.00", "")

	const n = 12
	cmds := make([]app.ProcessWagerCommand, n)
	exts := make([]string, n)
	for i := range n {
		exts[i] = fmt.Sprintf("rev-%d", i)
		cmds[i] = w.cmd(t, reversalKind(i), exts[i], "30.00", "rev-bet")
	}
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		start   = make(chan struct{})
		errs    []error
		results = make([]app.WagerResult, n)
	)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := uc.Execute(context.Background(), cmds[i])
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", exts[i], err))
				return
			}
			results[i] = res
		}()
	}
	close(start)
	wg.Wait()
	if len(errs) != 0 {
		t.Fatalf("erros: %v", errs)
	}
	processed := 0
	for i, r := range results {
		switch {
		case r.Status == domain.WagerStatusProcessed:
			processed++
			expectResult(t, r, domain.WagerStatusProcessed, "100.00", "")
		case r.Status == domain.WagerStatusRejected && r.FailureCode == domain.FailureCodeAlreadyReversed:
		default:
			t.Fatalf("%s: %+v", exts[i], r)
		}
	}
	if processed != 1 {
		t.Fatalf("%d reversões processadas, quer 1", processed)
	}
	winner := assertOneReversal(t, e, "rev-bet", exts)
	if bal, ver, entries := walletState(t, e.pool(), w.id); bal != 10000 || ver != 3 || entries != 3 {
		t.Fatalf("carteira %d v%d lançamentos %d (quer 100.00 v3, abertura+débito+um crédito)", bal, ver, entries)
	}
	assertReconciled(t, e.pool(), w.id)

	// Reenvios: replays com o mesmo resultado, sem movimento nem eventos.
	var outboxBefore int
	if err := e.pool().QueryRow(t.Context(), `SELECT count(*) FROM outbox`).Scan(&outboxBefore); err != nil {
		t.Fatal(err)
	}
	for i := range n {
		again := process(t, uc, cmds[i])
		if !again.IdempotentReplay || again.TransactionID != results[i].TransactionID ||
			again.Status != results[i].Status || again.FailureCode != results[i].FailureCode {
			t.Fatalf("%s: replay %+v, original %+v", exts[i], again, results[i])
		}
		if again.Status == domain.WagerStatusProcessed && again.TransactionID != winner {
			t.Fatalf("replay de outra reversão vencedora: %s", again.TransactionID)
		}
	}
	var outboxAfter int
	if err := e.pool().QueryRow(t.Context(), `SELECT count(*) FROM outbox`).Scan(&outboxAfter); err != nil {
		t.Fatal(err)
	}
	if outboxAfter != outboxBefore {
		t.Fatalf("replays gravaram %d eventos", outboxAfter-outboxBefore)
	}
	if bal, ver, entries := walletState(t, e.pool(), w.id); bal != 10000 || ver != 3 || entries != 3 {
		t.Fatalf("reenvios alteraram a carteira: %d v%d %d", bal, ver, entries)
	}
}

// Reversões disputando com a própria referência: N reversões e a BET são
// enviadas juntas. Reversões que chegam antes da BET ficam PENDING_REFERENCE;
// depois, várias "instâncias" do worker retomam as pendências em paralelo.
// Ao final: exatamente uma reversão PROCESSED, as demais ALREADY_REVERSED,
// nenhuma pendência, saldo = 100.00 e ledger reconciliado.
func TestIntegrationProcessWager_ConcurrentReversalsRaceReference(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	// Backoff mínimo: as pendências ficam vencidas logo (o worker as vê).
	uc, err := app.NewProcessWagerTransaction(e.uow, app.WithReferenceRetryDelay(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	policy := app.PendingWagerPolicy{MaxAttempts: 50, TTL: time.Minute,
		RetryBaseDelay: time.Millisecond, RetryMaxDelay: 5 * time.Millisecond, MaxResumeFailures: 3}
	resume, err := app.NewResumeWagerTransaction(e.uow, policy)
	if err != nil {
		t.Fatal(err)
	}
	w := e.openWallet(t, "100.00")

	const n = 10
	bet := w.cmd(t, domain.WagerKindBet, "race-bet", "40.00", "")
	exts := make([]string, n)
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		start = make(chan struct{})
		errs  []error
	)
	for i := range n + 1 {
		cmd := bet
		if i < n {
			exts[i] = fmt.Sprintf("race-rev-%d", i)
			cmd = w.cmd(t, reversalKind(i), exts[i], "40.00", "race-bet")
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := uc.Execute(context.Background(), cmd)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				errs = append(errs, fmt.Errorf("%s: %w", cmd.ExternalTransactionID, err))
			case cmd.Kind == domain.WagerKindBet && res.Status != domain.WagerStatusProcessed:
				errs = append(errs, fmt.Errorf("BET: %+v", res))
			case cmd.Kind != domain.WagerKindBet && res.Status != domain.WagerStatusProcessed &&
				res.Status != domain.WagerStatusPendingReference &&
				(res.Status != domain.WagerStatusRejected || res.FailureCode != domain.FailureCodeAlreadyReversed):
				errs = append(errs, fmt.Errorf("%s: %+v", cmd.ExternalTransactionID, res))
			}
		}()
	}
	close(start)
	wg.Wait()
	if len(errs) != 0 {
		t.Fatalf("erros: %v", errs)
	}

	// Três "workers" retomam as pendências em paralelo até não sobrar nenhuma.
	pending := func() int {
		var c int
		if err := e.pool().QueryRow(t.Context(), `SELECT count(*) FROM wager_transactions
 WHERE status IN ('PENDING', 'PENDING_REFERENCE')`).Scan(&c); err != nil {
			t.Fatal(err)
		}
		return c
	}
	deadline := time.Now().Add(30 * time.Second)
	for pending() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d pendências não resolvidas", pending())
		}
		var wwg sync.WaitGroup
		var werrs []error
		for range 3 {
			wwg.Add(1)
			go func() {
				defer wwg.Done()
				token := uuid.New()
				claimed, err := resume.Claim(context.Background(), app.ClaimRequest{
					Token: token, Lease: 30 * time.Second, Limit: 4, OrphanGrace: time.Minute,
				})
				if err == nil {
					for _, c := range claimed {
						if _, rerr := resume.Resume(context.Background(), c.ID, token); rerr != nil {
							err = rerr
							break
						}
					}
				}
				if err != nil {
					mu.Lock()
					werrs = append(werrs, err)
					mu.Unlock()
				}
			}()
		}
		wwg.Wait()
		if len(werrs) != 0 {
			t.Fatalf("worker: %v", werrs)
		}
		time.Sleep(5 * time.Millisecond)
	}

	assertOneReversal(t, e, "race-bet", exts)
	if bal, ver, entries := walletState(t, e.pool(), w.id); bal != 10000 || ver != 3 || entries != 3 {
		t.Fatalf("carteira %d v%d lançamentos %d (quer 100.00 v3)", bal, ver, entries)
	}
	assertReconciled(t, e.pool(), w.id)
}
