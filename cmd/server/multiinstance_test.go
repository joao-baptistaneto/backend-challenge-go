//go:build integration && multiinstance

package main

// Testes multi-instância e de recuperação de falhas (T26, README §3, §8 e
// "Testes de concorrência e recuperação" itens 4–8; ARCHITECTURE.md ADR-028).
// Cada instância é um processo cmd/server independente (ver o harness).
//
// Ao final de todo cenário: saldo armazenado == créditos − débitos do ledger
// em toda carteira (SQL e POST /wallets/{id}/reconciliation), nenhum débito
// duplicado, outbox coerente e inteiramente entregue na fila de eventos (sem
// perda; cópia repetida só com o mesmo eventId e corpo), fila de entrada e
// DLQ vazias.

import (
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
	"github.com/junglegaming/backend-challenge-go/internal/platform/failpoint"
)

// finish executa as verificações finais comuns.
func (c *cluster) finish(inboxCompleted int) int {
	c.t.Helper()
	c.on(c.alive()).waitSQS(inboxCompleted)
	if n := c.scalar(`SELECT count(*) FROM wager_transactions WHERE status IN ('PENDING', 'PENDING_REFERENCE')`); n != 0 {
		c.t.Fatalf("%d transações ainda pendentes", n)
	}
	dups := c.assertEventsDelivered(120 * time.Second)
	c.t.Logf("eventos: %d na outbox, %d cópias repetidas (mesmo eventId) na fila", c.scalar(`SELECT count(*) FROM outbox`), dups)
	c.on(c.alive()).assertConsistent()
	return dups
}

// rollback é a reversão de uma BET (mesma rodada e valor).
func rollback(w ccWallet, ext string, ref ccWager) ccWager {
	return ccWager{w: w, ext: ext, kind: "ROLLBACK", amount: ref.amount, ref: ref.ext, roundID: ref.roundID}
}

func refund(w ccWallet, ext string, ref ccWager) ccWager {
	return ccWager{w: w, ext: ext, kind: "REFUND", amount: ref.amount, ref: ref.ext, roundID: ref.roundID}
}

// Item 4 (com 2 e 3): três processos independentes recebem, ao mesmo tempo,
// a disputa 80/80 em várias carteiras (cada aposta por um processo
// diferente, algumas pelo SQS, consumidas por qualquer um dos três) e a
// mesma aposta 50 vezes espalhada pelos três processos e pelo SQS. Reenvios
// cruzados (outro processo, outro transporte) são replays sem efeito.
func TestMultiInstanceConcurrency(t *testing.T) {
	c := newCluster(t, miOptions{})
	ins := c.start("A", "B", "C")
	t.Logf("processos: A pid %d, B pid %d, C pid %d", ins[0].cmd.Process.Pid, ins[1].cmd.Process.Pid, ins[2].cmd.Process.Pid)

	const nWallets = 6
	wallets := make([]ccWallet, nWallets)
	for i := range wallets {
		wallets[i] = c.on(ins[i%3]).openWallet("100.00")
	}
	same := c.on(ins[0]).openWallet("100.00")
	sameBet := bet(same, "mi-same", "25.00")

	type sub struct {
		op  ccWager
		in  *instance // nil → SQS
		msg string
	}
	var subs []sub
	for i, w := range wallets {
		a, b := bet(w, fmt.Sprintf("mi80-%d-a", i), "80.00"), bet(w, fmt.Sprintf("mi80-%d-b", i), "80.00")
		subs = append(subs, sub{op: a, in: ins[i%3]})
		if i%3 == 2 {
			subs = append(subs, sub{op: b, msg: "mi80-msg-" + b.ext}) // SQS
		} else {
			subs = append(subs, sub{op: b, in: ins[(i+1)%3]})
		}
	}
	for i := range 45 {
		subs = append(subs, sub{op: sameBet, in: ins[i%3]})
	}
	for i := range 5 {
		subs = append(subs, sub{op: sameBet, msg: fmt.Sprintf("mi-same-msg-%d", i)})
	}
	sqsMsgs := 0
	for _, s := range subs {
		if s.in == nil {
			sqsMsgs++
		}
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		start   = make(chan struct{})
		errs    []error
		results = map[string][]ccResult{}
	)
	for _, s := range subs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if s.in == nil {
				if err := c.sendSQS(s.op, s.msg, s.op.w.id); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
				return
			}
			r, err := c.on(s.in).submit(s.op)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
			}
			results[s.op.ext] = append(results[s.op.ext], r)
		}()
	}
	close(start)
	wg.Wait()
	if len(errs) != 0 {
		t.Fatalf("envios falharam: %v", errs)
	}
	c.on(ins[0]).waitSQS(sqsMsgs)

	// 80/80: em cada carteira uma PROCESSED e uma INSUFFICIENT_FUNDS.
	finals := map[string][2]string{}
	for i, w := range wallets {
		processed, rejected := 0, 0
		for _, ext := range []string{fmt.Sprintf("mi80-%d-a", i), fmt.Sprintf("mi80-%d-b", i)} {
			st, fc := c.txState(ext)
			finals[ext] = [2]string{st, fc}
			switch {
			case st == "PROCESSED" && fc == "":
				processed++
			case st == "REJECTED" && fc == "INSUFFICIENT_FUNDS":
				rejected++
			}
			for _, r := range results[ext] {
				if r.status != st || r.fc != fc || r.replay {
					t.Fatalf("%s: resposta %+v, banco %s %s", ext, r, st, fc)
				}
			}
		}
		if processed != 1 || rejected != 1 {
			t.Fatalf("carteira %d: processed %d rejected %d", i, processed, rejected)
		}
		c.expectWallet(w.id, 2000, 2, 1, 1)
	}
	// Mesma aposta: um débito, uma resposta fresca no máximo (o SQS pode ter
	// processado primeiro), todas com a mesma transação e o saldo original.
	fresh := 0
	txID := c.scalar(`SELECT count(*) FROM wager_transactions WHERE external_transaction_id = 'mi-same'`)
	if txID != 1 {
		t.Fatalf("%d transações para mi-same", txID)
	}
	for _, r := range results["mi-same"] {
		if r.code != http.StatusOK || r.status != "PROCESSED" || r.balance != "75.00" || r.txID != results["mi-same"][0].txID {
			t.Fatalf("mi-same: %+v", r)
		}
		if !r.replay {
			fresh++
		}
	}
	if fresh > 1 {
		t.Fatalf("mi-same: %d respostas não-replay", fresh)
	}
	c.expectWallet(same.id, 7500, 2, 1, 1)

	// Reenvios cruzados: cada operação por outro processo; as que vieram pelo
	// SQS também pelo HTTP. Nenhum efeito.
	ledgerBefore := c.scalar(`SELECT count(*) FROM wallet_ledger_entries`)
	outboxBefore := c.outboxCount()
	for i, s := range subs[:2*nWallets] {
		r, err := c.on(ins[(i+2)%3]).submit(s.op)
		if err != nil {
			t.Fatal(err)
		}
		f := finals[s.op.ext]
		if !r.replay || r.status != f[0] || r.fc != f[1] {
			t.Fatalf("reenvio de %s: %+v, original %v", s.op.ext, r, f)
		}
	}
	if n := c.scalar(`SELECT count(*) FROM wallet_ledger_entries`); n != ledgerBefore {
		t.Fatalf("reenvios gravaram %d lançamentos", n-ledgerBefore)
	}
	if n := c.outboxCount(); n != outboxBefore {
		t.Fatalf("reenvios gravaram %d eventos", n-outboxBefore)
	}
	c.finish(sqsMsgs)
}

// Item 5: o consumidor do processo A morre (kill no ponto de falha) depois
// do commit do tratamento durável e antes do DeleteMessage. A mensagem
// continua na fila (em voo); quando a visibilidade vence, outro processo a
// recebe de novo, encontra a inbox COMPLETED, devolve o replay e só então a
// apaga. A segunda mensagem do mesmo grupo (outra aposta de 80.00) é
// processada depois, pela instância nova: INSUFFICIENT_FUNDS.
func TestFailureConsumerKilledAfterCommitBeforeDelete(t *testing.T) {
	c := newCluster(t, miOptions{})
	a := c.launch("A", nil, failpoint.SQSConsumerAfterCommit+"=crash")
	c.waitReady(a)
	w := c.on(a).openWallet("100.00")
	first, second := bet(w, "kill-a", "80.00"), bet(w, "kill-b", "80.00")
	if err := c.sendSQS(first, "kill-msg-a", w.id); err != nil {
		t.Fatal(err)
	}
	if err := c.sendSQS(second, "kill-msg-b", w.id); err != nil {
		t.Fatal(err)
	}
	a.waitCrash(t, failpoint.SQSConsumerAfterCommit, 30*time.Second)

	// Commit feito, mensagem não apagada.
	if st, fc := c.txState(first.ext); st != "PROCESSED" || fc != "" {
		t.Fatalf("%s depois do crash: %s %s", first.ext, st, fc)
	}
	if n := c.scalar(`SELECT count(*) FROM inbox WHERE message_id = 'kill-msg-a' AND completed_at IS NOT NULL`); n != 1 {
		t.Fatal("inbox da mensagem processada não está COMPLETED")
	}
	if n := c.scalar(`SELECT count(*) FROM wager_transactions WHERE external_transaction_id = $1`, second.ext); n != 0 {
		t.Fatal("a segunda mensagem do grupo foi processada antes da reentrega da primeira")
	}
	visible, inFlight := c.queueDepth()
	if visible == "0" && inFlight == "0" {
		t.Fatal("a mensagem sumiu da fila sem DeleteMessage")
	}
	t.Logf("depois do crash: %s visíveis, %s em voo", visible, inFlight)
	c.expectWallet(w.id, 2000, 2, 1, 1)

	// Outras duas instâncias assumem a fila.
	ins := c.start("B", "C")
	c.on(ins[0]).waitSQS(2)
	if st, fc := c.txState(first.ext); st != "PROCESSED" || fc != "" {
		t.Fatalf("%s: %s %s", first.ext, st, fc)
	}
	if st, fc := c.txState(second.ext); st != "REJECTED" || fc != "INSUFFICIENT_FUNDS" {
		t.Fatalf("%s: %s %s", second.ext, st, fc)
	}
	// A reentrega foi um replay (inbox), registrado por B ou C.
	var replays []string
	for _, in := range ins {
		replays = append(replays, in.logLines(`"messageId":"kill-msg-a"`, `"idempotentReplay":true`)...)
	}
	if len(replays) == 0 {
		t.Fatal("nenhuma instância registrou a reentrega de kill-msg-a como replay")
	}
	if n := c.scalar(`SELECT count(*) FROM inbox`); n != 2 {
		t.Fatalf("inbox com %d mensagens, quer 2", n)
	}
	c.expectWallet(w.id, 2000, 2, 1, 1)
	c.finish(2)
}

// Item 6: dois publishers disputam a mesma outbox e morrem: P1 depois de
// enviar ao SQS e antes de marcar (publicação sem confirmação na outbox), P2
// depois do claim e antes de enviar (commit sem publicação). Os leases
// gravados por eles expiram e outros dois publishers (P3, P4), também em
// disputa, publicam tudo. Republicações preservam o eventId (o FIFO
// deduplica; o corpo é o snapshot).
func TestFailureOutboxPublishersCrash(t *testing.T) {
	c := newCluster(t, miOptions{})
	// G atende o HTTP com o publisher praticamente parado (poll de 50 min:
	// o primeiro ciclo, no start, encontra a outbox vazia).
	g := c.launch("G", map[string]string{
		config.EnvOutboxPollInterval: "50m", config.EnvOutboxLease: "1h",
	})
	c.waitReady(g)
	h := c.on(g)
	for i := range 8 {
		w := h.openWallet("100.00")
		for j := range 3 {
			if r, err := h.submit(bet(w, fmt.Sprintf("ob-%d-%d", i, j), "10.00")); err != nil || r.status != "PROCESSED" {
				t.Fatalf("aposta: %+v %v", r, err)
			}
		}
	}
	published, total := c.outboxState()
	if published != 0 || total < 50 {
		t.Fatalf("outbox antes dos publishers: %d/%d publicados", published, total)
	}

	small := map[string]string{config.EnvOutboxBatchSize: "4"}
	p1 := c.launch("P1", small, failpoint.OutboxAfterSend+"=crash")
	p2 := c.launch("P2", small, failpoint.OutboxAfterClaim+"=crash")
	p1.waitCrash(t, failpoint.OutboxAfterSend, 60*time.Second)
	p2.waitCrash(t, failpoint.OutboxAfterClaim, 60*time.Second)
	orphan := c.scalar(`SELECT count(*) FROM outbox WHERE published_at IS NULL AND locked_by IS NOT NULL`)
	if orphan == 0 {
		t.Fatal("nenhum lease órfão deixado pelos publishers mortos")
	}
	published, _ = c.outboxState()
	t.Logf("depois dos crashes: %d publicados, %d com lease órfão, de %d", published, orphan, total)

	// P3 e P4 disputam a outbox e recuperam o trabalho abandonado.
	c.start("P3", "P4")
	c.finish(0)
	if n := c.scalar(`SELECT count(*) FROM outbox WHERE attempts >= 2`); n == 0 {
		t.Fatal("nenhum evento foi reivindicado de novo depois dos crashes")
	}
}

// Item 7 entre processos: reversões chegam antes das referências por
// processos e transportes diferentes. As que recebem a referência depois
// (enviada por outro processo) são resolvidas pelo worker de qualquer
// instância; as outras expiram com REFERENCE_NOT_FOUND no TTL. Uma
// referência que chega depois da expiração não reabre a reversão. Replays
// devolvem o estado final.
func TestFailureReversalBeforeReference(t *testing.T) {
	c := newCluster(t, miOptions{env: map[string]string{config.EnvPendingReferenceTTL: "8s"}})
	ins := c.start("A", "B", "C")

	var ws [4]ccWallet
	for i := range ws {
		ws[i] = c.on(ins[i%3]).openWallet("100.00")
	}
	refs := [4]ccWager{
		bet(ws[0], "late-bet-0", "30.00"), bet(ws[1], "late-bet-1", "30.00"),
		bet(ws[2], "never-bet-2", "30.00"), bet(ws[3], "never-bet-3", "30.00"),
	}
	revs := [4]ccWager{
		rollback(ws[0], "rev-0", refs[0]), refund(ws[1], "rev-1", refs[1]),
		rollback(ws[2], "rev-2", refs[2]), refund(ws[3], "rev-3", refs[3]),
	}
	// 0 e 2 pelo HTTP (processos A e B); 1 e 3 pelo SQS.
	for i, o := range revs {
		if i%2 == 1 {
			if err := c.sendSQS(o, "rev-msg-"+o.ext, o.w.id); err != nil {
				t.Fatal(err)
			}
			continue
		}
		r, err := c.on(ins[i/2]).submit(o)
		if err != nil || r.code != http.StatusAccepted || r.status != "PENDING_REFERENCE" {
			t.Fatalf("%s: %+v %v", o.ext, r, err)
		}
	}
	c.on(ins[0]).waitSQS(2)
	for _, o := range revs {
		if st, _ := c.txState(o.ext); st != "PENDING_REFERENCE" {
			t.Fatalf("%s: %s, quer PENDING_REFERENCE", o.ext, st)
		}
	}

	// As referências 0 e 1 chegam por outros processos/transportes.
	if r, err := c.on(ins[2]).submit(refs[0]); err != nil || r.status != "PROCESSED" {
		t.Fatalf("referência 0: %+v %v", r, err)
	}
	if err := c.sendSQS(refs[1], "ref-msg-1", ws[1].id); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, "resolução e expiração das reversões", func() (bool, string) {
		n := c.scalar(`SELECT count(*) FROM wager_transactions WHERE status = 'PENDING_REFERENCE'`)
		return n == 0, fmt.Sprintf("%d pendentes", n)
	})
	for i, o := range revs {
		st, fc := c.txState(o.ext)
		want := [2]string{"PROCESSED", ""}
		if i >= 2 {
			want = [2]string{"REJECTED", "REFERENCE_NOT_FOUND"}
		}
		if st != want[0] || fc != want[1] {
			t.Fatalf("%s: %s %s, quer %v", o.ext, st, fc, want)
		}
	}
	c.expectWallet(ws[0].id, 10000, 3, 2, 1)
	c.expectWallet(ws[1].id, 10000, 3, 2, 1)
	c.expectWallet(ws[2].id, 10000, 1, 1, 0)
	c.expectWallet(ws[3].id, 10000, 1, 1, 0)

	// A referência que chega depois da expiração é processada normalmente,
	// sem reabrir a reversão rejeitada.
	if r, err := c.on(ins[1]).submit(refs[2]); err != nil || r.status != "PROCESSED" || r.balance != "70.00" {
		t.Fatalf("referência tardia: %+v %v", r, err)
	}
	if st, fc := c.txState(revs[2].ext); st != "REJECTED" || fc != "REFERENCE_NOT_FOUND" {
		t.Fatalf("reversão expirada reaberta: %s %s", st, fc)
	}

	// Replays (por outro processo) devolvem o estado final.
	for i, o := range revs {
		r, err := c.on(ins[(i+1)%3]).submit(o)
		if err != nil || !r.replay {
			t.Fatalf("replay de %s: %+v %v", o.ext, r, err)
		}
		st, fc := c.txState(o.ext)
		wantCode := http.StatusOK
		if st == "REJECTED" {
			wantCode = http.StatusUnprocessableEntity
		}
		if r.code != wantCode || r.status != st || r.fc != fc {
			t.Fatalf("replay de %s: %+v, banco %s %s", o.ext, r, st, fc)
		}
	}
	if n := c.scalar(`SELECT count(*) FROM outbox WHERE event_type = 'WagerTransactionPendingReference'`); n != 4 {
		t.Fatalf("%d eventos WagerTransactionPendingReference, quer 4", n)
	}
	c.finish(3)
}

// Item 8: os três processos são mortos (kill, sem shutdown) com operações
// concluídas, mensagens consumidas e reversões em PENDING_REFERENCE. Um
// processo novo reivindica as pendências e morre com o lease gravado (ponto
// de falha). Outros três processos novos: os replays (HTTP e SQS) devolvem
// os resultados originais sem efeito; as pendências preservadas (tentativas
// e TTL persistidos) são resolvidas quando a referência chega ou expiram, e
// o lease do processo morto é respeitado e depois reassumido.
func TestFailureRestartPreservesIdempotencyAndPending(t *testing.T) {
	c := newCluster(t, miOptions{env: map[string]string{config.EnvPendingReferenceTTL: "40s"}})
	old := c.start("A", "B", "C")

	w80 := c.on(old[0]).openWallet("100.00")
	wRes := c.on(old[1]).openWallet("100.00")
	wExp := c.on(old[2]).openWallet("100.00")
	ops := []ccWager{bet(w80, "rs-80-a", "80.00"), bet(w80, "rs-80-b", "80.00")}
	first := map[string]ccResult{}
	for i, r := range c.on(old[0]).submitParallel(ops) {
		first[ops[i].ext] = r
	}
	// Uma aposta pelo SQS.
	sqsBet := bet(wRes, "rs-sqs-bet", "5.00")
	if err := c.sendSQS(sqsBet, "rs-msg-1", wRes.id); err != nil {
		t.Fatal(err)
	}
	c.on(old[0]).waitSQS(1)
	// Reversões antes das referências.
	resRef, expRef := bet(wRes, "rs-ref-res", "20.00"), bet(wExp, "rs-ref-exp", "20.00")
	pend := []ccWager{rollback(wRes, "rs-rev-res", resRef), refund(wExp, "rs-rev-exp", expRef)}
	for i, o := range pend {
		r, err := c.on(old[i+1]).submit(o)
		if err != nil || r.status != "PENDING_REFERENCE" {
			t.Fatalf("%s: %+v %v", o.ext, r, err)
		}
		first[o.ext] = r
	}
	// Espera ao menos uma retomada sem sucesso (tentativas > 1) antes do kill.
	eventually(t, 15*time.Second, "retomadas antes do restart", func() (bool, string) {
		n := c.scalar(`SELECT count(*) FROM wager_transactions WHERE status = 'PENDING_REFERENCE' AND attempts >= 2`)
		return n == 2, fmt.Sprintf("%d pendências com tentativas >= 2", n)
	})

	for _, in := range old {
		in.kill(t)
	}
	attemptsBefore := c.scalar(`SELECT sum(attempts) FROM wager_transactions WHERE status = 'PENDING_REFERENCE'`)
	txBefore := c.scalar(`SELECT count(*) FROM wager_transactions`)
	ledgerBefore := c.scalar(`SELECT count(*) FROM wallet_ledger_entries`)

	// D reivindica as pendências e morre com o lease gravado.
	d := c.launch("D", nil, failpoint.PendingAfterClaim+"=crash")
	d.waitCrash(t, failpoint.PendingAfterClaim, 60*time.Second)
	leased := c.scalar(`SELECT count(*) FROM wager_transactions WHERE claimed_until > now()`)
	if leased == 0 {
		t.Fatal("o processo morto não deixou lease gravado")
	}
	t.Logf("pendências com lease do processo morto: %d", leased)

	fresh := c.start("E", "F", "G")
	// Replays HTTP em processos novos: resultado original, sem efeito.
	for i, o := range slices.Concat(ops, pend) {
		r, err := c.on(fresh[i%3]).submit(o)
		if err != nil {
			t.Fatal(err)
		}
		f := first[o.ext]
		if !r.replay || r.txID != f.txID || r.code != f.code || r.fc != f.fc || r.balance != f.balance {
			t.Fatalf("replay de %s depois do restart: %+v, original %+v", o.ext, r, f)
		}
	}
	// Reentrega SQS do mesmo messageId: replay pela inbox.
	if err := c.sendSQS(sqsBet, "rs-msg-1", wRes.id); err != nil {
		t.Fatal(err)
	}
	c.on(fresh[0]).waitSQS(1)
	if n := c.scalar(`SELECT count(*) FROM wager_transactions`); n != txBefore {
		t.Fatalf("replays criaram %d transações", n-txBefore)
	}
	if n := c.scalar(`SELECT count(*) FROM wallet_ledger_entries`); n != ledgerBefore {
		t.Fatalf("replays criaram %d lançamentos", n-ledgerBefore)
	}
	c.expectWallet(w80.id, 2000, 2, 1, 1)

	// A referência de uma pendência chega; a outra nunca chega.
	if r, err := c.on(fresh[1]).submit(resRef); err != nil || r.status != "PROCESSED" {
		t.Fatalf("referência: %+v %v", r, err)
	}
	eventually(t, 90*time.Second, "resolução e expiração depois do restart", func() (bool, string) {
		n := c.scalar(`SELECT count(*) FROM wager_transactions WHERE status = 'PENDING_REFERENCE'`)
		return n == 0, fmt.Sprintf("%d pendentes", n)
	})
	if st, fc := c.txState("rs-rev-res"); st != "PROCESSED" || fc != "" {
		t.Fatalf("rs-rev-res: %s %s", st, fc)
	}
	if st, fc := c.txState("rs-rev-exp"); st != "REJECTED" || fc != "REFERENCE_NOT_FOUND" {
		t.Fatalf("rs-rev-exp: %s %s", st, fc)
	}
	if n := c.scalar(`SELECT sum(attempts) FROM wager_transactions WHERE external_transaction_id IN ('rs-rev-res', 'rs-rev-exp')`); n < attemptsBefore {
		t.Fatalf("tentativas regrediram no restart: %d < %d", n, attemptsBefore)
	}
	c.expectWallet(wRes.id, 9500, 4, 2, 2) // abertura, aposta SQS, referência, rollback
	c.expectWallet(wExp.id, 10000, 1, 1, 0)
	c.finish(1)
}

// --- Indisponibilidade temporária -------------------------------------------------

// workload envia apostas de 1.00 (3 de cada 4 por HTTP, com o provedor
// at-least-once reenviando a mesma operação em 503/erro de transporte; 1 de
// cada 4 pelo SQS) a três processos, em todas as carteiras, enquanto faults
// provoca as falhas, e continua um pouco depois da recuperação. Devolve o
// número de operações por carteira e de mensagens SQS.
// wlResult resume o workload.
type wlResult struct {
	perWallet []int
	sqsMsgs   int
	retried   int // operações HTTP que precisaram de reenvio (503 / transporte)
	replayed  int // das reenviadas, as que voltaram como replay (commit ambíguo)
}

func (c *cluster) workload(ins []*instance, wallets []ccWallet, faults func()) wlResult {
	t := c.t
	t.Helper()
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
		stop = make(chan struct{})
	)
	var res wlResult
	res.perWallet = make([]int, len(wallets))
	for wi, w := range wallets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; ; j++ {
				select {
				case <-stop:
					return
				default:
				}
				o := bet(w, fmt.Sprintf("wl-%d-%d", wi, j), "1.00")
				var err error
				if j%4 == 3 {
					err = c.sendSQS(o, "wl-msg-"+o.ext, w.id)
					if err == nil {
						mu.Lock()
						res.sqsMsgs++
						mu.Unlock()
					}
				} else {
					r, n, serr := c.on(ins[(wi+j)%len(ins)]).submitRetry(o, 90*time.Second)
					err = serr
					if err == nil && r.status != "PROCESSED" {
						err = fmt.Errorf("%s: %+v", o.ext, r)
					}
					if n > 1 {
						mu.Lock()
						res.retried++
						if r.replay {
							res.replayed++
						}
						mu.Unlock()
					}
				}
				mu.Lock()
				res.perWallet[wi]++
				if err != nil {
					errs = append(errs, err)
				}
				mu.Unlock()
				if err != nil {
					return
				}
				time.Sleep(200 * time.Millisecond)
			}
		}()
	}
	faults()
	time.Sleep(time.Second) // carga também depois da recuperação
	close(stop)
	wg.Wait()
	if len(errs) != 0 {
		t.Fatalf("workload: %v", errs)
	}
	t.Logf("workload: %v operações por carteira, %d pelo SQS; %d operações HTTP reenviadas, %d delas como replay",
		res.perWallet, res.sqsMsgs, res.retried, res.replayed)
	return res
}

// expectWorkload: toda operação do workload processada exatamente uma vez.
func (c *cluster) expectWorkload(wallets []ccWallet, perWallet []int) {
	t := c.t
	t.Helper()
	total := 0
	for wi, w := range wallets {
		n := perWallet[wi]
		total += n
		c.expectWallet(w.id, int64(100000-100*n), int64(1+n), 1, int64(n))
	}
	if n := c.scalar(`SELECT count(*) FROM wager_transactions WHERE external_transaction_id LIKE 'wl-%'`); n != int64(total) {
		t.Fatalf("%d operações gravadas, quer %d", n, total)
	}
	if n := c.scalar(`SELECT count(*) FROM wager_transactions WHERE external_transaction_id LIKE 'wl-%' AND status <> 'PROCESSED'`); n != 0 {
		t.Fatalf("%d operações não processadas", n)
	}
}

// readyDown espera alguma instância relatar 503 no readiness com a
// verificação name no corpo durante a falha.
func (c *cluster) readyDown(ins []*instance, name string, timeout time.Duration) {
	c.t.Helper()
	eventually(c.t, timeout, "readiness DOWN de "+name, func() (bool, string) {
		for _, in := range ins {
			code, body, err := c.ready(in)
			if err == nil && code == http.StatusServiceUnavailable && strings.Contains(fmt.Sprint(body), name) {
				return true, ""
			}
		}
		return false, "todas prontas"
	})
}

// outageScript: indisponibilidade "reset" de 3s (readiness DOWN, depois
// pronto de novo), 4s normais e indisponibilidade "pause" de 3s.
func (c *cluster) outageScript(ins []*instance, p *tcpProxy, check string) func() {
	return func() {
		time.Sleep(time.Second)
		c.t.Logf("%s: reset", check)
		p.outage("reset")
		c.readyDown(ins, check, 10*time.Second)
		time.Sleep(3 * time.Second)
		p.restore()
		c.waitReady(ins...)
		time.Sleep(4 * time.Second)
		c.t.Logf("%s: pause", check)
		p.outage("pause")
		time.Sleep(3 * time.Second)
		p.restore()
		c.t.Logf("%s: restaurado", check)
	}
}

func (c *cluster) openWallets(ins []*instance, n int, amount string) []ccWallet {
	c.t.Helper()
	wallets := make([]ccWallet, n)
	for i := range wallets {
		wallets[i] = c.on(ins[i%len(ins)]).openWallet(amount)
	}
	return wallets
}

// PostgreSQL indisponível (conexões derrubadas e recusadas; depois tráfego
// congelado, como docker pause) com três processos sob carga HTTP e SQS. O
// readiness mostra a falha; as operações HTTP recebem 503 e são reenviadas
// pelo provedor com a mesma chave (inclusive as de commit ambíguo); as
// mensagens SQS voltam com backoff. Nada se perde nem duplica, a DLQ fica
// vazia e a outbox é entregue.
func TestFailurePostgresUnavailable(t *testing.T) {
	pg := newTCPProxy(t, mustParseURL(t, adminDSN).Host)
	c := newCluster(t, miOptions{pgProxy: pg})
	ins := c.start("A", "B", "C")
	wallets := c.openWallets(ins, 6, "1000.00")
	wl := c.workload(ins, wallets, c.outageScript(ins, pg, "postgres"))
	if wl.retried == 0 {
		t.Fatal("nenhuma operação HTTP precisou de reenvio: a indisponibilidade não foi exercitada")
	}
	c.waitReady(ins...)
	c.on(ins[0]).waitSQS(wl.sqsMsgs)
	c.expectWorkload(wallets, wl.perWallet)
	if n := c.scalar(`SELECT count(*) FROM inbox WHERE completed_at IS NULL`); n != 0 {
		t.Fatalf("%d mensagens na inbox sem conclusão", n)
	}
	c.finish(wl.sqsMsgs)
}

// SQS indisponível (conexões derrubadas e recusadas; depois congeladas) para
// os três processos: o readiness mostra a falha; o consumidor volta a
// receber depois; o publisher registra a falha de envio (backoff persistido)
// e publica tudo quando o SQS volta. Nenhuma mensagem vai à DLQ, nenhum
// evento se perde.
func TestFailureSQSUnavailable(t *testing.T) {
	endpoint := mustParseURL(t, strings.TrimSuffix(envOr("SQS_ENDPOINT", "http://localhost:4566"), "/"))
	proxy := newTCPProxy(t, endpoint.Host)
	c := newCluster(t, miOptions{sqsProxy: proxy})
	ins := c.start("A", "B", "C")
	wallets := c.openWallets(ins, 6, "1000.00")
	wl := c.workload(ins, wallets, c.outageScript(ins, proxy, "sqs"))
	c.waitReady(ins...)
	c.on(ins[0]).waitSQS(wl.sqsMsgs)
	c.expectWorkload(wallets, wl.perWallet)
	if n := c.scalar(`SELECT count(*) FROM outbox WHERE last_error IS NOT NULL OR attempts >= 2`); n == 0 {
		t.Fatal("nenhuma publicação falhou durante a indisponibilidade do SQS (a falha não foi exercitada)")
	}
	c.finish(wl.sqsMsgs)
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}
