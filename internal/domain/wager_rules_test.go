package domain

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

// --- Fixtures -------------------------------------------------------------------

var (
	otherPlayerID = uuid.MustParse("0192f28f-5dc0-7d58-bdb2-814ad6a0f4b2")
	otherWalletID = uuid.MustParse("0192f291-27dd-7d3f-8071-5f8685deef48")
)

func ruleID(name string) uuid.UUID { return uuid.NewSHA1(uuid.NameSpaceOID, []byte(name)) }

// txSpec descreve uma transação externa; campos vazios assumem os valores
// padrão (provider-a, round-987, testPlayerID, testWalletID, BRL).
type txSpec struct {
	kind     WagerKind
	ext      string
	ref      string
	amount   string
	currency string
	provider string
	round    string
	player   uuid.UUID
	wallet   uuid.UUID
}

func buildTx(t *testing.T, s txSpec) *WagerTransaction {
	t.Helper()
	def := func(v, d string) string {
		if v == "" {
			return d
		}
		return v
	}
	amount := def(s.amount, "25.00")
	if s.kind == WagerKindLoss && s.amount == "" {
		amount = "0.00"
	}
	player, wallet := s.player, s.wallet
	if player == uuid.Nil {
		player = testPlayerID
	}
	if wallet == uuid.Nil {
		wallet = testWalletID
	}
	provider := def(s.provider, "provider-a")
	tx, err := NewExternalWagerTransaction(NewExternalWagerTransactionParams{
		ID:                             ruleID(provider + "/" + s.ext),
		ProviderID:                     provider,
		ExternalTransactionID:          s.ext,
		IdempotencyKey:                 provider + ":" + s.ext,
		RequestHash:                    testHash,
		WalletID:                       wallet,
		PlayerID:                       player,
		RoundID:                        def(s.round, "round-987"),
		GameID:                         "fortune-chimp",
		Kind:                           s.kind,
		Money:                          mustParse(t, amount, def(s.currency, "BRL")),
		ReferenceExternalTransactionID: s.ref,
		Now:                            testT0,
	})
	if err != nil {
		t.Fatalf("buildTx(%+v): %v", s, err)
	}
	return tx
}

// withStatus leva tx ao estado pedido por transições válidas. PROCESSED usa
// refID como referência resolvida quando tx tem referência.
func withStatus(t *testing.T, tx *WagerTransaction, s WagerStatus, refID uuid.UUID) *WagerTransaction {
	t.Helper()
	now := testT0.Add(time.Second)
	var err error
	switch s {
	case WagerStatusPending:
	case WagerStatusPendingReference:
		err = tx.MarkPendingReference(MarkWagerPendingReferenceParams{NextAttemptAt: now.Add(time.Minute), Now: now})
	case WagerStatusProcessed:
		var ref uuid.UUID
		if tx.HasReference() {
			ref = refID
		}
		err = tx.MarkProcessed(MarkWagerProcessedParams{BalanceAfter: mustParse(t, "100.00", tx.Money().Currency().String()), ReferenceTransactionID: ref, Now: now})
	case WagerStatusRejected:
		err = tx.Reject(RejectWagerParams{Code: FailureCodeCurrencyMismatch, Now: now})
	case WagerStatusFailed:
		err = tx.Fail(FailWagerParams{Code: FailureCodeProcessingFailed, Now: now})
	}
	if err != nil {
		t.Fatalf("withStatus(%s, %s): %v", tx.Kind(), s, err)
	}
	return tx
}

// Referências padrão (todas PROCESSED salvo indicação).
func betRef(t *testing.T, s WagerStatus) *WagerTransaction {
	return withStatus(t, buildTx(t, txSpec{kind: WagerKindBet, ext: "bet-1"}), s, uuid.Nil)
}

func refOf(t *testing.T, k WagerKind, amount string, s WagerStatus) *WagerTransaction {
	t.Helper()
	sp := txSpec{kind: k, ext: "ref-" + string(k), amount: amount}
	if k.RequiresReference() {
		sp.ref = "bet-0"
	}
	return withStatus(t, buildTx(t, sp), s, ruleID("bet-0"))
}

func reversalOf(t *testing.T, k WagerKind, ext string, ref *WagerTransaction, s WagerStatus) *WagerTransaction {
	t.Helper()
	tx := buildTx(t, txSpec{kind: k, ext: ext, ref: ref.ExternalTransactionID(), amount: ref.Money().Amount()})
	return withStatus(t, tx, s, ref.ID())
}

func openingRef(t *testing.T) *WagerTransaction {
	t.Helper()
	tx, err := NewOpeningWagerTransaction(NewOpeningWagerTransactionParams{
		ID: testOpenTxID, WalletID: testWalletID, PlayerID: testPlayerID, Amount: mustParse(t, "100.00", "BRL"), Now: testT0,
	})
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func walletWith(t *testing.T, balance, currency string, player uuid.UUID) *Wallet {
	t.Helper()
	w, err := RehydrateWallet(RehydrateWalletParams{
		ID: testWalletID, PlayerID: player, Balance: mustParse(t, balance, currency),
		Version: 3, CreatedAt: testT0, UpdatedAt: testT0,
	})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// --- DecideWager: tabela ----------------------------------------------------------

type ruleCase struct {
	name      string
	tx        func(t *testing.T) *WagerTransaction
	wallet    func(t *testing.T) *Wallet // nil => carteira 100.00 BRL do jogador
	noWallet  bool
	ref       func(t *testing.T) *WagerTransaction
	reversals func(t *testing.T, ref *WagerTransaction) []*WagerTransaction

	outcome WagerDecisionOutcome
	dir     LedgerDirection
	code    FailureCode
	await   WagerAwaitReason
	after   string // saldo projetado/observado; "" => Money{}
	refID   bool   // decisão carrega o id da referência
}

func txOf(s txSpec) func(t *testing.T) *WagerTransaction {
	return func(t *testing.T) *WagerTransaction { return buildTx(t, s) }
}

func rev(k WagerKind, ref, amount string) func(t *testing.T) *WagerTransaction {
	return txOf(txSpec{kind: k, ext: "rev-" + string(k), ref: ref, amount: amount})
}

func ruleCases() []ruleCase {
	bal := func(b string) func(t *testing.T) *Wallet {
		return func(t *testing.T) *Wallet { return walletWith(t, b, "BRL", testPlayerID) }
	}
	bet := func(s WagerStatus) func(t *testing.T) *WagerTransaction {
		return func(t *testing.T) *WagerTransaction { return betRef(t, s) }
	}
	kindRef := func(k WagerKind, amount string, s WagerStatus) func(t *testing.T) *WagerTransaction {
		return func(t *testing.T) *WagerTransaction { return refOf(t, k, amount, s) }
	}
	betWith := func(mut func(*txSpec)) func(t *testing.T) *WagerTransaction {
		return func(t *testing.T) *WagerTransaction {
			s := txSpec{kind: WagerKindBet, ext: "bet-1"}
			mut(&s)
			return withStatus(t, buildTx(t, s), WagerStatusProcessed, uuid.Nil)
		}
	}
	reversals := func(specs ...struct {
		k WagerKind
		s WagerStatus
	}) func(t *testing.T, ref *WagerTransaction) []*WagerTransaction {
		return func(t *testing.T, ref *WagerTransaction) []*WagerTransaction {
			var out []*WagerTransaction
			for i, sp := range specs {
				out = append(out, reversalOf(t, sp.k, fmt.Sprintf("prev-%d", i), ref, sp.s))
			}
			return out
		}
	}
	type rs = struct {
		k WagerKind
		s WagerStatus
	}
	refundBet := rev(WagerKindRefund, "bet-1", "25.00")
	rollbackBet := rev(WagerKindRollback, "bet-1", "25.00")

	cases := []ruleCase{
		// BET
		{name: "BET debits", tx: txOf(txSpec{kind: WagerKindBet, ext: "b"}), outcome: WagerDecisionApplyMovement, dir: LedgerDirectionDebit, after: "75.00"},
		{name: "BET exact balance", tx: txOf(txSpec{kind: WagerKindBet, ext: "b", amount: "100.00"}), outcome: WagerDecisionApplyMovement, dir: LedgerDirectionDebit, after: "0.00"},
		{name: "BET insufficient funds", tx: txOf(txSpec{kind: WagerKindBet, ext: "b", amount: "100.01"}), outcome: WagerDecisionReject, code: FailureCodeInsufficientFunds, after: "100.00"},
		{name: "BET on empty wallet", tx: txOf(txSpec{kind: WagerKindBet, ext: "b", amount: "0.01"}), wallet: bal("0.00"), outcome: WagerDecisionReject, code: FailureCodeInsufficientFunds, after: "0.00"},
		// LOSS
		{name: "LOSS no movement", tx: txOf(txSpec{kind: WagerKindLoss, ext: "l"}), outcome: WagerDecisionNoMovement, after: "100.00"},
		{name: "LOSS on empty wallet", tx: txOf(txSpec{kind: WagerKindLoss, ext: "l"}), wallet: bal("0.00"), outcome: WagerDecisionNoMovement, after: "0.00"},
		// WIN
		{name: "WIN without reference credits", tx: txOf(txSpec{kind: WagerKindWin, ext: "w", amount: "50.00"}), outcome: WagerDecisionApplyMovement, dir: LedgerDirectionCredit, after: "150.00"},
		{name: "WIN with BET reference credits own amount", tx: txOf(txSpec{kind: WagerKindWin, ext: "w", ref: "bet-1", amount: "60.00"}), ref: bet(WagerStatusProcessed), outcome: WagerDecisionApplyMovement, dir: LedgerDirectionCredit, after: "160.00", refID: true},
		{name: "WIN reference missing", tx: txOf(txSpec{kind: WagerKindWin, ext: "w", ref: "bet-1"}), outcome: WagerDecisionAwaitReference, await: WagerAwaitReferenceMissing},
		{name: "WIN reference pending", tx: txOf(txSpec{kind: WagerKindWin, ext: "w", ref: "bet-1"}), ref: bet(WagerStatusPending), outcome: WagerDecisionAwaitReference, await: WagerAwaitReferencePending},
		{name: "WIN reference rejected", tx: txOf(txSpec{kind: WagerKindWin, ext: "w", ref: "bet-1"}), ref: bet(WagerStatusRejected), outcome: WagerDecisionReject, code: FailureCodeReferenceNotProcessed, after: "100.00", refID: true},
		{name: "WIN reference failed", tx: txOf(txSpec{kind: WagerKindWin, ext: "w", ref: "bet-1"}), ref: bet(WagerStatusFailed), outcome: WagerDecisionReject, code: FailureCodeReferenceNotProcessed, after: "100.00", refID: true},
		{name: "WIN reference other round", tx: txOf(txSpec{kind: WagerKindWin, ext: "w", ref: "bet-1"}), ref: betWith(func(s *txSpec) { s.round = "round-1" }), outcome: WagerDecisionReject, code: FailureCodeReferenceMismatch, after: "100.00", refID: true},
		{name: "WIN referencing WIN", tx: txOf(txSpec{kind: WagerKindWin, ext: "w", ref: "ref-WIN"}), ref: kindRef(WagerKindWin, "25.00", WagerStatusProcessed), outcome: WagerDecisionReject, code: FailureCodeReferenceNotReversible, after: "100.00", refID: true},
		{name: "WIN referencing LOSS", tx: txOf(txSpec{kind: WagerKindWin, ext: "w", ref: "ref-LOSS"}), ref: kindRef(WagerKindLoss, "0.00", WagerStatusProcessed), outcome: WagerDecisionReject, code: FailureCodeReferenceNotReversible, after: "100.00", refID: true},

		// REFUND
		{name: "REFUND of BET credits", tx: refundBet, ref: bet(WagerStatusProcessed), outcome: WagerDecisionApplyMovement, dir: LedgerDirectionCredit, after: "125.00", refID: true},
		{name: "REFUND of WIN not reversible", tx: rev(WagerKindRefund, "ref-WIN", "25.00"), ref: kindRef(WagerKindWin, "25.00", WagerStatusProcessed), outcome: WagerDecisionReject, code: FailureCodeReferenceNotReversible, after: "100.00", refID: true},
		{name: "REFUND of LOSS not reversible", tx: rev(WagerKindRefund, "ref-LOSS", "25.00"), ref: kindRef(WagerKindLoss, "0.00", WagerStatusProcessed), outcome: WagerDecisionReject, code: FailureCodeReferenceNotReversible, after: "100.00", refID: true},
		{name: "REFUND of REFUND not reversible", tx: rev(WagerKindRefund, "ref-REFUND", "25.00"), ref: kindRef(WagerKindRefund, "25.00", WagerStatusProcessed), outcome: WagerDecisionReject, code: FailureCodeReferenceNotReversible, after: "100.00", refID: true},
		{name: "REFUND of ROLLBACK not reversible", tx: rev(WagerKindRefund, "ref-ROLLBACK", "25.00"), ref: kindRef(WagerKindRollback, "25.00", WagerStatusProcessed), outcome: WagerDecisionReject, code: FailureCodeReferenceNotReversible, after: "100.00", refID: true},
		{name: "REFUND of OPENING not reversible", tx: rev(WagerKindRefund, "opening", "100.00"), ref: openingRef, outcome: WagerDecisionReject, code: FailureCodeReferenceNotReversible, after: "100.00", refID: true},

		// ROLLBACK
		{name: "ROLLBACK of BET credits", tx: rollbackBet, ref: bet(WagerStatusProcessed), outcome: WagerDecisionApplyMovement, dir: LedgerDirectionCredit, after: "125.00", refID: true},
		{name: "ROLLBACK of WIN debits", tx: rev(WagerKindRollback, "ref-WIN", "40.00"), ref: kindRef(WagerKindWin, "40.00", WagerStatusProcessed), outcome: WagerDecisionApplyMovement, dir: LedgerDirectionDebit, after: "60.00", refID: true},
		{name: "ROLLBACK of REFUND debits", tx: rev(WagerKindRollback, "ref-REFUND", "25.00"), ref: kindRef(WagerKindRefund, "25.00", WagerStatusProcessed), outcome: WagerDecisionApplyMovement, dir: LedgerDirectionDebit, after: "75.00", refID: true},
		{name: "ROLLBACK of WIN exact balance", tx: rev(WagerKindRollback, "ref-WIN", "100.00"), ref: kindRef(WagerKindWin, "100.00", WagerStatusProcessed), outcome: WagerDecisionApplyMovement, dir: LedgerDirectionDebit, after: "0.00", refID: true},
		{name: "ROLLBACK of WIN without funds", tx: rev(WagerKindRollback, "ref-WIN", "40.00"), wallet: bal("39.99"), ref: kindRef(WagerKindWin, "40.00", WagerStatusProcessed), outcome: WagerDecisionReject, code: FailureCodeReversalInsufficientFunds, after: "39.99", refID: true},
		{name: "ROLLBACK of REFUND without funds", tx: rev(WagerKindRollback, "ref-REFUND", "25.00"), wallet: bal("10.00"), ref: kindRef(WagerKindRefund, "25.00", WagerStatusProcessed), outcome: WagerDecisionReject, code: FailureCodeReversalInsufficientFunds, after: "10.00", refID: true},
		{name: "ROLLBACK of BET on empty wallet credits", tx: rollbackBet, wallet: bal("0.00"), ref: bet(WagerStatusProcessed), outcome: WagerDecisionApplyMovement, dir: LedgerDirectionCredit, after: "25.00", refID: true},
		{name: "ROLLBACK of LOSS not reversible", tx: rev(WagerKindRollback, "ref-LOSS", "25.00"), ref: kindRef(WagerKindLoss, "0.00", WagerStatusProcessed), outcome: WagerDecisionReject, code: FailureCodeReferenceNotReversible, after: "100.00", refID: true},
		{name: "ROLLBACK of ROLLBACK not reversible", tx: rev(WagerKindRollback, "ref-ROLLBACK", "25.00"), ref: kindRef(WagerKindRollback, "25.00", WagerStatusProcessed), outcome: WagerDecisionReject, code: FailureCodeReferenceNotReversible, after: "100.00", refID: true},
		{name: "ROLLBACK of OPENING not reversible", tx: rev(WagerKindRollback, "opening", "100.00"), ref: openingRef, outcome: WagerDecisionReject, code: FailureCodeReferenceNotReversible, after: "100.00", refID: true},

		// Carteira
		{name: "wallet not found", tx: txOf(txSpec{kind: WagerKindBet, ext: "b"}), noWallet: true, outcome: WagerDecisionReject, code: FailureCodeWalletNotFound},
		{name: "wallet of another player", tx: txOf(txSpec{kind: WagerKindBet, ext: "b"}), wallet: func(t *testing.T) *Wallet { return walletWith(t, "100.00", "BRL", otherPlayerID) }, outcome: WagerDecisionReject, code: FailureCodeWalletPlayerMismatch},
		{name: "wallet currency differs", tx: txOf(txSpec{kind: WagerKindBet, ext: "b"}), wallet: func(t *testing.T) *Wallet { return walletWith(t, "100.00", "USD", testPlayerID) }, outcome: WagerDecisionReject, code: FailureCodeCurrencyMismatch},
		{name: "LOSS currency differs from wallet", tx: txOf(txSpec{kind: WagerKindLoss, ext: "l", currency: "USD"}), outcome: WagerDecisionReject, code: FailureCodeCurrencyMismatch},
		{name: "wallet check precedes missing reference", tx: refundBet, noWallet: true, outcome: WagerDecisionReject, code: FailureCodeWalletNotFound},

		// Mismatches com a referência
		{name: "reference other provider", tx: refundBet, ref: betWith(func(s *txSpec) { s.provider = "provider-b" }), outcome: WagerDecisionReject, code: FailureCodeReferenceMismatch, after: "100.00", refID: true},
		{name: "reference other player", tx: refundBet, ref: betWith(func(s *txSpec) { s.player = otherPlayerID }), outcome: WagerDecisionReject, code: FailureCodeReferenceMismatch, after: "100.00", refID: true},
		{name: "reference other wallet", tx: refundBet, ref: betWith(func(s *txSpec) { s.wallet = otherWalletID }), outcome: WagerDecisionReject, code: FailureCodeReferenceMismatch, after: "100.00", refID: true},
		{name: "reference other round", tx: rollbackBet, ref: betWith(func(s *txSpec) { s.round = "round-2" }), outcome: WagerDecisionReject, code: FailureCodeReferenceMismatch, after: "100.00", refID: true},
		{name: "reference other currency", tx: refundBet, ref: betWith(func(s *txSpec) { s.currency = "USD" }), outcome: WagerDecisionReject, code: FailureCodeCurrencyMismatch, after: "100.00", refID: true},
		{name: "REFUND amount differs", tx: rev(WagerKindRefund, "bet-1", "24.99"), ref: bet(WagerStatusProcessed), outcome: WagerDecisionReject, code: FailureCodeAmountMismatch, after: "100.00", refID: true},
		{name: "ROLLBACK amount differs", tx: rev(WagerKindRollback, "bet-1", "25.01"), ref: bet(WagerStatusProcessed), outcome: WagerDecisionReject, code: FailureCodeAmountMismatch, after: "100.00", refID: true},
		// Precedência
		{name: "mismatch precedes not reversible", tx: rev(WagerKindRefund, "bet-1", "25.00"), ref: func(t *testing.T) *WagerTransaction {
			return withStatus(t, buildTx(t, txSpec{kind: WagerKindLoss, ext: "bet-1", player: otherPlayerID}), WagerStatusProcessed, uuid.Nil)
		}, outcome: WagerDecisionReject, code: FailureCodeReferenceMismatch, after: "100.00", refID: true},
		{name: "not reversible precedes amount", tx: rev(WagerKindRollback, "ref-LOSS", "25.00"), ref: kindRef(WagerKindLoss, "0.00", WagerStatusPending), outcome: WagerDecisionReject, code: FailureCodeReferenceNotReversible, after: "100.00", refID: true},
		{name: "amount mismatch does not wait for pending reference", tx: rev(WagerKindRefund, "bet-1", "1.00"), ref: bet(WagerStatusPending), outcome: WagerDecisionReject, code: FailureCodeAmountMismatch, after: "100.00", refID: true},
		{name: "already reversed precedes funds", tx: rev(WagerKindRollback, "ref-WIN", "40.00"), wallet: bal("0.00"), ref: kindRef(WagerKindWin, "40.00", WagerStatusProcessed), reversals: reversals(rs{WagerKindRollback, WagerStatusProcessed}), outcome: WagerDecisionReject, code: FailureCodeAlreadyReversed, after: "0.00", refID: true},

		// Combinações REFUND x ROLLBACK sobre a mesma BET
		{name: "REFUND after REFUND", tx: refundBet, ref: bet(WagerStatusProcessed), reversals: reversals(rs{WagerKindRefund, WagerStatusProcessed}), outcome: WagerDecisionReject, code: FailureCodeAlreadyReversed, after: "100.00", refID: true},
		{name: "ROLLBACK after ROLLBACK", tx: rollbackBet, ref: bet(WagerStatusProcessed), reversals: reversals(rs{WagerKindRollback, WagerStatusProcessed}), outcome: WagerDecisionReject, code: FailureCodeAlreadyReversed, after: "100.00", refID: true},
		{name: "ROLLBACK after REFUND of same BET", tx: rollbackBet, ref: bet(WagerStatusProcessed), reversals: reversals(rs{WagerKindRefund, WagerStatusProcessed}), outcome: WagerDecisionReject, code: FailureCodeAlreadyReversed, after: "100.00", refID: true},
		{name: "REFUND after ROLLBACK of same BET", tx: refundBet, ref: bet(WagerStatusProcessed), reversals: reversals(rs{WagerKindRollback, WagerStatusProcessed}), outcome: WagerDecisionReject, code: FailureCodeAlreadyReversed, after: "100.00", refID: true},
		{name: "unsuccessful reversals do not count", tx: refundBet, ref: bet(WagerStatusProcessed), reversals: reversals(
			rs{WagerKindRefund, WagerStatusRejected}, rs{WagerKindRollback, WagerStatusFailed},
			rs{WagerKindRollback, WagerStatusPending}, rs{WagerKindRefund, WagerStatusPendingReference},
		), outcome: WagerDecisionApplyMovement, dir: LedgerDirectionCredit, after: "125.00", refID: true},
		{name: "ROLLBACK of REFUND after its ROLLBACK", tx: rev(WagerKindRollback, "ref-REFUND", "25.00"), ref: kindRef(WagerKindRefund, "25.00", WagerStatusProcessed), reversals: reversals(rs{WagerKindRollback, WagerStatusProcessed}), outcome: WagerDecisionReject, code: FailureCodeAlreadyReversed, after: "100.00", refID: true},
		{name: "ROLLBACK of WIN twice", tx: rev(WagerKindRollback, "ref-WIN", "40.00"), ref: kindRef(WagerKindWin, "40.00", WagerStatusProcessed), reversals: reversals(rs{WagerKindRollback, WagerStatusProcessed}), outcome: WagerDecisionReject, code: FailureCodeAlreadyReversed, after: "100.00", refID: true},
		{name: "transaction itself in reversal list is ignored", tx: refundBet, ref: bet(WagerStatusProcessed), reversals: func(t *testing.T, ref *WagerTransaction) []*WagerTransaction {
			self := buildTx(t, txSpec{kind: WagerKindRefund, ext: "rev-REFUND", ref: "bet-1"})
			return []*WagerTransaction{self}
		}, outcome: WagerDecisionApplyMovement, dir: LedgerDirectionCredit, after: "125.00", refID: true},
	}

	// Todos os estados da referência, para REFUND e ROLLBACK (e WIN acima).
	for _, k := range []WagerKind{WagerKindRefund, WagerKindRollback} {
		cases = append(cases,
			ruleCase{name: k.String() + " reference missing", tx: rev(k, "bet-1", "25.00"), outcome: WagerDecisionAwaitReference, await: WagerAwaitReferenceMissing},
			ruleCase{name: k.String() + " reference PENDING", tx: rev(k, "bet-1", "25.00"), ref: bet(WagerStatusPending), outcome: WagerDecisionAwaitReference, await: WagerAwaitReferencePending},
			ruleCase{name: k.String() + " reference PROCESSED", tx: rev(k, "bet-1", "25.00"), ref: bet(WagerStatusProcessed), outcome: WagerDecisionApplyMovement, dir: LedgerDirectionCredit, after: "125.00", refID: true},
			ruleCase{name: k.String() + " reference REJECTED", tx: rev(k, "bet-1", "25.00"), ref: bet(WagerStatusRejected), outcome: WagerDecisionReject, code: FailureCodeReferenceNotProcessed, after: "100.00", refID: true},
			ruleCase{name: k.String() + " reference FAILED", tx: rev(k, "bet-1", "25.00"), ref: bet(WagerStatusFailed), outcome: WagerDecisionReject, code: FailureCodeReferenceNotProcessed, after: "100.00", refID: true},
		)
	}
	// Referência em PENDING_REFERENCE (só reversões/WIN com referência chegam lá).
	cases = append(cases,
		ruleCase{name: "ROLLBACK of REFUND in PENDING_REFERENCE", tx: rev(WagerKindRollback, "ref-REFUND", "25.00"), ref: kindRef(WagerKindRefund, "25.00", WagerStatusPendingReference), outcome: WagerDecisionAwaitReference, await: WagerAwaitReferencePending},
		ruleCase{name: "ROLLBACK of REFUND REJECTED", tx: rev(WagerKindRollback, "ref-REFUND", "25.00"), ref: kindRef(WagerKindRefund, "25.00", WagerStatusRejected), outcome: WagerDecisionReject, code: FailureCodeReferenceNotProcessed, after: "100.00", refID: true},
		ruleCase{name: "ROLLBACK of WIN in PENDING_REFERENCE", tx: rev(WagerKindRollback, "ref-WIN", "25.00"), ref: func(t *testing.T) *WagerTransaction {
			return withStatus(t, buildTx(t, txSpec{kind: WagerKindWin, ext: "ref-WIN", ref: "bet-0"}), WagerStatusPendingReference, uuid.Nil)
		}, outcome: WagerDecisionAwaitReference, await: WagerAwaitReferencePending},
	)
	return cases
}

func TestDecideWager_Table(t *testing.T) {
	for _, c := range ruleCases() {
		t.Run(c.name, func(t *testing.T) {
			tx := c.tx(t)
			var w *Wallet
			if !c.noWallet {
				if c.wallet != nil {
					w = c.wallet(t)
				} else {
					w = walletWith(t, "100.00", "BRL", testPlayerID)
				}
			}
			var ref *WagerTransaction
			if c.ref != nil {
				ref = c.ref(t)
			}
			var revs []*WagerTransaction
			if c.reversals != nil {
				revs = c.reversals(t, ref)
			}
			txBefore, refBefore := *tx, snapshotTx(ref)
			var wBefore Wallet
			if w != nil {
				wBefore = *w
			}

			d, err := DecideWager(WagerRuleInput{Transaction: tx, Wallet: w, Reference: ref, ReferenceReversals: revs})
			if err != nil {
				t.Fatalf("DecideWager: %v", err)
			}
			// Pureza: nenhuma entrada é mutada.
			if !reflect.DeepEqual(txBefore, *tx) || !reflect.DeepEqual(refBefore, snapshotTx(ref)) || (w != nil && !reflect.DeepEqual(wBefore, *w)) {
				t.Fatal("DecideWager mutated its input")
			}
			assertDecision(t, c, d, tx, ref)
			assertApply(t, d, tx, w)
		})
	}
}

func snapshotTx(tx *WagerTransaction) WagerTransaction {
	if tx == nil {
		return WagerTransaction{}
	}
	return *tx
}

func assertDecision(t *testing.T, c ruleCase, d WagerDecision, tx, ref *WagerTransaction) {
	t.Helper()
	if !d.IsInitialized() || d.TransactionID() != tx.ID() || d.Kind() != tx.Kind() {
		t.Fatalf("decision not bound to transaction: %+v", d)
	}
	if d.Outcome() != c.outcome || d.Direction() != c.dir || d.FailureCode() != c.code || d.AwaitReason() != c.await {
		t.Fatalf("got outcome=%s dir=%q code=%q await=%q (%s); want outcome=%s dir=%q code=%q await=%q",
			d.Outcome(), d.Direction(), d.FailureCode(), d.AwaitReason(), d.Detail(), c.outcome, c.dir, c.code, c.await)
	}
	wantAfter := Money{}
	if c.after != "" {
		wantAfter = mustParse(t, c.after, "BRL")
	}
	if d.BalanceAfter() != wantAfter {
		t.Fatalf("balanceAfter = %v, want %v", d.BalanceAfter(), wantAfter)
	}
	switch {
	case c.refID && d.ReferenceTransactionID() != ref.ID():
		t.Fatalf("reference id = %s, want %s", d.ReferenceTransactionID(), ref.ID())
	case !c.refID && d.ReferenceTransactionID() != uuid.Nil:
		t.Fatalf("unexpected reference id %s", d.ReferenceTransactionID())
	}
	if c.outcome == WagerDecisionApplyMovement {
		if d.Amount() != tx.Money() {
			t.Fatalf("amount = %v, want %v", d.Amount(), tx.Money())
		}
	} else if d.Amount() != (Money{}) || d.Direction() != "" {
		t.Fatal("non-movement decision carries a movement")
	}
	if d.Detail() == "" {
		t.Fatal("empty detail")
	}
}

// assertApply aplica a decisão em cópias e confere o efeito e a
// persistibilidade (reidratação) do resultado.
func assertApply(t *testing.T, d WagerDecision, tx *WagerTransaction, w *Wallet) {
	t.Helper()
	txc := *tx
	var wc *Wallet
	if w != nil {
		c := *w
		wc = &c
	}
	now := testT0.Add(time.Hour)
	entry, err := ApplyWagerDecision(ApplyWagerDecisionParams{Transaction: &txc, Wallet: wc, Decision: d, NextAttemptAt: now.Add(time.Minute), Now: now})
	if err != nil {
		t.Fatalf("ApplyWagerDecision: %v", err)
	}
	if _, err := RehydrateWagerTransaction(rehydrateParamsOf(&txc)); err != nil {
		t.Fatalf("applied transaction is not persistable: %v", err)
	}
	switch d.Outcome() {
	case WagerDecisionApplyMovement:
		if !entry.IsInitialized() || entry.Direction() != d.Direction() || entry.Amount() != d.Amount() ||
			entry.TransactionID() != tx.ID() || entry.BalanceBefore() != w.Balance() || d.BalanceBefore() != w.Balance() || entry.BalanceAfter() != d.BalanceAfter() {
			t.Fatalf("unexpected entry %+v", entry)
		}
		if wc.Balance() != d.BalanceAfter() || wc.Version() != w.Version()+1 {
			t.Fatalf("wallet balance %v version %d", wc.Balance(), wc.Version())
		}
		if txc.Status() != WagerStatusProcessed || txc.BalanceAfter() != d.BalanceAfter() || txc.ReferenceTransactionID() != d.ReferenceTransactionID() {
			t.Fatal("transaction not processed with the decision outcome")
		}
		return
	case WagerDecisionNoMovement:
		if txc.Status() != WagerStatusProcessed || txc.BalanceAfter() != w.Balance() {
			t.Fatal("LOSS not processed with unchanged balance")
		}
	case WagerDecisionAwaitReference:
		if txc.Status() != WagerStatusPendingReference || txc.Attempts() != tx.Attempts()+1 {
			t.Fatalf("status %s attempts %d", txc.Status(), txc.Attempts())
		}
	case WagerDecisionReject:
		if txc.Status() != WagerStatusRejected || txc.FailureCode() != d.FailureCode() || txc.BalanceAfter() != d.BalanceAfter() ||
			txc.ReferenceTransactionID() != d.ReferenceTransactionID() {
			t.Fatal("rejection not recorded")
		}
	}
	if entry.IsInitialized() {
		t.Fatal("ledger entry without movement")
	}
	if w != nil && !reflect.DeepEqual(*w, *wc) {
		t.Fatal("wallet changed without movement (LOSS/rejection must not bump version)")
	}
}

// --- DecideWager: erros de entrada -------------------------------------------------

func TestDecideWager_InvalidInput(t *testing.T) {
	wallet := func(t *testing.T) *Wallet { return walletWith(t, "100.00", "BRL", testPlayerID) }
	refund := func(t *testing.T) *WagerTransaction {
		return buildTx(t, txSpec{kind: WagerKindRefund, ext: "r", ref: "bet-1"})
	}
	cases := []struct {
		name string
		in   func(t *testing.T) WagerRuleInput
		errs []error
	}{
		{"nil transaction", func(t *testing.T) WagerRuleInput { return WagerRuleInput{Wallet: wallet(t)} }, []error{ErrUninitializedWagerTransaction}},
		{"zero-value transaction", func(t *testing.T) WagerRuleInput {
			return WagerRuleInput{Transaction: &WagerTransaction{}, Wallet: wallet(t)}
		}, []error{ErrUninitializedWagerTransaction}},
		{"terminal transaction", func(t *testing.T) WagerRuleInput {
			return WagerRuleInput{Transaction: betRef(t, WagerStatusProcessed), Wallet: wallet(t)}
		}, []error{ErrWagerTransactionTerminal}},
		{"internal transaction", func(t *testing.T) WagerRuleInput {
			return WagerRuleInput{Transaction: openingRef(t), Wallet: wallet(t)}
		}, nil},
		{"uninitialized wallet", func(t *testing.T) WagerRuleInput {
			return WagerRuleInput{Transaction: betRef(t, WagerStatusPending), Wallet: &Wallet{}}
		}, []error{ErrUninitializedWallet}},
		{"wallet with another id", func(t *testing.T) WagerRuleInput {
			return WagerRuleInput{Transaction: buildTx(t, txSpec{kind: WagerKindBet, ext: "b", wallet: otherWalletID}), Wallet: wallet(t)}
		}, nil},
		{"zero-value reference", func(t *testing.T) WagerRuleInput {
			return WagerRuleInput{Transaction: refund(t), Wallet: wallet(t), Reference: &WagerTransaction{}}
		}, []error{ErrUninitializedWagerTransaction}},
		{"reference is the transaction", func(t *testing.T) WagerRuleInput {
			tx := refund(t)
			return WagerRuleInput{Transaction: tx, Wallet: wallet(t), Reference: tx}
		}, nil},
		{"reference with another external id", func(t *testing.T) WagerRuleInput {
			other := withStatus(t, buildTx(t, txSpec{kind: WagerKindBet, ext: "bet-9"}), WagerStatusProcessed, uuid.Nil)
			return WagerRuleInput{Transaction: refund(t), Wallet: wallet(t), Reference: other}
		}, nil},
		{"uninitialized reversal", func(t *testing.T) WagerRuleInput {
			return WagerRuleInput{Transaction: refund(t), Wallet: wallet(t), Reference: betRef(t, WagerStatusProcessed), ReferenceReversals: []*WagerTransaction{nil}}
		}, []error{ErrUninitializedWagerTransaction}},
		{"reversal of another reference", func(t *testing.T) WagerRuleInput {
			ref := betRef(t, WagerStatusProcessed)
			other := reversalOf(t, WagerKindRefund, "x", withStatus(t, buildTx(t, txSpec{kind: WagerKindBet, ext: "bet-9"}), WagerStatusProcessed, uuid.Nil), WagerStatusProcessed)
			return WagerRuleInput{Transaction: refund(t), Wallet: wallet(t), Reference: ref, ReferenceReversals: []*WagerTransaction{other}}
		}, nil},
		{"non-reversal in reversal list", func(t *testing.T) WagerRuleInput {
			ref := betRef(t, WagerStatusProcessed)
			win := withStatus(t, buildTx(t, txSpec{kind: WagerKindWin, ext: "w", ref: "bet-1"}), WagerStatusProcessed, ref.ID())
			return WagerRuleInput{Transaction: refund(t), Wallet: wallet(t), Reference: ref, ReferenceReversals: []*WagerTransaction{win}}
		}, nil},
		{"reversal resolved to another id", func(t *testing.T) WagerRuleInput {
			ref := betRef(t, WagerStatusProcessed)
			r := withStatus(t, buildTx(t, txSpec{kind: WagerKindRollback, ext: "x", ref: "bet-1"}), WagerStatusProcessed, testRefWagerID)
			return WagerRuleInput{Transaction: refund(t), Wallet: wallet(t), Reference: ref, ReferenceReversals: []*WagerTransaction{r}}
		}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, err := DecideWager(c.in(t))
			assertErrs(t, err, append([]error{ErrInvalidWagerRuleInput}, c.errs...)...)
			if d.IsInitialized() {
				t.Fatal("decision returned with error")
			}
		})
	}
}

func TestDecideWager_CreditOverflowIsError(t *testing.T) {
	w, err := RehydrateWallet(RehydrateWalletParams{
		ID: testWalletID, PlayerID: testPlayerID, Balance: Money{minor: 1<<63 - 1, currency: "BRL"},
		Version: 1, CreatedAt: testT0, UpdatedAt: testT0,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = DecideWager(WagerRuleInput{Transaction: buildTx(t, txSpec{kind: WagerKindWin, ext: "w"}), Wallet: w})
	assertErrs(t, err, ErrMoneyOverflow)
}

// --- Cenários sequenciais (decidir + aplicar) -------------------------------------

type ledgerSim struct {
	t       *testing.T
	wallet  *Wallet
	entries []WalletLedgerEntry
	now     time.Time
}

func (s *ledgerSim) process(tx, ref *WagerTransaction, reversals ...*WagerTransaction) WagerDecision {
	s.t.Helper()
	d, err := DecideWager(WagerRuleInput{Transaction: tx, Wallet: s.wallet, Reference: ref, ReferenceReversals: reversals})
	if err != nil {
		s.t.Fatalf("decide %s: %v", tx.ExternalTransactionID(), err)
	}
	s.now = s.now.Add(time.Second)
	entry, err := ApplyWagerDecision(ApplyWagerDecisionParams{Transaction: tx, Wallet: s.wallet, Decision: d, NextAttemptAt: s.now.Add(time.Minute), Now: s.now})
	if err != nil {
		s.t.Fatalf("apply %s: %v", tx.ExternalTransactionID(), err)
	}
	if entry.IsInitialized() {
		s.entries = append(s.entries, entry)
	}
	return d
}

// reconcile confere saldo == soma do ledger a partir de opening.
func (s *ledgerSim) reconcile(opening string) {
	s.t.Helper()
	sum := mustParse(s.t, opening, "BRL")
	var err error
	for _, e := range s.entries {
		if e.Direction() == LedgerDirectionCredit {
			sum, err = sum.Add(e.Amount())
		} else {
			sum, err = sum.Sub(e.Amount())
		}
		if err != nil {
			s.t.Fatal(err)
		}
	}
	if sum != s.wallet.Balance() {
		s.t.Fatalf("ledger sum %v != balance %v", sum, s.wallet.Balance())
	}
}

func TestWagerRules_TwoBetsOf80On100(t *testing.T) {
	sim := &ledgerSim{t: t, wallet: walletWith(t, "100.00", "BRL", testPlayerID), now: testT0}
	b1 := buildTx(t, txSpec{kind: WagerKindBet, ext: "bet-80-a", amount: "80.00"})
	b2 := buildTx(t, txSpec{kind: WagerKindBet, ext: "bet-80-b", amount: "80.00"})

	d1 := sim.process(b1, nil)
	d2 := sim.process(b2, nil)

	if d1.Outcome() != WagerDecisionApplyMovement || b1.Status() != WagerStatusProcessed {
		t.Fatalf("first bet: %s %s", d1.Outcome(), b1.Status())
	}
	if d2.Outcome() != WagerDecisionReject || b2.Status() != WagerStatusRejected || b2.FailureCode() != FailureCodeInsufficientFunds {
		t.Fatalf("second bet: %s %s %s", d2.Outcome(), b2.Status(), b2.FailureCode())
	}
	if got := sim.wallet.Balance(); got != mustParse(t, "20.00", "BRL") {
		t.Fatalf("balance = %v", got)
	}
	if len(sim.entries) != 1 || sim.entries[0].Direction() != LedgerDirectionDebit || sim.entries[0].TransactionID() != b1.ID() {
		t.Fatalf("expected a single debit, got %d entries", len(sim.entries))
	}
	if sim.wallet.Version() != 4 { // 3 + um único movimento
		t.Fatalf("version = %d", sim.wallet.Version())
	}
	if b1.BalanceAfter() != mustParse(t, "20.00", "BRL") || b2.BalanceAfter() != mustParse(t, "20.00", "BRL") {
		t.Fatal("persisted results do not reflect observed balances")
	}
	// Replay: transações terminais não são reavaliadas.
	_, err := DecideWager(WagerRuleInput{Transaction: b1, Wallet: sim.wallet})
	assertErrs(t, err, ErrInvalidWagerRuleInput, ErrWagerTransactionTerminal)
	sim.reconcile("100.00")
}

func TestWagerRules_RefundRollbackLifecycle(t *testing.T) {
	sim := &ledgerSim{t: t, wallet: walletWith(t, "100.00", "BRL", testPlayerID), now: testT0}

	bet := buildTx(t, txSpec{kind: WagerKindBet, ext: "bet-1"})
	sim.process(bet, nil) // 75.00

	refund := buildTx(t, txSpec{kind: WagerKindRefund, ext: "refund-1", ref: "bet-1"})
	sim.process(refund, bet) // 100.00
	if refund.Status() != WagerStatusProcessed || refund.ReferenceTransactionID() != bet.ID() {
		t.Fatal("refund not processed")
	}

	// ROLLBACK da mesma BET depois do REFUND: devolveria o débito duas vezes.
	rbBet := buildTx(t, txSpec{kind: WagerKindRollback, ext: "rb-bet", ref: "bet-1"})
	sim.process(rbBet, bet, refund)
	if rbBet.FailureCode() != FailureCodeAlreadyReversed || rbBet.ReferenceTransactionID() != bet.ID() {
		t.Fatalf("rollback after refund: %s %s", rbBet.Status(), rbBet.FailureCode())
	}
	// Segundo REFUND (outra operação externa) da mesma BET.
	refund2 := buildTx(t, txSpec{kind: WagerKindRefund, ext: "refund-2", ref: "bet-1"})
	sim.process(refund2, bet, refund, rbBet)
	if refund2.FailureCode() != FailureCodeAlreadyReversed {
		t.Fatalf("second refund: %s", refund2.FailureCode())
	}

	// ROLLBACK do REFUND: estorna a devolução (débito).
	rbRefund := buildTx(t, txSpec{kind: WagerKindRollback, ext: "rb-refund", ref: "refund-1"})
	sim.process(rbRefund, refund) // 75.00
	if rbRefund.Status() != WagerStatusProcessed {
		t.Fatalf("rollback of refund: %s %s", rbRefund.Status(), rbRefund.FailureCode())
	}
	rbRefund2 := buildTx(t, txSpec{kind: WagerKindRollback, ext: "rb-refund-2", ref: "refund-1"})
	sim.process(rbRefund2, refund, rbRefund)
	if rbRefund2.FailureCode() != FailureCodeAlreadyReversed {
		t.Fatalf("double rollback of refund: %s", rbRefund2.FailureCode())
	}
	// ROLLBACK de ROLLBACK não existe.
	rbRb := buildTx(t, txSpec{kind: WagerKindRollback, ext: "rb-rb", ref: "rb-refund"})
	sim.process(rbRb, rbRefund)
	if rbRb.FailureCode() != FailureCodeReferenceNotReversible {
		t.Fatalf("rollback of rollback: %s", rbRb.FailureCode())
	}

	if sim.wallet.Balance() != mustParse(t, "75.00", "BRL") || len(sim.entries) != 3 {
		t.Fatalf("balance %v entries %d", sim.wallet.Balance(), len(sim.entries))
	}
	sim.reconcile("100.00")
}

func TestWagerRules_RollbackWinWithoutFundsThenLoss(t *testing.T) {
	sim := &ledgerSim{t: t, wallet: walletWith(t, "10.00", "BRL", testPlayerID), now: testT0}
	win := buildTx(t, txSpec{kind: WagerKindWin, ext: "win-1", amount: "50.00"})
	sim.process(win, nil) // 60.00
	bet := buildTx(t, txSpec{kind: WagerKindBet, ext: "bet-1", amount: "55.00"})
	sim.process(bet, nil) // 5.00
	rb := buildTx(t, txSpec{kind: WagerKindRollback, ext: "rb-win", ref: "win-1", amount: "50.00"})
	d := sim.process(rb, win)
	if d.FailureCode() != FailureCodeReversalInsufficientFunds || rb.FailureCode() != FailureCodeReversalInsufficientFunds ||
		rb.BalanceAfter() != mustParse(t, "5.00", "BRL") {
		t.Fatalf("rollback without funds: %s", rb.FailureCode())
	}
	// Uma reversão rejeitada não bloqueia uma nova tentativa com saldo.
	sim.process(buildTx(t, txSpec{kind: WagerKindWin, ext: "win-2", amount: "45.00"}), nil) // 50.00
	rb2 := buildTx(t, txSpec{kind: WagerKindRollback, ext: "rb-win-2", ref: "win-1", amount: "50.00"})
	sim.process(rb2, win, rb)
	if rb2.Status() != WagerStatusProcessed || sim.wallet.Balance() != mustParse(t, "0.00", "BRL") {
		t.Fatalf("rollback with funds: %s balance %v", rb2.Status(), sim.wallet.Balance())
	}
	versionBefore := sim.wallet.Version()
	loss := buildTx(t, txSpec{kind: WagerKindLoss, ext: "loss-1"})
	sim.process(loss, nil)
	if loss.Status() != WagerStatusProcessed || sim.wallet.Version() != versionBefore {
		t.Fatal("LOSS must not bump version")
	}
	sim.reconcile("10.00")
}

func TestWagerRules_ReversalArrivesBeforeReference(t *testing.T) {
	sim := &ledgerSim{t: t, wallet: walletWith(t, "100.00", "BRL", testPlayerID), now: testT0}
	refund := buildTx(t, txSpec{kind: WagerKindRefund, ext: "refund-1", ref: "bet-1"})

	d := sim.process(refund, nil)
	if d.Outcome() != WagerDecisionAwaitReference || refund.Status() != WagerStatusPendingReference || refund.Attempts() != 1 {
		t.Fatalf("first attempt: %s %s %d", d.Outcome(), refund.Status(), refund.Attempts())
	}
	sim.process(refund, nil) // worker: ainda ausente
	if refund.Attempts() != 2 || refund.Status() != WagerStatusPendingReference {
		t.Fatalf("reschedule: %d", refund.Attempts())
	}
	// A BET chega, mas ainda está PENDING (outra instância processando).
	bet := buildTx(t, txSpec{kind: WagerKindBet, ext: "bet-1"})
	d = sim.process(refund, bet)
	if d.AwaitReason() != WagerAwaitReferencePending || refund.Attempts() != 3 {
		t.Fatalf("pending reference: %s %d", d.AwaitReason(), refund.Attempts())
	}
	sim.process(bet, nil)    // 75.00
	sim.process(refund, bet) // 100.00
	if refund.Status() != WagerStatusProcessed || refund.ReferenceTransactionID() != bet.ID() || refund.Attempts() != 3 {
		t.Fatalf("resolution: %s", refund.Status())
	}
	if _, err := RehydrateWagerTransaction(rehydrateParamsOf(refund)); err != nil {
		t.Fatal(err)
	}
	sim.reconcile("100.00")
}

// --- ApplyWagerDecision ------------------------------------------------------------

func TestApplyWagerDecision_StaleDecision(t *testing.T) {
	w := walletWith(t, "100.00", "BRL", testPlayerID)
	b1 := buildTx(t, txSpec{kind: WagerKindBet, ext: "a", amount: "80.00"})
	b2 := buildTx(t, txSpec{kind: WagerKindBet, ext: "b", amount: "80.00"})
	d1, err := DecideWager(WagerRuleInput{Transaction: b1, Wallet: w})
	if err != nil {
		t.Fatal(err)
	}
	d2, err := DecideWager(WagerRuleInput{Transaction: b2, Wallet: w}) // mesmo snapshot (sem lock)
	if err != nil {
		t.Fatal(err)
	}
	now := testT0.Add(time.Minute)
	if _, err := ApplyWagerDecision(ApplyWagerDecisionParams{Transaction: b1, Wallet: w, Decision: d1, Now: now}); err != nil {
		t.Fatal(err)
	}
	wBefore, b2Before := *w, *b2
	_, err = ApplyWagerDecision(ApplyWagerDecisionParams{Transaction: b2, Wallet: w, Decision: d2, Now: now})
	assertErrs(t, err, ErrStaleWagerDecision)
	if !reflect.DeepEqual(wBefore, *w) || !reflect.DeepEqual(b2Before, *b2) {
		t.Fatal("stale application mutated state")
	}
	// Rejeição decidida sobre snapshot antigo também é recusada (o saldo
	// observado ficaria errado).
	b3 := buildTx(t, txSpec{kind: WagerKindBet, ext: "c", amount: "500.00"})
	old := walletWith(t, "100.00", "BRL", testPlayerID)
	d3, err := DecideWager(WagerRuleInput{Transaction: b3, Wallet: old})
	if err != nil {
		t.Fatal(err)
	}
	_, err = ApplyWagerDecision(ApplyWagerDecisionParams{Transaction: b3, Wallet: w, Decision: d3, Now: now})
	assertErrs(t, err, ErrStaleWagerDecision)
}

func TestApplyWagerDecision_FailureAtomicity(t *testing.T) {
	w := walletWith(t, "100.00", "BRL", testPlayerID)
	bet := buildTx(t, txSpec{kind: WagerKindBet, ext: "a"})
	d, err := DecideWager(WagerRuleInput{Transaction: bet, Wallet: w})
	if err != nil {
		t.Fatal(err)
	}
	// A transação foi concluída por outro caminho: o débito na cópia da carteira
	// acontece, mas MarkProcessed falha e nada é publicado.
	done := *bet
	if err := done.MarkProcessed(MarkWagerProcessedParams{BalanceAfter: mustParse(t, "75.00", "BRL"), Now: testT0.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	wBefore := *w
	_, err = ApplyWagerDecision(ApplyWagerDecisionParams{Transaction: &done, Wallet: w, Decision: d, Now: testT0.Add(time.Minute)})
	assertErrs(t, err, ErrWagerTransactionTerminal)
	if !reflect.DeepEqual(wBefore, *w) {
		t.Fatal("wallet mutated although the transaction transition failed")
	}
	// Instante zero: falha no movimento, nada muda.
	betBefore := *bet
	_, err = ApplyWagerDecision(ApplyWagerDecisionParams{Transaction: bet, Wallet: w, Decision: d})
	assertErrs(t, err, ErrInvalidMovement)
	if !reflect.DeepEqual(wBefore, *w) || !reflect.DeepEqual(betBefore, *bet) {
		t.Fatal("state mutated on failure")
	}
}

func TestApplyWagerDecision_AwaitRequiresSchedule(t *testing.T) {
	w := walletWith(t, "100.00", "BRL", testPlayerID)
	refund := buildTx(t, txSpec{kind: WagerKindRefund, ext: "r", ref: "bet-1"})
	d, err := DecideWager(WagerRuleInput{Transaction: refund, Wallet: w})
	if err != nil {
		t.Fatal(err)
	}
	before := *refund
	now := testT0.Add(time.Minute)
	for _, next := range []time.Time{{}, now.Add(-time.Second)} {
		_, err = ApplyWagerDecision(ApplyWagerDecisionParams{Transaction: refund, Wallet: w, Decision: d, NextAttemptAt: next, Now: now})
		assertErrs(t, err, ErrInvalidWagerTransaction)
		if !reflect.DeepEqual(before, *refund) {
			t.Fatal("failed scheduling mutated the transaction")
		}
	}
}

func TestApplyWagerDecision_InvalidInput(t *testing.T) {
	w := walletWith(t, "100.00", "BRL", testPlayerID)
	bet := buildTx(t, txSpec{kind: WagerKindBet, ext: "a"})
	other := buildTx(t, txSpec{kind: WagerKindBet, ext: "b"})
	d, err := DecideWager(WagerRuleInput{Transaction: bet, Wallet: w})
	if err != nil {
		t.Fatal(err)
	}
	now := testT0.Add(time.Minute)
	cases := []struct {
		name string
		p    ApplyWagerDecisionParams
		errs []error
	}{
		{"zero decision", ApplyWagerDecisionParams{Transaction: bet, Wallet: w, Now: now}, nil},
		{"nil transaction", ApplyWagerDecisionParams{Wallet: w, Decision: d, Now: now}, []error{ErrUninitializedWagerTransaction}},
		{"decision of another transaction", ApplyWagerDecisionParams{Transaction: other, Wallet: w, Decision: d, Now: now}, nil},
		{"missing wallet", ApplyWagerDecisionParams{Transaction: bet, Decision: d, Now: now}, nil},
		{"another wallet", ApplyWagerDecisionParams{Transaction: bet, Wallet: &Wallet{}, Decision: d, Now: now}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			before := *bet
			_, err := ApplyWagerDecision(c.p)
			assertErrs(t, err, append([]error{ErrInvalidWagerRuleInput}, c.errs...)...)
			if !reflect.DeepEqual(before, *bet) {
				t.Fatal("transaction mutated")
			}
		})
	}
}

func TestApplyWagerDecision_WalletNotFoundWithoutWallet(t *testing.T) {
	bet := buildTx(t, txSpec{kind: WagerKindBet, ext: "a"})
	d, err := DecideWager(WagerRuleInput{Transaction: bet})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := ApplyWagerDecision(ApplyWagerDecisionParams{Transaction: bet, Decision: d, Now: testT0.Add(time.Minute)})
	if err != nil || entry.IsInitialized() {
		t.Fatalf("apply: %v", err)
	}
	if bet.Status() != WagerStatusRejected || bet.FailureCode() != FailureCodeWalletNotFound || bet.BalanceAfter() != (Money{}) {
		t.Fatal("WALLET_NOT_FOUND not recorded")
	}
}

func TestWagerDecisionOutcome_Values(t *testing.T) {
	for _, o := range []WagerDecisionOutcome{WagerDecisionApplyMovement, WagerDecisionNoMovement, WagerDecisionAwaitReference, WagerDecisionReject} {
		if !o.IsValid() || o.String() == "" {
			t.Fatalf("%q invalid", o)
		}
	}
	if WagerDecisionOutcome("apply").IsValid() || (WagerDecision{}).IsInitialized() {
		t.Fatal("unexpected validity")
	}
	if WagerAwaitReferenceMissing.String() != "REFERENCE_MISSING" || WagerAwaitReferencePending.String() != "REFERENCE_PENDING" {
		t.Fatal("await reasons changed")
	}
	if !errors.Is(fmt.Errorf("x: %w", ErrStaleWagerDecision), ErrStaleWagerDecision) {
		t.Fatal("sentinel not comparable")
	}
}
