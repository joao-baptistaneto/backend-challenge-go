package domain

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Decisão arquitetural (ADR-009 em ARCHITECTURE.md): regras de operação e
// reversão como serviço de domínio PURO, separado em duas etapas.
//
//   - DecideWager(WagerRuleInput) só lê: recebe a transação nova (PENDING ou
//     PENDING_REFERENCE), o snapshot da carteira, a referência resolvida por
//     (providerId, referenceExternalTransactionId) e as reversões já existentes
//     dessa referência, e devolve uma WagerDecision: APPLY_MOVEMENT (DEBIT/CREDIT
//     + valor), NO_MOVEMENT (LOSS), AWAIT_REFERENCE ou REJECT (FailureCode). Não
//     faz I/O, não lê relógio e não muta nada, então é testável por tabela e
//     pode ser chamada de novo após um retry sem efeito colateral.
//   - ApplyWagerDecision aplica a decisão: Wallet.Debit/Credit e a transição da
//     WagerTransaction. Opera sobre cópias e só substitui carteira e transação
//     quando ambas as mudanças valem (atomicidade de falha em memória; a
//     atomicidade durável é a transação SQL do caso de uso, T15). Recusa decisão
//     obsoleta (ErrStaleWagerDecision) se a carteira mudou desde DecideWager.
//   - Concorrência: o caso de uso (T15/T21) chama as duas etapas dentro da
//     transação SQL, com a linha da carteira travada (SELECT ... FOR UPDATE,
//     ADR-005). Referência e reversões pertencem obrigatoriamente à MESMA
//     carteira (senão REFERENCE_MISMATCH), então o lock da carteira também
//     serializa todas as reversões de uma referência: o snapshot lido é atual.
//     A defesa final contra reversão dupla é o índice único parcial do schema
//     (T10), que espelha a política abaixo.
//   - Ordem de avaliação (determinística; o primeiro critério que falha decide):
//     1. entrada inválida (programação) -> erro ErrInvalidWagerRuleInput;
//     2. carteira: inexistente (WALLET_NOT_FOUND), de outro jogador
//     (WALLET_PLAYER_MISMATCH), outra moeda (CURRENCY_MISMATCH);
//     3. sem referência: BET debita (INSUFFICIENT_FUNDS), LOSS não movimenta,
//     WIN credita;
//     4. com referência (WIN opcional, REFUND/ROLLBACK obrigatória): ausente ->
//     AWAIT_REFERENCE; depois, os critérios estáticos (que não mudam com o
//     tempo): identidade (REFERENCE_MISMATCH: provedor, jogador, carteira,
//     rodada), moeda (CURRENCY_MISMATCH), tipo (REFERENCE_NOT_REVERSIBLE),
//     valor (AMOUNT_MISMATCH, só reversões). Só então o estado: PENDING/
//     PENDING_REFERENCE -> AWAIT_REFERENCE; REJECTED/FAILED ->
//     REFERENCE_NOT_PROCESSED. Por fim, reversão já existente
//     (ALREADY_REVERSED) e saldo (REVERSAL_INSUFFICIENT_FUNDS).
//     A identidade vem antes do tipo/estado para não vazar informação de uma
//     transação de outro jogador ou rodada; os critérios estáticos vêm antes
//     do estado para que o resultado não dependa de a referência ainda estar
//     pendente.
//   - Política REFUND x ROLLBACK: cada referência admite NO MÁXIMO UMA reversão
//     bem-sucedida, de qualquer tipo. Isso cumpre "não duas reversões do mesmo
//     tipo" e impede devolução dupla do mesmo débito (REFUND + ROLLBACK da mesma
//     BET -> o segundo é ALREADY_REVERSED). Para desfazer um REFUND, o provedor
//     envia ROLLBACK referenciando o REFUND (debita o valor devolvido). Uma BET
//     que já teve devolução não volta a ser devolvida, mesmo que essa devolução
//     tenha sido desfeita: é uma limitação consciente, que mantém a regra
//     expressável num único índice único parcial por reference_transaction_id.

// Erros sentinela das regras de operação (errors.Is). Indicam erro de
// programação/orquestração, nunca rejeição de negócio: rejeições são decisões.
var (
	// ErrInvalidWagerRuleInput indica entrada inconsistente para DecideWager ou
	// ApplyWagerDecision (transação terminal ou não inicializada, carteira de
	// outro id, referência que não corresponde ao referenceExternalTransactionId,
	// reversão listada que não aponta para a referência etc.).
	ErrInvalidWagerRuleInput = errors.New("wager rules: invalid input")
	// ErrStaleWagerDecision indica decisão aplicada a uma carteira diferente do
	// snapshot usado para decidir (versão/saldo mudaram). O caso de uso deve
	// reler sob lock e decidir de novo.
	ErrStaleWagerDecision = errors.New("wager rules: stale decision")
)

// WagerDecisionOutcome é o resultado de DecideWager.
type WagerDecisionOutcome string

const (
	// WagerDecisionApplyMovement: movimentar a carteira (Direction + Amount) e
	// concluir em PROCESSED.
	WagerDecisionApplyMovement WagerDecisionOutcome = "APPLY_MOVEMENT"
	// WagerDecisionNoMovement: concluir em PROCESSED sem lançamento (LOSS).
	WagerDecisionNoMovement WagerDecisionOutcome = "NO_MOVEMENT"
	// WagerDecisionAwaitReference: aguardar a referência em PENDING_REFERENCE.
	WagerDecisionAwaitReference WagerDecisionOutcome = "AWAIT_REFERENCE"
	// WagerDecisionReject: concluir em REJECTED com FailureCode.
	WagerDecisionReject WagerDecisionOutcome = "REJECT"
)

// IsValid indica um dos quatro resultados conhecidos.
func (o WagerDecisionOutcome) IsValid() bool {
	switch o {
	case WagerDecisionApplyMovement, WagerDecisionNoMovement, WagerDecisionAwaitReference, WagerDecisionReject:
		return true
	}
	return false
}

// String devolve o nome do resultado.
func (o WagerDecisionOutcome) String() string { return string(o) }

// WagerAwaitReason explica por que a operação aguarda a referência (logs e
// métricas; não é persistido como código de falha).
type WagerAwaitReason string

const (
	// WagerAwaitReferenceMissing: a referência ainda não foi recebida.
	WagerAwaitReferenceMissing WagerAwaitReason = "REFERENCE_MISSING"
	// WagerAwaitReferencePending: a referência existe, mas está PENDING ou
	// PENDING_REFERENCE.
	WagerAwaitReferencePending WagerAwaitReason = "REFERENCE_PENDING"
)

// String devolve o nome do motivo.
func (r WagerAwaitReason) String() string { return string(r) }

// WagerRuleInput agrupa o estado lido (sob o lock da carteira) para decidir uma
// operação.
type WagerRuleInput struct {
	// Transaction é a operação externa a decidir, em PENDING ou
	// PENDING_REFERENCE. Obrigatória.
	Transaction *WagerTransaction
	// Wallet é o snapshot da carteira Transaction.WalletID(); nil significa
	// carteira inexistente (WALLET_NOT_FOUND).
	Wallet *Wallet
	// Reference é a transação resolvida por (providerId,
	// referenceExternalTransactionId); nil significa ainda não recebida.
	// Ignorada quando Transaction não tem referência.
	Reference *WagerTransaction
	// ReferenceReversals são as reversões (REFUND/ROLLBACK) que referenciam
	// Reference, em qualquer estado. Só as PROCESSED contam; a própria
	// Transaction, se listada, é ignorada. Usada apenas por REFUND/ROLLBACK.
	ReferenceReversals []*WagerTransaction
}

// WagerDecision é o resultado imutável de DecideWager. Só pode ser obtida por
// DecideWager (campos privados) e carrega o snapshot da carteira usado, que
// ApplyWagerDecision confere antes de aplicar.
type WagerDecision struct {
	outcome                WagerDecisionOutcome
	transactionID          uuid.UUID
	kind                   WagerKind
	direction              LedgerDirection
	amount                 Money
	failureCode            FailureCode
	awaitReason            WagerAwaitReason
	referenceTransactionID uuid.UUID
	walletID               uuid.UUID
	walletVersion          int64
	balanceBefore          Money
	balanceAfter           Money
	detail                 string
}

// Outcome devolve o resultado da decisão.
func (d WagerDecision) Outcome() WagerDecisionOutcome { return d.outcome }

// TransactionID devolve a transação decidida.
func (d WagerDecision) TransactionID() uuid.UUID { return d.transactionID }

// Kind devolve o tipo da transação decidida.
func (d WagerDecision) Kind() WagerKind { return d.kind }

// Direction devolve DEBIT/CREDIT em APPLY_MOVEMENT ("" nos demais).
func (d WagerDecision) Direction() LedgerDirection { return d.direction }

// Amount devolve o valor a movimentar em APPLY_MOVEMENT (Money{} nos demais).
func (d WagerDecision) Amount() Money { return d.amount }

// FailureCode devolve o código em REJECT ("" nos demais).
func (d WagerDecision) FailureCode() FailureCode { return d.failureCode }

// AwaitReason devolve o motivo em AWAIT_REFERENCE ("" nos demais).
func (d WagerDecision) AwaitReason() WagerAwaitReason { return d.awaitReason }

// ReferenceTransactionID devolve a referência interna resolvida (uuid.Nil se
// não houver ou não tiver sido resolvida).
func (d WagerDecision) ReferenceTransactionID() uuid.UUID { return d.referenceTransactionID }

// BalanceBefore devolve o saldo do snapshot usado (Money{} quando a carteira
// não pôde ser usada: inexistente, de outro jogador ou de outra moeda).
func (d WagerDecision) BalanceBefore() Money { return d.balanceBefore }

// BalanceAfter devolve o saldo projetado (APPLY_MOVEMENT), inalterado
// (NO_MOVEMENT e REJECT com carteira válida) ou Money{} (AWAIT_REFERENCE e
// rejeições de carteira).
func (d WagerDecision) BalanceAfter() Money { return d.balanceAfter }

// Detail devolve uma explicação curta para logs (sem valores monetários).
func (d WagerDecision) Detail() string { return d.detail }

// IsInitialized indica decisão produzida por DecideWager.
func (d WagerDecision) IsInitialized() bool {
	return d.outcome.IsValid() && d.transactionID != uuid.Nil
}

// DecideWager aplica as regras de BET, WIN, LOSS, REFUND e ROLLBACK sem efeitos
// colaterais. Rejeições de negócio são decisões (REJECT), nunca erro; o erro é
// reservado a entrada inconsistente (ErrInvalidWagerRuleInput) e a overflow de
// saldo em crédito (ErrMoneyOverflow), ambos tratados pela aplicação como falha
// interna.
func DecideWager(in WagerRuleInput) (WagerDecision, error) {
	tx := in.Transaction
	if err := checkDecidableTransaction(tx); err != nil {
		return WagerDecision{}, err
	}
	d := WagerDecision{transactionID: tx.ID(), kind: tx.Kind()}

	// 2. Carteira.
	w := in.Wallet
	if w == nil {
		return d.reject(FailureCodeWalletNotFound, "wallet not found"), nil
	}
	if !w.IsInitialized() {
		return WagerDecision{}, fmt.Errorf("%w: wallet: %w", ErrInvalidWagerRuleInput, ErrUninitializedWallet)
	}
	if w.ID() != tx.WalletID() {
		return WagerDecision{}, fmt.Errorf("%w: wallet %s is not the transaction wallet %s", ErrInvalidWagerRuleInput, w.ID(), tx.WalletID())
	}
	if w.PlayerID() != tx.PlayerID() {
		// Sem saldo na decisão: não expor dados da carteira de outro jogador.
		return d.reject(FailureCodeWalletPlayerMismatch, "wallet belongs to another player"), nil
	}
	if w.Currency() != tx.Money().Currency() {
		return d.reject(FailureCodeCurrencyMismatch, "operation currency differs from wallet currency"), nil
	}
	d.walletID = w.ID()
	d.walletVersion = w.Version()
	d.balanceBefore = w.Balance()

	// 3. Operações sem referência.
	switch {
	case tx.Kind() == WagerKindBet:
		return d.move(LedgerDirectionDebit, tx.Money(), FailureCodeInsufficientFunds)
	case tx.Kind() == WagerKindLoss:
		d.outcome = WagerDecisionNoMovement
		d.balanceAfter = d.balanceBefore
		d.detail = "loss has no movement"
		return d, nil
	case tx.Kind() == WagerKindWin && !tx.HasReference():
		return d.move(LedgerDirectionCredit, tx.Money(), "")
	}

	// 4. Operações com referência (WIN com referência, REFUND, ROLLBACK).
	return decideWithReference(d, tx, in.Reference, in.ReferenceReversals)
}

func checkDecidableTransaction(tx *WagerTransaction) error {
	if !tx.IsInitialized() {
		return fmt.Errorf("%w: transaction: %w", ErrInvalidWagerRuleInput, ErrUninitializedWagerTransaction)
	}
	if tx.Origin() != WagerOriginExternal {
		return fmt.Errorf("%w: %s is not an external operation", ErrInvalidWagerRuleInput, tx.Kind())
	}
	if tx.IsTerminal() {
		// Replay lê o resultado persistido; regras nunca reavaliam terminais.
		return fmt.Errorf("%w: %w: %s", ErrInvalidWagerRuleInput, ErrWagerTransactionTerminal, tx.Status())
	}
	return nil
}

func decideWithReference(d WagerDecision, tx, ref *WagerTransaction, reversals []*WagerTransaction) (WagerDecision, error) {
	if ref == nil {
		return d.await(WagerAwaitReferenceMissing, "reference not received yet"), nil
	}
	if !ref.IsInitialized() {
		return WagerDecision{}, fmt.Errorf("%w: reference: %w", ErrInvalidWagerRuleInput, ErrUninitializedWagerTransaction)
	}
	if ref.ID() == tx.ID() {
		return WagerDecision{}, fmt.Errorf("%w: transaction cannot reference itself", ErrInvalidWagerRuleInput)
	}
	d.referenceTransactionID = ref.ID()

	// OPENING não tem ID externo e não é alvo de nenhuma operação de provedor.
	if ref.Origin() != WagerOriginExternal {
		return d.reject(FailureCodeReferenceNotReversible, "internal transaction cannot be referenced"), nil
	}
	if ref.ExternalTransactionID() != tx.ReferenceExternalTransactionID() {
		return WagerDecision{}, fmt.Errorf("%w: reference external id %q does not match %q",
			ErrInvalidWagerRuleInput, ref.ExternalTransactionID(), tx.ReferenceExternalTransactionID())
	}

	// Critérios estáticos: identidade, moeda, tipo e valor.
	switch {
	case ref.ProviderID() != tx.ProviderID():
		return d.reject(FailureCodeReferenceMismatch, "reference belongs to another provider"), nil
	case ref.PlayerID() != tx.PlayerID():
		return d.reject(FailureCodeReferenceMismatch, "reference belongs to another player"), nil
	case ref.WalletID() != tx.WalletID():
		return d.reject(FailureCodeReferenceMismatch, "reference belongs to another wallet"), nil
	case ref.RoundID() != tx.RoundID():
		return d.reject(FailureCodeReferenceMismatch, "reference belongs to another round"), nil
	case ref.Money().Currency() != tx.Money().Currency():
		return d.reject(FailureCodeCurrencyMismatch, "operation currency differs from reference currency"), nil
	}
	if !referenceKindAccepted(tx.Kind(), ref.Kind()) {
		return d.reject(FailureCodeReferenceNotReversible, fmt.Sprintf("%s cannot reference %s", tx.Kind(), ref.Kind())), nil
	}
	if tx.Kind().IsReversal() && ref.Money() != tx.Money() {
		// Mesma moeda (checada acima): igualdade de Money é igualdade de valor.
		return d.reject(FailureCodeAmountMismatch, "reversal amount differs from referenced amount"), nil
	}

	// Estado da referência.
	switch ref.Status() {
	case WagerStatusPending, WagerStatusPendingReference:
		return d.await(WagerAwaitReferencePending, "reference is still pending"), nil
	case WagerStatusRejected, WagerStatusFailed:
		return d.reject(FailureCodeReferenceNotProcessed, fmt.Sprintf("reference ended as %s", ref.Status())), nil
	}

	if tx.Kind() == WagerKindWin {
		// WIN só usa a referência como vínculo à BET da rodada; o crédito é o
		// próprio valor do WIN.
		return d.move(LedgerDirectionCredit, tx.Money(), "")
	}

	// Reversões: no máximo uma reversão bem-sucedida por referência.
	reversed, err := hasProcessedReversal(tx, ref, reversals)
	if err != nil {
		return WagerDecision{}, err
	}
	if reversed {
		return d.reject(FailureCodeAlreadyReversed, "reference already has a successful reversal"), nil
	}

	// Movimento contrário ao original: BET debitou -> devolve (CREDIT);
	// WIN/REFUND creditaram -> estorna (DEBIT).
	dir := LedgerDirectionCredit
	if ref.Kind() == WagerKindWin || ref.Kind() == WagerKindRefund {
		dir = LedgerDirectionDebit
	}
	return d.move(dir, tx.Money(), FailureCodeReversalInsufficientFunds)
}

// referenceKindAccepted: WIN referencia BET; REFUND devolve BET; ROLLBACK
// desfaz BET, WIN ou REFUND. LOSS (sem movimento), ROLLBACK (não se desfaz um
// desfazimento) e OPENING não são alvos.
func referenceKindAccepted(kind, refKind WagerKind) bool {
	switch kind {
	case WagerKindWin, WagerKindRefund:
		return refKind == WagerKindBet
	case WagerKindRollback:
		return refKind == WagerKindBet || refKind == WagerKindWin || refKind == WagerKindRefund
	}
	return false
}

// hasProcessedReversal valida a lista de reversões e indica se alguma delas
// (diferente de tx) foi PROCESSED.
func hasProcessedReversal(tx, ref *WagerTransaction, reversals []*WagerTransaction) (bool, error) {
	found := false
	for i, r := range reversals {
		if !r.IsInitialized() {
			return false, fmt.Errorf("%w: reversal[%d]: %w", ErrInvalidWagerRuleInput, i, ErrUninitializedWagerTransaction)
		}
		if r.ID() == tx.ID() {
			continue
		}
		if !r.Kind().IsReversal() || r.ProviderID() != ref.ProviderID() ||
			r.ReferenceExternalTransactionID() != ref.ExternalTransactionID() ||
			(r.ReferenceTransactionID() != uuid.Nil && r.ReferenceTransactionID() != ref.ID()) {
			return false, fmt.Errorf("%w: reversal[%d] %s does not reverse reference %s", ErrInvalidWagerRuleInput, i, r.ID(), ref.ID())
		}
		if r.Status() == WagerStatusProcessed {
			found = true
		}
	}
	return found, nil
}

// move projeta o movimento sobre o snapshot. insufficient é o código usado
// quando um débito deixaria o saldo negativo.
func (d WagerDecision) move(dir LedgerDirection, amount Money, insufficient FailureCode) (WagerDecision, error) {
	var after Money
	var err error
	if dir == LedgerDirectionDebit {
		after, err = d.balanceBefore.Sub(amount)
		if err != nil {
			return WagerDecision{}, err
		}
		if after.IsNegative() {
			return d.reject(insufficient, "debit exceeds available balance"), nil
		}
	} else {
		after, err = d.balanceBefore.Add(amount)
		if err != nil {
			return WagerDecision{}, fmt.Errorf("credit: %w", err)
		}
	}
	d.outcome = WagerDecisionApplyMovement
	d.direction = dir
	d.amount = amount
	d.balanceAfter = after
	d.detail = fmt.Sprintf("%s %s", d.kind, dir)
	return d, nil
}

// reject registra a rejeição. O saldo observado (inalterado) acompanha a
// decisão sempre que a carteira já foi validada.
func (d WagerDecision) reject(code FailureCode, detail string) WagerDecision {
	d.outcome = WagerDecisionReject
	d.failureCode = code
	d.balanceAfter = d.balanceBefore
	d.detail = detail
	return d
}

func (d WagerDecision) await(reason WagerAwaitReason, detail string) WagerDecision {
	d.outcome = WagerDecisionAwaitReference
	d.awaitReason = reason
	// Referência pendente não é "resolvida" para fins de persistência:
	// PENDING_REFERENCE não grava referenceTransactionID.
	d.referenceTransactionID = uuid.Nil
	d.detail = detail
	return d
}

// ApplyWagerDecisionParams agrupa a aplicação de uma decisão.
type ApplyWagerDecisionParams struct {
	// Transaction é a mesma transação passada a DecideWager.
	Transaction *WagerTransaction
	// Wallet é a carteira do snapshot. Obrigatória quando a decisão foi tomada
	// com carteira válida; ignorada em WALLET_NOT_FOUND, WALLET_PLAYER_MISMATCH
	// e CURRENCY_MISMATCH de carteira.
	Wallet *Wallet
	// Decision deve ter sido produzida por DecideWager para Transaction.
	Decision WagerDecision
	// NextAttemptAt é o próximo instante de busca da referência (backoff da
	// aplicação, T21). Obrigatório em AWAIT_REFERENCE e >= Now.
	NextAttemptAt time.Time
	// Now é o instante da aplicação (injetado; nada de relógio global).
	Now time.Time
}

// ApplyWagerDecision executa a decisão: movimenta a carteira (quando
// APPLY_MOVEMENT) e transiciona a transação (PROCESSED, PENDING_REFERENCE ou
// REJECTED). Devolve o lançamento do ledger, ou o zero-value
// (IsInitialized() == false) quando não há movimento.
//
// Em erro, carteira e transação ficam intactas: as mudanças são feitas em
// cópias e só publicadas no fim. A decisão é conferida contra a carteira atual
// (id, versão e saldo); divergência devolve ErrStaleWagerDecision.
func ApplyWagerDecision(p ApplyWagerDecisionParams) (WalletLedgerEntry, error) {
	d := p.Decision
	tx := p.Transaction
	if !d.IsInitialized() {
		return WalletLedgerEntry{}, fmt.Errorf("%w: uninitialized decision", ErrInvalidWagerRuleInput)
	}
	if !tx.IsInitialized() {
		return WalletLedgerEntry{}, fmt.Errorf("%w: transaction: %w", ErrInvalidWagerRuleInput, ErrUninitializedWagerTransaction)
	}
	if d.transactionID != tx.ID() {
		return WalletLedgerEntry{}, fmt.Errorf("%w: decision for %s applied to %s", ErrInvalidWagerRuleInput, d.transactionID, tx.ID())
	}
	if d.walletID != uuid.Nil {
		w := p.Wallet
		if !w.IsInitialized() || w.ID() != d.walletID {
			return WalletLedgerEntry{}, fmt.Errorf("%w: decision wallet %s not provided", ErrInvalidWagerRuleInput, d.walletID)
		}
		if w.Version() != d.walletVersion || w.Balance() != d.balanceBefore {
			return WalletLedgerEntry{}, fmt.Errorf("%w: wallet version %d, decided on %d", ErrStaleWagerDecision, w.Version(), d.walletVersion)
		}
	}

	next := *tx
	switch d.outcome {
	case WagerDecisionApplyMovement:
		wallet := *p.Wallet
		var entry WalletLedgerEntry
		var err error
		if d.direction == LedgerDirectionDebit {
			entry, err = wallet.Debit(d.amount, tx.ID(), p.Now)
		} else {
			entry, err = wallet.Credit(d.amount, tx.ID(), p.Now)
		}
		if err != nil {
			return WalletLedgerEntry{}, err
		}
		if entry.BalanceAfter() != d.balanceAfter {
			return WalletLedgerEntry{}, fmt.Errorf("%w: projected balance differs from applied balance", ErrStaleWagerDecision)
		}
		if err := next.MarkProcessed(MarkWagerProcessedParams{
			BalanceAfter:           entry.BalanceAfter(),
			ReferenceTransactionID: d.referenceTransactionID,
			Now:                    p.Now,
		}); err != nil {
			return WalletLedgerEntry{}, err
		}
		*p.Wallet = wallet
		*tx = next
		return entry, nil

	case WagerDecisionNoMovement:
		if err := next.MarkProcessed(MarkWagerProcessedParams{
			BalanceAfter:           d.balanceAfter,
			ReferenceTransactionID: d.referenceTransactionID,
			Now:                    p.Now,
		}); err != nil {
			return WalletLedgerEntry{}, err
		}

	case WagerDecisionAwaitReference:
		sched := MarkWagerPendingReferenceParams{NextAttemptAt: p.NextAttemptAt, Now: p.Now}
		var err error
		if next.Status() == WagerStatusPendingReference {
			err = next.RescheduleReferenceRetry(sched)
		} else {
			err = next.MarkPendingReference(sched)
		}
		if err != nil {
			return WalletLedgerEntry{}, err
		}

	case WagerDecisionReject:
		if err := next.Reject(RejectWagerParams{
			Code:                   d.failureCode,
			ReferenceTransactionID: d.referenceTransactionID,
			BalanceAfter:           d.balanceAfter,
			Now:                    p.Now,
		}); err != nil {
			return WalletLedgerEntry{}, err
		}
	}
	*tx = next
	return WalletLedgerEntry{}, nil
}
