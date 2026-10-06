package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// Caso de uso ResumeWagerTransaction: retomada durável de transações não
// terminais pelo worker (ARCHITECTURE.md, ADR-023, T21).
//
// Decisões:
//
//   - O que é retomado. PENDING_REFERENCE vencido (next_attempt_at <= agora)
//     e PENDING órfão. O fluxo síncrono nunca confirma PENDING (ADR-017), então
//     uma linha PENDING confirmada só existe se um aceite foi interrompido (ou
//     se um aceite assíncrono futuro gravá-la). Ela é órfã quando venceu o seu
//     next_attempt_at ou, sem ele, a carência (OrphanGrace) desde a criação.
//   - Claim e processamento separados. Claim grava um lease na linha (token +
//     claimed_until) numa transação curta; cada item é então reaplicado na sua
//     própria transação. Um worker que morre não perde o item (o lease expira
//     e outra instância o reassume) e um lote não segura locks de várias
//     carteiras ao mesmo tempo.
//   - Reaplicação pelo mesmo caminho do caso de uso (ProcessWagerTransaction):
//     lock da carteira (GetForUpdate) ANTES da linha, a mesma ordem de quem
//     insere uma operação nova (carteira → KEY SHARE da FK de referência), logo
//     sem deadlock entre worker e HTTP/SQS; depois o lock da própria linha com
//     fencing (GetClaimedForUpdate: só se ainda for nossa); referência e
//     reversões (loadReference), DecideWager, ApplyWagerDecision, wagerEvents e
//     persistWagerOutcome — os mesmos de ProcessWagerTransaction, com Update no
//     lugar de Insert. Wallets.Update, Ledger.Append e Transactions.Update só
//     rodam com a carteira e a linha travadas (contratos dos ADR-014/017).
//   - Exatamente uma vez. Duas instâncias nunca aplicam a mesma linha: a que
//     perdeu o lease não passa do fencing, e a linha terminal não é mais
//     elegível nem atualizável (WHERE status não terminal + trigger). Os UNIQUE
//     do ledger (wallet, transaction) e da reversão única são a barreira final.
//   - Limite. attempts conta as buscas de referência sem sucesso (ADR-008).
//     Quando a referência continua indisponível (ausente ou ainda pendente) e
//     esta busca é a de número MaxAttempts, ou o TTL desde a criação venceu, a
//     transação vira REJECTED REFERENCE_NOT_FOUND com WagerTransactionRejected
//     no mesmo commit. A busca vem antes da expiração: uma referência que
//     chegou é aplicada mesmo na última tentativa. Referência que existe mas
//     terminou sem sucesso não espera: DecideWager rejeita com
//     REFERENCE_NOT_PROCESSED (ADR-009).
//   - Backoff exponencial persistido: next_attempt_at = agora +
//     RetryBaseDelay·2^(attempts−1), com teto RetryMaxDelay, e nunca depois do
//     fim do TTL (a expiração acontece no prazo). A primeira espera é a do
//     aceite (ProcessWagerTransaction, attempts = 1).
//   - Eventos: PROCESSED/REJECTED geram os mesmos eventos do fluxo síncrono;
//     PENDING → PENDING_REFERENCE gera WagerTransactionPendingReference; o
//     reagendamento não gera evento (ADR-012). correlationId = id da
//     transação e causationId = token do claim (rastreável até o lote).
//   - Falhas: transitórias fazem rollback (retry limitado do UnitOfWork);
//     esgotado, o lease continua gravado e o item volta quando ele expirar.

// Defaults da política de referências pendentes (README §7).
const (
	DefaultPendingReferenceMaxAttempts = 10
	DefaultPendingReferenceTTL         = 10 * time.Minute
	DefaultPendingReferenceMaxDelay    = time.Minute
)

// PendingWagerPolicy é a política de retomada de referências pendentes.
type PendingWagerPolicy struct {
	// MaxAttempts é o número máximo de buscas da referência, contando a do
	// aceite (>= 1).
	MaxAttempts int
	// TTL é o prazo, desde a criação, para a referência ficar disponível (> 0).
	TTL time.Duration
	// RetryBaseDelay e RetryMaxDelay definem o backoff entre buscas (0 <
	// base <= max).
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
}

// DefaultPendingWagerPolicy devolve a política padrão.
func DefaultPendingWagerPolicy() PendingWagerPolicy {
	return PendingWagerPolicy{
		MaxAttempts: DefaultPendingReferenceMaxAttempts, TTL: DefaultPendingReferenceTTL,
		RetryBaseDelay: DefaultReferenceRetryDelay, RetryMaxDelay: DefaultPendingReferenceMaxDelay,
	}
}

// Validate valida a política.
func (p PendingWagerPolicy) Validate() error {
	switch {
	case p.MaxAttempts < 1 || p.MaxAttempts > math.MaxInt32:
		return fmt.Errorf("app: pending policy: max attempts %d (1..%d)", p.MaxAttempts, math.MaxInt32)
	case p.TTL <= 0:
		return fmt.Errorf("app: pending policy: TTL %v must be positive", p.TTL)
	case p.RetryBaseDelay <= 0 || p.RetryMaxDelay < p.RetryBaseDelay:
		return fmt.Errorf("app: pending policy: retry delays base %v / max %v (0 < base <= max)", p.RetryBaseDelay, p.RetryMaxDelay)
	}
	return nil
}

// Delay devolve a espera depois da attempts-ésima busca sem sucesso:
// RetryBaseDelay·2^(attempts−1), com teto RetryMaxDelay e sem overflow.
func (p PendingWagerPolicy) Delay(attempts int32) time.Duration {
	d := p.RetryBaseDelay
	for i := int32(1); i < attempts && d < p.RetryMaxDelay; i++ {
		if d > math.MaxInt64/2 {
			return p.RetryMaxDelay
		}
		d *= 2
	}
	return min(d, p.RetryMaxDelay)
}

// deadline é o fim do TTL da transação.
func (p PendingWagerPolicy) deadline(t *domain.WagerTransaction) time.Time {
	return t.CreatedAt().Add(p.TTL)
}

// exhausted indica que a busca atual (a de número attempts+1) é a última:
// atingiu MaxAttempts ou o TTL venceu.
func (p PendingWagerPolicy) exhausted(t *domain.WagerTransaction, now time.Time) bool {
	return int64(t.Attempts())+1 >= int64(p.MaxAttempts) || !now.Before(p.deadline(t))
}

// ClaimRequest são os parâmetros de ResumeWagerTransaction.Claim.
type ClaimRequest struct {
	// Token identifica o lote (fencing); o worker gera um por ciclo.
	Token uuid.UUID
	// Lease é a duração da reivindicação: precisa cobrir o processamento do
	// lote inteiro.
	Lease time.Duration
	// Limit é o tamanho máximo do lote.
	Limit int
	// OrphanGrace é a idade mínima de um PENDING sem nextAttemptAt.
	OrphanGrace time.Duration
}

// ResumeOutcome é o resultado de uma retomada.
type ResumeOutcome string

const (
	// ResumeResolved: a operação terminou (PROCESSED ou REJECTED por regra de
	// negócio) porque a referência ficou disponível (ou o PENDING órfão foi
	// concluído).
	ResumeResolved ResumeOutcome = "RESOLVED"
	// ResumeExpired: tentativas/TTL esgotados; REJECTED REFERENCE_NOT_FOUND.
	ResumeExpired ResumeOutcome = "EXPIRED"
	// ResumeRescheduled: a referência continua indisponível; nova busca em
	// NextAttemptAt (inclui PENDING órfão que passou a PENDING_REFERENCE).
	ResumeRescheduled ResumeOutcome = "RESCHEDULED"
	// ResumeSkipped: nada a fazer. A linha não existe, já é terminal ou o
	// lease foi reassumido por outra instância (fencing).
	ResumeSkipped ResumeOutcome = "SKIPPED"
)

// ResumeResult é o resultado de ResumeWagerTransaction.Resume.
type ResumeResult struct {
	Outcome       ResumeOutcome
	TransactionID uuid.UUID
	WalletID      uuid.UUID
	ProviderID    string
	Kind          domain.WagerKind
	// PreviousStatus é o estado antes da retomada (PENDING ou
	// PENDING_REFERENCE); vazio em ResumeSkipped sem linha.
	PreviousStatus domain.WagerStatus
	Status         domain.WagerStatus
	FailureCode    domain.FailureCode
	Attempts       int32
	NextAttemptAt  time.Time
}

// ResumeWagerTransaction retoma transações pendentes reivindicadas pelo worker.
type ResumeWagerTransaction struct {
	uow    UnitOfWork
	logger *slog.Logger
	now    func() time.Time
	policy PendingWagerPolicy
}

const opResumeWager = "resume wager"

// NewResumeWagerTransaction cria o caso de uso.
func NewResumeWagerTransaction(uow UnitOfWork, policy PendingWagerPolicy, opts ...Option) (*ResumeWagerTransaction, error) {
	if uow == nil {
		return nil, errors.New("app: resume wager: nil UnitOfWork")
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	o := buildOptions(opts)
	if o.now == nil {
		return nil, errors.New("app: resume wager: nil clock")
	}
	return &ResumeWagerTransaction{
		uow: uow, now: o.now, policy: policy,
		logger: o.logger.With(slog.String("component", "app.resume_wager")),
	}, nil
}

// Policy devolve a política em uso.
func (uc *ResumeWagerTransaction) Policy() PendingWagerPolicy { return uc.policy }

// Claim reivindica um lote de transações vencidas, numa transação curta e
// própria. O lease fica gravado: devolvido o lote, cada item deve ser
// processado com Resume(id, req.Token) antes de o lease expirar.
func (uc *ResumeWagerTransaction) Claim(ctx context.Context, req ClaimRequest) ([]ClaimedWager, error) {
	if req.Token == uuid.Nil {
		return nil, &Error{Kind: KindInvalidInput, Code: CodeInvalidClaim, Op: opResumeWager, Err: errors.New("nil claim token")}
	}
	var claimed []ClaimedWager
	err := uc.uow.WithinTx(ctx, TxOptions{Operation: OperationPendingReference, Retry: true},
		func(ctx context.Context, tx Tx) error {
			var err error
			claimed, err = tx.Transactions().ClaimDue(ctx, ClaimDueParams{
				Now: uc.now().UTC(), Token: req.Token, Lease: req.Lease, Limit: req.Limit, OrphanGrace: req.OrphanGrace,
			})
			return err
		})
	if err != nil {
		return nil, uc.classify(err)
	}
	return claimed, nil
}

// Resume reaplica a transação id, reivindicada com token, e grava o
// resultado (com eventos) num único commit. Não é erro encontrar a linha já
// resolvida ou reassumida: o resultado é ResumeSkipped.
func (uc *ResumeWagerTransaction) Resume(ctx context.Context, id, token uuid.UUID) (ResumeResult, error) {
	if id == uuid.Nil || token == uuid.Nil {
		return ResumeResult{}, &Error{Kind: KindInvalidInput, Code: CodeInvalidClaim, Op: opResumeWager,
			Err: errors.New("nil transaction id or claim token")}
	}
	if err := ctx.Err(); err != nil {
		return ResumeResult{}, err
	}
	var res ResumeResult
	err := uc.uow.WithinTx(ctx, TxOptions{Operation: OperationPendingReference, Retry: true},
		func(ctx context.Context, tx Tx) error {
			r, err := uc.resume(ctx, tx, id, token)
			if err != nil {
				return err
			}
			res = r
			return nil
		})
	if err != nil {
		err = uc.classify(err)
		uc.logger.WarnContext(ctx, "retomada de transação pendente falhou",
			slog.String("transactionId", id.String()), slog.String("error", err.Error()))
		return ResumeResult{}, err
	}
	uc.logResult(ctx, res)
	return res, nil
}

func (uc *ResumeWagerTransaction) resume(ctx context.Context, tx Tx, id, token uuid.UUID) (ResumeResult, error) {
	now := uc.now().UTC()
	txs := tx.Transactions()

	// 1. Leitura sem lock só para descobrir a carteira (wallet_id é imutável).
	peek, err := lookup(txs.GetByID(ctx, id))
	if err != nil {
		return ResumeResult{}, err
	}
	if peek == nil {
		return ResumeResult{Outcome: ResumeSkipped, TransactionID: id}, nil
	}
	if peek.IsTerminal() {
		return skippedResult(peek), nil
	}

	// 2. Lock da carteira antes da linha (mesma ordem do caso de uso).
	wallet, err := tx.Wallets().GetForUpdate(ctx, peek.WalletID())
	switch {
	case errors.Is(err, ErrRecordNotFound):
		wallet = nil // WALLET_NOT_FOUND (decidido pelo domínio)
	case err != nil:
		return ResumeResult{}, err
	}

	// 3. Lock da linha com fencing: só se o lease ainda for nosso.
	t, err := lookup(txs.GetClaimedForUpdate(ctx, id, token))
	if err != nil {
		return ResumeResult{}, err
	}
	if t == nil {
		return skippedResult(peek), nil
	}
	if t.IsTerminal() {
		return skippedResult(t), nil
	}
	previous := t.Status()

	// 4. Mesma decisão do fluxo síncrono.
	ref, reversals, err := loadReference(ctx, txs, t)
	if err != nil {
		return ResumeResult{}, err
	}
	decision, err := domain.DecideWager(domain.WagerRuleInput{
		Transaction: t, Wallet: wallet, Reference: ref, ReferenceReversals: reversals,
	})
	if err != nil {
		return ResumeResult{}, fmt.Errorf("app: %s: decide: %w", opResumeWager, err)
	}

	outcome := ResumeResolved
	var entry domain.WalletLedgerEntry
	switch {
	case decision.Outcome() == domain.WagerDecisionAwaitReference && uc.policy.exhausted(t, now):
		// Expira: a referência continua indisponível na última busca.
		outcome = ResumeExpired
		var observed domain.Money
		if wallet != nil {
			observed = wallet.Balance() // saldo observado, como nas demais rejeições
		}
		if err := t.Reject(domain.RejectWagerParams{
			Code: domain.FailureCodeReferenceNotFound, BalanceAfter: observed, Now: now,
		}); err != nil {
			return ResumeResult{}, fmt.Errorf("app: %s: expire: %w", opResumeWager, err)
		}
	default:
		if decision.Outcome() == domain.WagerDecisionAwaitReference {
			outcome = ResumeRescheduled
		}
		next := now.Add(uc.policy.Delay(t.Attempts() + 1))
		if deadline := uc.policy.deadline(t); next.After(deadline) {
			next = deadline // a expiração acontece no prazo do TTL
		}
		entry, err = domain.ApplyWagerDecision(domain.ApplyWagerDecisionParams{
			Transaction: t, Wallet: wallet, Decision: decision, NextAttemptAt: next, Now: now,
		})
		if err != nil {
			return ResumeResult{}, fmt.Errorf("app: %s: apply decision: %w", opResumeWager, err)
		}
	}

	meta := domain.EventMetadata{CorrelationID: t.ID().String(), CausationID: token.String()}
	events, err := wagerEvents(opResumeWager, t, entry, wallet, meta, previous == domain.WagerStatusPending)
	if err != nil {
		return ResumeResult{}, err
	}
	if err := persistWagerOutcome(ctx, tx, t, wallet, entry, events, txs.Update); err != nil {
		return ResumeResult{}, err
	}
	r := resumeResultOf(t, outcome)
	r.PreviousStatus = previous
	return r, nil
}

func skippedResult(t *domain.WagerTransaction) ResumeResult {
	r := resumeResultOf(t, ResumeSkipped)
	r.PreviousStatus = t.Status()
	return r
}

func resumeResultOf(t *domain.WagerTransaction, outcome ResumeOutcome) ResumeResult {
	return ResumeResult{
		Outcome:       outcome,
		TransactionID: t.ID(),
		WalletID:      t.WalletID(),
		ProviderID:    t.ProviderID(),
		Kind:          t.Kind(),
		Status:        t.Status(),
		FailureCode:   t.FailureCode(),
		Attempts:      t.Attempts(),
		NextAttemptAt: t.NextAttemptAt(),
	}
}

// classify marca falhas transitórias com KindUnavailable; as demais voltam
// embrulhadas (erro interno).
func (uc *ResumeWagerTransaction) classify(err error) error {
	if _, ok := AsError(err); ok {
		return err
	}
	if errors.Is(err, ErrUnavailable) {
		return &Error{Kind: KindUnavailable, Code: CodeTemporarilyUnavailable, Op: opResumeWager, Err: err}
	}
	return fmt.Errorf("app: %s: %w", opResumeWager, err)
}

func (uc *ResumeWagerTransaction) logResult(ctx context.Context, r ResumeResult) {
	attrs := []any{
		slog.String("transactionId", r.TransactionID.String()),
		slog.String("outcome", string(r.Outcome)),
	}
	if r.WalletID != uuid.Nil {
		attrs = append(attrs,
			slog.String("walletId", r.WalletID.String()),
			slog.String("providerId", r.ProviderID),
			slog.String("kind", r.Kind.String()),
			slog.String("previousStatus", r.PreviousStatus.String()),
			slog.String("status", r.Status.String()),
			slog.Int("attempts", int(r.Attempts)),
		)
	}
	if r.FailureCode != "" {
		attrs = append(attrs, slog.String("failureCode", r.FailureCode.String()))
	}
	if !r.NextAttemptAt.IsZero() {
		attrs = append(attrs, slog.Time("nextAttemptAt", r.NextAttemptAt))
	}
	level := slog.LevelInfo
	if r.Outcome == ResumeSkipped || r.Outcome == ResumeRescheduled {
		level = slog.LevelDebug
	}
	uc.logger.Log(ctx, level, "transação pendente retomada", attrs...)
}
