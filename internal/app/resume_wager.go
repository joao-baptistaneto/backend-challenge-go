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
//   - Falhas (T30, ADR-031). Transitórias (ErrUnavailable: conexão,
//     serialização, deadlock, timeout/prazo; e cancelamento) fazem rollback
//     (retry limitado do UnitOfWork) e NÃO contam: esgotado o retry, o lease
//     continua gravado e o item volta quando ele expirar, como antes, então
//     uma queda do banco nunca leva um item bom a FAILED. Qualquer outro erro
//     (linha, carteira ou referência que não reidrata, violação de
//     constraint, erro de domínio inesperado) é uma falha NÃO transitória da
//     retomada: numa transação curta e própria, com o fencing do lease,
//     resume_failures += 1 e o lease é estendido por um backoff exponencial
//     (Delay(falhas)); o item só volta depois dele. Na falha de número
//     MaxResumeFailures, ou depois do TTL desde a criação, a linha vira
//     FAILED RETRIES_EXHAUSTED numa segunda transação curta, DEPOIS do
//     commit da contagem (WagerTransaction.Fail + Update; ou, se a linha não
//     reidrata por qualquer erro não transitório ou a gravação pelo agregado
//     falha, domain.ValidateFailTransition + FailClaimedUnreadable numa
//     transação nova), sem lock de carteira, sem movimentar saldo, sem
//     ledger e sem evento (FAILED é registro de auditoria, ADR-031). Como a
//     contagem já é durável, uma falha ao gravar FAILED nunca desfaz o
//     incremento: o item volta depois do backoff, conta de novo e tenta outra
//     vez (sem loop abaixo do limite). Uma retomada bem-sucedida zera o
//     contador (falhas consecutivas).

// Defaults da política de referências pendentes (README §7).
const (
	DefaultPendingReferenceMaxAttempts = 10
	DefaultPendingReferenceTTL         = 10 * time.Minute
	DefaultPendingReferenceMaxDelay    = time.Minute
	// DefaultPendingWorkerMaxFailures é o padrão de falhas consecutivas não
	// transitórias da retomada antes de FAILED (T30).
	DefaultPendingWorkerMaxFailures = 5
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
	// MaxResumeFailures é o número de falhas consecutivas NÃO transitórias
	// da retomada de um item que o leva a FAILED RETRIES_EXHAUSTED (>= 1).
	// O backoff entre elas usa RetryBaseDelay/RetryMaxDelay (Delay). O TTL
	// também encerra: uma falha depois dele já é a última. (T30, ADR-031.)
	MaxResumeFailures int
}

// DefaultPendingWagerPolicy devolve a política padrão.
func DefaultPendingWagerPolicy() PendingWagerPolicy {
	return PendingWagerPolicy{
		MaxAttempts: DefaultPendingReferenceMaxAttempts, TTL: DefaultPendingReferenceTTL,
		RetryBaseDelay: DefaultReferenceRetryDelay, RetryMaxDelay: DefaultPendingReferenceMaxDelay,
		MaxResumeFailures: DefaultPendingWorkerMaxFailures,
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
	case p.MaxResumeFailures < 1 || p.MaxResumeFailures > math.MaxInt32:
		return fmt.Errorf("app: pending policy: max resume failures %d (1..%d)", p.MaxResumeFailures, math.MaxInt32)
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
	// ResumeDeferred: a retomada falhou por erro não transitório; a falha foi
	// contada (ResumeFailures) e o item fica retido até HeldUntil (backoff).
	// Resume devolve este resultado JUNTO com o erro original (T30).
	ResumeDeferred ResumeOutcome = "DEFERRED"
	// ResumeFailed: falhas não transitórias esgotadas (MaxResumeFailures ou
	// TTL); a transação foi gravada FAILED RETRIES_EXHAUSTED. Terminal: Resume
	// devolve erro nil e a causa em Cause (T30).
	ResumeFailed ResumeOutcome = "FAILED"
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
	// ResumeFailures são as falhas consecutivas não transitórias registradas
	// (ResumeDeferred e ResumeFailed).
	ResumeFailures int32
	// HeldUntil é o fim do backoff de uma falha contada (ResumeDeferred).
	HeldUntil time.Time
	// Cause é o erro da última retomada (ResumeFailed).
	Cause error
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
		if !countsAsResumeFailure(ctx, err) {
			uc.logger.WarnContext(ctx, "retomada de transação pendente falhou (transitória; o item volta quando o lease expirar)",
				slog.String("transactionId", id.String()), slog.String("error", err.Error()))
			return ResumeResult{}, err
		}
		return uc.handleFailure(ctx, id, token, err)
	}
	uc.logResult(ctx, res)
	return res, nil
}

// countsAsResumeFailure decide se o erro de uma retomada conta para o limite
// de falhas (T30, ADR-031). Não contam: cancelamento/prazo do próprio ctx
// (shutdown, ProcessTimeout) e falhas transitórias de infraestrutura
// (ErrUnavailable: conexão, serialização, deadlock, lock/statement timeout,
// commit ambíguo). Elas não dizem nada sobre o item e, contadas, uma queda do
// banco levaria itens bons a FAILED. Todo o resto (reidratação, constraint,
// erro de domínio inesperado, erro interno) conta.
func countsAsResumeFailure(ctx context.Context, err error) bool {
	switch {
	case err == nil, ctx.Err() != nil:
		return false
	case errors.Is(err, ErrUnavailable), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return false
	}
	return true
}

// handleFailure registra uma falha não transitória e devolve:
//   - ResumeFailed e erro nil, se a linha foi gravada FAILED;
//   - ResumeDeferred e a causa, se a falha foi contada e o item retido
//     (inclusive quando o limite esgotou mas FAILED não pôde ser gravado);
//   - resultado vazio e a causa, se nada foi registrado (lease perdido ou o
//     registro também falhou: o item volta quando o lease expirar).
//
// São duas etapas com commits próprios (ADR-031): (1) contar a falha e
// estender o lease pelo backoff; (2) só se esgotou, gravar FAILED. A contagem
// é durável ANTES de qualquer tentativa de encerramento: um erro na etapa 2
// (de qualquer natureza) não desfaz o incremento, então toda rodada com erro
// não transitório avança o contador e o item nunca fica preso abaixo do
// limite (o loop infinito que um rollback conjunto permitia).
func (uc *ResumeWagerTransaction) handleFailure(ctx context.Context, id, token uuid.UUID, cause error) (ResumeResult, error) {
	attrs := []any{slog.String("transactionId", id.String()), slog.String("error", cause.Error())}
	var rec failureRecord
	err := uc.uow.WithinTx(ctx, TxOptions{Operation: OperationPendingReference, Retry: true},
		func(ctx context.Context, tx Tx) error {
			r, err := uc.countFailure(ctx, tx, id, token)
			if err != nil {
				return err
			}
			rec = r
			return nil
		})
	if err != nil {
		uc.logger.ErrorContext(ctx, "retomada falhou e a falha não pôde ser registrada; o item volta quando o lease expirar",
			append(attrs, slog.String("recordError", uc.classify(err).Error()))...)
		return ResumeResult{}, cause
	}
	if rec.leaseLost {
		uc.logger.WarnContext(ctx, "retomada falhou; lease já reassumido, falha não registrada", attrs...)
		return ResumeResult{}, cause
	}
	deferred := ResumeResult{
		Outcome: ResumeDeferred, TransactionID: id, PreviousStatus: rec.state.Status, Status: rec.state.Status,
		ResumeFailures: rec.failures, HeldUntil: rec.heldUntil,
	}
	if !rec.exhausted {
		uc.logger.WarnContext(ctx, "retomada falhou (não transitória); falha contada e item retido com backoff",
			append(attrs, slog.Int("resumeFailures", int(rec.failures)),
				slog.Int("maxResumeFailures", uc.policy.MaxResumeFailures), slog.Time("heldUntil", rec.heldUntil))...)
		return deferred, cause
	}

	res, err := uc.failExhausted(ctx, id, token)
	switch {
	case err != nil:
		// A contagem já é durável: o item volta depois do backoff (no máximo
		// RetryMaxDelay), conta de novo e tenta outra vez gravar FAILED.
		uc.logger.ErrorContext(ctx, "falhas não transitórias esgotadas, mas FAILED não pôde ser gravado; falha contada e item retido com backoff",
			append(attrs, slog.String("failError", uc.classify(err).Error()), slog.Int("resumeFailures", int(rec.failures)),
				slog.Time("heldUntil", rec.heldUntil))...)
		return deferred, cause
	case res.Outcome != ResumeFailed:
		uc.logger.WarnContext(ctx, "retomada falhou; lease já reassumido ou linha já terminal, FAILED não gravado", attrs...)
		return ResumeResult{}, cause
	}
	res.Cause = cause
	res.ResumeFailures = rec.failures
	uc.logger.ErrorContext(ctx, "transação pendente encerrada como FAILED: falhas não transitórias esgotadas",
		append(attrs, slog.String("walletId", res.WalletID.String()), slog.String("previousStatus", res.PreviousStatus.String()),
			slog.String("failureCode", res.FailureCode.String()), slog.Int("resumeFailures", int(res.ResumeFailures)))...)
	return res, nil
}

// failureRecord é o resultado de countFailure.
type failureRecord struct {
	leaseLost bool // perdeu o lease ou a linha já é terminal: nada gravado
	state     ResumeFailureState
	failures  int32
	heldUntil time.Time
	exhausted bool // atingiu MaxResumeFailures ou o TTL: gravar FAILED
}

// countFailure conta a falha da linha reivindicada por token e estende o
// lease pelo backoff. Não reidrata nem trava a carteira: a única trava é a da
// própria linha (sem ciclo de espera com quem trava carteira → KEY SHARE da
// referência, porque FOR NO KEY UPDATE não conflita com KEY SHARE).
func (uc *ResumeWagerTransaction) countFailure(ctx context.Context, tx Tx, id, token uuid.UUID) (failureRecord, error) {
	now := uc.now().UTC()
	txs := tx.Transactions()
	st, err := txs.LockClaimedForFailure(ctx, id, token)
	switch {
	case errors.Is(err, ErrRecordNotFound):
		return failureRecord{leaseLost: true}, nil
	case err != nil:
		return failureRecord{}, err
	}
	if st.Status.IsTerminal() { // defensivo: linha terminal não fica reivindicada (CHECK)
		return failureRecord{leaseLost: true, state: st}, nil
	}
	failures := st.Failures
	if failures < math.MaxInt32 {
		failures++
	}
	held := now.Add(uc.policy.Delay(failures))
	if err := txs.RecordResumeFailure(ctx, RecordResumeFailureParams{ID: id, Token: token, Failures: failures, HoldUntil: held}); err != nil {
		if errors.Is(err, ErrRecordNotFound) {
			return failureRecord{leaseLost: true, state: st}, nil
		}
		return failureRecord{}, err
	}
	return failureRecord{
		state: st, failures: failures, heldUntil: held,
		exhausted: int64(failures) >= int64(uc.policy.MaxResumeFailures) || !now.Before(st.CreatedAt.Add(uc.policy.TTL)),
	}, nil
}

// errFailViaAggregate marca uma falha não transitória do caminho pelo
// agregado em failExhausted: a gravação recorre ao caminho sem agregado.
var errFailViaAggregate = errors.New("app: resume wager: fail via aggregate")

// failExhausted grava FAILED RETRIES_EXHAUSTED numa linha cuja contagem de
// falhas esgotou, sem lock de carteira e sem movimentar saldo, ledger ou
// outbox (ADR-031), com o fencing do lease. Primeiro pelo agregado
// (GetClaimedForUpdate + WagerTransaction.Fail + Update). Se a linha não
// reidrata — por QUALQUER erro não transitório, não só
// ErrCorruptedWagerTransaction — ou a gravação pelo agregado falha de forma
// não transitória, grava numa transação nova (a anterior pode ter sido
// abortada pelo servidor) pelo caminho sem agregado:
// domain.ValidateFailTransition + FailClaimedUnreadable sobre o estado lido
// por LockClaimedForFailure. Outcome vazio: lease perdido ou linha terminal.
func (uc *ResumeWagerTransaction) failExhausted(ctx context.Context, id, token uuid.UUID) (ResumeResult, error) {
	const code = domain.FailureCodeRetriesExhausted
	opts := TxOptions{Operation: OperationPendingReference, Retry: true}
	var res ResumeResult
	err := uc.uow.WithinTx(ctx, opts, func(ctx context.Context, tx Tx) error {
		res = ResumeResult{}
		now := uc.now().UTC()
		txs := tx.Transactions()
		t, err := txs.GetClaimedForUpdate(ctx, id, token)
		switch {
		case errors.Is(err, ErrRecordNotFound):
			return nil
		case err != nil:
			return viaAggregateFailure(ctx, err)
		case t.IsTerminal():
			return nil
		}
		previous := t.Status()
		if err := t.Fail(domain.FailWagerParams{Code: code, Now: now}); err != nil {
			return viaAggregateFailure(ctx, fmt.Errorf("app: %s: fail: %w", opResumeWager, err))
		}
		if err := txs.Update(ctx, t); err != nil {
			if errors.Is(err, ErrRecordNotFound) {
				return nil
			}
			return viaAggregateFailure(ctx, err)
		}
		res = resumeResultOf(t, ResumeFailed)
		res.PreviousStatus = previous
		return nil
	})
	if !errors.Is(err, errFailViaAggregate) {
		return res, err
	}
	uc.logger.WarnContext(ctx, "FAILED pelo agregado falhou; gravando pelo caminho sem agregado",
		slog.String("transactionId", id.String()), slog.String("error", err.Error()))

	err = uc.uow.WithinTx(ctx, opts, func(ctx context.Context, tx Tx) error {
		res = ResumeResult{}
		now := uc.now().UTC()
		txs := tx.Transactions()
		st, err := txs.LockClaimedForFailure(ctx, id, token)
		switch {
		case errors.Is(err, ErrRecordNotFound):
			return nil
		case err != nil:
			return err
		case st.Status.IsTerminal():
			return nil
		}
		// A regra da transição continua sendo do domínio; o UPDATE só grava
		// as colunas de resultado.
		if err := domain.ValidateFailTransition(st.Status, code); err != nil {
			return fmt.Errorf("app: %s: fail unreadable: %w", opResumeWager, err)
		}
		if err := txs.FailClaimedUnreadable(ctx, FailClaimedParams{ID: id, Token: token, From: st.Status, Code: code, Now: now}); err != nil {
			if errors.Is(err, ErrRecordNotFound) {
				return nil
			}
			return err
		}
		res = ResumeResult{
			Outcome: ResumeFailed, TransactionID: id, PreviousStatus: st.Status,
			Status: domain.WagerStatusFailed, FailureCode: code,
		}
		return nil
	})
	return res, err
}

// viaAggregateFailure decide o destino de um erro do caminho pelo agregado:
// transitório (ErrUnavailable, cancelamento) volta como está, para o retry do
// UnitOfWork e, esgotado, para a próxima rodada; qualquer outro desvia para o
// caminho sem agregado.
func viaAggregateFailure(ctx context.Context, err error) error {
	if !countsAsResumeFailure(ctx, err) {
		return err
	}
	return fmt.Errorf("%w: %w", errFailViaAggregate, err)
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
