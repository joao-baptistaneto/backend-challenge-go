package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/domain/idempotency"
)

// Caso de uso ProcessWagerTransaction (ARCHITECTURE.md, ADR-017). É o mesmo
// para HTTP (T18) e SQS (T20).
//
// Decisões:
//
//   - Uma única transação SQL por operação. O aceite não tem commit
//     intermediário: a transação é gravada já no estado final (PROCESSED,
//     REJECTED ou PENDING_REFERENCE), junto com carteira, ledger, outbox e
//     inbox. Por isso não existe PENDING confirmado no fluxo síncrono.
//   - Ordem dentro da transação:
//     1. inbox.Register (só SQS). Hash divergente → conflito MESSAGE_CONFLICT.
//     COMPLETED → replay do resultado gravado, sem tocar na carteira;
//     2. leitura sem lock por (provider, key). Encontrou → replay ou conflito
//     (caminho rápido, sem esperar o lock da carteira);
//     3. lock da carteira (GetForUpdate). Inexistente segue sem lock e vira
//     WALLET_NOT_FOUND;
//     4. nova leitura por (provider, key) e por (provider, externalId). Em READ
//     COMMITTED cada comando vê os commits anteriores, então, depois do lock,
//     a duplicata confirmada por quem segurava o lock é vista aqui;
//     5. referência e reversões existentes (mesma carteira, logo serializadas
//     pelo mesmo lock), DecideWager e ApplyWagerDecision;
//     6. escritas: carteira (Update condicional à versão), transação, ledger
//     (com a carteira travada, contrato do ADR-014), outbox e
//     inbox.Complete.
//   - Idempotência: mesma key e mesmo hash → replay com o resultado
//     persistido (balance_after gravado, não o saldo atual) e
//     idempotentReplay=true; mesma key e outro hash → IDEMPOTENCY_KEY_CONFLICT;
//     mesmo (provider, externalId) sob outra key →
//     EXTERNAL_TRANSACTION_CONFLICT. Os UNIQUE do banco são a barreira final:
//     numa corrida sem lock comum (carteira inexistente), o perdedor recebe
//     ErrDuplicateWagerTransaction, e o retry (RetryIf) relê a linha vencedora
//     e devolve o replay.
//   - O corpo é reexecutável: tudo é lido do tx, a transação nova é clonada
//     do modelo validado a cada tentativa e nada sai do banco. Retry limitado
//     de falhas transitórias (DefaultRetryPolicy) + RetryIf de duplicata.
//   - Conflitos e entradas inválidas são *Error e desfazem a transação (nada
//     é gravado, nem a inbox). Rejeições de negócio NÃO são erro: são
//     resultados persistidos (REJECTED + failureCode + evento).

// DefaultReferenceRetryDelay é o atraso da primeira nova busca de uma
// referência ausente ou pendente (next_attempt_at). O backoff seguinte e o
// limite de tentativas são do worker (T21).
const DefaultReferenceRetryDelay = 2 * time.Second

// InboxMessageRef identifica a mensagem de entrada (SQS) para a inbox. O hash
// registrado é o mesmo hash canônico da operação (T08), calculado pelo caso
// de uso; reentregas com o mesmo messageId e outro conteúdo são conflito.
type InboxMessageRef struct {
	ConsumerName string
	MessageID    string
}

// ProcessWagerCommand é a entrada do caso de uso. Money e Kind já vêm
// parseados pela borda (domain.ParseNonNegative, domain.ParseExternalWagerKind);
// ProviderID vem da identidade autenticada (HTTP) ou do envelope autorizado
// (SQS).
type ProcessWagerCommand struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PlayerID                       uuid.UUID
	WalletID                       uuid.UUID
	RoundID                        string
	GameID                         string
	Kind                           domain.WagerKind
	Money                          domain.Money
	ReferenceExternalTransactionID string
	// CorrelationID é obrigatório (eventos e logs).
	CorrelationID string
	// CausationID é opcional (ex.: messageId SQS).
	CausationID string
	// Inbox é opcional: presente na entrada SQS, registra e conclui a
	// mensagem na mesma transação dos efeitos.
	Inbox *InboxMessageRef
}

// WagerResult é o resultado devolvido ao provedor.
type WagerResult struct {
	TransactionID uuid.UUID
	WalletID      uuid.UUID
	Kind          domain.WagerKind
	Status        domain.WagerStatus
	// Balance é o saldo observado no processamento original (balance_after).
	// Money{} quando não se aplica (PENDING_REFERENCE) ou quando a rejeição
	// não pode expor a carteira (WALLET_NOT_FOUND, WALLET_PLAYER_MISMATCH,
	// CURRENCY_MISMATCH).
	Balance domain.Money
	// FailureCode é preenchido em REJECTED/FAILED.
	FailureCode domain.FailureCode
	// ReferenceTransactionID é a referência interna resolvida; só em
	// PROCESSED (em rejeições ela poderia ser de outra carteira).
	ReferenceTransactionID uuid.UUID
	// IdempotentReplay indica resultado lido de uma execução anterior.
	IdempotentReplay bool
}

// ProcessWagerTransaction processa operações de provedor.
type ProcessWagerTransaction struct {
	uow            UnitOfWork
	logger         *slog.Logger
	now            func() time.Time
	newID          func() (uuid.UUID, error)
	referenceDelay time.Duration
}

// WithReferenceRetryDelay define o atraso da primeira nova busca de
// referência (padrão DefaultReferenceRetryDelay).
func WithReferenceRetryDelay(d time.Duration) Option {
	return func(o *options) { o.referenceDelay = d }
}

// NewProcessWagerTransaction cria o caso de uso.
func NewProcessWagerTransaction(uow UnitOfWork, opts ...Option) (*ProcessWagerTransaction, error) {
	if uow == nil {
		return nil, errors.New("app: process wager: nil UnitOfWork")
	}
	o := buildOptions(opts)
	if o.now == nil || o.newID == nil {
		return nil, errors.New("app: process wager: nil clock or id generator")
	}
	delay := o.referenceDelay
	if delay == 0 {
		delay = DefaultReferenceRetryDelay
	}
	if delay < 0 {
		return nil, errors.New("app: process wager: negative reference retry delay")
	}
	return &ProcessWagerTransaction{
		uow: uow, now: o.now, newID: o.newID, referenceDelay: delay,
		logger: o.logger.With(slog.String("component", "app.process_wager")),
	}, nil
}

const opProcessWager = "process wager"

// wagerPlan é a entrada validada, montada antes da transação.
type wagerPlan struct {
	cmd     ProcessWagerCommand
	meta    domain.EventMetadata
	hash    string
	pending *domain.WagerTransaction // modelo PENDING; clonado a cada tentativa
}

// Execute processa a operação (ou devolve o replay).
func (uc *ProcessWagerTransaction) Execute(ctx context.Context, cmd ProcessWagerCommand) (WagerResult, error) {
	if err := ctx.Err(); err != nil {
		return WagerResult{}, err
	}
	plan, err := uc.plan(cmd)
	if err != nil {
		return WagerResult{}, err
	}

	var res WagerResult
	err = uc.uow.WithinTx(ctx, TxOptions{
		Operation: OperationProcessTransaction,
		Retry:     true,
		RetryIf:   isDuplicateWager,
	}, func(ctx context.Context, tx Tx) error {
		r, err := uc.run(ctx, tx, plan)
		if err != nil {
			return err
		}
		res = r
		return nil
	})
	if err != nil {
		return WagerResult{}, uc.classify(ctx, plan, err)
	}
	uc.logResult(ctx, plan, res)
	return res, nil
}

func isDuplicateWager(err error) bool { return errors.Is(err, ErrDuplicateWagerTransaction) }

func (uc *ProcessWagerTransaction) plan(cmd ProcessWagerCommand) (wagerPlan, error) {
	invalid := func(code string, err error) error {
		return &Error{Kind: KindInvalidInput, Code: code, Op: opProcessWager, Err: err}
	}
	meta, err := validateEventMetadata(opProcessWager, cmd.CorrelationID, cmd.CausationID)
	if err != nil {
		return wagerPlan{}, err
	}
	if in := cmd.Inbox; in != nil {
		if err := validateInboxRef(*in); err != nil {
			return wagerPlan{}, invalid(CodeInvalidMessage, err)
		}
	}
	if !cmd.Money.IsValid() {
		return wagerPlan{}, invalid(string(domain.FailureCodeInvalidAmount), domain.ErrUninitializedMoney)
	}
	hash, err := idempotency.RequestHash(idempotency.RequestFields{
		ProviderID:                     cmd.ProviderID,
		ExternalTransactionID:          cmd.ExternalTransactionID,
		PlayerID:                       cmd.PlayerID,
		WalletID:                       cmd.WalletID,
		RoundID:                        cmd.RoundID,
		GameID:                         cmd.GameID,
		Kind:                           cmd.Kind,
		Money:                          cmd.Money,
		ReferenceExternalTransactionID: cmd.ReferenceExternalTransactionID,
	})
	if err != nil {
		return wagerPlan{}, invalid(string(domain.FailureCodeInvalidRequest), err)
	}
	id, err := uc.newID()
	if err != nil {
		return wagerPlan{}, fmt.Errorf("app: %s: generate transaction id: %w", opProcessWager, err)
	}
	pending, err := domain.NewExternalWagerTransaction(domain.NewExternalWagerTransactionParams{
		ID:                             id,
		ProviderID:                     cmd.ProviderID,
		ExternalTransactionID:          cmd.ExternalTransactionID,
		IdempotencyKey:                 cmd.IdempotencyKey,
		RequestHash:                    hash,
		WalletID:                       cmd.WalletID,
		PlayerID:                       cmd.PlayerID,
		RoundID:                        cmd.RoundID,
		GameID:                         cmd.GameID,
		Kind:                           cmd.Kind,
		Money:                          cmd.Money,
		ReferenceExternalTransactionID: cmd.ReferenceExternalTransactionID,
		Now:                            uc.now().UTC(),
	})
	if err != nil {
		code, ok := domain.ValidationFailureCode(err)
		if !ok {
			return wagerPlan{}, fmt.Errorf("app: %s: build transaction: %w", opProcessWager, err)
		}
		return wagerPlan{}, invalid(string(code), err)
	}
	return wagerPlan{cmd: cmd, meta: meta, hash: hash, pending: pending}, nil
}

// validateInboxRef espelha os CHECKs da inbox (1..255 bytes).
func validateInboxRef(in InboxMessageRef) error {
	const maxLen = 255
	if l := len(in.ConsumerName); l < 1 || l > maxLen {
		return fmt.Errorf("consumerName with %d bytes (1..%d)", l, maxLen)
	}
	if l := len(in.MessageID); l < 1 || l > maxLen {
		return fmt.Errorf("messageId with %d bytes (1..%d)", l, maxLen)
	}
	return nil
}

// run é o corpo da transação (reexecutável).
func (uc *ProcessWagerTransaction) run(ctx context.Context, tx Tx, p wagerPlan) (WagerResult, error) {
	now := uc.now().UTC()
	in := p.cmd.Inbox
	if in != nil {
		rec, err := tx.Inbox().Register(ctx, in.ConsumerName, in.MessageID, p.hash, now)
		if err != nil {
			if errors.Is(err, ErrInboxHashMismatch) {
				return WagerResult{}, uc.conflict(CodeMessageConflict, err)
			}
			return WagerResult{}, err
		}
		if rec.Status == InboxCompleted {
			// Mensagem já tratada e confirmada: devolve o resultado gravado,
			// sem reprocessar e sem tocar na carteira.
			return uc.replayCompletedMessage(ctx, tx, p, rec)
		}
	}

	res, err := uc.process(ctx, tx, p, now)
	if err != nil {
		return WagerResult{}, err
	}

	if in != nil {
		// Mesmo commit dos efeitos (ou do replay): uma reentrega depois do
		// commit encontra COMPLETED.
		if err := tx.Inbox().Complete(ctx, in.ConsumerName, in.MessageID, p.hash, res.TransactionID, now); err != nil {
			return WagerResult{}, err
		}
	}
	return res, nil
}

func (uc *ProcessWagerTransaction) replayCompletedMessage(ctx context.Context, tx Tx, p wagerPlan, rec InboxRecord) (WagerResult, error) {
	if rec.TransactionID == uuid.Nil {
		return WagerResult{}, uc.conflict(CodeMessageConflict,
			errors.New("message already completed without a transaction"))
	}
	existing, err := tx.Transactions().GetByID(ctx, rec.TransactionID)
	if err != nil {
		return WagerResult{}, err
	}
	return uc.replayOrConflict(existing, p)
}

// process aplica a idempotência, trava a carteira, decide e grava.
func (uc *ProcessWagerTransaction) process(ctx context.Context, tx Tx, p wagerPlan, now time.Time) (WagerResult, error) {
	cmd := p.cmd
	txs := tx.Transactions()

	// Caminho rápido: replay sem esperar o lock da carteira.
	if existing, err := lookup(txs.GetByProviderIdempotencyKey(ctx, cmd.ProviderID, cmd.IdempotencyKey)); err != nil {
		return WagerResult{}, err
	} else if existing != nil {
		return uc.replayOrConflict(existing, p)
	}

	wallet, err := tx.Wallets().GetForUpdate(ctx, cmd.WalletID)
	switch {
	case errors.Is(err, ErrRecordNotFound):
		wallet = nil // WALLET_NOT_FOUND (decidido pelo domínio)
	case err != nil:
		return WagerResult{}, err
	}

	// Depois do lock: vê o que quem segurava o lock confirmou.
	if existing, err := lookup(txs.GetByProviderIdempotencyKey(ctx, cmd.ProviderID, cmd.IdempotencyKey)); err != nil {
		return WagerResult{}, err
	} else if existing != nil {
		return uc.replayOrConflict(existing, p)
	}
	if existing, err := lookup(txs.GetByProviderExternalID(ctx, cmd.ProviderID, cmd.ExternalTransactionID)); err != nil {
		return WagerResult{}, err
	} else if existing != nil {
		// Sem lock comum (carteira inexistente), a linha pode ter sido
		// confirmada entre as duas leituras com a MESMA chave: replayOrConflict
		// compara chave e hash em vez de presumir chave diferente.
		return uc.replayOrConflict(existing, p)
	}

	clone := *p.pending
	t := &clone

	ref, reversals, err := loadReference(ctx, txs, t)
	if err != nil {
		return WagerResult{}, err
	}
	decision, err := domain.DecideWager(domain.WagerRuleInput{
		Transaction: t, Wallet: wallet, Reference: ref, ReferenceReversals: reversals,
	})
	if err != nil {
		return WagerResult{}, fmt.Errorf("app: %s: decide: %w", opProcessWager, err)
	}
	entry, err := domain.ApplyWagerDecision(domain.ApplyWagerDecisionParams{
		Transaction: t, Wallet: wallet, Decision: decision,
		NextAttemptAt: now.Add(uc.referenceDelay), Now: now,
	})
	if err != nil {
		return WagerResult{}, fmt.Errorf("app: %s: apply decision: %w", opProcessWager, err)
	}

	events, err := wagerEvents(opProcessWager, t, entry, wallet, p.meta, true)
	if err != nil {
		return WagerResult{}, err
	}
	if err := persistWagerOutcome(ctx, tx, t, wallet, entry, events, txs.Insert); err != nil {
		return WagerResult{}, err
	}
	return resultOf(t, false), nil
}

// loadReference lê, sob o lock da carteira, a referência de t por (provider,
// referenceExternalTransactionId) e as reversões já existentes dela. Sem
// referência (ou ainda não recebida): nil. Compartilhado com o worker (T21).
func loadReference(ctx context.Context, txs WagerTransactionStore, t *domain.WagerTransaction) (*domain.WagerTransaction, []*domain.WagerTransaction, error) {
	if !t.HasReference() {
		return nil, nil, nil
	}
	ref, err := lookup(txs.GetByProviderExternalID(ctx, t.ProviderID(), t.ReferenceExternalTransactionID()))
	if err != nil || ref == nil || !t.Kind().IsReversal() {
		return ref, nil, err
	}
	all, err := txs.ListByProviderReference(ctx, t.ProviderID(), t.ReferenceExternalTransactionID())
	if err != nil {
		return nil, nil, err
	}
	// Só reversões contam (um WIN também pode citar a BET).
	var reversals []*domain.WagerTransaction
	for _, r := range all {
		if r.Kind().IsReversal() {
			reversals = append(reversals, r)
		}
	}
	return ref, reversals, nil
}

// persistWagerOutcome grava o resultado de uma decisão na ordem exigida pelas
// FKs e pelo contrato de lock (ADR-017): carteira (só com movimento; lida com
// GetForUpdate nesta transação), a transação (save = Insert no caso de uso,
// Update no worker), o lançamento (carteira travada; a transação já existe) e
// os eventos na outbox.
func persistWagerOutcome(ctx context.Context, tx Tx, t *domain.WagerTransaction, wallet *domain.Wallet,
	entry domain.WalletLedgerEntry, events []domain.IntegrationEvent,
	save func(context.Context, *domain.WagerTransaction) error,
) error {
	if entry.IsInitialized() {
		if err := tx.Wallets().Update(ctx, wallet); err != nil {
			return err
		}
	}
	if err := save(ctx, t); err != nil {
		return err
	}
	if entry.IsInitialized() {
		if err := tx.Ledger().Append(ctx, entry); err != nil {
			return err
		}
	}
	return tx.Outbox().Enqueue(ctx, events...)
}

// wagerEvents monta os eventos do estado final, na ordem de publicação.
// pendingEvent controla WagerTransactionPendingReference: emitido só na
// transição PENDING -> PENDING_REFERENCE (ADR-012); o reagendamento do worker
// (PENDING_REFERENCE -> PENDING_REFERENCE) não gera evento.
func wagerEvents(op string, t *domain.WagerTransaction, entry domain.WalletLedgerEntry, w *domain.Wallet,
	meta domain.EventMetadata, pendingEvent bool,
) ([]domain.IntegrationEvent, error) {
	var (
		ev  domain.IntegrationEvent
		err error
	)
	switch t.Status() {
	case domain.WagerStatusProcessed:
		ev, err = domain.NewWagerTransactionProcessedEvent(t, meta)
	case domain.WagerStatusRejected:
		ev, err = domain.NewWagerTransactionRejectedEvent(t, meta)
	case domain.WagerStatusPendingReference:
		if !pendingEvent {
			return nil, nil
		}
		ev, err = domain.NewWagerTransactionPendingReferenceEvent(t, meta)
	default:
		return nil, fmt.Errorf("app: %s: unexpected status %s after decision", op, t.Status())
	}
	if err != nil {
		return nil, fmt.Errorf("app: %s: build event: %w", op, err)
	}
	events := []domain.IntegrationEvent{ev}
	if entry.IsInitialized() {
		// Mesma instância de carteira que produziu o lançamento (ADR-012).
		changed, err := domain.NewWalletBalanceChangedEvent(entry, w, meta)
		if err != nil {
			return nil, fmt.Errorf("app: %s: build balance changed event: %w", op, err)
		}
		events = append(events, changed)
	}
	return events, nil
}

// replayOrConflict compara a transação gravada (encontrada pela chave, pelo
// externalId ou pela inbox) com o pedido atual. Chave diferente vence o hash:
// a mesma operação sob outra chave é EXTERNAL_TRANSACTION_CONFLICT, mesmo com
// conteúdo igual. Mesma chave com outro conteúdo é IDEMPOTENCY_KEY_CONFLICT.
func (uc *ProcessWagerTransaction) replayOrConflict(existing *domain.WagerTransaction, p wagerPlan) (WagerResult, error) {
	if existing.ProviderID() != p.cmd.ProviderID || existing.IdempotencyKey() != p.cmd.IdempotencyKey {
		return WagerResult{}, uc.conflict(CodeExternalTransactionConflict,
			errors.New("operation already registered under another idempotency key"))
	}
	if existing.RequestHash() != p.hash {
		return WagerResult{}, uc.conflict(CodeIdempotencyKeyConflict,
			errors.New("idempotency key already used with another payload"))
	}
	return resultOf(existing, true), nil
}

func (uc *ProcessWagerTransaction) conflict(code string, err error) error {
	return &Error{Kind: KindConflict, Code: code, Op: opProcessWager, Err: err}
}

// lookup converte "não encontrado" em (nil, nil).
func lookup(t *domain.WagerTransaction, err error) (*domain.WagerTransaction, error) {
	if errors.Is(err, ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return t, nil
}

// hiddenBalanceCodes: rejeições que não expõem saldo (mesma regra do evento,
// ADR-012).
var hiddenBalanceCodes = []domain.FailureCode{
	domain.FailureCodeWalletNotFound,
	domain.FailureCodeWalletPlayerMismatch,
	domain.FailureCodeCurrencyMismatch,
}

func resultOf(t *domain.WagerTransaction, replay bool) WagerResult {
	r := WagerResult{
		TransactionID:    t.ID(),
		WalletID:         t.WalletID(),
		Kind:             t.Kind(),
		Status:           t.Status(),
		FailureCode:      t.FailureCode(),
		IdempotentReplay: replay,
	}
	if b := t.BalanceAfter(); b.IsValid() && !slices.Contains(hiddenBalanceCodes, t.FailureCode()) {
		r.Balance = b
	}
	if t.Status() == domain.WagerStatusProcessed {
		r.ReferenceTransactionID = t.ReferenceTransactionID()
	}
	return r
}

// classify converte o erro da transação no erro tipado do caso de uso.
func (uc *ProcessWagerTransaction) classify(ctx context.Context, p wagerPlan, err error) error {
	attrs := uc.logAttrs(p)
	if ae, ok := AsError(err); ok {
		if ae.Kind == KindConflict {
			uc.logger.InfoContext(ctx, "operação recusada por conflito de idempotência",
				append(attrs, slog.String("code", ae.Code))...)
		}
		return err
	}
	switch {
	case errors.Is(err, ErrUnavailable), errors.Is(err, ErrDuplicateWagerTransaction):
		// Duplicata ainda em disputa depois das tentativas: o chamador repete
		// e a idempotência devolve o replay.
		uc.logger.WarnContext(ctx, "processamento falhou por indisponibilidade transitória",
			append(attrs, slog.String("error", err.Error()))...)
		return &Error{Kind: KindUnavailable, Code: CodeTemporarilyUnavailable, Op: opProcessWager, Err: err}
	default:
		uc.logger.ErrorContext(ctx, "processamento falhou", append(attrs, slog.String("error", err.Error()))...)
		return fmt.Errorf("app: %s: %w", opProcessWager, err)
	}
}

func (uc *ProcessWagerTransaction) logAttrs(p wagerPlan) []any {
	attrs := []any{
		slog.String("correlationId", p.cmd.CorrelationID),
		slog.String("providerId", p.cmd.ProviderID),
		slog.String("walletId", p.cmd.WalletID.String()),
		slog.String("kind", p.cmd.Kind.String()),
	}
	if p.cmd.Inbox != nil {
		attrs = append(attrs, slog.String("messageId", p.cmd.Inbox.MessageID))
	}
	return attrs
}

func (uc *ProcessWagerTransaction) logResult(ctx context.Context, p wagerPlan, r WagerResult) {
	attrs := append(uc.logAttrs(p),
		slog.String("transactionId", r.TransactionID.String()),
		slog.String("status", r.Status.String()),
		slog.Bool("idempotentReplay", r.IdempotentReplay),
	)
	if r.FailureCode != "" {
		attrs = append(attrs, slog.String("failureCode", r.FailureCode.String()))
	}
	uc.logger.InfoContext(ctx, "operação processada", attrs...)
}
