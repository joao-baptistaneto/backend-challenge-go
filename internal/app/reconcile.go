package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// Caso de uso ReconcileWallet (ARCHITECTURE.md, ADR-018).
//
// Decisões:
//
//   - Uma transação REPEATABLE READ e READ ONLY. O snapshot é fixado no
//     primeiro comando (leitura da carteira), então o saldo gravado e a soma
//     do ledger vêm do MESMO estado confirmado, mesmo com operações
//     concorrentes confirmando no meio. Nenhum lock é tomado: a reconciliação
//     não bloqueia nem é bloqueada pelos escritores. O modo READ ONLY faz o
//     PostgreSQL recusar qualquer escrita (25006): a reconciliação não pode
//     alterar saldo nem nenhum outro dado, nem por engano.
//   - calculated = Σ créditos − Σ débitos do ledger (inclusive a abertura),
//     somado no banco em NUMERIC; difference = stored − calculated.
//   - Divergência não é erro: o relatório volta com consistent=false, um log
//     WARN estruturado (walletId, valores exatos em string, lançamentos) e a
//     métrica de divergência (por moeda) é incrementada. Toda reconciliação
//     concluída também conta em wallet_reconciliations_total{outcome}.
//   - A métrica entra por uma porta (ReconciliationRecorder), satisfeita por
//     *metrics.Metrics: app não importa platform.

// ReconciliationRecorder registra o resultado de uma reconciliação (métrica).
// *metrics.Metrics satisfaz esta interface.
type ReconciliationRecorder interface {
	Reconciliation(currency string, divergent bool)
}

// ReconcileCommand é a entrada da reconciliação.
type ReconcileCommand struct {
	WalletID uuid.UUID
	// CorrelationID é opcional (só vai para os logs).
	CorrelationID string
}

// ReconciliationReport é o resultado da reconciliação.
type ReconciliationReport struct {
	WalletID uuid.UUID
	// StoredBalance é o saldo gravado na carteira.
	StoredBalance domain.Money
	// CalculatedBalance é o saldo reconstruído do ledger.
	CalculatedBalance domain.Money
	// Difference = StoredBalance − CalculatedBalance.
	Difference domain.Money
	// Consistent indica Difference zero.
	Consistent bool
	// CheckedEntries é o número de lançamentos somados.
	CheckedEntries int64
	// WalletVersion é a versão da carteira no snapshot.
	WalletVersion int64
}

// ReconcileWallet compara o saldo gravado com o saldo do ledger.
type ReconcileWallet struct {
	uow      UnitOfWork
	recorder ReconciliationRecorder
	logger   *slog.Logger
}

// WithReconciliationRecorder define o destino da métrica de reconciliação
// (nil = não registra).
func WithReconciliationRecorder(r ReconciliationRecorder) Option {
	return func(o *options) { o.reconciliations = r }
}

// NewReconcileWallet cria o caso de uso.
func NewReconcileWallet(uow UnitOfWork, opts ...Option) (*ReconcileWallet, error) {
	if uow == nil {
		return nil, errors.New("app: reconcile wallet: nil UnitOfWork")
	}
	o := buildOptions(opts)
	return &ReconcileWallet{
		uow: uow, recorder: o.reconciliations,
		logger: o.logger.With(slog.String("component", "app.reconcile_wallet")),
	}, nil
}

const opReconcile = "reconcile wallet"

// reconcileTxOptions são as opções da transação de reconciliação.
var reconcileTxOptions = TxOptions{
	Operation: OperationReconcile,
	// Só leituras: o corpo é reexecutável.
	Retry:     true,
	Isolation: IsolationRepeatableRead,
	ReadOnly:  true,
}

// Execute reconcilia a carteira. Não altera nenhum dado.
func (uc *ReconcileWallet) Execute(ctx context.Context, cmd ReconcileCommand) (ReconciliationReport, error) {
	if err := ctx.Err(); err != nil {
		return ReconciliationReport{}, err
	}
	if cmd.WalletID == uuid.Nil {
		return ReconciliationReport{}, &Error{Kind: KindInvalidInput, Code: CodeInvalidWalletID, Op: opReconcile,
			Err: errors.New("walletId is required")}
	}

	start := time.Now()
	var (
		wallet  *domain.Wallet
		summary LedgerSummary
	)
	err := uc.uow.WithinTx(ctx, reconcileTxOptions, func(ctx context.Context, tx Tx) error {
		// O primeiro comando fixa o snapshot (REPEATABLE READ).
		w, err := tx.Wallets().Get(ctx, cmd.WalletID)
		if err != nil {
			return err
		}
		s, err := tx.Ledger().Summarize(ctx, cmd.WalletID)
		if err != nil {
			return err
		}
		wallet, summary = w, s
		return nil
	})
	if err != nil {
		return ReconciliationReport{}, classifyQuery(opReconcile, CodeWalletNotFound, err)
	}

	report, err := buildReport(wallet, summary)
	if err != nil {
		// Total fora do intervalo representável: dado corrompido, erro interno.
		uc.logger.ErrorContext(ctx, "reconciliação falhou ao calcular a diferença",
			slog.String("correlationId", cmd.CorrelationID),
			slog.String("walletId", cmd.WalletID.String()), slog.String("error", err.Error()))
		return ReconciliationReport{}, fmt.Errorf("app: %s: %w", opReconcile, err)
	}

	if uc.recorder != nil {
		uc.recorder.Reconciliation(report.StoredBalance.Currency().String(), !report.Consistent)
	}
	attrs := []any{
		slog.String("correlationId", cmd.CorrelationID),
		slog.String("walletId", report.WalletID.String()),
		slog.String("currency", report.StoredBalance.Currency().String()),
		slog.String("storedBalance", report.StoredBalance.Amount()),
		slog.String("calculatedBalance", report.CalculatedBalance.Amount()),
		slog.String("difference", report.Difference.Amount()),
		slog.Int64("checkedEntries", report.CheckedEntries),
		slog.Int64("walletVersion", report.WalletVersion),
		slog.Duration("elapsed", time.Since(start)),
	}
	if report.Consistent {
		uc.logger.InfoContext(ctx, "reconciliação consistente", attrs...)
	} else {
		uc.logger.WarnContext(ctx, "divergência de reconciliação", attrs...)
	}
	return report, nil
}

func buildReport(w *domain.Wallet, s LedgerSummary) (ReconciliationReport, error) {
	stored := w.Balance()
	calculated, err := domain.FromMinorUnits(s.NetMinorUnits, stored.Currency().String())
	if err != nil {
		return ReconciliationReport{}, fmt.Errorf("calculated balance: %w", err)
	}
	diff, err := stored.Sub(calculated)
	if err != nil {
		return ReconciliationReport{}, fmt.Errorf("difference: %w", err)
	}
	return ReconciliationReport{
		WalletID:          w.ID(),
		StoredBalance:     stored,
		CalculatedBalance: calculated,
		Difference:        diff,
		Consistent:        diff.IsZero(),
		CheckedEntries:    s.Entries,
		WalletVersion:     w.Version(),
	}, nil
}

// classifyQuery converte o erro de uma transação de leitura no erro tipado:
// registro ausente → KindNotFound com notFoundCode; cursor inválido →
// KindInvalidInput; indisponibilidade → KindUnavailable; o resto é interno.
func classifyQuery(op, notFoundCode string, err error) error {
	if _, ok := AsError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, ErrRecordNotFound):
		return &Error{Kind: KindNotFound, Code: notFoundCode, Op: op, Err: err}
	case errors.Is(err, ErrInvalidCursor):
		return &Error{Kind: KindInvalidInput, Code: CodeInvalidCursor, Op: op, Err: err}
	case errors.Is(err, ErrUnavailable):
		return &Error{Kind: KindUnavailable, Code: CodeTemporarilyUnavailable, Op: op, Err: err}
	default:
		return fmt.Errorf("app: %s: %w", op, err)
	}
}
