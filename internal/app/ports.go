package app

import (
	"context"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// Portas de saída consumidas pelos casos de uso (ARCHITECTURE.md, ADR-016).
//
// Decisões:
//
//   - A fronteira transacional continua explícita e pertence ao caso de uso
//     (ADR-013): ele chama UnitOfWork.WithinTx e usa SOMENTE os stores do Tx
//     recebido. Todos os stores de um Tx compartilham a mesma transação SQL,
//     então carteira, transação, ledger e outbox entram no mesmo commit. Não
//     há transação escondida no context.
//   - As portas falam apenas a língua do domínio: nenhum tipo do pgx ou do
//     adapter aparece aqui. O adapter postgres implementa estas interfaces
//     (postgres.UnitOfWork) e traduz seus erros para as sentinelas desta
//     camada (ErrWalletAlreadyExists, ErrUnavailable).
//   - Cada store expõe só os métodos que algum caso de uso já usa; as
//     próximas tarefas (T15/T16/T21) acrescentam o que precisarem.

// Operation* são os rótulos de operação das transações (métricas e logs do
// adapter). Os valores coincidem com metrics.Operation* (teste no adapter).
const (
	OperationOpenWallet = "open_wallet"
)

// TxOptions configura uma chamada de UnitOfWork.WithinTx.
type TxOptions struct {
	// Operation é o rótulo da operação (Operation*).
	Operation string
	// Retry habilita o retry limitado de falhas transitórias em que nada foi
	// confirmado (serialização, deadlock, conexão antes do COMMIT). Só pode
	// ser usado quando fn é reexecutável: sem efeitos fora do banco e
	// recalculando (ou regravando) tudo a partir do tx.
	Retry bool
}

// TxFunc é o corpo de uma unidade de trabalho. tx só vale durante a chamada.
type TxFunc func(ctx context.Context, tx Tx) error

// UnitOfWork executa fn numa transação: commit se fn devolver nil, rollback
// em erro ou panic. O erro de fn volta intacto (com %w). Falhas transitórias
// da infraestrutura (inclusive commit de resultado desconhecido) casam com
// ErrUnavailable.
type UnitOfWork interface {
	WithinTx(ctx context.Context, opts TxOptions, fn TxFunc) error
}

// Tx dá acesso aos stores ligados a UMA transação.
type Tx interface {
	Wallets() WalletStore
	Transactions() WagerTransactionStore
	Ledger() LedgerStore
	Outbox() OutboxStore
}

// WalletStore persiste o agregado Wallet.
type WalletStore interface {
	// Insert grava uma carteira nova (versão 1). (playerId, currency) já
	// existente devolve erro que casa com ErrWalletAlreadyExists. A linha
	// inserida fica, até o commit, invisível e exclusiva desta transação, o
	// que equivale à carteira travada para Ledger.Append.
	Insert(ctx context.Context, w *domain.Wallet) error
}

// WagerTransactionStore persiste WagerTransaction.
type WagerTransactionStore interface {
	// Insert grava uma transação nova (qualquer estado).
	Insert(ctx context.Context, t *domain.WagerTransaction) error
}

// LedgerStore grava o ledger append-only.
type LedgerStore interface {
	// Append grava um lançamento. Contrato: só pode ser chamado com a
	// carteira do lançamento travada nesta transação (inserida nela ou lida
	// com lock), porque a ordem de paginação (seq) só segue a ordem de commit
	// sob esse lock (ADR-014).
	Append(ctx context.Context, e domain.WalletLedgerEntry) error
}

// OutboxStore grava eventos de integração na transactional outbox.
type OutboxStore interface {
	// Enqueue grava os eventos na ordem recebida, no commit da transação.
	Enqueue(ctx context.Context, events ...domain.IntegrationEvent) error
}
