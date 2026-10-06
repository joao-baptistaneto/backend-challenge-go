package app

import (
	"context"
	"time"

	"github.com/google/uuid"

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
//     próximas tarefas (T16/T21) acrescentam o que precisarem. A T15
//     acrescentou GetForUpdate/Update da carteira, as consultas de
//     idempotência e de referência, Update da transação e a inbox (ADR-017).
//     A T16 acrescentou as leituras sem lock (Wallets.Get, Ledger.List,
//     Ledger.Summarize) e o isolamento/modo de acesso em TxOptions (ADR-018).

// Operation* são os rótulos de operação das transações (métricas e logs do
// adapter). Os valores coincidem com metrics.Operation* (teste no adapter).
const (
	OperationOpenWallet         = "open_wallet"
	OperationProcessTransaction = "process_transaction"
	OperationReconcile          = "reconcile"
	OperationQuery              = "query"
	// OperationPendingReference: claim e retomada do worker de pendências (T21).
	OperationPendingReference = "pending_reference"
)

// IsolationLevel é o nível de isolamento de uma unidade de trabalho.
type IsolationLevel string

const (
	// IsolationReadCommitted é o padrão (valor zero). A consistência das
	// escritas vem do lock por carteira, não do isolamento (ADR-013).
	IsolationReadCommitted IsolationLevel = ""
	// IsolationRepeatableRead fixa um snapshot no primeiro comando: todas as
	// leituras da transação veem o mesmo estado confirmado (reconciliação).
	IsolationRepeatableRead IsolationLevel = "REPEATABLE_READ"
	// IsolationSerializable é o isolamento serializável.
	IsolationSerializable IsolationLevel = "SERIALIZABLE"
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
	// RetryIf, com Retry, amplia os erros que provocam nova tentativa (além
	// das falhas transitórias). Recebe o erro devolvido por fn, já com as
	// sentinelas desta camada (ex.: ErrDuplicateWagerTransaction). Nunca é
	// consultado para commit de resultado desconhecido.
	RetryIf func(error) bool
	// Isolation é o nível de isolamento (padrão READ COMMITTED). Valor
	// desconhecido é recusado pelo adapter antes do BEGIN.
	Isolation IsolationLevel
	// ReadOnly abre a transação em modo somente leitura: o banco recusa
	// qualquer escrita (SQLSTATE 25006), inclusive por engano do caso de uso.
	ReadOnly bool
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
	Inbox() InboxStore
}

// WalletStore persiste o agregado Wallet.
type WalletStore interface {
	// Insert grava uma carteira nova (versão 1). (playerId, currency) já
	// existente devolve erro que casa com ErrWalletAlreadyExists. A linha
	// inserida fica, até o commit, invisível e exclusiva desta transação, o
	// que equivale à carteira travada para Ledger.Append.
	Insert(ctx context.Context, w *domain.Wallet) error
	// Get lê a carteira SEM lock (consultas e reconciliação). Inexistente:
	// erro que casa com ErrRecordNotFound.
	Get(ctx context.Context, id uuid.UUID) (*domain.Wallet, error)
	// GetForUpdate lê e TRAVA a carteira até o fim da transação (SELECT ...
	// FOR UPDATE). Escritores da mesma carteira esperam o commit; carteiras
	// diferentes seguem em paralelo. Inexistente: erro que casa com
	// ErrRecordNotFound.
	GetForUpdate(ctx context.Context, id uuid.UUID) (*domain.Wallet, error)
	// Update grava saldo, versão e updatedAt de uma carteira que sofreu no
	// máximo UM movimento desde a leitura (sem movimento, não faz nada).
	// Contrato: a carteira deve ter sido lida por GetForUpdate NESTA
	// transação. A escrita ainda é condicional à versão lida (defesa contra
	// lost update), e uma divergência devolve erro sem gravar nada.
	Update(ctx context.Context, w *domain.Wallet) error
}

// WagerTransactionStore persiste WagerTransaction.
type WagerTransactionStore interface {
	// Insert grava uma transação nova (qualquer estado). Violação das
	// unicidades de idempotência ((provider, externalId), (provider, key)) ou
	// da reversão única por referência devolve erro que casa com
	// ErrDuplicateWagerTransaction: a operação concorrente venceu, e uma nova
	// tentativa encontra a linha dela.
	Insert(ctx context.Context, t *domain.WagerTransaction) error
	// Update grava a transição feita em memória de uma transação ainda não
	// terminal. Contrato: a linha deve ter sido lida sob lock nesta transação
	// (o lock da carteira, que serializa as operações dela, ou o lock da
	// própria linha no worker), porque Update não tem versão otimista.
	// Transação já terminal devolve erro. Update também solta o lease do
	// worker (ClaimDue), no mesmo comando.
	Update(ctx context.Context, t *domain.WagerTransaction) error
	// ClaimDue reivindica até p.Limit transações vencidas (PENDING_REFERENCE
	// com nextAttemptAt <= p.Now e PENDING órfãos) e sem lease vigente,
	// gravando na linha o token e o fim do lease (p.Now + p.Lease). Instâncias
	// concorrentes recebem conjuntos disjuntos (SKIP LOCKED). O lease é
	// persistido: se o worker morrer, a linha volta a ser elegível quando ele
	// expirar. Usado pelo worker (T21, ADR-023) numa transação própria e curta.
	ClaimDue(ctx context.Context, p ClaimDueParams) ([]ClaimedWager, error)
	// GetClaimedForUpdate lê e trava a transação SE ela ainda estiver
	// reivindicada por token (fencing). Reassumida por outro token (lease
	// expirado) ou inexistente: erro que casa com ErrRecordNotFound.
	GetClaimedForUpdate(ctx context.Context, id, token uuid.UUID) (*domain.WagerTransaction, error)
	// GetByID lê pelo id interno. Inexistente: ErrRecordNotFound.
	GetByID(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error)
	// GetByProviderIdempotencyKey lê por (providerId, idempotencyKey).
	// Inexistente: ErrRecordNotFound.
	GetByProviderIdempotencyKey(ctx context.Context, providerID, idempotencyKey string) (*domain.WagerTransaction, error)
	// GetByProviderExternalID lê por (providerId, externalTransactionId), para
	// idempotência e resolução de referências. Inexistente: ErrRecordNotFound.
	GetByProviderExternalID(ctx context.Context, providerID, externalTransactionID string) (*domain.WagerTransaction, error)
	// ListByProviderReference lista as operações do provedor que citam
	// referenceExternalTransactionId, em qualquer estado e tipo.
	ListByProviderReference(ctx context.Context, providerID, referenceExternalTransactionID string) ([]*domain.WagerTransaction, error)
}

// ClaimDueParams são os parâmetros de WagerTransactionStore.ClaimDue.
type ClaimDueParams struct {
	// Now é o instante de referência (relógio da aplicação).
	Now time.Time
	// Token identifica o lote reivindicado (fencing).
	Token uuid.UUID
	// Lease é a duração da reivindicação.
	Lease time.Duration
	// Limit é o tamanho máximo do lote.
	Limit int
	// OrphanGrace é a idade mínima de um PENDING (sem nextAttemptAt) para ser
	// retomado como órfão.
	OrphanGrace time.Duration
}

// ClaimedWager é uma transação reivindicada por ClaimDue.
type ClaimedWager struct {
	ID     uuid.UUID
	Status domain.WagerStatus
}

// LedgerStore grava o ledger append-only.
type LedgerStore interface {
	// Append grava um lançamento. Contrato: só pode ser chamado com a
	// carteira do lançamento travada nesta transação (inserida nela ou lida
	// com lock), porque a ordem de paginação (seq) só segue a ordem de commit
	// sob esse lock (ADR-014).
	Append(ctx context.Context, e domain.WalletLedgerEntry) error
	// List devolve até limit lançamentos da carteira depois do cursor opaco
	// (vazio = início), em ordem de lançamento (seq). limit em
	// [1, MaxLedgerPageLimit]. Cursor malformado ou de outra carteira: erro
	// que casa com ErrInvalidCursor. Não verifica a existência da carteira.
	List(ctx context.Context, walletID uuid.UUID, cursor string, limit int) (LedgerPage, error)
	// Summarize reconstrói o saldo a partir do ledger: soma dos créditos
	// menos a dos débitos, em centavos (calculada em NUMERIC, sem overflow;
	// um total fora de int64 é erro), e o número de lançamentos. Carteira sem
	// lançamentos: 0 e 0.
	Summarize(ctx context.Context, walletID uuid.UUID) (LedgerSummary, error)
}

// LedgerPage é uma página do extrato.
type LedgerPage struct {
	Entries []domain.WalletLedgerEntry
	// NextCursor é vazio quando não havia mais lançamentos na leitura.
	NextCursor string
}

// LedgerSummary é o saldo reconstruído do ledger de uma carteira.
type LedgerSummary struct {
	// NetMinorUnits = Σ créditos − Σ débitos, em centavos, na moeda da
	// carteira (as FKs compostas do ledger garantem a moeda).
	NetMinorUnits int64
	// Entries é o número de lançamentos somados.
	Entries int64
}

// OutboxStore grava eventos de integração na transactional outbox.
type OutboxStore interface {
	// Enqueue grava os eventos na ordem recebida, no commit da transação.
	Enqueue(ctx context.Context, events ...domain.IntegrationEvent) error
}

// InboxStatus é o estado de uma mensagem na inbox após InboxStore.Register.
type InboxStatus int

const (
	// InboxNew: mensagem registrada agora; processar.
	InboxNew InboxStatus = iota + 1
	// InboxInProgress: registrada antes, com o mesmo hash, sem conclusão
	// confirmada.
	InboxInProgress
	// InboxCompleted: já processada (mesmo hash); o resultado está em
	// TransactionID.
	InboxCompleted
)

// InboxRecord é o resultado de InboxStore.Register.
type InboxRecord struct {
	Status InboxStatus
	// TransactionID é a transação resultante (uuid.Nil se não houver).
	TransactionID uuid.UUID
}

// InboxStore deduplica mensagens de entrada por (consumerName, messageId).
// Registro, conclusão e efeitos de domínio precisam entrar no MESMO commit.
type InboxStore interface {
	// Register registra a recepção. A mesma (consumer, messageId) com outro
	// hash devolve erro que casa com ErrInboxHashMismatch. Uma entrega
	// concorrente da mesma mensagem espera o commit de quem a registrou.
	Register(ctx context.Context, consumerName, messageID, requestHash string, now time.Time) (InboxRecord, error)
	// Complete conclui a mensagem e associa a transação resultante.
	Complete(ctx context.Context, consumerName, messageID, requestHash string, transactionID uuid.UUID, now time.Time) error
}
