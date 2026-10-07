package domain

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Decisão arquitetural (ADR-008 em ARCHITECTURE.md): WagerTransaction registra
// uma operação financeira (externa ou a abertura interna) e sua máquina de
// estados. Este arquivo tem apenas lógica pura de domínio: as regras que decidem
// débito, crédito e reversão ficam em T06, a persistência em T10/T12 e a
// orquestração em T15/T21.
//
//   - Origem: OPENING é a única operação INTERNAL (abertura de carteira, sem
//     provedor, ID externo, chave, hash, rodada, jogo nem referência). Todas as
//     demais são EXTERNAL. Origem e tipo andam juntos e o schema (T10) usa a
//     origem nos CHECKs. OPENING enviado por HTTP/SQS é rejeitado já no
//     construtor externo (ErrOpeningNotExternal, código INVALID_KIND).
//   - Construção: NewExternalWagerTransaction nasce em PENDING.
//     NewOpeningWagerTransaction nasce em PROCESSED, porque a abertura é
//     confirmada no mesmo commit da carteira e não existe estado intermediário
//     observável (README §9). RehydrateWagerTransaction reconstrói a partir do
//     banco sem transicionar nada, mas revalida TODAS as invariantes (mesma função
//     da construção). Dado persistido inconsistente falha com
//     ErrCorruptedWagerTransaction.
//   - Máquina de estados (wagerStatusTransitions):
//     PENDING -> PROCESSED | PENDING_REFERENCE | REJECTED | FAILED
//     PENDING_REFERENCE -> PENDING_REFERENCE (reagendamento) | PROCESSED |
//     REJECTED | FAILED
//     PROCESSED, REJECTED e FAILED são terminais e imutáveis: qualquer método de
//     transição devolve ErrWagerTransactionTerminal. O replay lê o resultado
//     persistido e nunca chama transições.
//   - Atomicidade de falha: cada transição aplica a mudança numa cópia, revalida
//     o conjunto inteiro de invariantes e só então substitui o receptor. Um erro
//     deixa a transação intacta.
//   - Transitório x permanente: falhas transitórias (timeout, serialização,
//     deadlock, conexão) NÃO são estados. A transação SQL sofre rollback e a
//     operação é refeita (retry da aplicação, nova entrega SQS ou worker), então
//     o registro continua PENDING/PENDING_REFERENCE ou nem chega a existir. FAILED
//     registra só a falha permanente de infraestrutura que esgotou as tentativas,
//     para auditoria, com código da categoria INFRASTRUCTURE. REJECTED registra
//     uma recusa por regra de negócio, com código INPUT (entrada corrigível) ou
//     BUSINESS (resultado definitivo).
//   - Política de zero (README §7): LOSS exige exatamente 0.00. BET, WIN, REFUND,
//     ROLLBACK e OPENING exigem valor > 0. Negativos são sempre rejeitados.
//   - Referências: REFUND e ROLLBACK exigem referenceExternalTransactionId, WIN
//     aceita opcionalmente e BET/LOSS não aceitam. A referência interna resolvida
//     (ReferenceTransactionID) é obrigatória em PROCESSED quando há referência
//     externa.
//   - Tempo: injetado e normalizado para UTC. updatedAt é monotônico
//     (max(updatedAt, now)), como na Wallet. completedAt é o updatedAt da
//     transição terminal.

// Erros sentinela de WagerTransaction (errors.Is). Os erros de valor reutilizam
// os sentinelas de Money (ErrInvalidAmount, ErrNegativeAmount,
// ErrUninitializedMoney, ErrCurrencyMismatch).
var (
	// ErrInvalidWagerTransaction indica parâmetros de construção ou estado que
	// violam as invariantes da transação. Todos os erros de validação abaixo
	// também satisfazem errors.Is(err, ErrInvalidWagerTransaction).
	ErrInvalidWagerTransaction = errors.New("wager transaction: invalid transaction")
	// ErrInvalidWagerKind indica tipo desconhecido ou inaplicável à origem
	// (código INVALID_KIND).
	ErrInvalidWagerKind = errors.New("wager transaction: invalid kind")
	// ErrOpeningNotExternal indica OPENING recebido por HTTP/SQS. Também
	// satisfaz errors.Is(err, ErrInvalidWagerKind).
	ErrOpeningNotExternal = fmt.Errorf("%w: OPENING is reserved for internal wallet opening", ErrInvalidWagerKind)
	// ErrReferenceRequired indica REFUND/ROLLBACK sem
	// referenceExternalTransactionId (código REFERENCE_REQUIRED).
	ErrReferenceRequired = errors.New("wager transaction: reference external transaction id required")
	// ErrReferenceNotAllowed indica referência em BET/LOSS ou referência a si
	// mesma (código REFERENCE_NOT_ALLOWED).
	ErrReferenceNotAllowed = errors.New("wager transaction: reference external transaction id not allowed")
	// ErrInvalidFailureCode indica código de falha desconhecido, ausente ou
	// incompatível com o estado/tipo da transação.
	ErrInvalidFailureCode = errors.New("wager transaction: invalid failure code")
	// ErrWagerTransitionNotAllowed indica transição de estado fora da máquina de
	// estados.
	ErrWagerTransitionNotAllowed = errors.New("wager transaction: transition not allowed")
	// ErrWagerTransactionTerminal indica tentativa de transicionar uma transação
	// terminal (PROCESSED, REJECTED, FAILED). Também satisfaz
	// errors.Is(err, ErrWagerTransitionNotAllowed).
	ErrWagerTransactionTerminal = fmt.Errorf("%w: transaction is in a terminal state", ErrWagerTransitionNotAllowed)
	// ErrUninitializedWagerTransaction indica uso do zero-value (ou nil).
	ErrUninitializedWagerTransaction = errors.New("wager transaction: uninitialized transaction")
	// ErrCorruptedWagerTransaction marca erros de RehydrateWagerTransaction: o
	// dado persistido viola invariantes. A aplicação deve tratá-lo como erro
	// interno (500/alerta), nunca como entrada inválida do provedor.
	ErrCorruptedWagerTransaction = errors.New("wager transaction: corrupted persisted state")
)

// MaxWagerIdentifierLength é o tamanho máximo, em bytes, dos identificadores
// externos (providerId, externalTransactionId, idempotencyKey, roundId, gameId,
// referenceExternalTransactionId). O schema (T10) usa o mesmo limite.
const MaxWagerIdentifierLength = 255

// RequestHashLength é o tamanho do hash de idempotência: SHA-256 em hex
// minúsculo (T08).
const RequestHashLength = 64

// --- Tipo ---------------------------------------------------------------------

// WagerKind é o tipo da operação.
type WagerKind string

const (
	// WagerKindOpening é a abertura interna de carteira (crédito inicial > 0).
	WagerKindOpening WagerKind = "OPENING"
	// WagerKindBet debita a aposta.
	WagerKindBet WagerKind = "BET"
	// WagerKindWin credita um ganho; pode referenciar uma BET da mesma rodada.
	WagerKindWin WagerKind = "WIN"
	// WagerKindLoss registra perda sem movimentação (valor 0.00).
	WagerKindLoss WagerKind = "LOSS"
	// WagerKindRefund devolve integralmente uma BET processada.
	WagerKindRefund WagerKind = "REFUND"
	// WagerKindRollback desfaz integralmente uma BET, WIN ou REFUND processada.
	WagerKindRollback WagerKind = "ROLLBACK"
)

// IsValid indica um dos seis tipos conhecidos (sensível a caixa).
func (k WagerKind) IsValid() bool {
	switch k {
	case WagerKindOpening, WagerKindBet, WagerKindWin, WagerKindLoss, WagerKindRefund, WagerKindRollback:
		return true
	}
	return false
}

// IsExternal indica um tipo aceito por HTTP/SQS (válido e diferente de OPENING).
func (k WagerKind) IsExternal() bool { return k.IsValid() && k != WagerKindOpening }

// IsReversal indica REFUND ou ROLLBACK.
func (k WagerKind) IsReversal() bool { return k == WagerKindRefund || k == WagerKindRollback }

// RequiresReference indica que referenceExternalTransactionId é obrigatório.
func (k WagerKind) RequiresReference() bool { return k.IsReversal() }

// AcceptsReference indica que referenceExternalTransactionId é permitido
// (obrigatório em REFUND/ROLLBACK, opcional em WIN).
func (k WagerKind) AcceptsReference() bool { return k == WagerKindWin || k.IsReversal() }

// Origin devolve a origem associada ao tipo (OPENING é INTERNAL; os demais são
// EXTERNAL). Para tipos inválidos devolve "".
func (k WagerKind) Origin() WagerOrigin {
	switch {
	case k == WagerKindOpening:
		return WagerOriginInternal
	case k.IsExternal():
		return WagerOriginExternal
	}
	return ""
}

// String devolve o nome do tipo.
func (k WagerKind) String() string { return string(k) }

// ParseExternalWagerKind converte o campo "kind" de HTTP/SQS. Rejeita OPENING
// (ErrOpeningNotExternal) e valores desconhecidos (ErrInvalidWagerKind), ambos
// mapeados para INVALID_KIND. Não normaliza caixa: "bet" é inválido.
func ParseExternalWagerKind(s string) (WagerKind, error) {
	k := WagerKind(s)
	if k == WagerKindOpening {
		return "", fmt.Errorf("%w: %w", ErrInvalidWagerTransaction, ErrOpeningNotExternal)
	}
	if !k.IsExternal() {
		return "", fmt.Errorf("%w: %w: %q", ErrInvalidWagerTransaction, ErrInvalidWagerKind, s)
	}
	return k, nil
}

// validateAmount aplica a política de zero por tipo: LOSS == 0.00; demais > 0.
func (k WagerKind) validateAmount(m Money) error {
	if !m.IsValid() {
		return fmt.Errorf("%w: %w: %w", ErrInvalidWagerTransaction, ErrInvalidAmount, ErrUninitializedMoney)
	}
	if m.IsNegative() {
		return fmt.Errorf("%w: %w: %w: %s", ErrInvalidWagerTransaction, ErrInvalidAmount, ErrNegativeAmount, m)
	}
	if k == WagerKindLoss {
		if !m.IsZero() {
			return fmt.Errorf("%w: %w: LOSS amount must be 0.00, got %s", ErrInvalidWagerTransaction, ErrInvalidAmount, m)
		}
		return nil
	}
	if !m.IsPositive() {
		return fmt.Errorf("%w: %w: %s amount must be greater than zero", ErrInvalidWagerTransaction, ErrInvalidAmount, k)
	}
	return nil
}

// --- Origem -------------------------------------------------------------------

// WagerOrigin distingue a abertura interna das operações de provedores.
type WagerOrigin string

const (
	// WagerOriginInternal é a origem de OPENING.
	WagerOriginInternal WagerOrigin = "INTERNAL"
	// WagerOriginExternal é a origem de BET, WIN, LOSS, REFUND e ROLLBACK.
	WagerOriginExternal WagerOrigin = "EXTERNAL"
)

// IsValid indica INTERNAL ou EXTERNAL.
func (o WagerOrigin) IsValid() bool { return o == WagerOriginInternal || o == WagerOriginExternal }

// String devolve o nome da origem.
func (o WagerOrigin) String() string { return string(o) }

// --- Estado -------------------------------------------------------------------

// WagerStatus é o estado da transação.
type WagerStatus string

const (
	// WagerStatusPending indica registro aceito com processamento não concluído.
	WagerStatusPending WagerStatus = "PENDING"
	// WagerStatusPendingReference indica espera por referência indisponível.
	WagerStatusPendingReference WagerStatus = "PENDING_REFERENCE"
	// WagerStatusProcessed indica sucesso (terminal).
	WagerStatusProcessed WagerStatus = "PROCESSED"
	// WagerStatusRejected indica recusa por regra de negócio (terminal).
	WagerStatusRejected WagerStatus = "REJECTED"
	// WagerStatusFailed indica falha permanente de infraestrutura (terminal).
	WagerStatusFailed WagerStatus = "FAILED"
)

// IsValid indica um dos cinco estados conhecidos.
func (s WagerStatus) IsValid() bool {
	switch s {
	case WagerStatusPending, WagerStatusPendingReference, WagerStatusProcessed, WagerStatusRejected, WagerStatusFailed:
		return true
	}
	return false
}

// IsTerminal indica PROCESSED, REJECTED ou FAILED.
func (s WagerStatus) IsTerminal() bool {
	return s == WagerStatusProcessed || s == WagerStatusRejected || s == WagerStatusFailed
}

// String devolve o nome do estado.
func (s WagerStatus) String() string { return string(s) }

// wagerStatusTransitions é a máquina de estados. PENDING_REFERENCE ->
// PENDING_REFERENCE é o reagendamento do worker de referências (T21).
// Estados terminais não têm saída.
var wagerStatusTransitions = map[WagerStatus]map[WagerStatus]bool{
	WagerStatusPending: {
		WagerStatusPendingReference: true,
		WagerStatusProcessed:        true,
		WagerStatusRejected:         true,
		WagerStatusFailed:           true,
	},
	WagerStatusPendingReference: {
		WagerStatusPendingReference: true,
		WagerStatusProcessed:        true,
		WagerStatusRejected:         true,
		WagerStatusFailed:           true,
	},
}

// CanTransitionWagerStatus indica se from -> to pertence à máquina de estados.
func CanTransitionWagerStatus(from, to WagerStatus) bool {
	return wagerStatusTransitions[from][to]
}

// --- Códigos de falha -----------------------------------------------------------

// FailureCode é o código estável de rejeição/falha exposto a provedores
// (HTTP 422, eventos WagerTransactionRejected) e persistido em failure_code.
// Os valores são contrato público: não renomear.
type FailureCode string

const (
	// FailureCodeInvalidRequest: campo obrigatório ausente ou malformado (INPUT).
	FailureCodeInvalidRequest FailureCode = "INVALID_REQUEST"
	// FailureCodeInvalidKind: tipo desconhecido ou OPENING externo (INPUT).
	FailureCodeInvalidKind FailureCode = "INVALID_KIND"
	// FailureCodeInvalidAmount: valor malformado, negativo ou fora da política
	// de zero (INPUT).
	FailureCodeInvalidAmount FailureCode = "INVALID_AMOUNT"
	// FailureCodeInvalidCurrency: moeda inválida ou não suportada (INPUT).
	FailureCodeInvalidCurrency FailureCode = "INVALID_CURRENCY"
	// FailureCodeReferenceRequired: REFUND/ROLLBACK sem referência (INPUT).
	FailureCodeReferenceRequired FailureCode = "REFERENCE_REQUIRED"
	// FailureCodeReferenceNotAllowed: referência em BET/LOSS ou a si mesma (INPUT).
	FailureCodeReferenceNotAllowed FailureCode = "REFERENCE_NOT_ALLOWED"
	// FailureCodeWalletNotFound: carteira inexistente (INPUT).
	FailureCodeWalletNotFound FailureCode = "WALLET_NOT_FOUND"
	// FailureCodeWalletPlayerMismatch: a carteira não pertence ao jogador (INPUT).
	FailureCodeWalletPlayerMismatch FailureCode = "WALLET_PLAYER_MISMATCH"
	// FailureCodeCurrencyMismatch: moeda da operação difere da carteira ou da
	// referência (INPUT).
	FailureCodeCurrencyMismatch FailureCode = "CURRENCY_MISMATCH"
	// FailureCodeReferenceMismatch: referência de outro provedor, jogador,
	// carteira ou rodada (INPUT).
	FailureCodeReferenceMismatch FailureCode = "REFERENCE_MISMATCH"
	// FailureCodeAmountMismatch: valor da reversão difere do referenciado (INPUT).
	FailureCodeAmountMismatch FailureCode = "AMOUNT_MISMATCH"

	// FailureCodeInsufficientFunds: BET sem saldo suficiente (BUSINESS).
	FailureCodeInsufficientFunds FailureCode = "INSUFFICIENT_FUNDS"
	// FailureCodeReversalInsufficientFunds: ROLLBACK que debitaria mais que o
	// saldo disponível (BUSINESS). Distinto de INSUFFICIENT_FUNDS (README §7).
	FailureCodeReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
	// FailureCodeReferenceNotFound: referência não chegou dentro do limite de
	// tentativas/TTL (BUSINESS).
	FailureCodeReferenceNotFound FailureCode = "REFERENCE_NOT_FOUND"
	// FailureCodeReferenceNotReversible: o tipo referenciado não admite essa
	// reversão (ex.: REFUND de WIN, ROLLBACK de LOSS) ou WIN referenciando algo
	// que não é BET (BUSINESS).
	FailureCodeReferenceNotReversible FailureCode = "REFERENCE_NOT_REVERSIBLE"
	// FailureCodeReferenceNotProcessed: a referência existe mas terminou sem
	// sucesso (REJECTED/FAILED) (BUSINESS). Referência ainda pendente não
	// rejeita: a operação aguarda em PENDING_REFERENCE.
	FailureCodeReferenceNotProcessed FailureCode = "REFERENCE_NOT_PROCESSED"
	// FailureCodeAlreadyReversed: a referência já foi revertida (mesmo tipo ou
	// combinação REFUND/ROLLBACK que devolveria o débito duas vezes) (BUSINESS).
	FailureCodeAlreadyReversed FailureCode = "ALREADY_REVERSED"

	// FailureCodeProcessingFailed: falha permanente de infraestrutura (FAILED).
	FailureCodeProcessingFailed FailureCode = "PROCESSING_FAILED"
	// FailureCodeRetriesExhausted: tentativas de falhas transitórias esgotadas
	// (FAILED).
	FailureCodeRetriesExhausted FailureCode = "RETRIES_EXHAUSTED"
)

// FailureCategory classifica um FailureCode (README §7: distinguir entradas
// corrigíveis de resultados definitivos).
type FailureCategory string

const (
	// FailureCategoryInput: entrada corrigível. A operação com esse
	// externalTransactionId fica REJECTED; o provedor corrige e envia uma nova.
	FailureCategoryInput FailureCategory = "INPUT"
	// FailureCategoryBusiness: resultado definitivo de regra de negócio.
	FailureCategoryBusiness FailureCategory = "BUSINESS"
	// FailureCategoryInfrastructure: falha permanente de infraestrutura (FAILED).
	FailureCategoryInfrastructure FailureCategory = "INFRASTRUCTURE"
)

var failureCodeCategories = map[FailureCode]FailureCategory{
	FailureCodeInvalidRequest:            FailureCategoryInput,
	FailureCodeInvalidKind:               FailureCategoryInput,
	FailureCodeInvalidAmount:             FailureCategoryInput,
	FailureCodeInvalidCurrency:           FailureCategoryInput,
	FailureCodeReferenceRequired:         FailureCategoryInput,
	FailureCodeReferenceNotAllowed:       FailureCategoryInput,
	FailureCodeWalletNotFound:            FailureCategoryInput,
	FailureCodeWalletPlayerMismatch:      FailureCategoryInput,
	FailureCodeCurrencyMismatch:          FailureCategoryInput,
	FailureCodeReferenceMismatch:         FailureCategoryInput,
	FailureCodeAmountMismatch:            FailureCategoryInput,
	FailureCodeInsufficientFunds:         FailureCategoryBusiness,
	FailureCodeReversalInsufficientFunds: FailureCategoryBusiness,
	FailureCodeReferenceNotFound:         FailureCategoryBusiness,
	FailureCodeReferenceNotReversible:    FailureCategoryBusiness,
	FailureCodeReferenceNotProcessed:     FailureCategoryBusiness,
	FailureCodeAlreadyReversed:           FailureCategoryBusiness,
	FailureCodeProcessingFailed:          FailureCategoryInfrastructure,
	FailureCodeRetriesExhausted:          FailureCategoryInfrastructure,
}

// AllFailureCodes devolve todos os códigos conhecidos (documentação, schema e
// testes de contrato).
func AllFailureCodes() []FailureCode {
	return []FailureCode{
		FailureCodeInvalidRequest, FailureCodeInvalidKind, FailureCodeInvalidAmount,
		FailureCodeInvalidCurrency, FailureCodeReferenceRequired, FailureCodeReferenceNotAllowed,
		FailureCodeWalletNotFound, FailureCodeWalletPlayerMismatch, FailureCodeCurrencyMismatch,
		FailureCodeReferenceMismatch, FailureCodeAmountMismatch, FailureCodeInsufficientFunds,
		FailureCodeReversalInsufficientFunds, FailureCodeReferenceNotFound,
		FailureCodeReferenceNotReversible, FailureCodeReferenceNotProcessed,
		FailureCodeAlreadyReversed, FailureCodeProcessingFailed, FailureCodeRetriesExhausted,
	}
}

// IsValid indica um código conhecido.
func (c FailureCode) IsValid() bool { _, ok := failureCodeCategories[c]; return ok }

// Category devolve a categoria do código ("" se desconhecido).
func (c FailureCode) Category() FailureCategory { return failureCodeCategories[c] }

// IsCorrectable indica entrada corrigível pelo provedor (categoria INPUT).
func (c FailureCode) IsCorrectable() bool { return c.Category() == FailureCategoryInput }

// String devolve o código.
func (c FailureCode) String() string { return string(c) }

// requiresReference indica códigos que só fazem sentido quando a operação tem
// referenceExternalTransactionId.
func (c FailureCode) requiresReference() bool {
	switch c {
	case FailureCodeReferenceNotFound, FailureCodeReferenceMismatch, FailureCodeReferenceNotReversible,
		FailureCodeReferenceNotProcessed, FailureCodeAlreadyReversed, FailureCodeAmountMismatch:
		return true
	}
	return false
}

// allowedForKind restringe códigos de saldo ao tipo que pode debitar:
// INSUFFICIENT_FUNDS só em BET; REVERSAL_INSUFFICIENT_FUNDS só em ROLLBACK (o
// único tipo de reversão que pode debitar: ROLLBACK de WIN/REFUND).
func (c FailureCode) allowedForKind(k WagerKind) bool {
	switch c {
	case FailureCodeInsufficientFunds:
		return k == WagerKindBet
	case FailureCodeReversalInsufficientFunds:
		return k == WagerKindRollback
	case FailureCodeAlreadyReversed, FailureCodeAmountMismatch:
		return k.IsReversal()
	}
	return true
}

// ValidationFailureCode mapeia erros de construção (NewExternalWagerTransaction,
// ParseExternalWagerKind, parsing de Money) para o FailureCode do corpo de erro
// HTTP/SQS. Devolve false para erros que não são de entrada (inclusive
// ErrCorruptedWagerTransaction), que a aplicação trata como erro interno.
func ValidationFailureCode(err error) (FailureCode, bool) {
	switch {
	case err == nil, errors.Is(err, ErrCorruptedWagerTransaction):
		return "", false
	case errors.Is(err, ErrInvalidWagerKind):
		return FailureCodeInvalidKind, true
	case errors.Is(err, ErrReferenceRequired):
		return FailureCodeReferenceRequired, true
	case errors.Is(err, ErrReferenceNotAllowed):
		return FailureCodeReferenceNotAllowed, true
	case errors.Is(err, ErrInvalidCurrency):
		return FailureCodeInvalidCurrency, true
	case errors.Is(err, ErrCurrencyMismatch):
		return FailureCodeCurrencyMismatch, true
	case errors.Is(err, ErrInvalidAmount), errors.Is(err, ErrNegativeAmount),
		errors.Is(err, ErrAmountScale), errors.Is(err, ErrMoneyOverflow),
		errors.Is(err, ErrUninitializedMoney):
		return FailureCodeInvalidAmount, true
	case errors.Is(err, ErrInvalidWagerTransaction):
		return FailureCodeInvalidRequest, true
	}
	return "", false
}

// --- Agregado -------------------------------------------------------------------

// WagerTransaction é a operação financeira e sua máquina de estados. Campos
// privados: o estado só muda pelos métodos de transição.
type WagerTransaction struct {
	id       uuid.UUID
	origin   WagerOrigin
	kind     WagerKind
	status   WagerStatus
	walletID uuid.UUID
	playerID uuid.UUID
	money    Money

	// Metadados externos (vazios em OPENING).
	providerID                     string
	externalTransactionID          string
	idempotencyKey                 string
	requestHash                    string
	roundID                        string
	gameID                         string
	referenceExternalTransactionID string

	// Resultado.
	referenceTransactionID uuid.UUID // referência interna resolvida
	failureCode            FailureCode
	balanceAfter           Money // saldo devolvido ao provedor (replay)

	// Retomada durável (T21).
	attempts      int32
	nextAttemptAt time.Time

	createdAt   time.Time
	updatedAt   time.Time
	completedAt time.Time
}

// NewExternalWagerTransactionParams agrupa os dados de uma operação de provedor
// (HTTP ou SQS), já autenticada e com Money parseado.
type NewExternalWagerTransactionParams struct {
	// ID interno (UUIDv7 gerado pelo caso de uso).
	ID uuid.UUID
	// ProviderID vem da identidade autenticada (README §2).
	ProviderID string
	// ExternalTransactionID é único por provedor (UNIQUE no schema).
	ExternalTransactionID string
	// IdempotencyKey é a chave recebida, usada como está (sem substituição).
	IdempotencyKey string
	// RequestHash é o SHA-256 hex minúsculo do JSON canônico (T08).
	RequestHash string
	WalletID    uuid.UUID
	PlayerID    uuid.UUID
	RoundID     string
	GameID      string
	// Kind deve ser externo; OPENING é rejeitado.
	Kind WagerKind
	// Money segue a política de zero do tipo.
	Money Money
	// ReferenceExternalTransactionID: obrigatório em REFUND/ROLLBACK, opcional
	// em WIN, proibido em BET/LOSS.
	ReferenceExternalTransactionID string
	Now                            time.Time
}

// NewExternalWagerTransaction cria uma operação externa em PENDING. Erros
// satisfazem ErrInvalidWagerTransaction e o sentinela específico (ver
// ValidationFailureCode).
func NewExternalWagerTransaction(p NewExternalWagerTransactionParams) (*WagerTransaction, error) {
	if p.Kind == WagerKindOpening {
		return nil, fmt.Errorf("%w: %w", ErrInvalidWagerTransaction, ErrOpeningNotExternal)
	}
	if !p.Kind.IsExternal() {
		return nil, fmt.Errorf("%w: %w: %q", ErrInvalidWagerTransaction, ErrInvalidWagerKind, p.Kind)
	}
	now := p.Now.UTC()
	t := &WagerTransaction{
		id:                             p.ID,
		origin:                         WagerOriginExternal,
		kind:                           p.Kind,
		status:                         WagerStatusPending,
		walletID:                       p.WalletID,
		playerID:                       p.PlayerID,
		money:                          p.Money,
		providerID:                     p.ProviderID,
		externalTransactionID:          p.ExternalTransactionID,
		idempotencyKey:                 p.IdempotencyKey,
		requestHash:                    p.RequestHash,
		roundID:                        p.RoundID,
		gameID:                         p.GameID,
		referenceExternalTransactionID: p.ReferenceExternalTransactionID,
		createdAt:                      now,
		updatedAt:                      now,
	}
	if err := t.validate(); err != nil {
		return nil, err
	}
	return t, nil
}

// NewOpeningWagerTransactionParams agrupa os dados da abertura interna.
type NewOpeningWagerTransactionParams struct {
	// ID interno estável; é o mesmo OpeningTransactionID passado a NewWallet.
	ID       uuid.UUID
	WalletID uuid.UUID
	PlayerID uuid.UUID
	// Amount é o saldo inicial (> 0). Saldo zero não cria OPENING (T14).
	Amount Money
	Now    time.Time
}

// NewOpeningWagerTransaction cria a transação OPENING já em PROCESSED, com
// balanceAfter = Amount (a carteira parte de zero). Não tem provedor, ID
// externo, chave, hash, rodada, jogo nem referência.
func NewOpeningWagerTransaction(p NewOpeningWagerTransactionParams) (*WagerTransaction, error) {
	now := p.Now.UTC()
	t := &WagerTransaction{
		id:           p.ID,
		origin:       WagerOriginInternal,
		kind:         WagerKindOpening,
		status:       WagerStatusProcessed,
		walletID:     p.WalletID,
		playerID:     p.PlayerID,
		money:        p.Amount,
		balanceAfter: p.Amount,
		createdAt:    now,
		updatedAt:    now,
		completedAt:  now,
	}
	if err := t.validate(); err != nil {
		return nil, err
	}
	return t, nil
}

// RehydrateWagerTransactionParams agrupa o estado persistido. Campos externos
// ficam vazios em OPENING; BalanceAfter é Money{} quando NULL no banco.
type RehydrateWagerTransactionParams struct {
	ID                             uuid.UUID
	Origin                         WagerOrigin
	Kind                           WagerKind
	Status                         WagerStatus
	WalletID                       uuid.UUID
	PlayerID                       uuid.UUID
	Money                          Money
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	RequestHash                    string
	RoundID                        string
	GameID                         string
	ReferenceExternalTransactionID string
	ReferenceTransactionID         uuid.UUID
	FailureCode                    FailureCode
	BalanceAfter                   Money
	Attempts                       int32
	NextAttemptAt                  time.Time
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
	CompletedAt                    time.Time
}

// RehydrateWagerTransaction reconstrói uma transação persistida sem aplicar
// transições nem efeitos, revalidando todas as invariantes. Erros satisfazem
// ErrCorruptedWagerTransaction e ErrInvalidWagerTransaction.
func RehydrateWagerTransaction(p RehydrateWagerTransactionParams) (*WagerTransaction, error) {
	t := &WagerTransaction{
		id:                             p.ID,
		origin:                         p.Origin,
		kind:                           p.Kind,
		status:                         p.Status,
		walletID:                       p.WalletID,
		playerID:                       p.PlayerID,
		money:                          p.Money,
		providerID:                     p.ProviderID,
		externalTransactionID:          p.ExternalTransactionID,
		idempotencyKey:                 p.IdempotencyKey,
		requestHash:                    p.RequestHash,
		roundID:                        p.RoundID,
		gameID:                         p.GameID,
		referenceExternalTransactionID: p.ReferenceExternalTransactionID,
		referenceTransactionID:         p.ReferenceTransactionID,
		failureCode:                    p.FailureCode,
		balanceAfter:                   p.BalanceAfter,
		attempts:                       p.Attempts,
		nextAttemptAt:                  utcOrZero(p.NextAttemptAt),
		createdAt:                      utcOrZero(p.CreatedAt),
		updatedAt:                      utcOrZero(p.UpdatedAt),
		completedAt:                    utcOrZero(p.CompletedAt),
	}
	if err := t.validate(); err != nil {
		return nil, fmt.Errorf("%w: rehydrate wager transaction %s: %w", ErrCorruptedWagerTransaction, p.ID, err)
	}
	return t, nil
}

func utcOrZero(t time.Time) time.Time {
	if t.IsZero() {
		return time.Time{}
	}
	return t.UTC()
}

// validate concentra TODAS as invariantes; é usada pelos construtores, pela
// reidratação e por cada transição (sobre a cópia), para que nunca divirjam.
func (t *WagerTransaction) validate() error {
	if t.id == uuid.Nil {
		return fmt.Errorf("%w: nil id", ErrInvalidWagerTransaction)
	}
	if !t.kind.IsValid() {
		return fmt.Errorf("%w: %w: %q", ErrInvalidWagerTransaction, ErrInvalidWagerKind, t.kind)
	}
	if t.origin != t.kind.Origin() {
		return fmt.Errorf("%w: %w: origin %q incompatible with %s", ErrInvalidWagerTransaction, ErrInvalidWagerKind, t.origin, t.kind)
	}
	if !t.status.IsValid() {
		return fmt.Errorf("%w: status %q", ErrInvalidWagerTransaction, t.status)
	}
	if t.walletID == uuid.Nil {
		return fmt.Errorf("%w: nil wallet id", ErrInvalidWagerTransaction)
	}
	if t.playerID == uuid.Nil {
		return fmt.Errorf("%w: nil player id", ErrInvalidWagerTransaction)
	}
	if err := t.kind.validateAmount(t.money); err != nil {
		return err
	}
	if t.origin == WagerOriginExternal {
		if err := t.validateExternalMetadata(); err != nil {
			return err
		}
	} else if err := t.validateOpeningMetadata(); err != nil {
		return err
	}
	if err := t.validateOutcome(); err != nil {
		return err
	}
	if err := t.validateScheduling(); err != nil {
		return err
	}
	return t.validateTimestamps()
}

func (t *WagerTransaction) validateExternalMetadata() error {
	ids := []struct{ name, value string }{
		{"providerId", t.providerID},
		{"externalTransactionId", t.externalTransactionID},
		{"idempotencyKey", t.idempotencyKey},
		{"roundId", t.roundID},
		{"gameId", t.gameID},
	}
	for _, f := range ids {
		if err := validateWagerIdentifier(f.name, f.value); err != nil {
			return err
		}
	}
	if !isLowerHex(t.requestHash, RequestHashLength) {
		return fmt.Errorf("%w: requestHash must be %d lowercase hex characters", ErrInvalidWagerTransaction, RequestHashLength)
	}
	ref := t.referenceExternalTransactionID
	switch {
	case ref == "" && t.kind.RequiresReference():
		return fmt.Errorf("%w: %w: %s", ErrInvalidWagerTransaction, ErrReferenceRequired, t.kind)
	case ref != "" && !t.kind.AcceptsReference():
		return fmt.Errorf("%w: %w: %s", ErrInvalidWagerTransaction, ErrReferenceNotAllowed, t.kind)
	case ref != "":
		if err := validateWagerIdentifier("referenceExternalTransactionId", ref); err != nil {
			return err
		}
		if ref == t.externalTransactionID {
			return fmt.Errorf("%w: %w: transaction cannot reference itself", ErrInvalidWagerTransaction, ErrReferenceNotAllowed)
		}
	}
	return nil
}

func (t *WagerTransaction) validateOpeningMetadata() error {
	if t.providerID != "" || t.externalTransactionID != "" || t.idempotencyKey != "" || t.requestHash != "" ||
		t.roundID != "" || t.gameID != "" || t.referenceExternalTransactionID != "" {
		return fmt.Errorf("%w: OPENING must not carry external metadata", ErrInvalidWagerTransaction)
	}
	if t.referenceTransactionID != uuid.Nil {
		return fmt.Errorf("%w: OPENING must not reference another transaction", ErrInvalidWagerTransaction)
	}
	if t.status != WagerStatusProcessed {
		return fmt.Errorf("%w: OPENING must be PROCESSED, got %s", ErrInvalidWagerTransaction, t.status)
	}
	if t.balanceAfter != t.money {
		return fmt.Errorf("%w: OPENING balanceAfter %s must equal amount %s", ErrInvalidWagerTransaction, t.balanceAfter, t.money)
	}
	if t.attempts != 0 {
		return fmt.Errorf("%w: OPENING must not have attempts", ErrInvalidWagerTransaction)
	}
	return nil
}

// validateOutcome valida failureCode, balanceAfter e referência interna conforme
// o estado.
func (t *WagerTransaction) validateOutcome() error {
	// Código de falha.
	switch t.status {
	case WagerStatusRejected, WagerStatusFailed:
		if err := t.validateFailureCode(t.status, t.failureCode); err != nil {
			return err
		}
	default:
		if t.failureCode != "" {
			return fmt.Errorf("%w: %w: %s must not have failure code %q", ErrInvalidWagerTransaction, ErrInvalidFailureCode, t.status, t.failureCode)
		}
	}

	// Saldo devolvido ao provedor.
	switch t.status {
	case WagerStatusProcessed:
		if !t.balanceAfter.IsValid() {
			return fmt.Errorf("%w: PROCESSED requires balanceAfter: %w", ErrInvalidWagerTransaction, ErrUninitializedMoney)
		}
		if err := t.checkBalanceAfter(); err != nil {
			return err
		}
	case WagerStatusRejected:
		// Opcional: saldo observado na rejeição (ex.: INSUFFICIENT_FUNDS).
		if t.balanceAfter.IsValid() {
			if err := t.checkBalanceAfter(); err != nil {
				return err
			}
		}
	default:
		if t.balanceAfter != (Money{}) {
			return fmt.Errorf("%w: %s must not have balanceAfter", ErrInvalidWagerTransaction, t.status)
		}
	}

	// Referência interna resolvida.
	if t.referenceExternalTransactionID == "" {
		if t.referenceTransactionID != uuid.Nil {
			return fmt.Errorf("%w: %w: resolved reference without referenceExternalTransactionId", ErrInvalidWagerTransaction, ErrReferenceNotAllowed)
		}
		return nil
	}
	if t.referenceTransactionID == t.id {
		return fmt.Errorf("%w: %w: transaction cannot reference itself", ErrInvalidWagerTransaction, ErrReferenceNotAllowed)
	}
	if t.status == WagerStatusProcessed && t.referenceTransactionID == uuid.Nil {
		return fmt.Errorf("%w: PROCESSED %s requires the resolved reference transaction id", ErrInvalidWagerTransaction, t.kind)
	}
	return nil
}

func (t *WagerTransaction) checkBalanceAfter() error {
	if t.balanceAfter.Currency() != t.money.Currency() {
		return fmt.Errorf("%w: %w: balanceAfter %s, money %s", ErrInvalidWagerTransaction, ErrCurrencyMismatch, t.balanceAfter.Currency(), t.money.Currency())
	}
	if t.balanceAfter.IsNegative() {
		return fmt.Errorf("%w: %w: balanceAfter %s", ErrInvalidWagerTransaction, ErrNegativeAmount, t.balanceAfter)
	}
	return nil
}

// validateFailureCode verifica se code é aceitável como resultado status para
// esta transação.
func (t *WagerTransaction) validateFailureCode(status WagerStatus, code FailureCode) error {
	if !code.IsValid() {
		return fmt.Errorf("%w: %w: %q", ErrInvalidWagerTransaction, ErrInvalidFailureCode, code)
	}
	cat := code.Category()
	if status == WagerStatusFailed && cat != FailureCategoryInfrastructure {
		return fmt.Errorf("%w: %w: FAILED requires an INFRASTRUCTURE code, got %s", ErrInvalidWagerTransaction, ErrInvalidFailureCode, code)
	}
	if status == WagerStatusRejected && cat == FailureCategoryInfrastructure {
		return fmt.Errorf("%w: %w: REJECTED requires an INPUT or BUSINESS code, got %s", ErrInvalidWagerTransaction, ErrInvalidFailureCode, code)
	}
	if code.requiresReference() && t.referenceExternalTransactionID == "" {
		return fmt.Errorf("%w: %w: %s requires a reference", ErrInvalidWagerTransaction, ErrInvalidFailureCode, code)
	}
	// REFERENCE_NOT_FOUND significa que a referência nunca foi resolvida; um
	// referenceTransactionID preenchido contradiz o próprio código (ADR-008).
	if code == FailureCodeReferenceNotFound && t.referenceTransactionID != uuid.Nil {
		return fmt.Errorf("%w: %w: %s must not carry a resolved reference transaction id", ErrInvalidWagerTransaction, ErrInvalidFailureCode, code)
	}
	if !code.allowedForKind(t.kind) {
		return fmt.Errorf("%w: %w: %s not applicable to %s", ErrInvalidWagerTransaction, ErrInvalidFailureCode, code, t.kind)
	}
	return nil
}

// validateScheduling valida attempts/nextAttemptAt (retomada durável, T21).
func (t *WagerTransaction) validateScheduling() error {
	if t.attempts < 0 {
		return fmt.Errorf("%w: negative attempts %d", ErrInvalidWagerTransaction, t.attempts)
	}
	// attempts conta SOMENTE buscas de referência sem sucesso (ADR-008). Logo:
	//   - exige referenceExternalTransactionId (BET/LOSS/WIN sem referência e
	//     OPENING nunca têm attempts > 0);
	//   - não existe em PENDING, porque só MarkPendingReference/
	//     RescheduleReferenceRetry incrementam e não há aresta de volta para
	//     PENDING. A retomada de PENDING órfão (T21) usa apenas nextAttemptAt e
	//     não consome attempts.
	if t.attempts > 0 {
		if t.referenceExternalTransactionID == "" {
			return fmt.Errorf("%w: attempts %d without reference: attempts only count reference lookups", ErrInvalidWagerTransaction, t.attempts)
		}
		if t.status == WagerStatusPending {
			return fmt.Errorf("%w: PENDING must not have attempts (got %d)", ErrInvalidWagerTransaction, t.attempts)
		}
	}
	switch {
	case t.status == WagerStatusPendingReference:
		if t.referenceExternalTransactionID == "" {
			return fmt.Errorf("%w: PENDING_REFERENCE requires a reference", ErrInvalidWagerTransaction)
		}
		if t.attempts < 1 {
			return fmt.Errorf("%w: PENDING_REFERENCE requires attempts >= 1", ErrInvalidWagerTransaction)
		}
		if t.nextAttemptAt.IsZero() {
			return fmt.Errorf("%w: PENDING_REFERENCE requires nextAttemptAt", ErrInvalidWagerTransaction)
		}
	case t.status.IsTerminal():
		if !t.nextAttemptAt.IsZero() {
			return fmt.Errorf("%w: terminal transaction must not be scheduled", ErrInvalidWagerTransaction)
		}
	}
	// PENDING pode ter nextAttemptAt (retomada de PENDING órfão, T21).
	if !t.nextAttemptAt.IsZero() && t.nextAttemptAt.Before(t.createdAt) {
		return fmt.Errorf("%w: nextAttemptAt before createdAt", ErrInvalidWagerTransaction)
	}
	return nil
}

func (t *WagerTransaction) validateTimestamps() error {
	if t.createdAt.IsZero() || t.updatedAt.IsZero() {
		return fmt.Errorf("%w: zero timestamp", ErrInvalidWagerTransaction)
	}
	if t.updatedAt.Before(t.createdAt) {
		return fmt.Errorf("%w: updatedAt before createdAt", ErrInvalidWagerTransaction)
	}
	if t.status.IsTerminal() {
		if t.completedAt.IsZero() {
			return fmt.Errorf("%w: terminal transaction requires completedAt", ErrInvalidWagerTransaction)
		}
		if t.completedAt.Before(t.createdAt) || t.completedAt.After(t.updatedAt) {
			return fmt.Errorf("%w: completedAt outside [createdAt, updatedAt]", ErrInvalidWagerTransaction)
		}
	} else if !t.completedAt.IsZero() {
		return fmt.Errorf("%w: non-terminal transaction must not have completedAt", ErrInvalidWagerTransaction)
	}
	return nil
}

// validateWagerIdentifier exige identificador não vazio, até
// MaxWagerIdentifierLength bytes, sem espaços nas bordas e sem caracteres de
// controle. Não normalizamos (trim/caixa): a mesma string entra no hash (T08) e
// nas chaves únicas do banco.
func validateWagerIdentifier(name, v string) error {
	if v == "" {
		return fmt.Errorf("%w: %s is required", ErrInvalidWagerTransaction, name)
	}
	if len(v) > MaxWagerIdentifierLength {
		return fmt.Errorf("%w: %s exceeds %d bytes", ErrInvalidWagerTransaction, name, MaxWagerIdentifierLength)
	}
	if strings.TrimSpace(v) != v {
		return fmt.Errorf("%w: %s has leading or trailing whitespace", ErrInvalidWagerTransaction, name)
	}
	for _, r := range v {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: %s contains control characters", ErrInvalidWagerTransaction, name)
		}
	}
	return nil
}

func isLowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// --- Transições -----------------------------------------------------------------

// MarkWagerProcessedParams agrupa o resultado de uma operação bem-sucedida.
type MarkWagerProcessedParams struct {
	// BalanceAfter é o saldo da carteira após a operação (inalterado em LOSS),
	// devolvido em replays mesmo após outras movimentações.
	BalanceAfter Money
	// ReferenceTransactionID é a referência interna resolvida; obrigatória
	// quando há referenceExternalTransactionId, proibida caso contrário.
	ReferenceTransactionID uuid.UUID
	Now                    time.Time
}

// MarkProcessed conclui a operação com sucesso (PENDING|PENDING_REFERENCE ->
// PROCESSED).
func (t *WagerTransaction) MarkProcessed(p MarkWagerProcessedParams) error {
	return t.transition(WagerStatusProcessed, p.Now, func(n *WagerTransaction) error {
		n.balanceAfter = p.BalanceAfter
		n.referenceTransactionID = p.ReferenceTransactionID
		n.nextAttemptAt = time.Time{}
		return nil
	})
}

// MarkWagerPendingReferenceParams agrupa o agendamento da espera por referência.
type MarkWagerPendingReferenceParams struct {
	// NextAttemptAt é quando o worker deve tentar resolver a referência
	// (backoff calculado pela aplicação, T21). Deve ser >= Now.
	NextAttemptAt time.Time
	Now           time.Time
}

// MarkPendingReference registra a espera pela referência (PENDING ->
// PENDING_REFERENCE). Conta a busca que acabou de falhar: attempts += 1.
func (t *WagerTransaction) MarkPendingReference(p MarkWagerPendingReferenceParams) error {
	if t.IsInitialized() && t.status == WagerStatusPendingReference {
		return fmt.Errorf("%w: already PENDING_REFERENCE; use RescheduleReferenceRetry", ErrWagerTransitionNotAllowed)
	}
	return t.scheduleReference(p.NextAttemptAt, p.Now)
}

// RescheduleReferenceRetry registra mais uma tentativa sem sucesso do worker
// (PENDING_REFERENCE -> PENDING_REFERENCE): attempts += 1 e novo nextAttemptAt.
// O limite de tentativas/TTL é política da aplicação, que chama Reject com
// REFERENCE_NOT_FOUND ao esgotá-lo.
func (t *WagerTransaction) RescheduleReferenceRetry(p MarkWagerPendingReferenceParams) error {
	if t.IsInitialized() && t.status == WagerStatusPending {
		return fmt.Errorf("%w: PENDING must use MarkPendingReference", ErrWagerTransitionNotAllowed)
	}
	return t.scheduleReference(p.NextAttemptAt, p.Now)
}

func (t *WagerTransaction) scheduleReference(next, now time.Time) error {
	return t.transition(WagerStatusPendingReference, now, func(n *WagerTransaction) error {
		if next.IsZero() || next.Before(now) {
			return fmt.Errorf("%w: nextAttemptAt must be >= now", ErrInvalidWagerTransaction)
		}
		if n.attempts == math.MaxInt32 {
			return fmt.Errorf("%w: attempts overflow", ErrInvalidWagerTransaction)
		}
		n.attempts++
		n.nextAttemptAt = next.UTC()
		return nil
	})
}

// RejectWagerParams agrupa uma rejeição de negócio.
type RejectWagerParams struct {
	// Code deve ser da categoria INPUT ou BUSINESS e aplicável ao tipo.
	Code FailureCode
	// ReferenceTransactionID opcional: referência interna resolvida, quando a
	// rejeição ocorreu após resolvê-la (ex.: ALREADY_REVERSED, AMOUNT_MISMATCH).
	// Proibida com REFERENCE_NOT_FOUND (a referência não foi resolvida).
	ReferenceTransactionID uuid.UUID
	// BalanceAfter opcional: saldo observado na rejeição (inalterado).
	BalanceAfter Money
	Now          time.Time
}

// Reject finaliza a operação como REJECTED (PENDING|PENDING_REFERENCE ->
// REJECTED). Não há movimentação nem lançamento.
func (t *WagerTransaction) Reject(p RejectWagerParams) error {
	return t.transition(WagerStatusRejected, p.Now, func(n *WagerTransaction) error {
		n.failureCode = p.Code
		n.referenceTransactionID = p.ReferenceTransactionID
		n.balanceAfter = p.BalanceAfter
		n.nextAttemptAt = time.Time{}
		return nil
	})
}

// FailWagerParams agrupa uma falha permanente de infraestrutura.
type FailWagerParams struct {
	// Code deve ser da categoria INFRASTRUCTURE.
	Code FailureCode
	Now  time.Time
}

// Fail finaliza a operação como FAILED, para auditoria
// (PENDING|PENDING_REFERENCE -> FAILED). Falhas transitórias NÃO devem chamar
// Fail: fazem rollback e são refeitas.
func (t *WagerTransaction) Fail(p FailWagerParams) error {
	return t.transition(WagerStatusFailed, p.Now, func(n *WagerTransaction) error {
		n.failureCode = p.Code
		n.nextAttemptAt = time.Time{}
		return nil
	})
}

// ValidateFailTransition valida a transição from -> FAILED com code sem o
// agregado: from precisa ser um estado conhecido e não terminal com aresta para
// FAILED, e code precisa ser um código conhecido da categoria INFRASTRUCTURE.
// Existe para o único caso em que a transação não pode ser reidratada (linha
// persistida que viola as invariantes do domínio, ErrCorruptedWagerTransaction):
// o worker de pendências (T30, ADR-031) ainda precisa encerrá-la como FAILED
// RETRIES_EXHAUSTED, e a regra da transição continua sendo do domínio. Com o
// agregado em mãos, use Fail (que aplica esta mesma regra e revalida todas as
// invariantes).
func ValidateFailTransition(from WagerStatus, code FailureCode) error {
	if !from.IsValid() {
		return fmt.Errorf("%w: unknown status %q", ErrWagerTransitionNotAllowed, from)
	}
	if from.IsTerminal() {
		return fmt.Errorf("%w: %s -> %s", ErrWagerTransactionTerminal, from, WagerStatusFailed)
	}
	if !CanTransitionWagerStatus(from, WagerStatusFailed) {
		return fmt.Errorf("%w: %s -> %s", ErrWagerTransitionNotAllowed, from, WagerStatusFailed)
	}
	if !code.IsValid() || code.Category() != FailureCategoryInfrastructure {
		return fmt.Errorf("%w: %w: FAILED requires an INFRASTRUCTURE code, got %q", ErrInvalidWagerTransaction, ErrInvalidFailureCode, code)
	}
	return nil
}

// transition valida a aresta da máquina de estados (antes de validar
// parâmetros, para que "não inicializada" e "terminal" tenham precedência),
// aplica mutate numa cópia, revalida todas as invariantes e só então substitui o
// receptor.
func (t *WagerTransaction) transition(to WagerStatus, now time.Time, mutate func(*WagerTransaction) error) error {
	if !t.IsInitialized() {
		return ErrUninitializedWagerTransaction
	}
	if t.status.IsTerminal() {
		return fmt.Errorf("%w: %s -> %s", ErrWagerTransactionTerminal, t.status, to)
	}
	if !CanTransitionWagerStatus(t.status, to) {
		return fmt.Errorf("%w: %s -> %s", ErrWagerTransitionNotAllowed, t.status, to)
	}
	if now.IsZero() {
		return fmt.Errorf("%w: zero transition time", ErrInvalidWagerTransaction)
	}
	n := *t
	n.status = to
	if err := mutate(&n); err != nil {
		return err
	}
	now = now.UTC()
	if now.After(n.updatedAt) {
		n.updatedAt = now
	}
	if to.IsTerminal() {
		n.completedAt = n.updatedAt
	}
	if err := n.validate(); err != nil {
		return err
	}
	*t = n
	return nil
}

// --- Getters --------------------------------------------------------------------

// ID devolve o id interno.
func (t *WagerTransaction) ID() uuid.UUID { return t.id }

// Origin devolve INTERNAL (OPENING) ou EXTERNAL.
func (t *WagerTransaction) Origin() WagerOrigin { return t.origin }

// Kind devolve o tipo.
func (t *WagerTransaction) Kind() WagerKind { return t.kind }

// Status devolve o estado atual.
func (t *WagerTransaction) Status() WagerStatus { return t.status }

// IsTerminal indica estado terminal.
func (t *WagerTransaction) IsTerminal() bool { return t.status.IsTerminal() }

// WalletID devolve a carteira.
func (t *WagerTransaction) WalletID() uuid.UUID { return t.walletID }

// PlayerID devolve o jogador.
func (t *WagerTransaction) PlayerID() uuid.UUID { return t.playerID }

// Money devolve o valor da operação (0.00 em LOSS).
func (t *WagerTransaction) Money() Money { return t.money }

// ProviderID devolve o provedor ("" em OPENING).
func (t *WagerTransaction) ProviderID() string { return t.providerID }

// ExternalTransactionID devolve o id externo ("" em OPENING).
func (t *WagerTransaction) ExternalTransactionID() string { return t.externalTransactionID }

// IdempotencyKey devolve a chave recebida ("" em OPENING).
func (t *WagerTransaction) IdempotencyKey() string { return t.idempotencyKey }

// RequestHash devolve o hash canônico do payload ("" em OPENING).
func (t *WagerTransaction) RequestHash() string { return t.requestHash }

// RoundID devolve a rodada ("" em OPENING).
func (t *WagerTransaction) RoundID() string { return t.roundID }

// GameID devolve o jogo ("" em OPENING).
func (t *WagerTransaction) GameID() string { return t.gameID }

// ReferenceExternalTransactionID devolve a referência externa ("" se ausente).
func (t *WagerTransaction) ReferenceExternalTransactionID() string {
	return t.referenceExternalTransactionID
}

// HasReference indica referenceExternalTransactionId informado.
func (t *WagerTransaction) HasReference() bool { return t.referenceExternalTransactionID != "" }

// ReferenceTransactionID devolve a referência interna resolvida (uuid.Nil se
// não resolvida).
func (t *WagerTransaction) ReferenceTransactionID() uuid.UUID { return t.referenceTransactionID }

// FailureCode devolve o código de falha ("" fora de REJECTED/FAILED).
func (t *WagerTransaction) FailureCode() FailureCode { return t.failureCode }

// BalanceAfter devolve o saldo resultante (Money{} quando não aplicável).
func (t *WagerTransaction) BalanceAfter() Money { return t.balanceAfter }

// Attempts devolve as tentativas de resolução de referência sem sucesso (0 em
// transações sem referência e em PENDING).
func (t *WagerTransaction) Attempts() int32 { return t.attempts }

// NextAttemptAt devolve o próximo instante de retomada (zero se não agendado).
func (t *WagerTransaction) NextAttemptAt() time.Time { return t.nextAttemptAt }

// CreatedAt devolve o instante de criação (UTC).
func (t *WagerTransaction) CreatedAt() time.Time { return t.createdAt }

// UpdatedAt devolve o instante da última transição (UTC).
func (t *WagerTransaction) UpdatedAt() time.Time { return t.updatedAt }

// CompletedAt devolve o instante da transição terminal (zero se não terminal).
func (t *WagerTransaction) CompletedAt() time.Time { return t.completedAt }

// IsInitialized indica construção por New*/Rehydrate (zero-value e nil não são).
func (t *WagerTransaction) IsInitialized() bool {
	return t != nil && t.id != uuid.Nil && t.status.IsValid()
}
