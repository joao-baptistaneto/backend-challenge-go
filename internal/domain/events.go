package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

// Decisão arquitetural (ADR-012 em ARCHITECTURE.md): eventos de integração
// tipados, publicados via transactional outbox (T13/T22).
//
//   - Envelope: {eventId, eventType, aggregateId, correlationId, causationId?,
//     occurredAt, version, data}. eventId é UUIDv7 gerado no construtor e é a
//     identidade estável do evento: o publisher (T22) o usa como
//     MessageDeduplicationId e republicações preservam o mesmo id, porque o
//     payload é lido da outbox e nunca reconstruído.
//   - Tipo e versão: cada tipo de data (sealed, EventData) declara seu
//     eventType e sua versão de schema. O construtor público de cada evento é o
//     único caminho para criar um IntegrationEvent, então o chamador não escolhe
//     tipo nem versão. Mudança incompatível de payload = nova versão.
//   - Snapshot imutável: o construtor serializa o envelope inteiro uma única vez
//     e guarda os bytes. Payload()/MarshalJSON() devolvem CÓPIAS, Data()
//     devolve uma cópia profunda e não existe setter. Alterar a transação ou a
//     carteira depois de construir o evento não altera o evento. A outbox
//     persiste exatamente Payload().
//   - aggregateId: eventos de WagerTransaction usam o id da transação (a ordem
//     PENDING_REFERENCE -> PROCESSED/REJECTED da mesma transação fica no mesmo
//     MessageGroupId FIFO). WalletBalanceChanged usa o id da carteira (todas as
//     mudanças de saldo da carteira ficam em ordem no mesmo grupo). Ordem entre
//     tipos diferentes não é garantida; consumidores usam walletVersion para
//     ordenar/deduplicar mudanças de saldo.
//   - occurredAt (RFC 3339, UTC, precisão de microssegundos para coincidir com
//     timestamptz do PostgreSQL): Processed/Rejected usam CompletedAt da
//     transação (ADR-008), PendingReference usa UpdatedAt (instante do registro
//     da espera) e WalletBalanceChanged usa CreatedAt do lançamento.
//   - Dinheiro: Money serializa como {"amount":"25.00","currency":"BRL"}
//     (string decimal, ADR-004). Nenhum valor monetário passa por tipo numérico
//     JSON.
//   - Dados sensíveis: idempotencyKey e requestHash nunca entram nos eventos
//     (são detalhes de transporte/idempotência). WagerTransactionRejected nunca
//     carrega referenceTransactionId: em REFERENCE_MISMATCH ele é o id interno
//     de uma transação de OUTRA carteira/jogador (ressalva da T06), e nos demais
//     códigos a referência externa (do próprio provedor) basta. Rejeições
//     WALLET_NOT_FOUND, WALLET_PLAYER_MISMATCH e CURRENCY_MISMATCH nunca levam
//     saldo, mesmo que a transação tenha um, para não expor a carteira de outro
//     jogador (ADR-009).
//   - OPENING (abertura com saldo > 0): gera WagerTransactionProcessed (origin
//     INTERNAL, sem metadados externos: os campos são omitidos do JSON) e
//     WalletBalanceChanged (CREDIT 0 -> saldo inicial, walletVersion 1), ambos
//     no mesmo commit da carteira. Saldo zero não gera eventos. OPENING nunca é
//     rejeitado nem fica pendente, logo os demais construtores não se aplicam.
//   - FAILED (falha permanente de infraestrutura) não gera evento de
//     integração: não é rejeição de negócio. Fica visível em métricas/logs e na
//     consulta da transação.
//   - correlationId é obrigatório (vem do request HTTP, do envelope SQS ou, no
//     worker, do contexto de retomada); causationId é opcional (ex.: messageId
//     SQS que causou o evento). Ambos seguem as regras dos identificadores
//     externos: não vazios, <= 255 bytes, sem espaço nas bordas nem caracteres
//     de controle.

// ErrInvalidIntegrationEvent indica que o evento não pode ser construído:
// origem não inicializada ou em estado incompatível com o tipo, carteira e
// lançamento inconsistentes, metadados inválidos ou falha do gerador de ids.
var ErrInvalidIntegrationEvent = errors.New("integration event: invalid event")

// EventType é o nome do tipo de evento (contrato público estável).
type EventType string

const (
	// EventTypeWagerTransactionProcessed: conclusão bem-sucedida (inclui LOSS e
	// OPENING).
	EventTypeWagerTransactionProcessed EventType = "WagerTransactionProcessed"
	// EventTypeWagerTransactionRejected: rejeição definitiva (REJECTED).
	EventTypeWagerTransactionRejected EventType = "WagerTransactionRejected"
	// EventTypeWalletBalanceChanged: alteração efetiva de saldo.
	EventTypeWalletBalanceChanged EventType = "WalletBalanceChanged"
	// EventTypeWagerTransactionPendingReference: registro de espera pela
	// referência.
	EventTypeWagerTransactionPendingReference EventType = "WagerTransactionPendingReference"
)

// String devolve o nome do tipo.
func (t EventType) String() string { return string(t) }

// Versões de schema de cada tipo. Fixadas pelo tipo de data; o chamador não as
// escolhe.
const (
	WagerTransactionProcessedVersion        = 1
	WagerTransactionRejectedVersion         = 1
	WalletBalanceChangedVersion             = 1
	WagerTransactionPendingReferenceVersion = 1
)

// newEventID gera o eventId. UUIDv7 é ordenável por tempo (favorece o índice da
// outbox). Variável de pacote apenas para testes determinísticos.
var newEventID = uuid.NewV7

// EventMetadata agrupa os metadados de rastreio do envelope.
type EventMetadata struct {
	// CorrelationID é obrigatório e propagado do request/mensagem de origem.
	CorrelationID string
	// CausationID é opcional: id da mensagem/evento que causou este evento.
	CausationID string
}

func (m EventMetadata) validate() error {
	if err := validateEventIdentifier("correlationId", m.CorrelationID); err != nil {
		return err
	}
	if m.CausationID != "" {
		if err := validateEventIdentifier("causationId", m.CausationID); err != nil {
			return err
		}
	}
	return nil
}

func validateEventIdentifier(name, v string) error {
	if v == "" {
		return fmt.Errorf("%w: %s is required", ErrInvalidIntegrationEvent, name)
	}
	if len(v) > MaxWagerIdentifierLength {
		return fmt.Errorf("%w: %s longer than %d bytes", ErrInvalidIntegrationEvent, name, MaxWagerIdentifierLength)
	}
	for i, r := range v {
		if unicode.IsControl(r) || r == unicode.ReplacementChar {
			return fmt.Errorf("%w: %s has invalid character at byte %d", ErrInvalidIntegrationEvent, name, i)
		}
	}
	first, _ := utf8.DecodeRuneInString(v)
	last, _ := utf8.DecodeLastRuneInString(v)
	if unicode.IsSpace(first) || unicode.IsSpace(last) {
		return fmt.Errorf("%w: %s has leading or trailing whitespace", ErrInvalidIntegrationEvent, name)
	}
	return nil
}

// EventData é o payload tipado (campo data) de um evento. É uma interface
// selada: só os tipos deste pacote a implementam, e cada um fixa seu tipo e
// versão.
type EventData interface {
	// EventType devolve o tipo fixado pelo payload.
	EventType() EventType
	// SchemaVersion devolve a versão fixada pelo payload.
	SchemaVersion() int
	cloneEventData() EventData
}

// WagerTransactionProcessedData é o payload de WagerTransactionProcessed.
// Campos externos são omitidos em OPENING (origin INTERNAL).
type WagerTransactionProcessedData struct {
	TransactionID                  uuid.UUID   `json:"transactionId"`
	WalletID                       uuid.UUID   `json:"walletId"`
	PlayerID                       uuid.UUID   `json:"playerId"`
	Kind                           WagerKind   `json:"kind"`
	Origin                         WagerOrigin `json:"origin"`
	Status                         WagerStatus `json:"status"`
	Money                          Money       `json:"money"`
	BalanceAfter                   Money       `json:"balanceAfter"`
	ProviderID                     string      `json:"providerId,omitempty"`
	ExternalTransactionID          string      `json:"externalTransactionId,omitempty"`
	RoundID                        string      `json:"roundId,omitempty"`
	GameID                         string      `json:"gameId,omitempty"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId,omitempty"`
	// ReferenceTransactionID é a referência interna resolvida. Em PROCESSED ela
	// pertence obrigatoriamente à mesma carteira (ADR-009), então não vaza dados.
	ReferenceTransactionID uuid.UUID `json:"referenceTransactionId,omitzero"`
}

// EventType implementa EventData.
func (WagerTransactionProcessedData) EventType() EventType {
	return EventTypeWagerTransactionProcessed
}

// SchemaVersion implementa EventData.
func (WagerTransactionProcessedData) SchemaVersion() int { return WagerTransactionProcessedVersion }

func (d WagerTransactionProcessedData) cloneEventData() EventData { return d }

// WagerTransactionRejectedData é o payload de WagerTransactionRejected. Não tem
// referenceTransactionId (ver decisão no topo do arquivo).
type WagerTransactionRejectedData struct {
	TransactionID                  uuid.UUID       `json:"transactionId"`
	WalletID                       uuid.UUID       `json:"walletId"`
	PlayerID                       uuid.UUID       `json:"playerId"`
	Kind                           WagerKind       `json:"kind"`
	Origin                         WagerOrigin     `json:"origin"`
	Status                         WagerStatus     `json:"status"`
	Money                          Money           `json:"money"`
	FailureCode                    FailureCode     `json:"failureCode"`
	FailureCategory                FailureCategory `json:"failureCategory"`
	ProviderID                     string          `json:"providerId,omitempty"`
	ExternalTransactionID          string          `json:"externalTransactionId,omitempty"`
	RoundID                        string          `json:"roundId,omitempty"`
	GameID                         string          `json:"gameId,omitempty"`
	ReferenceExternalTransactionID string          `json:"referenceExternalTransactionId,omitempty"`
	// BalanceAfter é o saldo observado na rejeição, quando houver e for seguro
	// expor; nil omite o campo.
	BalanceAfter *Money `json:"balanceAfter,omitempty"`
}

// EventType implementa EventData.
func (WagerTransactionRejectedData) EventType() EventType { return EventTypeWagerTransactionRejected }

// SchemaVersion implementa EventData.
func (WagerTransactionRejectedData) SchemaVersion() int { return WagerTransactionRejectedVersion }

func (d WagerTransactionRejectedData) cloneEventData() EventData {
	if d.BalanceAfter != nil {
		b := *d.BalanceAfter
		d.BalanceAfter = &b
	}
	return d
}

// WalletBalanceChangedData é o payload de WalletBalanceChanged.
type WalletBalanceChangedData struct {
	WalletID      uuid.UUID       `json:"walletId"`
	TransactionID uuid.UUID       `json:"transactionId"`
	Direction     LedgerDirection `json:"direction"`
	Money         Money           `json:"money"`
	BalanceBefore Money           `json:"balanceBefore"`
	BalanceAfter  Money           `json:"balanceAfter"`
	// WalletVersion é a versão resultante do lançamento: Wallet.Version() da
	// instância que acabou de produzi-lo (1 na abertura).
	WalletVersion int64 `json:"walletVersion"`
}

// EventType implementa EventData.
func (WalletBalanceChangedData) EventType() EventType { return EventTypeWalletBalanceChanged }

// SchemaVersion implementa EventData.
func (WalletBalanceChangedData) SchemaVersion() int { return WalletBalanceChangedVersion }

func (d WalletBalanceChangedData) cloneEventData() EventData { return d }

// WagerTransactionPendingReferenceData é o payload de
// WagerTransactionPendingReference.
type WagerTransactionPendingReferenceData struct {
	TransactionID                  uuid.UUID   `json:"transactionId"`
	WalletID                       uuid.UUID   `json:"walletId"`
	PlayerID                       uuid.UUID   `json:"playerId"`
	Kind                           WagerKind   `json:"kind"`
	Origin                         WagerOrigin `json:"origin"`
	Status                         WagerStatus `json:"status"`
	Money                          Money       `json:"money"`
	ProviderID                     string      `json:"providerId"`
	ExternalTransactionID          string      `json:"externalTransactionId"`
	RoundID                        string      `json:"roundId"`
	GameID                         string      `json:"gameId"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId"`
	// Attempts conta as buscas de referência sem sucesso (ADR-008).
	Attempts int32 `json:"attempts"`
	// NextAttemptAt é o próximo agendamento do worker (RFC 3339, UTC).
	NextAttemptAt time.Time `json:"nextAttemptAt"`
}

// EventType implementa EventData.
func (WagerTransactionPendingReferenceData) EventType() EventType {
	return EventTypeWagerTransactionPendingReference
}

// SchemaVersion implementa EventData.
func (WagerTransactionPendingReferenceData) SchemaVersion() int {
	return WagerTransactionPendingReferenceVersion
}

func (d WagerTransactionPendingReferenceData) cloneEventData() EventData { return d }

// IntegrationEvent é um evento de integração imutável, pronto para a outbox.
// O zero-value não é um evento válido (IsInitialized() == false).
type IntegrationEvent struct {
	id            uuid.UUID
	eventType     EventType
	aggregateID   uuid.UUID
	correlationID string
	causationID   string
	occurredAt    time.Time
	version       int
	data          EventData
	payload       []byte // snapshot do envelope; nunca exposto sem cópia
}

// eventEnvelopeJSON é o contrato de serialização do envelope.
type eventEnvelopeJSON struct {
	EventID       uuid.UUID `json:"eventId"`
	EventType     EventType `json:"eventType"`
	AggregateID   uuid.UUID `json:"aggregateId"`
	CorrelationID string    `json:"correlationId"`
	CausationID   string    `json:"causationId,omitempty"`
	OccurredAt    string    `json:"occurredAt"`
	Version       int       `json:"version"`
	Data          EventData `json:"data"`
}

// eventTime normaliza instantes do evento: UTC e precisão de microssegundos
// (a mesma do timestamptz), para que o occurredAt do payload e o da coluna da
// outbox sejam idênticos.
func eventTime(t time.Time) time.Time { return t.UTC().Truncate(time.Microsecond) }

// formatEventTime formata em RFC 3339 UTC ("Z"), com fração só quando houver.
func formatEventTime(t time.Time) string { return eventTime(t).Format(time.RFC3339Nano) }

// newIntegrationEvent é o único caminho de construção: gera o eventId, fixa
// tipo/versão a partir do data e serializa o snapshot.
func newIntegrationEvent(aggregateID uuid.UUID, occurredAt time.Time, meta EventMetadata, data EventData) (IntegrationEvent, error) {
	if err := meta.validate(); err != nil {
		return IntegrationEvent{}, err
	}
	if aggregateID == uuid.Nil {
		return IntegrationEvent{}, fmt.Errorf("%w: nil aggregate id", ErrInvalidIntegrationEvent)
	}
	if occurredAt.IsZero() {
		return IntegrationEvent{}, fmt.Errorf("%w: zero occurredAt", ErrInvalidIntegrationEvent)
	}
	id, err := newEventID()
	if err != nil {
		return IntegrationEvent{}, fmt.Errorf("%w: generate event id: %w", ErrInvalidIntegrationEvent, err)
	}
	if id == uuid.Nil {
		return IntegrationEvent{}, fmt.Errorf("%w: generated nil event id", ErrInvalidIntegrationEvent)
	}
	data = data.cloneEventData()
	e := IntegrationEvent{
		id:            id,
		eventType:     data.EventType(),
		aggregateID:   aggregateID,
		correlationID: meta.CorrelationID,
		causationID:   meta.CausationID,
		occurredAt:    eventTime(occurredAt),
		version:       data.SchemaVersion(),
		data:          data,
	}
	payload, err := json.Marshal(eventEnvelopeJSON{
		EventID:       e.id,
		EventType:     e.eventType,
		AggregateID:   e.aggregateID,
		CorrelationID: e.correlationID,
		CausationID:   e.causationID,
		OccurredAt:    formatEventTime(e.occurredAt),
		Version:       e.version,
		Data:          data,
	})
	if err != nil {
		return IntegrationEvent{}, fmt.Errorf("%w: marshal %s: %w", ErrInvalidIntegrationEvent, e.eventType, err)
	}
	e.payload = payload
	return e, nil
}

// checkTransaction valida a transação de origem e o status exigido.
func checkTransaction(tx *WagerTransaction, want WagerStatus, et EventType) error {
	if tx == nil || !tx.IsInitialized() {
		return fmt.Errorf("%w: %s: %w", ErrInvalidIntegrationEvent, et, ErrUninitializedWagerTransaction)
	}
	if tx.Status() != want {
		return fmt.Errorf("%w: %s requires status %s, got %s", ErrInvalidIntegrationEvent, et, want, tx.Status())
	}
	return nil
}

// NewWagerTransactionProcessedEvent cria WagerTransactionProcessed a partir de
// uma transação PROCESSED (externa ou OPENING). occurredAt = CompletedAt.
func NewWagerTransactionProcessedEvent(tx *WagerTransaction, meta EventMetadata) (IntegrationEvent, error) {
	if err := checkTransaction(tx, WagerStatusProcessed, EventTypeWagerTransactionProcessed); err != nil {
		return IntegrationEvent{}, err
	}
	data := WagerTransactionProcessedData{
		TransactionID:                  tx.ID(),
		WalletID:                       tx.WalletID(),
		PlayerID:                       tx.PlayerID(),
		Kind:                           tx.Kind(),
		Origin:                         tx.Origin(),
		Status:                         tx.Status(),
		Money:                          tx.Money(),
		BalanceAfter:                   tx.BalanceAfter(),
		ProviderID:                     tx.ProviderID(),
		ExternalTransactionID:          tx.ExternalTransactionID(),
		RoundID:                        tx.RoundID(),
		GameID:                         tx.GameID(),
		ReferenceExternalTransactionID: tx.ReferenceExternalTransactionID(),
		ReferenceTransactionID:         tx.ReferenceTransactionID(),
	}
	return newIntegrationEvent(tx.ID(), tx.CompletedAt(), meta, data)
}

// rejectionHidesBalance lista os códigos cuja rejeição nunca expõe saldo
// (ADR-009: a carteira pode ser de outro jogador ou de outra moeda).
var rejectionHidesBalance = []FailureCode{
	FailureCodeWalletNotFound,
	FailureCodeWalletPlayerMismatch,
	FailureCodeCurrencyMismatch,
}

// NewWagerTransactionRejectedEvent cria WagerTransactionRejected a partir de
// uma transação REJECTED. occurredAt = CompletedAt. Nunca inclui
// referenceTransactionId; o saldo observado só é incluído quando existe e o
// código permite (ver rejectionHidesBalance).
func NewWagerTransactionRejectedEvent(tx *WagerTransaction, meta EventMetadata) (IntegrationEvent, error) {
	if err := checkTransaction(tx, WagerStatusRejected, EventTypeWagerTransactionRejected); err != nil {
		return IntegrationEvent{}, err
	}
	data := WagerTransactionRejectedData{
		TransactionID:                  tx.ID(),
		WalletID:                       tx.WalletID(),
		PlayerID:                       tx.PlayerID(),
		Kind:                           tx.Kind(),
		Origin:                         tx.Origin(),
		Status:                         tx.Status(),
		Money:                          tx.Money(),
		FailureCode:                    tx.FailureCode(),
		FailureCategory:                tx.FailureCode().Category(),
		ProviderID:                     tx.ProviderID(),
		ExternalTransactionID:          tx.ExternalTransactionID(),
		RoundID:                        tx.RoundID(),
		GameID:                         tx.GameID(),
		ReferenceExternalTransactionID: tx.ReferenceExternalTransactionID(),
	}
	if b := tx.BalanceAfter(); b.IsValid() && !slices.Contains(rejectionHidesBalance, tx.FailureCode()) {
		data.BalanceAfter = &b
	}
	return newIntegrationEvent(tx.ID(), tx.CompletedAt(), meta, data)
}

// NewWagerTransactionPendingReferenceEvent cria WagerTransactionPendingReference
// a partir de uma transação PENDING_REFERENCE. occurredAt = UpdatedAt (instante
// do registro da espera). Política da aplicação (T15/T21): emitir na transição
// PENDING -> PENDING_REFERENCE (attempts = 1); reagendamentos do worker não
// precisam emitir de novo. O domínio não impõe isso, e attempts vai no payload
// para que o consumidor distinga.
func NewWagerTransactionPendingReferenceEvent(tx *WagerTransaction, meta EventMetadata) (IntegrationEvent, error) {
	if err := checkTransaction(tx, WagerStatusPendingReference, EventTypeWagerTransactionPendingReference); err != nil {
		return IntegrationEvent{}, err
	}
	data := WagerTransactionPendingReferenceData{
		TransactionID:                  tx.ID(),
		WalletID:                       tx.WalletID(),
		PlayerID:                       tx.PlayerID(),
		Kind:                           tx.Kind(),
		Origin:                         tx.Origin(),
		Status:                         tx.Status(),
		Money:                          tx.Money(),
		ProviderID:                     tx.ProviderID(),
		ExternalTransactionID:          tx.ExternalTransactionID(),
		RoundID:                        tx.RoundID(),
		GameID:                         tx.GameID(),
		ReferenceExternalTransactionID: tx.ReferenceExternalTransactionID(),
		Attempts:                       tx.Attempts(),
		NextAttemptAt:                  eventTime(tx.NextAttemptAt()),
	}
	return newIntegrationEvent(tx.ID(), tx.UpdatedAt(), meta, data)
}

// NewWalletBalanceChangedEvent cria WalletBalanceChanged a partir do lançamento
// devolvido por Wallet.Debit/Credit (ou NewWallet, na abertura) e da MESMA
// instância de carteira que o produziu, antes de qualquer outro movimento:
// walletVersion = wallet.Version(). Exige que o lançamento seja o último
// produzido por essa instância (id guardado em memória pelo agregado). Isso
// rejeita carteira reidratada, desatualizada ou movimentada de novo, inclusive
// no caso A-B-A em que outro movimento leva o saldo de volta ao balanceAfter do
// lançamento e só a versão difere. occurredAt = CreatedAt do lançamento.
func NewWalletBalanceChangedEvent(entry WalletLedgerEntry, wallet *Wallet, meta EventMetadata) (IntegrationEvent, error) {
	const et = EventTypeWalletBalanceChanged
	if !entry.IsInitialized() {
		return IntegrationEvent{}, fmt.Errorf("%w: %s: uninitialized ledger entry", ErrInvalidIntegrationEvent, et)
	}
	if wallet == nil || !wallet.IsInitialized() {
		return IntegrationEvent{}, fmt.Errorf("%w: %s: %w", ErrInvalidIntegrationEvent, et, ErrUninitializedWallet)
	}
	if entry.WalletID() != wallet.ID() {
		return IntegrationEvent{}, fmt.Errorf("%w: %s: entry wallet %s differs from wallet %s", ErrInvalidIntegrationEvent, et, entry.WalletID(), wallet.ID())
	}
	if entry.BalanceAfter() != wallet.Balance() {
		return IntegrationEvent{}, fmt.Errorf("%w: %s: wallet balance %s differs from entry balanceAfter %s (wallet must be read right after the movement)",
			ErrInvalidIntegrationEvent, et, wallet.Balance(), entry.BalanceAfter())
	}
	if wallet.lastEntryID != entry.ID() {
		return IntegrationEvent{}, fmt.Errorf("%w: %s: entry %s is not the last movement of this wallet instance (build the event right after Debit/Credit/NewWallet)",
			ErrInvalidIntegrationEvent, et, entry.ID())
	}
	data := WalletBalanceChangedData{
		WalletID:      entry.WalletID(),
		TransactionID: entry.TransactionID(),
		Direction:     entry.Direction(),
		Money:         entry.Amount(),
		BalanceBefore: entry.BalanceBefore(),
		BalanceAfter:  entry.BalanceAfter(),
		WalletVersion: wallet.Version(),
	}
	return newIntegrationEvent(wallet.ID(), entry.CreatedAt(), meta, data)
}

// ID devolve o eventId (estável; MessageDeduplicationId no SQS).
func (e IntegrationEvent) ID() uuid.UUID { return e.id }

// Type devolve o eventType.
func (e IntegrationEvent) Type() EventType { return e.eventType }

// Version devolve a versão de schema do evento.
func (e IntegrationEvent) Version() int { return e.version }

// AggregateID devolve o aggregateId (MessageGroupId no SQS FIFO).
func (e IntegrationEvent) AggregateID() uuid.UUID { return e.aggregateID }

// CorrelationID devolve o correlationId.
func (e IntegrationEvent) CorrelationID() string { return e.correlationID }

// CausationID devolve o causationId ("" quando ausente).
func (e IntegrationEvent) CausationID() string { return e.causationID }

// OccurredAt devolve o instante do fato (UTC, precisão de microssegundos).
func (e IntegrationEvent) OccurredAt() time.Time { return e.occurredAt }

// Data devolve uma cópia do payload tipado; use type switch/assertion para o
// tipo concreto (ex.: WalletBalanceChangedData). nil no zero-value.
func (e IntegrationEvent) Data() EventData {
	if e.data == nil {
		return nil
	}
	return e.data.cloneEventData()
}

// Payload devolve uma cópia do snapshot JSON do envelope (o que a outbox
// persiste e o publisher envia). nil no zero-value.
func (e IntegrationEvent) Payload() []byte { return slices.Clone(e.payload) }

// MarshalJSON devolve o snapshot (cópia), de modo que serializar o evento
// nunca reconstrói o JSON a partir de estado mutável.
func (e IntegrationEvent) MarshalJSON() ([]byte, error) {
	if !e.IsInitialized() {
		return nil, fmt.Errorf("%w: uninitialized event", ErrInvalidIntegrationEvent)
	}
	return e.Payload(), nil
}

// IsInitialized indica se o evento foi criado por um construtor.
func (e IntegrationEvent) IsInitialized() bool { return e.id != uuid.Nil && len(e.payload) > 0 }
