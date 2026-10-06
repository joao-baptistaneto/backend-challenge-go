package sqs

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/strictjson"
	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// Envelope de entrada (README §10):
//
//	{"messageId":"msg-123","type":"WagerTransactionRequested",
//	 "occurredAt":"2026-09-08T12:00:00.000Z","data":{...}}
//
// A decodificação usa internal/adapters/strictjson, com as mesmas regras do
// corpo HTTP (ADR-020): chaves com caixa exata, sem chaves desconhecidas ou
// repetidas, nenhum número JSON (Money só como strings) e nada depois do
// objeto. Qualquer violação é uma mensagem inválida (permanente): reenviar o
// mesmo corpo nunca a torna válida.

// MessageTypeWagerTransactionRequested é o único tipo aceito.
const MessageTypeWagerTransactionRequested = "WagerTransactionRequested"

// MaxBodyBytes limita o corpo aceito (mesmo limite do HTTP).
const MaxBodyBytes = 64 << 10

// Códigos de mensagem inválida (logs e métricas; o consumidor não responde a
// ninguém). Os de campo financeiro vêm do domínio (INVALID_AMOUNT,
// INVALID_CURRENCY, INVALID_KIND).
const (
	CodeInvalidMessage         = app.CodeInvalidMessage
	CodeUnsupportedMessageType = "UNSUPPORTED_MESSAGE_TYPE"
)

// ErrInvalidMessage casa (errors.Is) com todo *InvalidMessageError.
var ErrInvalidMessage = errors.New("sqs: invalid message")

// InvalidMessageError descreve uma mensagem que não pode ser processada.
// Field é o caminho do campo do contrato com problema ("" quando é o corpo
// todo ou uma chave desconhecida — chaves desconhecidas nunca são ecoadas).
// Nenhum valor recebido é incluído na mensagem de erro.
type InvalidMessageError struct {
	Code  string
	Field string
	Err   error
}

func (e *InvalidMessageError) Error() string {
	msg := "sqs: invalid message: " + e.Code
	if e.Field != "" {
		msg += " (" + e.Field + ")"
	}
	return msg
}

// Unwrap devolve a causa.
func (e *InvalidMessageError) Unwrap() error { return e.Err }

// Is casa com ErrInvalidMessage.
func (e *InvalidMessageError) Is(target error) bool { return target == ErrInvalidMessage }

// Envelope é a mensagem decodificada. Command traz os campos de negócio já
// parseados (Money e Kind estritos); Inbox, CorrelationID e CausationID são
// preenchidos pelo consumidor.
type Envelope struct {
	MessageID  string
	Type       string
	OccurredAt time.Time
	Command    app.ProcessWagerCommand
}

// DecodeEnvelope valida e decodifica o corpo de uma mensagem.
func DecodeEnvelope(body []byte) (Envelope, error) {
	if len(body) > MaxBodyBytes {
		return Envelope{}, &InvalidMessageError{Code: CodeInvalidMessage, Err: errors.New("body too large")}
	}
	if !utf8.Valid(body) {
		return Envelope{}, &InvalidMessageError{Code: CodeInvalidMessage, Err: errors.New("body is not valid UTF-8")}
	}
	var top struct{ messageID, typ, occurredAt, data json.RawMessage }
	if err := strictjson.DecodeObject(body, map[string]*json.RawMessage{
		"messageId":  &top.messageID,
		"type":       &top.typ,
		"occurredAt": &top.occurredAt,
		"data":       &top.data,
	}); err != nil {
		return Envelope{}, decodeError(err)
	}
	var env Envelope
	var err error
	if env.MessageID, err = identifier("messageId", top.messageID); err != nil {
		return Envelope{}, err
	}
	if env.Type, err = str("type", top.typ); err != nil {
		return Envelope{}, err
	}
	if env.Type != MessageTypeWagerTransactionRequested {
		return Envelope{}, &InvalidMessageError{Code: CodeUnsupportedMessageType, Field: "type",
			Err: errors.New("unsupported message type")}
	}
	occurred, err := str("occurredAt", top.occurredAt)
	if err != nil {
		return Envelope{}, err
	}
	t, perr := time.Parse(time.RFC3339Nano, occurred)
	if perr != nil {
		return Envelope{}, &InvalidMessageError{Code: CodeInvalidMessage, Field: "occurredAt", Err: errors.New("not an RFC 3339 timestamp")}
	}
	env.OccurredAt = t.UTC()
	if top.data == nil {
		return Envelope{}, &InvalidMessageError{Code: CodeInvalidMessage, Field: "data", Err: strictjson.ErrMissingField}
	}
	if env.Command, err = decodeData(top.data); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

// decodeData converte data no comando do caso de uso. Os identificadores de
// negócio (providerId, externalTransactionId, idempotencyKey, roundId,
// gameId) são validados pelo domínio no caso de uso, com os mesmos códigos
// do HTTP; aqui só se exige o tipo JSON correto.
func decodeData(raw json.RawMessage) (app.ProcessWagerCommand, error) {
	var f struct {
		providerID, externalID, key, playerID, walletID, roundID, gameID, kind, money, reference json.RawMessage
	}
	if err := strictjson.DecodeObject(raw, map[string]*json.RawMessage{
		"providerId":                     &f.providerID,
		"externalTransactionId":          &f.externalID,
		"idempotencyKey":                 &f.key,
		"playerId":                       &f.playerID,
		"walletId":                       &f.walletID,
		"roundId":                        &f.roundID,
		"gameId":                         &f.gameID,
		"kind":                           &f.kind,
		"money":                          &f.money,
		"referenceExternalTransactionId": &f.reference,
	}); err != nil {
		return app.ProcessWagerCommand{}, decodeError(prefixed(err))
	}
	var (
		cmd app.ProcessWagerCommand
		err error
	)
	if cmd.ProviderID, err = str("data.providerId", f.providerID); err != nil {
		return cmd, err
	}
	if cmd.ExternalTransactionID, err = str("data.externalTransactionId", f.externalID); err != nil {
		return cmd, err
	}
	// A chave é usada exatamente como recebida (nunca recalculada).
	if cmd.IdempotencyKey, err = identifier("data.idempotencyKey", f.key); err != nil {
		return cmd, err
	}
	if cmd.PlayerID, err = uuidField("data.playerId", f.playerID); err != nil {
		return cmd, err
	}
	if cmd.WalletID, err = uuidField("data.walletId", f.walletID); err != nil {
		return cmd, err
	}
	if cmd.RoundID, err = str("data.roundId", f.roundID); err != nil {
		return cmd, err
	}
	if cmd.GameID, err = str("data.gameId", f.gameID); err != nil {
		return cmd, err
	}
	kind, err := str("data.kind", f.kind)
	if err != nil {
		return cmd, err
	}
	k, kerr := domain.ParseExternalWagerKind(kind)
	if kerr != nil {
		// OPENING (reservado à abertura interna) e tipos desconhecidos.
		return cmd, &InvalidMessageError{Code: string(domain.FailureCodeInvalidKind), Field: "data.kind", Err: kerr}
	}
	cmd.Kind = k
	m, merr := strictjson.Money("data.money", f.money)
	if merr != nil {
		return cmd, decodeError(merr)
	}
	cmd.Money = m
	if f.reference != nil {
		ref, err := str("data.referenceExternalTransactionId", f.reference)
		if err != nil {
			return cmd, err
		}
		if ref == "" {
			return cmd, &InvalidMessageError{Code: CodeInvalidMessage, Field: "data.referenceExternalTransactionId",
				Err: errors.New("empty reference")}
		}
		cmd.ReferenceExternalTransactionID = ref
	}
	return cmd, nil
}

// prefixed acrescenta "data." ao campo de um erro do strictjson.
func prefixed(err error) error {
	var fe *strictjson.FieldError
	if errors.As(err, &fe) {
		field := "data"
		if fe.Field != "" {
			field += "." + fe.Field
		}
		return &strictjson.FieldError{Field: field, Err: fe.Err}
	}
	return err
}

// decodeError converte um erro do strictjson/domínio. Chave desconhecida não
// é ecoada (o nome vem do remetente).
func decodeError(err error) error {
	code := CodeInvalidMessage
	if fc, ok := domain.ValidationFailureCode(err); ok {
		code = string(fc)
	}
	field := strictjson.Field(err)
	if errors.Is(err, strictjson.ErrUnknownField) {
		field = ""
	}
	return &InvalidMessageError{Code: code, Field: field, Err: err}
}

func str(field string, raw json.RawMessage) (string, error) {
	s, err := strictjson.String(field, raw)
	if err != nil {
		return "", decodeError(err)
	}
	return s, nil
}

// identifier exige 1..255 bytes, UTF-8, sem caracteres de controle e sem
// espaço nas pontas (mesma regra da Idempotency-Key do HTTP).
func identifier(field string, raw json.RawMessage) (string, error) {
	s, err := str(field, raw)
	if err != nil {
		return "", err
	}
	if !validIdentifier(s) {
		return "", &InvalidMessageError{Code: CodeInvalidMessage, Field: field, Err: errors.New("invalid identifier")}
	}
	return s, nil
}

func validIdentifier(v string) bool {
	if v == "" || len(v) > domain.MaxWagerIdentifierLength || !utf8.ValidString(v) {
		return false
	}
	for _, r := range v {
		if unicode.IsControl(r) || r == utf8.RuneError {
			return false
		}
	}
	first, _ := utf8.DecodeRuneInString(v)
	last, _ := utf8.DecodeLastRuneInString(v)
	return !unicode.IsSpace(first) && !unicode.IsSpace(last)
}

// uuidField aceita só a forma canônica de 36 caracteres, não nula.
func uuidField(field string, raw json.RawMessage) (uuid.UUID, error) {
	s, err := str(field, raw)
	if err != nil {
		return uuid.Nil, err
	}
	if len(s) != 36 {
		return uuid.Nil, &InvalidMessageError{Code: CodeInvalidMessage, Field: field, Err: errors.New("not a canonical UUID")}
	}
	id, perr := uuid.Parse(s)
	if perr != nil || id == uuid.Nil {
		return uuid.Nil, &InvalidMessageError{Code: CodeInvalidMessage, Field: field, Err: fmt.Errorf("invalid UUID")}
	}
	return id, nil
}
