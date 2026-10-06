package logging

import (
	"context"
	"io"
	"log/slog"
)

// Decisões (ver ARCHITECTURE.md, ADR "Config, logging e métricas"):
//
//   - log/slog da stdlib com JSONHandler: sem dependência extra, formato JSON
//     estável, uma linha por evento.
//   - Os identificadores de rastreio (correlationId, messageId, transactionId,
//     walletId, providerId) usam exatamente esses nomes de chave (camelCase, iguais
//     ao contrato HTTP/SQS), centralizados em constantes. Podem ser anexados de
//     duas formas: atributos explícitos (CorrelationID(id) etc.) ou propagados pelo
//     context.Context (WithCorrelationID(ctx, id) etc.), que o handler injeta em
//     todo log emitido com *Context(ctx, ...). Assim um middleware/consumidor
//     define os IDs uma vez e todas as camadas abaixo os herdam.
//   - Defesa em profundidade contra vazamento: além de config.Secret se redigir
//     sozinho, o handler redige chaves sensíveis em qualquer profundidade
//     (atributos, grupos, chaves de map, campos de struct, query de URLs),
//     percorre valores KindAny (map, slice, struct, http.Header, error,
//     url.URL, ...) e limpa o texto de strings, mensagens e erros (Bearer/
//     Basic, userinfo de URL, pares chave=valor sensíveis). Detalhes e limites
//     em redact.go. Payloads financeiros completos não devem ser logados
//     (README §12): chaves como "payload"/"body" são redigidas por padrão.
//   - Nenhum logger global é alterado (slog.SetDefault não é chamado aqui): o
//     *slog.Logger é injetado via Fx, o que mantém testes paralelos isolados.

// Chaves canônicas dos identificadores de rastreio.
const (
	KeyCorrelationID = "correlationId"
	KeyMessageID     = "messageId"
	KeyTransactionID = "transactionId"
	KeyWalletID      = "walletId"
	KeyProviderID    = "providerId"
)

// RedactedValue substitui valores sensíveis nos logs.
const RedactedValue = "[REDACTED]"

// Options configura New.
type Options struct {
	// Level é o nível mínimo; nil equivale a slog.LevelInfo. Aceita
	// *slog.LevelVar para ajuste em runtime.
	Level slog.Leveler
	// AddSource inclui arquivo:linha em cada registro.
	AddSource bool
	// ExtraSensitiveKeys amplia a lista padrão de chaves redigidas.
	ExtraSensitiveKeys []string
}

// New cria um logger JSON que escreve em w, com redação de campos sensíveis e
// injeção dos IDs de rastreio presentes no contexto.
func New(w io.Writer, opts Options) *slog.Logger {
	level := opts.Level
	if level == nil {
		level = slog.LevelInfo
	}
	r := newRedactor(opts.ExtraSensitiveKeys)
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level:       level,
		AddSource:   opts.AddSource,
		ReplaceAttr: r.replaceAttr,
	})
	return slog.New(&contextHandler{root: h, derived: h})
}

// --- Atributos explícitos -------------------------------------------------

// CorrelationID devolve o atributo correlationId.
func CorrelationID(id string) slog.Attr { return slog.String(KeyCorrelationID, id) }

// MessageID devolve o atributo messageId.
func MessageID(id string) slog.Attr { return slog.String(KeyMessageID, id) }

// TransactionID devolve o atributo transactionId.
func TransactionID(id string) slog.Attr { return slog.String(KeyTransactionID, id) }

// WalletID devolve o atributo walletId.
func WalletID(id string) slog.Attr { return slog.String(KeyWalletID, id) }

// ProviderID devolve o atributo providerId.
func ProviderID(id string) slog.Attr { return slog.String(KeyProviderID, id) }

// Err devolve o atributo "error" com a mensagem do erro (nil → atributo vazio,
// descartado pelo handler).
func Err(err error) slog.Attr {
	if err == nil {
		return slog.Attr{}
	}
	return slog.String("error", err.Error())
}

// --- Propagação via context ----------------------------------------------

// IDs agrupa os identificadores de rastreio propagados pelo contexto. Campos
// vazios não são emitidos.
type IDs struct {
	CorrelationID string
	MessageID     string
	TransactionID string
	WalletID      string
	ProviderID    string
}

type ctxKey struct{}

// IDsFromContext devolve os IDs presentes no contexto (zero-value se nenhum).
func IDsFromContext(ctx context.Context) IDs {
	if ctx == nil {
		return IDs{}
	}
	ids, _ := ctx.Value(ctxKey{}).(IDs)
	return ids
}

// WithIDs mescla ids no contexto: campos não vazios de ids sobrescrevem os
// existentes; campos vazios preservam o valor anterior.
func WithIDs(ctx context.Context, ids IDs) context.Context {
	cur := IDsFromContext(ctx)
	merge := func(dst *string, src string) {
		if src != "" {
			*dst = src
		}
	}
	merge(&cur.CorrelationID, ids.CorrelationID)
	merge(&cur.MessageID, ids.MessageID)
	merge(&cur.TransactionID, ids.TransactionID)
	merge(&cur.WalletID, ids.WalletID)
	merge(&cur.ProviderID, ids.ProviderID)
	return context.WithValue(ctx, ctxKey{}, cur)
}

// WithCorrelationID anexa correlationId ao contexto.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return WithIDs(ctx, IDs{CorrelationID: id})
}

// WithMessageID anexa messageId ao contexto.
func WithMessageID(ctx context.Context, id string) context.Context {
	return WithIDs(ctx, IDs{MessageID: id})
}

// WithTransactionID anexa transactionId ao contexto.
func WithTransactionID(ctx context.Context, id string) context.Context {
	return WithIDs(ctx, IDs{TransactionID: id})
}

// WithWalletID anexa walletId ao contexto.
func WithWalletID(ctx context.Context, id string) context.Context {
	return WithIDs(ctx, IDs{WalletID: id})
}

// WithProviderID anexa providerId ao contexto.
func WithProviderID(ctx context.Context, id string) context.Context {
	return WithIDs(ctx, IDs{ProviderID: id})
}

// attrs converte os IDs não vazios em atributos, em ordem fixa.
func (ids IDs) attrs() []slog.Attr {
	out := make([]slog.Attr, 0, 5)
	add := func(k, v string) {
		if v != "" {
			out = append(out, slog.String(k, v))
		}
	}
	add(KeyCorrelationID, ids.CorrelationID)
	add(KeyMessageID, ids.MessageID)
	add(KeyTransactionID, ids.TransactionID)
	add(KeyWalletID, ids.WalletID)
	add(KeyProviderID, ids.ProviderID)
	return out
}

// contextHandler injeta os IDs do contexto em cada registro.
//
// Os IDs são adicionados no nível raiz do JSON mesmo quando o logger foi
// derivado com WithGroup, porque são chaves de correlação que ferramentas de
// busca esperam sempre no mesmo caminho. Para isso o handler guarda as
// operações WithAttrs/WithGroup e só as reaplica em Handle, depois dos IDs.
// Quando o contexto não traz IDs, usa o handler derivado pré-computado, sem
// custo extra por registro.
type contextHandler struct {
	root    slog.Handler // handler raiz (sem grupos/atributos derivados)
	derived slog.Handler // root com ops já aplicadas
	ops     []handlerOp
}

type handlerOp struct {
	group string      // não vazio → WithGroup
	attrs []slog.Attr // senão → WithAttrs
}

func (h *contextHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.root.Enabled(ctx, l)
}

func (h *contextHandler) Handle(ctx context.Context, r slog.Record) error {
	ids := IDsFromContext(ctx).attrs()
	if len(ids) == 0 {
		return h.derived.Handle(ctx, r)
	}
	next := h.root.WithAttrs(ids)
	for _, op := range h.ops {
		if op.group != "" {
			next = next.WithGroup(op.group)
		} else {
			next = next.WithAttrs(op.attrs)
		}
	}
	return next.Handle(ctx, r)
}

func (h *contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	return h.with(handlerOp{attrs: attrs})
}

func (h *contextHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return h.with(handlerOp{group: name})
}

func (h *contextHandler) with(op handlerOp) *contextHandler {
	ops := make([]handlerOp, len(h.ops), len(h.ops)+1)
	copy(ops, h.ops)
	derived := h.derived
	if op.group != "" {
		derived = derived.WithGroup(op.group)
	} else {
		derived = derived.WithAttrs(op.attrs)
	}
	return &contextHandler{root: h.root, derived: derived, ops: append(ops, op)}
}
