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

// contextHandler injeta os IDs do contexto em cada registro e garante que
// nenhum objeto JSON emitido tenha chaves duplicadas.
//
// Os IDs são adicionados no nível raiz do JSON mesmo quando o logger foi
// derivado com WithGroup, porque são chaves de correlação que ferramentas de
// busca esperam sempre no mesmo caminho. Para isso o handler guarda as
// operações WithAttrs/WithGroup e só as reaplica em Handle, depois dos IDs.
// Quando não há nada a injetar nem a descartar, usa o handler derivado
// pré-computado, sem custo extra por registro.
//
// Chaves duplicadas: o slog.JSONHandler não deduplica, e JSON com chave
// repetida é ambíguo para os consumidores de log (cada parser escolhe uma
// ocorrência). A regra adotada, por nível (raiz ou grupo aberto com
// WithGroup), é "o primeiro atributo explícito vence":
//
//   - atributos explícitos (With e do próprio registro) prevalecem sobre os
//     IDs do contexto: o ID do contexto só é injetado se a chave ainda não
//     existir na raiz. É o caso comum — um caso de uso loga correlationId/
//     providerId explicitamente e o middleware HTTP também os pôs no contexto;
//     o valor é o mesmo, então nada se perde;
//   - entre explícitos, vale a primeira ocorrência na ordem de emissão
//     (With antes do registro; no registro, a ordem dos atributos). Repetições
//     posteriores da mesma chave no mesmo nível são descartadas.
//
// A deduplicação olha as chaves de cada nível; atributos sem chave que
// carregam um grupo (inlined pelo slog) contam com as chaves internas. Grupos
// passados como valor (slog.Group) com chave repetida também são descartados
// por inteiro; o conteúdo interno de um grupo-valor não é reexaminado.
type contextHandler struct {
	root    slog.Handler // handler raiz (sem grupos/atributos derivados)
	derived slog.Handler // root com ops já aplicadas
	ops     []handlerOp
	// rootKeys: chaves já emitidas na raiz via With (antes de qualquer grupo).
	rootKeys keySet
	// levelKeys: chaves já emitidas via With no nível corrente (o grupo mais
	// interno aberto, ou a raiz quando não há grupo — então é igual a rootKeys).
	levelKeys keySet
	grouped   bool // há ao menos um WithGroup aplicado
}

type handlerOp struct {
	group string      // não vazio → WithGroup
	attrs []slog.Attr // senão → WithAttrs (já deduplicados)
}

// keySet é imutável após construído (cópia na derivação).
type keySet map[string]struct{}

func (s keySet) has(k string) bool { _, ok := s[k]; return ok }

func (s keySet) clone(extra int) keySet {
	out := make(keySet, len(s)+extra)
	for k := range s {
		out[k] = struct{}{}
	}
	return out
}

// dedupAttrs devolve attrs sem as chaves já presentes em seen nem repetições
// internas, e registra as chaves aceitas em seen. Atributos vazios (que o
// slog descarta) passam intactos; grupos sem chave são examinados pelos
// filhos, pois o slog os emite inline no nível corrente.
func dedupAttrs(attrs []slog.Attr, seen keySet) (out []slog.Attr, changed bool) {
	out = make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		if a.Equal(slog.Attr{}) {
			out = append(out, a)
			continue
		}
		if a.Key == "" && a.Value.Kind() == slog.KindGroup {
			inner, ch := dedupAttrs(a.Value.Group(), seen)
			if ch {
				changed = true
				a = slog.Attr{Value: slog.GroupValue(inner...)}
			}
			out = append(out, a)
			continue
		}
		if seen.has(a.Key) {
			changed = true
			continue
		}
		seen[a.Key] = struct{}{}
		out = append(out, a)
	}
	return out, changed
}

func (h *contextHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.root.Enabled(ctx, l)
}

func (h *contextHandler) Handle(ctx context.Context, r slog.Record) error {
	// 1. Atributos do registro: descarta chaves já emitidas via With no nível
	//    corrente e repetições dentro do próprio registro.
	if r.NumAttrs() > 0 {
		attrs := make([]slog.Attr, 0, r.NumAttrs())
		r.Attrs(func(a slog.Attr) bool { attrs = append(attrs, a); return true })
		seen := h.levelKeys.clone(len(attrs))
		filtered, changed := dedupAttrs(attrs, seen)
		if changed {
			nr := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
			nr.AddAttrs(filtered...)
			r = nr
		}
		if !h.grouped {
			// Sem grupo, os atributos do registro também ocupam a raiz.
			h = &contextHandler{root: h.root, derived: h.derived, ops: h.ops,
				rootKeys: seen, levelKeys: seen}
		}
	}

	// 2. IDs do contexto: só os que ainda não existem na raiz.
	var ids []slog.Attr
	for _, a := range IDsFromContext(ctx).attrs() {
		if !h.rootKeys.has(a.Key) {
			ids = append(ids, a)
		}
	}
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
	level := h.levelKeys.clone(len(attrs))
	filtered, _ := dedupAttrs(attrs, level)
	if len(filtered) == 0 {
		return h
	}
	derived := h.derived.WithAttrs(filtered)
	c := h.derive(handlerOp{attrs: filtered}, derived)
	c.levelKeys = level
	if !h.grouped {
		c.rootKeys = level
	}
	return c
}

func (h *contextHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	c := h.derive(handlerOp{group: name}, h.derived.WithGroup(name))
	if !h.grouped {
		// O grupo ocupa a chave name na raiz: um ID de contexto homônimo não
		// é injetado.
		c.rootKeys = h.rootKeys.clone(1)
		c.rootKeys[name] = struct{}{}
	}
	c.grouped = true
	c.levelKeys = keySet{}
	return c
}

func (h *contextHandler) derive(op handlerOp, derived slog.Handler) *contextHandler {
	ops := make([]handlerOp, len(h.ops), len(h.ops)+1)
	copy(ops, h.ops)
	return &contextHandler{
		root: h.root, derived: derived, ops: append(ops, op),
		rootKeys: h.rootKeys, levelKeys: h.levelKeys, grouped: h.grouped,
	}
}
