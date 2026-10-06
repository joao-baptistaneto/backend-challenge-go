package metrics

import (
	"net/http"
	"strconv"
	"sync"
)

// Normalização de rótulos: garante cardinalidade limitada independentemente
// do que o chamador passar. Cada rótulo tem um conjunto fechado; valores fora
// dele viram LabelValueOther (ou MethodOther para método HTTP).
//
// Os conjuntos de tipo/status de transação e de moeda espelham os do domínio
// (internal/domain), mas são locais de propósito: platform não importa domain
// (dependência apontaria para dentro do núcleo a partir da infraestrutura e
// acoplaria os pacotes). Um valor novo no domínio que não for acrescentado
// aqui aparece como "other", sem quebrar nem explodir a cardinalidade.

// LabelValueOther substitui valores desconhecidos de rótulo.
const LabelValueOther = "other"

// MethodOther substitui métodos HTTP fora do conjunto padrão.
const MethodOther = "OTHER"

// maxAllowListSize limita AllowRoutes/AllowQueues: protege contra uso
// incorreto (ex.: registrar r.URL.Path a cada requisição).
const maxAllowListSize = 256

type labelSet map[string]struct{}

func newLabelSet(values ...string) labelSet {
	s := make(labelSet, len(values))
	for _, v := range values {
		s[v] = struct{}{}
	}
	return s
}

var (
	sources = newLabelSet(SourceHTTP, SourceSQS, SourceWorker)

	transactionTypes = newLabelSet("OPENING", "BET", "WIN", "LOSS", "REFUND", "ROLLBACK")

	transactionStatuses = newLabelSet("PENDING", "PENDING_REFERENCE", "PROCESSED", "REJECTED", "FAILED")

	duplicateKinds = newLabelSet(DuplicateReplay, DuplicateInbox, DuplicateConflict)

	conflictKinds = newLabelSet(
		ConflictVersionMismatch, ConflictSerialization, ConflictDeadlock,
		ConflictLockTimeout, ConflictUniqueViolation,
	)

	operations = newLabelSet(
		OperationProcessTransaction, OperationOpenWallet, OperationReconcile,
		OperationOutboxPublish, OperationSQSConsume, OperationPendingReference, OperationQuery,
	)

	outcomes = newLabelSet(OutcomeSuccess, OutcomeRejected, OutcomeConflict, OutcomeTransient, OutcomeError)

	dlqReasons = newLabelSet(DLQReasonInvalidMessage, DLQReasonPermanentError, DLQReasonMaxReceives)

	// currencies: mesma lista fechada do domínio (ISO 4217).
	currencies = newLabelSet("BRL", "USD", "EUR", "GBP", "CAD", "AUD", "CHF", "MXN", "ARS", "COP", "PEN", "UYU")

	httpMethods = newLabelSet(
		http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace,
	)
)

// closedLabel devolve v se pertence a set, senão LabelValueOther.
func closedLabel(set labelSet, v string) string {
	if _, ok := set[v]; ok {
		return v
	}
	return LabelValueOther
}

func normSource(v string) string { return closedLabel(sources, v) }

// normMethod aceita só os métodos padrão. Métodos HTTP diferenciam
// maiúsculas (RFC 9110), então "get" é tratado como desconhecido.
func normMethod(v string) string {
	if _, ok := httpMethods[v]; ok {
		return v
	}
	return MethodOther
}

// httpCode aceita só códigos HTTP válidos (100-599): no máximo 500 valores.
func httpCode(code int) string {
	if code < 100 || code > 599 {
		return "unknown"
	}
	return strconv.Itoa(code)
}

// allowList é um conjunto fechado preenchido em runtime (rotas do roteador,
// filas da configuração), com tamanho máximo.
type allowList struct {
	mu     sync.RWMutex
	values labelSet
}

func (a *allowList) add(values ...string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.values == nil {
		a.values = make(labelSet)
	}
	for _, v := range values {
		if v == "" || v == LabelValueOther || len(a.values) >= maxAllowListSize {
			continue
		}
		a.values[v] = struct{}{}
	}
}

func (a *allowList) label(v string) string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if _, ok := a.values[v]; ok {
		return v
	}
	return LabelValueOther
}
