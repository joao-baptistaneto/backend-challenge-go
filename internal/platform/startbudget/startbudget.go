// Package startbudget limita os OnStart que repetem com backoff (ping do
// pool, chamadas iniciais ao SQS) a um orçamento menor que o prazo do start
// do Fx, reservando o final desse prazo para o rollback (ADR-025).
//
// O Fx executa o rollback de um start que falhou (os OnStop dos hooks já
// iniciados) com o MESMO ctx do start, e não executa nenhum OnStop se esse
// ctx já venceu. Um OnStart que repetisse até o prazo do ctx deixaria pool,
// workers e clients rodando. Com a reserva, o hook desiste antes e o
// rollback ainda tem a reserva inteira.
package startbudget

import (
	"context"
	"time"
)

// Reserve é a parte final do prazo do start do Fx reservada para o rollback.
// Zero (ou negativa) não reserva nada.
type Reserve time.Duration

// Context devolve ctx com prazo = prazo(ctx) − r. Sem prazo em ctx ou com
// r <= 0, o ctx derivado tem o mesmo prazo do pai. Se a reserva consumir o
// prazo inteiro, o ctx devolvido já vem expirado. Chame sempre o cancel.
func Context(ctx context.Context, r Reserve) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok || r <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithDeadline(ctx, deadline.Add(-time.Duration(r)))
}
