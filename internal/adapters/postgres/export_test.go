package postgres

import "github.com/junglegaming/backend-challenge-go/internal/app"

// QuerierOf expõe, só para testes, o pgx.Tx por trás de um app.Tx entregue
// por UnitOfWork.WithinTx (prova de isolamento/modo de acesso da transação).
func QuerierOf(tx app.Tx) Querier { return tx.(txStores).q }
