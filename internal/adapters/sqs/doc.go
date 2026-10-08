// Package sqs implementa, com aws-sdk-go-v2/sqs, o consumidor da fila de
// entrada wager-transactions.fifo (ARCHITECTURE.md, ADR-022) e o cliente
// compartilhado com o publisher da outbox (ADR-024).
//
//   - DecodeEnvelope: envelope WagerTransactionRequested decodificado de forma
//     estrita (internal/adapters/strictjson), com as regras do HTTP.
//   - Consumer: long polling, inbox no mesmo commit do caso de uso, delete só
//     depois do commit, backoff por visibilidade, DLQ por envio explícito
//     (redrive nativo como rede de segurança), ordem por
//     MessageGroupId e shutdown com drenagem/liberação.
//   - Module/ClientModule/ConsumerModule: composição Fx.
package sqs
