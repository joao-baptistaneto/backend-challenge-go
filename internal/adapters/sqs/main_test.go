//go:build !integration

package sqs

import (
	"testing"

	"go.uber.org/goleak"
)

// Sem a tag integration, nenhuma goroutine pode sobrar depois dos testes
// (consumidor, fila falsa, cliente). Com a tag, o TestMain de
// consumer_integration_test.go faz a mesma verificação ao fim de todos os
// testes do pacote (unitários e de integração; runWithLeakCheck), e
// TestIntegrationConsumerFxLifecycleNoLeak ainda verifica, dentro do teste,
// a composição Fx real com goleak.VerifyNone.
func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }
