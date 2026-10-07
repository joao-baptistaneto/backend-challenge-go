//go:build !integration

package outboxpublisher

import (
	"testing"

	"go.uber.org/goleak"
)

// Nenhuma goroutine do publisher pode sobrar depois dos testes. Com a tag
// integration, o TestMain de main_integration_test.go faz a mesma
// verificação.
func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }
