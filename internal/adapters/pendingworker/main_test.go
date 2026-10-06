//go:build !integration

package pendingworker

import (
	"testing"

	"go.uber.org/goleak"
)

// Nenhuma goroutine do worker pode sobrar depois dos testes. Com a tag
// integration, o TestMain de main_integration_test.go faz a mesma
// verificação.
func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }
