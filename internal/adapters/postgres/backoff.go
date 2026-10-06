package postgres

import (
	"math"
	"time"
)

// exponentialDelay devolve base·2^(attempt-1), limitado por maxDelay quando
// maxDelay > 0, sem overflow (ARCHITECTURE.md, ADR-015).
//
// Usado pelo retry do TxManager (com jitter por cima) e pelo backoff da outbox
// (determinístico). Regras:
//
//   - base <= 0 ⇒ 0 (sem espera); attempt < 1 é tratado como 1.
//   - O laço para de dobrar assim que atinge o teto ou quando dobrar
//     ultrapassaria math.MaxInt64; nesse caso satura em math.MaxInt64 (≈292
//     anos). Por isso roda no máximo ~63 iterações, mesmo para attempt enorme,
//     e nunca devolve valor negativo.
func exponentialDelay(base, maxDelay time.Duration, attempt int) time.Duration {
	if base <= 0 {
		return 0
	}
	d := base
	for i := 1; i < attempt; i++ {
		if maxDelay > 0 && d >= maxDelay {
			break
		}
		if d > math.MaxInt64/2 {
			d = math.MaxInt64
			break
		}
		d *= 2
	}
	if maxDelay > 0 && d > maxDelay {
		d = maxDelay
	}
	return d
}
