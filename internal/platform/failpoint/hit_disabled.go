//go:build !failpoint

package failpoint

// Enabled informa se o binário foi compilado com a tag failpoint.
const Enabled = false

// Hit não faz nada no build normal.
func Hit(string) {}
