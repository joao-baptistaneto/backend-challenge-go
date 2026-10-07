//go:build !race

package main

// raceEnabled: o teste foi compilado com -race; os processos filhos dos
// testes multi-instância (T26) também são.
const raceEnabled = false
