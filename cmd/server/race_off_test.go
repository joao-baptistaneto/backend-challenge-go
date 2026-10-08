//go:build !race

package main

// raceEnabled: o teste foi compilado com -race; os processos filhos dos
// testes multi-instância (ADR-028) também são.
const raceEnabled = false
