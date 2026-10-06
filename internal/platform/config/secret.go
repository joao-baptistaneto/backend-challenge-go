package config

import (
	"fmt"
	"io"
	"log/slog"
)

// redacted é o texto exibido no lugar de qualquer segredo.
const redacted = "[REDACTED]"

// Secret guarda um valor sensível (senha, DSN com credenciais, chave secreta).
//
// Decisão: em vez de depender de disciplina em cada chamada de log, o próprio
// tipo se redige em todas as vias usuais de vazamento: fmt (todo verbo válido,
// via fmt.Formatter: %v, %s, %q, %+v, %#v, %d, %x, %X, largura e
// flags), slog (LogValuer), encoding/json e encoding (MarshalText). O valor real
// só é obtido explicitamente via Reveal, o que torna os pontos de uso
// auditáveis com um simples grep.
type Secret string

// Reveal devolve o valor real. Use apenas para entregar o segredo à biblioteca
// que precisa dele (pgxpool, SDK AWS); nunca para logar.
func (s Secret) Reveal() string { return string(s) }

// IsEmpty informa se o segredo não foi configurado.
func (s Secret) IsEmpty() bool { return s == "" }

// String implementa fmt.Stringer.
func (s Secret) String() string { return s.mask() }

// GoString implementa fmt.GoStringer (%#v).
func (s Secret) GoString() string { return s.mask() }

// LogValue implementa slog.LogValuer.
func (s Secret) LogValue() slog.Value { return slog.StringValue(s.mask()) }

// MarshalText implementa encoding.TextMarshaler (cobre JSON também).
func (s Secret) MarshalText() ([]byte, error) { return []byte(s.mask()), nil }

// Format implementa fmt.Formatter. Sem ele, verbos que não se aplicam a
// strings (%d, %t, %e...) caem no caminho de erro do fmt, que imprime o valor
// bruto ("%!d(config.Secret=valor)") sem consultar String/GoString; isso vale
// também para Secret aninhado em structs (Config, *Config). Todo verbo
// formata a máscara: %q a cita, %#v a imprime crua (como GoString) e os demais
// a imprimem como %s, preservando flags e largura.
//
// Limite: %p e %w (fora de Errorf, ou com argumento que não é error) são
// tratados pelo fmt como verbo inválido antes de consultar qualquer método e
// imprimem o conteúdo bruto; não há como interceptá-los enquanto Secret for um
// tipo string. Só o %w é acusado pelo go vet (analisador printf). O %p aplicado
// direto a um Secret NÃO é acusado: como Secret implementa fmt.Formatter, o vet
// aceita qualquer verbo para ele. Evite %p com Secret (ou com structs que o
// contenham); isso fica a cargo da revisão de código.
func (s Secret) Format(f fmt.State, verb rune) {
	m := s.mask()
	switch {
	case verb == 'q':
		fmt.Fprintf(f, fmt.FormatString(f, 'q'), m)
	case verb == 'v' && f.Flag('#'):
		_, _ = io.WriteString(f, m)
	default:
		fmt.Fprintf(f, fmt.FormatString(f, 's'), m)
	}
}

func (s Secret) mask() string {
	if s == "" {
		return ""
	}
	return redacted
}
