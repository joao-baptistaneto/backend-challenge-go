// Package migrations embute as migrations SQL versionadas do schema PostgreSQL.
//
// Convenção de nomes: NNNN_descricao.up.sql e NNNN_descricao.down.sql, com
// versões sequenciais a partir de 0001 e pares up/down obrigatórios. O runner
// (internal/adapters/postgres/migrate) valida a convenção ao carregar FS.
//
// Decisão (ADR-011): os arquivos ficam na raiz do módulo, fora de internal/, para
// que sejam fáceis de revisar e de aplicar também por ferramentas externas (psql,
// golang-migrate) se necessário; o binário usa a cópia embutida, então a versão do
// schema aplicada é sempre a que foi compilada junto com o código.
package migrations

import "embed"

// FS contém todos os arquivos *.sql deste diretório.
//
//go:embed *.sql
var FS embed.FS
