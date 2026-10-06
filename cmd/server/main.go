// Command server é o ponto de entrada da aplicação: compõe os módulos Fx
// (configuração, conexões, repositórios, casos de uso, handlers e workers).
package main

import (
	"go.uber.org/fx"
)

// newApp monta o grafo de dependências da aplicação. Os módulos são
// adicionados progressivamente pelas próximas tarefas.
func newApp(opts ...fx.Option) *fx.App {
	return fx.New(append(appOptions(), opts...)...)
}

// appOptions retorna as opções Fx que compõem a aplicação.
func appOptions() []fx.Option {
	return []fx.Option{}
}

func main() {
	newApp().Run()
}
