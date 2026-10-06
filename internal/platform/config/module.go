package config

import "go.uber.org/fx"

// Module provê Config carregada do ambiente do processo.
//
// A validação acontece no construtor, isto é, durante fx.New: uma configuração
// inválida impede a aplicação de iniciar (app.Err() != nil / Run encerra com
// erro) antes de qualquer conexão ser aberta. Testes que precisem de outra
// configuração podem usar fx.Replace(cfg) ou fx.Decorate.
var Module = fx.Module("config",
	fx.Provide(LoadFromEnv),
)
