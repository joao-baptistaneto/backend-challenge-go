package auth

import (
	"context"
	"log/slog"
	"net/http"

	"go.uber.org/fx"

	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
)

// newSettingsFx converte e revalida a configuração: inválida → fx.New falha.
func newSettingsFx(c config.Config) (Settings, error) {
	s := SettingsFromConfig(c.OIDC)
	if err := s.Validate(); err != nil {
		return Settings{}, err
	}
	return s, nil
}

type componentParams struct {
	fx.In

	Settings Settings
	Logger   *slog.Logger `optional:"true"`
}

type authenticatorParams struct {
	fx.In

	Lifecycle fx.Lifecycle
	Settings  Settings
	Logger    *slog.Logger `optional:"true"`
}

// newAuthenticatorFx cria o Authenticator com um transporte HTTP próprio
// (JWKS/discovery) e fecha as conexões ociosas dele no OnStop (ADR-025,
// ADR-025): sem isso, a conexão keep-alive com o IdP sobreviveria ao Stop
// (goroutines de leitura/escrita do net/http). Como o Authenticator é
// construído antes do servidor HTTP que o usa, este OnStop roda depois do
// Stop do servidor.
func newAuthenticatorFx(p authenticatorParams) (*Authenticator, error) {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	a, err := NewAuthenticator(p.Settings, p.Logger,
		WithHTTPClient(&http.Client{Timeout: defaultFetchTimeout, Transport: tr}))
	if err != nil {
		return nil, err
	}
	p.Lifecycle.Append(fx.Hook{OnStop: func(context.Context) error {
		tr.CloseIdleConnections()
		return nil
	}})
	return a, nil
}

func newAuthorizerFx(p componentParams) *Authorizer {
	return NewAuthorizer(p.Settings, p.Logger)
}

// Module provê Settings, *Authenticator e *Authorizer. Depende de
// config.Config; *slog.Logger é opcional. A API HTTP (ADR-020) monta
// authenticator.Middleware("/health/live", "/health/ready") na frente do
// roteador e usa o Authorizer por rota.
var Module = fx.Module("auth",
	fx.Provide(newSettingsFx, newAuthenticatorFx, newAuthorizerFx),
)
