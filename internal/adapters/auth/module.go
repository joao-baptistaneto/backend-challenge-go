package auth

import (
	"log/slog"

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

func newAuthenticatorFx(p componentParams) (*Authenticator, error) {
	return NewAuthenticator(p.Settings, p.Logger)
}

func newAuthorizerFx(p componentParams) *Authorizer {
	return NewAuthorizer(p.Settings, p.Logger)
}

// Module provê Settings, *Authenticator e *Authorizer. Depende de
// config.Config; *slog.Logger é opcional. A API HTTP (T18) monta
// authenticator.Middleware("/health/live", "/health/ready") na frente do
// roteador e usa o Authorizer por rota.
var Module = fx.Module("auth",
	fx.Provide(newSettingsFx, newAuthenticatorFx, newAuthorizerFx),
)
