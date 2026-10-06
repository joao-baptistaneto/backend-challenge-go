// Package auth autentica e autoriza as requisições HTTP com tokens OAuth 2.0
// emitidos pelo IdP externo (Keycloak), conforme ARCHITECTURE.md, ADR-019.
//
//   - Authenticator valida o JWT Bearer (assinatura via JWKS com rotação de
//     chaves, alg assimétrico permitido, iss, aud, exp/nbf/iat com tolerância
//     pequena, typ) usando go-oidc + go-jose, e extrai o Principal
//     (providerId de uma claim configurável e papéis de realm_access e
//     resource_access).
//   - Authenticator.Middleware exige token em todas as rotas, exceto as
//     declaradas públicas (health), e põe o Principal no contexto.
//   - Authorizer decide o acesso: operações de carteira só para o papel de
//     serviço interno; consultas e envios de transação só para o papel de
//     provedor, sempre com o providerId da identidade.
//   - WriteError traduz os erros deste pacote em 401/403/503 com corpo JSON
//     padronizado (internal/adapters/http/apierror) e WWW-Authenticate.
//
// Nenhum log ou resposta contém o token, o cabeçalho Authorization ou
// detalhes da validação além de uma categoria (reason).
package auth
