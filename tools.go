//go:build tools

// Package tools fixa no go.mod as dependências base do projeto antes que
// sejam usadas pelo código de produção. Remova cada import quando a
// dependência passar a ser usada diretamente em algum pacote.
package tools

import (
	_ "github.com/aws/aws-sdk-go-v2/config"
	_ "github.com/aws/aws-sdk-go-v2/service/sqs"
	_ "github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/pgxpool"
)
