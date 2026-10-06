package sqs

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
)

// API é o subconjunto do cliente SQS usado pelo consumidor. *awssqs.Client
// o satisfaz; os testes unitários usam uma fila FIFO em memória.
type API interface {
	ReceiveMessage(ctx context.Context, in *awssqs.ReceiveMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *awssqs.DeleteMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *awssqs.ChangeMessageVisibilityInput, optFns ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error)
	GetQueueAttributes(ctx context.Context, in *awssqs.GetQueueAttributesInput, optFns ...func(*awssqs.Options)) (*awssqs.GetQueueAttributesOutput, error)
	// GetQueueUrl e SendMessage: resolução da DLQ (pela RedrivePolicy) e
	// envio explícito das mensagens inválidas/permanentes/esgotadas a ela.
	GetQueueUrl(ctx context.Context, in *awssqs.GetQueueUrlInput, optFns ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error)
	SendMessage(ctx context.Context, in *awssqs.SendMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error)
}

var _ API = (*awssqs.Client)(nil)

// Client é o cliente SQS da aplicação com um transporte HTTP próprio, para
// que Close feche as conexões ociosas no shutdown (nenhuma goroutine de
// conexão sobra depois do Stop).
type Client struct {
	sqs       *awssqs.Client
	transport *http.Transport
}

// NewClient cria o cliente a partir da configuração AWS validada. Com
// credenciais estáticas, nada é buscado na rede; sem elas vale a cadeia
// padrão do SDK. EndpointURL (LocalStack) vira o BaseEndpoint do SQS.
func NewClient(ctx context.Context, cfg config.AWSConfig) (*Client, error) {
	if cfg.Region == "" {
		return nil, errors.New("sqs: região AWS obrigatória")
	}
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
		// Sem Timeout no http.Client: o long polling espera até 20s e o
		// prazo de cada chamada vem do context.
		awsconfig.WithHTTPClient(&http.Client{Transport: tr}),
	}
	if cfg.HasStaticCredentials() {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey.Reveal(), "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		tr.CloseIdleConnections()
		return nil, fmt.Errorf("sqs: carregar configuração AWS: %w", err)
	}
	client := awssqs.NewFromConfig(awsCfg, func(o *awssqs.Options) {
		if cfg.EndpointURL != "" {
			o.BaseEndpoint = aws.String(cfg.EndpointURL)
		}
	})
	return &Client{sqs: client, transport: tr}, nil
}

// SQS devolve o cliente do SDK.
func (c *Client) SQS() *awssqs.Client { return c.sqs }

// Close fecha as conexões ociosas. Chamado no OnStop, depois dos
// componentes que usam o cliente.
func (c *Client) Close(context.Context) error {
	c.transport.CloseIdleConnections()
	return nil
}
