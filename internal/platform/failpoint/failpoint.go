// Package failpoint injeta falhas em pontos nomeados do código de produção,
// para os testes de recuperação com processos reais (ARCHITECTURE.md,
// ADR-028, T26).
//
// No build normal Hit é uma função vazia (inlined, custo zero) e nenhuma
// variável de ambiente é lida: o binário de produção não tem como ativar
// falhas. Só um binário compilado com a build tag `failpoint` lê
// WALLET_FAILPOINTS (ex.: "sqs-consumer-after-commit=crash") e, ao passar
// pelo ponto, executa a ação:
//
//   - crash: escreve "FAILPOINT-HIT <nome>" no stderr e encerra o PRÓPRIO
//     processo de forma abrupta (os.Process.Kill: SIGKILL no Linux,
//     TerminateProcess no Windows) — sem OnStop do Fx, sem defers, sem
//     DeleteMessage/MarkPublished pendentes; é o "kill -9" no ponto exato;
//   - hang: escreve o marcador e bloqueia a goroutine para sempre (o teste
//     mata o processo de fora quando vê o marcador).
//
// Pontos instrumentados (constantes abaixo): depois do commit e antes do
// DeleteMessage no consumidor SQS; depois do claim e depois do envio (antes
// da marcação) no publisher da outbox; depois do claim no worker de
// pendências.
package failpoint

import (
	"fmt"
	"strings"
)

// EnvVar é a variável lida pelos binários compilados com a tag failpoint.
const EnvVar = "WALLET_FAILPOINTS"

// Marker prefixa a linha escrita no stderr quando um ponto dispara.
const Marker = "FAILPOINT-HIT"

// Pontos de falha instrumentados.
const (
	// SQSConsumerAfterCommit: tratamento durável confirmado (inbox
	// COMPLETED no mesmo commit), DeleteMessage ainda não chamado.
	SQSConsumerAfterCommit = "sqs-consumer-after-commit"
	// OutboxAfterClaim: lote da outbox reivindicado (lease gravado), nada
	// enviado ao SQS.
	OutboxAfterClaim = "outbox-after-claim"
	// OutboxAfterSend: evento aceito pelo SQS, MarkPublished ainda não
	// chamado.
	OutboxAfterSend = "outbox-after-send"
	// PendingAfterClaim: lote de pendências reivindicado (lease gravado),
	// nenhuma retomada iniciada.
	PendingAfterClaim = "pending-after-claim"
)

// Action é o que acontece quando um ponto dispara.
type Action string

const (
	ActionCrash Action = "crash"
	ActionHang  Action = "hang"
)

// Parse lê a especificação "nome=ação[,nome=ação...]" (espaços ignorados).
func Parse(spec string) (map[string]Action, error) {
	out := map[string]Action{}
	for _, item := range strings.Split(spec, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		name, action, ok := strings.Cut(item, "=")
		name, action = strings.TrimSpace(name), strings.TrimSpace(action)
		if !ok || name == "" {
			return nil, fmt.Errorf("failpoint: item %q inválido (esperado nome=ação)", item)
		}
		switch Action(action) {
		case ActionCrash, ActionHang:
		default:
			return nil, fmt.Errorf("failpoint: ação %q inválida em %q (crash|hang)", action, name)
		}
		out[name] = Action(action)
	}
	return out, nil
}
