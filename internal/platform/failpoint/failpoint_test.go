package failpoint

import (
	"maps"
	"testing"
)

func TestParse(t *testing.T) {
	got, err := Parse(" sqs-consumer-after-commit=crash , outbox-after-send=hang,,")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]Action{SQSConsumerAfterCommit: ActionCrash, OutboxAfterSend: ActionHang}
	if !maps.Equal(got, want) {
		t.Fatalf("Parse = %v, quer %v", got, want)
	}
	if got, err := Parse(""); err != nil || len(got) != 0 {
		t.Fatalf("Parse vazio = %v, %v", got, err)
	}
	for _, bad := range []string{"x", "=crash", "x=explode", "x=crash,y"} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("Parse(%q) deveria falhar", bad)
		}
	}
}

// No build normal (sem a tag failpoint) Hit não lê o ambiente e não faz
// nada, mesmo com a variável definida.
func TestHitDisabledIsNoop(t *testing.T) {
	if Enabled {
		t.Skip("binário compilado com a tag failpoint")
	}
	t.Setenv(EnvVar, SQSConsumerAfterCommit+"=crash")
	Hit(SQSConsumerAfterCommit) // o processo de teste continua vivo
}
