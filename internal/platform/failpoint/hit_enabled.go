//go:build failpoint

package failpoint

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// Enabled informa se o binário foi compilado com a tag failpoint.
const Enabled = true

var (
	loadOnce sync.Once
	points   map[string]Action
)

func load() {
	spec := os.Getenv(EnvVar)
	p, err := Parse(spec)
	if err != nil {
		// Configuração de teste inválida: falhar alto é melhor que ignorar.
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}
	points = p
}

// Hit executa a ação configurada para name, se houver.
func Hit(name string) {
	loadOnce.Do(load)
	action, ok := points[name]
	if !ok {
		return
	}
	fmt.Fprintf(os.Stderr, "%s %s pid=%d\n", Marker, name, os.Getpid())
	switch action {
	case ActionCrash:
		if p, err := os.FindProcess(os.Getpid()); err == nil {
			_ = p.Kill()
		}
		// Kill do próprio processo é assíncrono em alguns SOs: nada mais
		// pode rodar depois do ponto.
		time.Sleep(5 * time.Second)
		os.Exit(137)
	case ActionHang:
		select {}
	}
}
