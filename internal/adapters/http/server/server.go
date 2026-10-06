// Package httpserver liga o http.Handler da API a um http.Server cujo ciclo de
// vida é controlado pelo fx.Lifecycle (ARCHITECTURE.md, ADR-021).
//
//   - Start: o listen é síncrono (porta ocupada falha o start da aplicação) e
//     o Serve roda em goroutine própria.
//   - Stop: /health/ready passa a DOWN, aguarda Config.DrainDelay, e então
//     Shutdown fecha o listener (novas conexões são recusadas), fecha as
//     conexões ociosas e espera as requisições em voo. Se o prazo
//     (Config.ShutdownTimeout ou o do ctx do OnStop, o que vencer antes)
//     estourar, Close derruba as conexões restantes e o contexto das
//     requisições ainda em curso é cancelado.
package httpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	httpadapter "github.com/junglegaming/backend-challenge-go/internal/adapters/http"
)

// ErrDraining é o erro da verificação de prontidão durante a parada.
var ErrDraining = errors.New("servidor HTTP em parada (draining)")

// ErrForcedClose indica que o prazo de parada estourou e as conexões
// restantes foram fechadas à força.
var ErrForcedClose = errors.New("prazo de shutdown do servidor HTTP estourado; conexões fechadas à força")

// ReadinessCheckName é o nome da verificação de prontidão do servidor.
const ReadinessCheckName = "http-server"

// Drain sinaliza que a instância está parando. É compartilhado entre o
// servidor (que o liga no OnStop) e /health/ready (que passa a DOWN).
type Drain struct{ draining atomic.Bool }

// NewDrain devolve um sinal ainda desligado.
func NewDrain() *Drain { return &Drain{} }

// Begin liga o sinal (idempotente).
func (d *Drain) Begin() { d.draining.Store(true) }

// Draining informa se a parada começou.
func (d *Drain) Draining() bool { return d.draining.Load() }

// ReadinessCheck devolve a verificação que falha depois de Begin.
func (d *Drain) ReadinessCheck() httpadapter.ReadinessCheck {
	return httpadapter.ReadinessCheck{
		Name: ReadinessCheckName,
		Check: func(context.Context) error {
			if d.Draining() {
				return ErrDraining
			}
			return nil
		},
	}
}

// Config são os parâmetros do servidor. Zero em um timeout do http.Server
// significa "sem limite" (semântica do net/http); a configuração validada
// (config.HTTPConfig) nunca produz zero nesses campos.
type Config struct {
	Addr              string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	// DrainDelay é a espera entre readiness DOWN e o fechamento do listener.
	DrainDelay time.Duration
	// ShutdownTimeout é o prazo total do Stop (DrainDelay incluído). Zero =
	// só o prazo do ctx do Stop.
	ShutdownTimeout time.Duration
}

// Server é o http.Server da API com Start/Stop para o fx.Lifecycle.
type Server struct {
	cfg    Config
	srv    *http.Server
	drain  *Drain
	logger *slog.Logger
	// onServeError é chamado se o Serve terminar com erro inesperado (a
	// composição Fx o liga ao fx.Shutdowner para derrubar a aplicação).
	onServeError func(error)

	baseCancel context.CancelFunc
	mu         sync.Mutex
	ln         net.Listener
	done       chan struct{}
}

// New monta o servidor; nada é aberto até Start. drain e logger podem ser
// nil.
func New(cfg Config, h http.Handler, drain *Drain, logger *slog.Logger) *Server {
	if drain == nil {
		drain = NewDrain()
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	logger = logger.With(slog.String("component", "http-server"))
	baseCtx, baseCancel := context.WithCancel(context.Background())
	s := &Server{cfg: cfg, drain: drain, logger: logger, baseCancel: baseCancel}
	s.srv = &http.Server{
		Addr:              cfg.Addr,
		Handler:           h,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
		// Contexto-base das requisições: cancelado no fechamento forçado,
		// para que handlers ainda em curso abandonem o trabalho.
		BaseContext: func(net.Listener) context.Context { return baseCtx },
	}
	return s
}

// Addr devolve o endereço efetivo do listener (útil com porta 0), ou nil
// antes de Start.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Start abre o listener de forma síncrona e serve em background.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return errors.New("servidor HTTP já iniciado")
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen HTTP em %q: %w", s.cfg.Addr, err)
	}
	s.ln = ln
	s.done = make(chan struct{})
	go s.serve(ln, s.done)
	s.logger.InfoContext(ctx, "servidor HTTP escutando", slog.String("addr", ln.Addr().String()))
	return nil
}

func (s *Server) serve(ln net.Listener, done chan<- struct{}) {
	defer close(done)
	err := s.srv.Serve(ln)
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return
	}
	s.logger.Error("servidor HTTP parou inesperadamente", slog.String("error", err.Error()))
	if s.onServeError != nil {
		s.onServeError(err)
	}
}

// Stop: readiness DOWN → espera DrainDelay → Shutdown (recusa novas conexões
// e drena as em voo) → se o prazo estourar, Close forçado. Sempre espera a
// goroutine do Serve terminar. Devolve ErrForcedClose (envolvido) se houve
// fechamento forçado.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	done := s.done
	s.mu.Unlock()
	defer s.baseCancel()
	if done == nil {
		return nil // nunca iniciado
	}

	if s.cfg.ShutdownTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.ShutdownTimeout)
		defer cancel()
	}

	s.drain.Begin()
	s.logger.InfoContext(ctx, "servidor HTTP em parada: readiness DOWN",
		slog.Duration("drainDelay", s.cfg.DrainDelay))
	if s.cfg.DrainDelay > 0 {
		t := time.NewTimer(s.cfg.DrainDelay)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
		}
	}

	var result error
	if err := s.srv.Shutdown(ctx); err != nil {
		s.logger.WarnContext(context.WithoutCancel(ctx), "shutdown HTTP não drenou no prazo; fechando conexões",
			slog.String("error", err.Error()))
		s.baseCancel()
		if cerr := s.srv.Close(); cerr != nil && !errors.Is(cerr, net.ErrClosed) {
			err = errors.Join(err, cerr)
		}
		result = fmt.Errorf("%w: %w", ErrForcedClose, err)
	}
	<-done
	if result == nil {
		s.logger.InfoContext(context.WithoutCancel(ctx), "servidor HTTP parado")
	}
	return result
}
