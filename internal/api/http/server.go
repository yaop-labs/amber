package http

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type Server struct {
	server *http.Server
	log    *slog.Logger
}

func NewServer(addr string, handler http.Handler, readTimeout, readHeaderTimeout, writeTimeout, idleTimeout time.Duration, log *slog.Logger) *Server {
	return &Server{
		server: &http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadTimeout:       readTimeout,
			ReadHeaderTimeout: readHeaderTimeout,
			WriteTimeout:      writeTimeout,
			IdleTimeout:       idleTimeout,
		},
		log: log,
	}
}

func (s *Server) Start() {
	go func() {
		s.log.Info("http server listening", "addr", s.server.Addr)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Error("http server error", "err", err)
		}
	}()
}

func (s *Server) Shutdown(ctx context.Context) error {
	if err := s.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}
	return nil
}
