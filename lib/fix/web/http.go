package web

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type (
	httpParams struct {
		Key     string
		Viper   *viper.Viper
		Logger  *zap.Logger
		Handler http.Handler
	}

	httpServer struct {
		name        string
		started     *int32
		logger      *zap.Logger
		shutdownTTL time.Duration
		server      server
	}
)

func (h *httpServer) Start(ctx context.Context) {
	if h == nil {
		return
	}

	if !atomic.CompareAndSwapInt32(h.started, 0, 1) {
		h.logger.Info("http: already started",
			zap.String("server", h.name))
		return
	}

	go func() {
		if err := h.server.serve(ctx); err != nil {
			if err != http.ErrServerClosed {
				h.logger.Error("http: could not start server",
					zap.Error(err))
			}
		}
	}()
}

func (h *httpServer) Stop() {
	if h == nil {
		return
	}

	if !atomic.CompareAndSwapInt32(h.started, 1, 0) {
		h.logger.Info("http: already stopped",
			zap.String("server", h.name))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), h.shutdownTTL)
	defer cancel()

	h.logger.Debug("http: try to stop server",
		zap.String("server", h.name))

	if err := h.server.shutdown(ctx); err != nil {
		h.logger.Error("http: could not stop server",
			zap.Error(err))
	}
}

const defaultShutdownTTL = 30 * time.Second

func newHTTPServer(p httpParams) *httpServer {
	var (
		address  string
		shutdown time.Duration
	)

	if address = p.Viper.GetString(p.Key + ".address"); address == "" {
		p.Logger.Info("Empty bind address, skip",
			zap.String("server", p.Key))
		return nil
	}
	if p.Handler == nil {
		p.Logger.Info("Empty handler, skip",
			zap.String("server", p.Key))
		return nil
	}

	p.Logger.Info("Create http.Server",
		zap.String("server", p.Key),
		zap.String("address", address))

	if shutdown = p.Viper.GetDuration(p.Key + ".shutdown_ttl"); shutdown <= 0 {
		shutdown = defaultShutdownTTL
	}

	return &httpServer{
		name:        p.Key,
		started:     new(int32),
		logger:      p.Logger,
		shutdownTTL: shutdown,
		server: newServer(params{
			Address: address,
			Name:    p.Key,
			Config:  p.Viper,
			Logger:  p.Logger,
			Handler: p.Handler,
		}),
	}
}
