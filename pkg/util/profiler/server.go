package profiler

import (
	"context"
	"net/http"

	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type (
	// Server is an interface of server.
	server interface {
		serve(ctx context.Context) error
		shutdown(ctx context.Context) error
	}

	contextServer struct {
		logger *zap.Logger
		server *http.Server
	}

	params struct {
		Address string
		Name    string
		Config  *viper.Viper
		Logger  *zap.Logger
		Handler http.Handler
	}
)

func newServer(p params) server {
	return &contextServer{
		logger: p.Logger,
		server: &http.Server{
			Addr:              p.Address,
			Handler:           p.Handler,
			ReadTimeout:       p.Config.GetDuration(p.Name + ".read_timeout"),
			ReadHeaderTimeout: p.Config.GetDuration(p.Name + ".read_header_timeout"),
			WriteTimeout:      p.Config.GetDuration(p.Name + ".write_timeout"),
			IdleTimeout:       p.Config.GetDuration(p.Name + ".idle_timeout"),
			MaxHeaderBytes:    p.Config.GetInt(p.Name + ".max_header_bytes"),
		},
	}
}

func (cs *contextServer) serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()

		if err := cs.server.Close(); err != nil {
			cs.logger.Info("something went wrong",
				zap.Error(err))
		}
	}()

	return cs.server.ListenAndServe()
}

func (cs *contextServer) shutdown(ctx context.Context) error {
	return cs.server.Shutdown(ctx)
}
