package web

import (
	"context"
	"expvar"
	"net/http"
	"net/http/pprof"

	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// Profiler is an interface of profiler.
type Profiler interface {
	Start(ctx context.Context)
	Stop()
}

const profilerKey = "pprof"

// NewProfiler is a profiler's constructor.
func NewProfiler(l *zap.Logger, v *viper.Viper) Profiler {
	if !v.GetBool(profilerKey + ".enabled") {
		l.Debug("pprof server disabled")
		return nil
	}

	mux := http.NewServeMux()

	mux.Handle("/debug/vars", expvar.Handler())

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return newHTTPServer(httpParams{
		Key:     profilerKey,
		Viper:   v,
		Logger:  l,
		Handler: mux,
	})
}
