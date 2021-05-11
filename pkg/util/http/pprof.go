package httputil

import (
	"net/http"
	"net/http/pprof"
)

// initializes pprof package in order to
// register Prometheus handlers on http.DefaultServeMux.
var _ = pprof.Handler("")

// Handler returns http.Handler for the
// Prometheus metrics collector.
func Handler() http.Handler {
	return http.DefaultServeMux
}
