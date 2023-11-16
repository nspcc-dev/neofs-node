package httputil

import (
	"context"
	"errors"
	"net"
	"net/http"
)

// ListenAndServe listens and serves the internal HTTP server.
//
// Returns any error returned by the internal server
// except http.ErrServerClosed.
//
// After Shutdown call, Serve has no effect and
// the returned error is always nil.
func (x *Server) ListenAndServe() error {
	ln, err := x.Listen()
	if err != nil {
		return err
	}
	return x.Serve(ln)
}

// Shutdown gracefully shuts down the internal HTTP server.
//
// Shutdown is called with the context which expires after
// the configured timeout.
//
// Once Shutdown has been called on a server, it may not be reused;
// future calls to Serve method will have no effect.
func (x *Server) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), x.shutdownTimeout)

	err := x.srv.Shutdown(ctx)

	cancel()

	return err
}

// Listen listens the internal HTTP server.
func (x *Server) Listen() (net.Listener, error) {
	addr := x.srv.Addr
	if addr == "" {
		addr = ":http"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return ln, nil
}

// Serve serves the internal HTTP server.
func (x *Server) Serve(listener net.Listener) error {
	err := x.srv.Serve(listener)

	// http.ErrServerClosed is returned on server shutdown
	// so we ignore this error.
	if err != nil && errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return err
}
