package httputil

import (
	"context"
	"errors"
	"net/http"
)

// Serve listens and serves the internal HTTP server.
//
// Returns any error returned by the internal server
// except http.ErrServerClosed.
//
// After Shutdown call, Serve has no effect and
// the returned error is always nil.
func (x *Server) Serve() error {
	err := x.srv.ListenAndServe()

	// http.ErrServerClosed is returned on server shutdown
	// so we ignore this error.
	if err != nil && errors.Is(err, http.ErrServerClosed) {
		err = nil
	}

	return err
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
