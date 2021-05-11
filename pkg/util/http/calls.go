package httputil

import (
	"context"
	"net/http"

	"github.com/pkg/errors"
)

// Serve listens and serves internal HTTP server.
//
// Returns any error returned by internal server
// except http.ErrServerClosed.
//
// After Shutdown call, Serve has no effect and
// returned error is always nil.
func (x *Server) Serve() error {
	err := x.srv.ListenAndServe()

	// http.ErrServerClosed is returned on server shutdown
	// so we ignore this error.
	if err != nil && errors.Is(err, http.ErrServerClosed) {
		err = nil
	}

	return err
}

// Shutdown gracefully shuts down internal HTTP server.
//
// Shutdown is called with context which expires after
// configured timeout.
//
// Once Shutdown has been called on a server, it may not be reused;
// future calls to Serve method will have no effect.
func (x *Server) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), x.shutdownTimeout)

	err := x.srv.Shutdown(ctx)

	cancel()

	return err
}
