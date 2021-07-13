package httputil

import (
	"fmt"
	"net/http"
	"time"
)

// Prm groups the required parameters of the Server's constructor.
//
// All values must comply with the requirements imposed on them.
// Passing incorrect parameter values will result in constructor
// failure (error or panic depending on the implementation).
type Prm struct {
	// TCP address for the server to listen on.
	//
	// Must be a valid TCP address.
	Address string

	// Must not be nil.
	Handler http.Handler
}

// Server represents a wrapper over http.Server
// that provides interface to start and stop
// listening routine.
//
// For correct operation, Server must be created
// using the constructor (New) based on the required parameters
// and optional components. After successful creation,
// Server is immediately ready to work through API.
type Server struct {
	shutdownTimeout time.Duration

	srv *http.Server
}

const invalidValFmt = "invalid %s %s (%T): %v"

func panicOnPrmValue(n string, v interface{}) {
	panicOnValue("parameter", n, v)
}

func panicOnOptValue(n string, v interface{}) {
	panicOnValue("option", n, v)
}

func panicOnValue(t, n string, v interface{}) {
	panic(fmt.Sprintf(invalidValFmt, t, n, v, v))
}

// New creates a new instance of the Server.
//
// Panics if at least one value of the parameters is invalid.
//
// Panics if at least one of next optional parameters is invalid:
//  * shutdown timeout is non-positive.
//
// The created Server does not require additional
// initialization and is completely ready for work.
func New(prm Prm, opts ...Option) *Server {
	switch {
	case prm.Address == "":
		panicOnPrmValue("Address", prm.Address)
	case prm.Handler == nil:
		panicOnPrmValue("Handler", prm.Handler)
	}

	c := defaultCfg()

	for _, o := range opts {
		o(c)
	}

	switch {
	case c.shutdownTimeout <= 0:
		panicOnOptValue("shutdown timeout", c.shutdownTimeout)
	}

	return &Server{
		shutdownTimeout: c.shutdownTimeout,
		srv: &http.Server{
			Addr:    prm.Address,
			Handler: prm.Handler,
		},
	}
}
