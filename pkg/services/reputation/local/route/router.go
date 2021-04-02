package reputationroute

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

// Prm groups the required parameters of the Router's constructor.
//
// All values must comply with the requirements imposed on them.
// Passing incorrect parameter values will result in constructor
// failure (error or panic depending on the implementation).
type Prm struct {
	// Characteristics of the local node's server.
	//
	// Must not be nil.
	LocalServerInfo ServerInfo

	// Component for sending values to a fixed route point.
	//
	// Must not be nil.
	RemoteWriterProvider RemoteWriterProvider

	// Route planner.
	//
	// Must not be nil.
	Builder Builder
}

// Router represents component responsible for routing
// local trust values over the network.
//
// For each fixed pair (node peer, epoch) there is a
// single value route on the network. Router provides the
// interface for writing values to the next point of the route.
//
// For correct operation, Router must be created using
// the constructor (New) based on the required parameters
// and optional components. After successful creation,
// the Router is immediately ready to work through API.
type Router struct {
	log *logger.Logger

	remoteProvider RemoteWriterProvider

	routeBuilder Builder

	localSrvInfo ServerInfo
}

const invalidPrmValFmt = "invalid parameter %s (%T):%v"

func panicOnPrmValue(n string, v interface{}) {
	panic(fmt.Sprintf(invalidPrmValFmt, n, v, v))
}

func New(prm Prm, opts ...Option) *Router {
	switch {
	case prm.RemoteWriterProvider == nil:
		panicOnPrmValue("RemoteWriterProvider", prm.RemoteWriterProvider)
	case prm.Builder == nil:
		panicOnPrmValue("Builder", prm.Builder)
	case prm.LocalServerInfo == nil:
		panicOnPrmValue("LocalServerInfo", prm.LocalServerInfo)
	}

	o := defaultOpts()

	for i := range opts {
		opts[i](o)
	}

	return &Router{
		log:            o.log,
		remoteProvider: prm.RemoteWriterProvider,
		routeBuilder:   prm.Builder,
		localSrvInfo:   prm.LocalServerInfo,
	}
}
