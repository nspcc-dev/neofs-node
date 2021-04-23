package router

import (
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
)

// Builder groups methods to route values in the network.
type Builder interface {
	// NextStage must return next group of route points
	// for passed epoch and trust values.
	// Implementation must take into account already passed route points.
	//
	// Empty passed list means being at the starting point of the route.
	//
	// Must return empty list and no error if the endpoint of the route is reached.
	NextStage(epoch uint64, t reputation.Trust, passed []common.ServerInfo) ([]common.ServerInfo, error)
}

// RemoteWriterProvider describes the component
// for sending values to a fixed route point.
type RemoteWriterProvider interface {
	// InitRemote must return WriterProvider to the route point
	// corresponding to info.
	//
	// Nil info matches the end of the route.
	InitRemote(info common.ServerInfo) (common.WriterProvider, error)
}
