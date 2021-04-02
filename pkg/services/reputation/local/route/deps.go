package reputationroute

import (
	reputationcontroller "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/controller"
)

// ServerInfo describes a set of
// characteristics of a point in a route.
type ServerInfo interface {
	// PublicKey returns public key of the node
	// from the route in a binary representation.
	PublicKey() []byte

	// Returns network address of the node
	// in the route.
	//
	// Can be empty.
	Address() string
}

// RemoteWriterProvider describes the component
// for sending values to a fixed route point.
type RemoteWriterProvider interface {
	// InitRemote must return WriterProvider to the route point
	// corresponding to info.
	//
	// Nil info matches the end of the route.
	InitRemote(info ServerInfo) (reputationcontroller.WriterProvider, error)
}
