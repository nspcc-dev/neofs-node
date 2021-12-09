package loadroute

import (
	loadcontroller "github.com/nspcc-dev/neofs-node/pkg/services/container/announcement/load/controller"
	"github.com/nspcc-dev/neofs-sdk-go/container"
)

// ServerInfo describes a set of
// characteristics of a point in a route.
type ServerInfo interface {
	// PublicKey returns public key of the node
	// from the route in a binary representation.
	PublicKey() []byte

	// Iterates over network addresses of the node
	// in the route. Breaks iterating on true return
	// of the handler.
	IterateAddresses(func(string) bool)

	// Returns number of server's network addresses.
	NumberOfAddresses() int
}

// Builder groups methods to route values in the network.
type Builder interface {
	// NextStage must return next group of route points for the value a
	// based on the passed route.
	//
	// Empty passed list means being at the starting point of the route.
	//
	// Must return empty list and no error if the endpoint of the route is reached.
	// If there are more than one point to go and the last passed point is included
	// in that list (means that point is the last point in one of the route groups),
	// returned route must contain nil point that should be interpreted as signal to,
	// among sending to other route points, save the announcement in that point.
	NextStage(a container.UsedSpaceAnnouncement, passed []ServerInfo) ([]ServerInfo, error)
}

// RemoteWriterProvider describes the component
// for sending values to a fixed route point.
type RemoteWriterProvider interface {
	// InitRemote must return WriterProvider to the route point
	// corresponding to info.
	//
	// Nil info matches the end of the route.
	InitRemote(info ServerInfo) (loadcontroller.WriterProvider, error)
}
