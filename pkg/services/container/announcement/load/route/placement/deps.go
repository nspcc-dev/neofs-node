package placementrouter

import (
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// PlacementBuilder describes interface of NeoFS placement calculator.
type PlacementBuilder interface {
	// BuildPlacement must compose and sort (according to a specific algorithm)
	// storage nodes from the container with identifier cid using network map
	// of particular epoch.
	BuildPlacement(epoch uint64, cid *cid.ID) ([]netmap.Nodes, error)
}
