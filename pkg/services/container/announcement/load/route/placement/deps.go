package placementrouter

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
)

// PlacementBuilder describes interface of NeoFS placement calculator.
type PlacementBuilder interface {
	// BuildPlacement must compose and sort (according to a specific algorithm)
	// storage nodes from the container with identifier cid using network map
	// of particular epoch.
	BuildPlacement(epoch uint64, cid *container.ID) ([]netmap.Nodes, error)
}
