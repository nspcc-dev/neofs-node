package placementrouter

import (
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// PlacementBuilder describes interface of NeoFS placement calculator.
type PlacementBuilder interface {
	// BuildPlacement must compose and sort (according to a specific algorithm)
	// storage nodes from the container by its identifier using network map
	// of particular epoch.
	BuildPlacement(epoch uint64, cnr cid.ID) ([][]netmap.NodeInfo, error)
}
