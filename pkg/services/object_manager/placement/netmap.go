package placement

import (
	"slices"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Builder is an interface of the
// object placement vector builder.
type Builder interface {
	// BuildPlacement returns the list of placement vectors
	// for object according to the placement policy.
	//
	// Must return all container nodes if object identifier
	// is nil.
	BuildPlacement(cid.ID, *oid.ID, netmapSDK.PlacementPolicy) ([][]netmapSDK.NodeInfo, error)
}

// FlattenNodes appends each row to the flat list.
func FlattenNodes(ns [][]netmapSDK.NodeInfo) []netmapSDK.NodeInfo {
	return slices.Concat(ns...)
}
