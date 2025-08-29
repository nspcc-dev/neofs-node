package placement

import (
	"slices"

	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// FlattenNodes appends each row to the flat list.
func FlattenNodes(ns [][]netmapSDK.NodeInfo) []netmapSDK.NodeInfo {
	return slices.Concat(ns...)
}
