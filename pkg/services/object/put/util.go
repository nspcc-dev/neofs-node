package putsvc

import (
	"slices"

	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

func localNodeInSets(n NeoFSNetwork, ss [][]netmap.NodeInfo) bool {
	return slices.ContainsFunc(ss, func(s []netmap.NodeInfo) bool {
		return localNodeInSet(n, s)
	})
}

func localNodeInSet(n NeoFSNetwork, nodes []netmap.NodeInfo) bool {
	return slices.ContainsFunc(nodes, func(node netmap.NodeInfo) bool {
		return n.IsLocalNodePublicKey(node.PublicKey())
	})
}
