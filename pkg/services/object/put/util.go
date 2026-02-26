package putsvc

import (
	"slices"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
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

func newIncompleteError(cause error) error {
	var e apistatus.Incomplete
	e.SetMessage(cause.Error())
	return e
}

func newBusyError(cause error) error {
	var e apistatus.Busy
	e.SetMessage(cause.Error())
	return e
}
