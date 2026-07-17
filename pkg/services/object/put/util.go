package putsvc

import (
	"slices"
	"strings"

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

func newCompletionError(cause error, incomplete bool) error {
	if incomplete {
		var inc = new(apistatus.Incomplete)
		inc.SetMessage(cause.Error())
		return inc
	}

	return cause
}

func addressLogString(ni netmap.NodeInfo) string {
	return strings.Join(slices.Collect(ni.NetworkEndpoints()), ", ")
}
