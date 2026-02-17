package putsvc

import (
	"bytes"
	"slices"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
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

func nodeIndexInSet(n netmap.NodeInfo, nn []netmap.NodeInfo) int {
	return slices.IndexFunc(nn, func(node netmap.NodeInfo) bool {
		return bytes.Equal(node.PublicKey(), n.PublicKey())
	})
}

func wrapIncompleteError(cause error) error {
	var st apistatus.Incomplete
	st.SetMessage(cause.Error())
	return st
}

func logNodeConversionError(l *zap.Logger, node netmap.NodeInfo, err error) {
	l.Error("failed to decode network endpoints of the storage node from the network map, skip the node",
		zap.String("public key", netmap.StringifyPublicKey(node)), zap.Error(err))
}
