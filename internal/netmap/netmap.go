package netmap

import (
	"slices"

	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
)

// NodeSetsContainPublicKeyFunc checks whether checkPubKeyFn is true for at
// least one element from nodeSets.
func NodeSetsContainPublicKeyFunc(nodeSets [][]netmap.NodeInfo, checkPubKeyFn func([]byte) bool) bool {
	return slices.ContainsFunc(nodeSets, func(nodeSet []netmap.NodeInfo) bool {
		return nodeSetContainsPublicKeyFunc(nodeSet, checkPubKeyFn)
	})
}

func nodeSetContainsPublicKeyFunc(nodeSet []netmap.NodeInfo, checkPubKeyFn func([]byte) bool) bool {
	return slices.ContainsFunc(nodeSet, func(node netmap.NodeInfo) bool {
		return checkPubKeyFn(node.PublicKey())
	})
}

// ZapEndpoints returns zap log field with node network endpoints.
func ZapEndpoints(node netmap.NodeInfo) zap.Field {
	return zap.Strings("address group", slices.Collect(node.NetworkEndpoints()))
}
