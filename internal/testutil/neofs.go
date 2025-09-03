package testutil

import (
	"strconv"

	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// Nodes returns n [netmap.NodeInfo] elements with unique public keys.
func Nodes(n int) []netmap.NodeInfo {
	nodes := make([]netmap.NodeInfo, n)
	for i := range nodes {
		nodes[i].SetPublicKey([]byte("public_key_" + strconv.Itoa(i)))
	}
	return nodes
}
