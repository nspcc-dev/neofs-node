package testutil

import (
	"strconv"

	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// Nodes returns n [netmap.NodeInfo] elements with unique public keys.
//
// Each node has two network addresses with localhost IP. Ports are sequential
// starting from 10000.
func Nodes(n int) []netmap.NodeInfo {
	const portsStart = 10_000
	nodes := make([]netmap.NodeInfo, n)
	for i := range nodes {
		nodes[i].SetPublicKey([]byte("public_key_" + strconv.Itoa(i)))
		nodes[i].SetNetworkEndpoints(
			"localhost:"+strconv.Itoa(portsStart+2*i),
			"localhost:"+strconv.Itoa(portsStart+2*i+1),
		)
	}
	return nodes
}
