package netmap

import (
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// Node is a named type of netmap.NodeInfo which provides interface needed
// in the current repository. Node is expected to be used everywhere instead
// of direct usage of netmap.NodeInfo, so it represents a type mediator.
type Node netmap.NodeInfo

// PublicKey returns public key bound to the storage node.
//
// Return value MUST NOT be mutated, make a copy first.
func (x Node) PublicKey() []byte {
	return (netmap.NodeInfo)(x).PublicKey()
}

// IterateAddresses iterates over all announced network addresses
// and passes them into f. Handler MUST NOT be nil.
func (x Node) IterateAddresses(f func(string) bool) {
	(netmap.NodeInfo)(x).NetworkEndpoints()(f)
}

// NumberOfAddresses returns number of announced network addresses.
func (x Node) NumberOfAddresses() int {
	return (netmap.NodeInfo)(x).NumberOfNetworkEndpoints()
}

// Nodes is a named type of []netmap.NodeInfo which provides interface needed
// in the current repository. Nodes is expected to be used everywhere instead
// of direct usage of []netmap.NodeInfo, so it represents a type mediator.
type Nodes []netmap.NodeInfo
