package netmap

import (
	"bytes"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/core/netmap/node"
	"github.com/nspcc-dev/netmap"
)

// Info represent node information.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/netmap/node.Info.
type Info = node.Info

// Bucket represents NeoFS network map as a graph.
//
// If is a type alias of
// github.com/nspcc-dev/netmap.Bucket.
type Bucket = netmap.Bucket

// NetMap represents NeoFS network map
// with concurrent access support.
type NetMap struct {
	mtx *sync.RWMutex

	root *Bucket

	items []Info
}

// New creates and initializes a new NetMap.
//
// Using the NetMap that has been created with new(NetMap)
// expression (or just declaring a NetMap variable) is unsafe
// and can lead to panic.
func New() *NetMap {
	return &NetMap{
		mtx:  new(sync.RWMutex),
		root: new(Bucket),
	}
}

// Root returns the root bucket of the network map.
//
// Changing the result is unsafe and
// affects the network map.
func (n NetMap) Root() *Bucket {
	n.mtx.RLock()
	defer n.mtx.RUnlock()

	return n.root
}

// SetRoot sets the root bucket of the network map.
//
// Subsequent changing the source bucket
// is unsafe and affects the network map.
func (n *NetMap) SetRoot(v *Bucket) {
	n.mtx.Lock()
	n.root = v
	n.mtx.Unlock()
}

// Nodes returns node list of the network map.
//
// Changing the result is unsafe and
// affects the network map.
func (n NetMap) Nodes() []Info {
	n.mtx.RLock()
	defer n.mtx.RUnlock()

	return n.items
}

// SetNodes sets node list of the network map.
//
// Subsequent changing the source slice
// is unsafe and affects the network map.
func (n *NetMap) SetNodes(v []Info) {
	n.mtx.Lock()
	n.items = v
	n.mtx.Unlock()
}

// AddNode adds node information to the network map
//
// If node with provided information is already presented
// in network map, nothing happens,
func (n *NetMap) AddNode(nodeInfo Info) error {
	n.mtx.Lock()
	defer n.mtx.Unlock()

	num := -1

	// looking for existed node info item
	for i := range n.items {
		if bytes.Equal(
			n.items[i].PublicKey(),
			nodeInfo.PublicKey(),
		) {
			num = i
			break
		}
	}

	// add node if it does not exist
	if num < 0 {
		n.items = append(n.items, nodeInfo)
		num = len(n.items) - 1
	}

	return n.root.AddStrawNode(netmap.Node{
		N: uint32(num),
		C: n.items[num].Capacity(),
		P: n.items[num].Price(),
	}, nodeInfo.Options()...)
}
