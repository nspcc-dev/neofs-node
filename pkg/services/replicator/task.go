package replicator

import (
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Task represents group of Replicator task parameters.
type Task struct {
	quantity uint32

	addr oid.Address

	obj *object.Object

	nodes []netmap.NodeInfo
}

// SetCopiesNumber sets number of copies to replicate.
func (t *Task) SetCopiesNumber(v uint32) {
	t.quantity = v
}

// SetObjectAddress sets address of local object.
func (t *Task) SetObjectAddress(v oid.Address) {
	t.addr = v
}

// SetObject sets object to avoid fetching it from the local storage.
func (t *Task) SetObject(obj *object.Object) {
	t.obj = obj
}

// SetNodes sets a list of potential object holders.
func (t *Task) SetNodes(v []netmap.NodeInfo) {
	t.nodes = v
}

// Nodes returns a list of potential object holders.
func (t Task) Nodes() []netmap.NodeInfo {
	return t.nodes
}
