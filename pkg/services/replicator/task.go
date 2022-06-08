package replicator

import (
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Task represents group of Replicator task parameters.
type Task struct {
	quantity uint32

	addr oid.Address

	nodes []netmap.NodeInfo
}

// WithCopiesNumber sets number of copies to replicate.
func (t *Task) WithCopiesNumber(v uint32) *Task {
	if t != nil {
		t.quantity = v
	}

	return t
}

// WithObjectAddress sets address of local object.
func (t *Task) WithObjectAddress(v oid.Address) *Task {
	if t != nil {
		t.addr = v
	}

	return t
}

// WithNodes sets a list of potential object holders.
func (t *Task) WithNodes(v []netmap.NodeInfo) *Task {
	if t != nil {
		t.nodes = v
	}

	return t
}
