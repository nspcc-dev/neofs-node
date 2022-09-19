package replicator

import (
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Task represents group of Replicator task parameters.
type Task struct {
	quantity uint32

	addr oid.Address

	obj *objectSDK.Object

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

// WithObject sets object to avoid fetching it from the local storage.
func (t *Task) WithObject(obj *objectSDK.Object) *Task {
	if t != nil {
		t.obj = obj
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
