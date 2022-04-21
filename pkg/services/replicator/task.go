package replicator

import (
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
)

// Task represents group of Replicator task parameters.
type Task struct {
	quantity uint32

	addr *addressSDK.Address

	nodes netmap.Nodes
}

// AddTask pushes replication task to Replicator queue.
//
// If task queue is full, log message is written.
func (p *Replicator) AddTask(t *Task) {
	select {
	case p.ch <- t:
	default:
		p.log.Warn("task queue is full")
	}
}

// WithCopiesNumber sets number of copies to replicate.
func (t *Task) WithCopiesNumber(v uint32) *Task {
	if t != nil {
		t.quantity = v
	}

	return t
}

// WithObjectAddress sets address of local object.
func (t *Task) WithObjectAddress(v *addressSDK.Address) *Task {
	if t != nil {
		t.addr = v
	}

	return t
}

// WithNodes sets a list of potential object holders.
func (t *Task) WithNodes(v netmap.Nodes) *Task {
	if t != nil {
		t.nodes = v
	}

	return t
}
