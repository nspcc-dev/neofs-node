package container

import (
	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
)

// RemovalWitness groups the information required
// to prove and verify the removal of a container.
type RemovalWitness struct {
	cid *cid.ID

	sig []byte
}

// ContainerID returns identifier of the container
// to be removed.
func (x RemovalWitness) ContainerID() *cid.ID {
	return x.cid
}

// SetContainerID sets identifier of the container
// to be removed.
func (x *RemovalWitness) SetContainerID(id *cid.ID) {
	x.cid = id
}

// Signature returns signature of the container identifier.
func (x RemovalWitness) Signature() []byte {
	return x.sig
}

// SetSignature sets signature of the container identifier.
func (x *RemovalWitness) SetSignature(sig []byte) {
	x.sig = sig
}
