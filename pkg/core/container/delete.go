package container

import (
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
)

// RemovalWitness groups the information required
// to prove and verify the removal of a container.
type RemovalWitness struct {
	cnr cid.ID

	sig []byte

	token *session.Container
}

// ContainerID returns the identifier of the container
// to be removed.
func (x RemovalWitness) ContainerID() cid.ID {
	return x.cnr
}

// SetContainerID sets the identifier of the container
// to be removed.
func (x *RemovalWitness) SetContainerID(id cid.ID) {
	x.cnr = id
}

// Signature returns the signature of the container identifier.
func (x RemovalWitness) Signature() []byte {
	return x.sig
}

// SetSignature sets a signature of the container identifier.
func (x *RemovalWitness) SetSignature(sig []byte) {
	x.sig = sig
}

// SessionToken returns the token of the session within
// which the container was removed.
func (x RemovalWitness) SessionToken() *session.Container {
	return x.token
}

// SetSessionToken sets the token of the session within
// which the container was removed.
func (x *RemovalWitness) SetSessionToken(tok *session.Container) {
	x.token = tok
}
