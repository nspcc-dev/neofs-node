package container

import (
	"github.com/pkg/errors"
)

// PutArgs groups the arguments
// of put container invocation call.
type PutArgs struct {
	ownerID []byte // container owner identifier

	cnr []byte // container in a binary format

	sig []byte // binary container signature
}

// SetOwnerID sets the container owner identifier
// in a binary format.
func (p *PutArgs) SetOwnerID(v []byte) {
	p.ownerID = v
}

// SetContainer sets the container structure
// in a binary format.
func (p *PutArgs) SetContainer(v []byte) {
	p.cnr = v
}

// SetSignature sets the container structure
// owner's signature.
func (p *PutArgs) SetSignature(v []byte) {
	p.sig = v
}

// Put invokes the call of put container method
// of NeoFS Container contract.
func (c *Client) Put(args PutArgs) error {
	return errors.Wrapf(c.client.Invoke(
		c.putMethod,
		args.ownerID,
		args.cnr,
		args.sig,
	), "could not invoke method (%s)", c.putMethod)
}
