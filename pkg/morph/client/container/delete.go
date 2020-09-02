package container

import "github.com/pkg/errors"

// DeleteArgs groups the arguments
// of delete container invocation call.
type DeleteArgs struct {
	cid []byte // container identifier

	sig []byte // container identifier signature
}

// SetCID sets the container identifier
// in a binary format.
func (p *DeleteArgs) SetCID(v []byte) {
	p.cid = v
}

// SetSignature sets the container identifier
// owner's signature.
func (p *DeleteArgs) SetSignature(v []byte) {
	p.sig = v
}

// Delete invokes the call of delete container
// method of NeoFS Container contract.
func (c *Client) Delete(args DeleteArgs) error {
	return errors.Wrapf(c.client.Invoke(
		c.deleteMethod,
		args.cid,
		args.sig,
	), "could not invoke method (%s)", c.deleteMethod)
}
