package container

import (
	"fmt"
)

// PutArgs groups the arguments
// of put container invocation call.
type PutArgs struct {
	cnr []byte // container in a binary format

	sig []byte // binary container signature

	publicKey []byte // public key of container owner

	token []byte // binary session token

	bindKey bool // bind key to account or not
}

// SetPublicKey sets the public key of container owner
// in a binary format.
func (p *PutArgs) SetPublicKey(v []byte) {
	p.publicKey = v
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
	err := c.client.Invoke(
		c.putMethod,
		args.cnr,
		args.sig,
		args.publicKey,
		args.token,
		args.bindKey,
	)

	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", c.putMethod, err)
	}
	return nil
}
