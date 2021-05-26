package container

import (
	"fmt"
)

// DeleteArgs groups the arguments
// of delete container invocation call.
type DeleteArgs struct {
	cid []byte // container identifier

	sig []byte // container identifier signature

	token []byte // binary session token
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

// SetSessionToken sets token of the session
// within which the container was removed
// in a NeoFS API binary format.
func (p *DeleteArgs) SetSessionToken(v []byte) {
	p.token = v
}

// Delete invokes the call of delete container
// method of NeoFS Container contract.
func (c *Client) Delete(args DeleteArgs) error {
	err := c.client.Invoke(
		c.deleteMethod,
		args.cid,
		args.sig,
		args.token,
	)

	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", c.deleteMethod, err)
	}
	return nil
}
