package container

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// DeletePrm groups parameters of Delete client operation.
type DeletePrm struct {
	cnr       []byte
	signature []byte
	token     []byte

	client.InvokePrmOptional
}

// SetCID sets container ID.
func (d *DeletePrm) SetCID(cid []byte) {
	d.cnr = cid
}

// SetSignature sets signature.
func (d *DeletePrm) SetSignature(signature []byte) {
	d.signature = signature
}

// SetToken sets session token.
func (d *DeletePrm) SetToken(token []byte) {
	d.token = token
}

// Delete removes the container from NeoFS system
// through Container contract call.
//
// Returns any error encountered that caused
// the removal to interrupt.
func (c *Client) Delete(p DeletePrm) error {
	if len(p.signature) == 0 {
		return errNilArgument
	}

	prm := client.InvokePrm{}
	prm.SetMethod(deleteMethod)
	prm.SetArgs(p.cnr, p.signature, p.token)
	prm.InvokePrmOptional = p.InvokePrmOptional

	// no magic bugs with notary requests anymore, this operation should
	// _always_ be notary signed so make it one more time even if it is
	// a repeated flag setting
	prm.RequireAlphabetSignature()

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", deleteMethod, err)
	}
	return nil
}
