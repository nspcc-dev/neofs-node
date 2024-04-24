package container

import (
	"crypto/sha256"
	"fmt"

	core "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// Delete marshals container ID, and passes it to Wrapper's Delete method
// along with signature and session token.
//
// Returns error if container ID is nil.
func Delete(c *Client, witness core.RemovalWitness) error {
	binCnr := make([]byte, sha256.Size)
	witness.ContainerID().Encode(binCnr)

	var prm DeletePrm

	prm.SetCID(binCnr)
	prm.SetSignature(witness.Signature())
	prm.RequireAlphabetSignature()

	if tok := witness.SessionToken(); tok != nil {
		prm.SetToken(tok.Marshal())
	}

	return c.Delete(prm)
}

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
