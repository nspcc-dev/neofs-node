package container

import (
	"fmt"
	"strings"

	containerrpc "github.com/nspcc-dev/neofs-contract/rpc/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	fschaincontracts "github.com/nspcc-dev/neofs-node/pkg/morph/contracts"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
)

// DeletePrm groups parameters of Delete client operation.
type DeletePrm struct {
	cnr       []byte
	signature []byte
	key       []byte
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

// SetKey sets public key.
func (d *DeletePrm) SetKey(key []byte) {
	d.key = key
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
//
// Returns [apistatus.ContainerLocked] if container is locked.
func (c *Client) Delete(p DeletePrm) error {
	if len(p.signature) == 0 {
		return errNilArgument
	}

	prm := client.InvokePrm{}
	prm.SetMethod(fschaincontracts.RemoveContainerMethod)
	prm.SetArgs(p.cnr, p.signature, p.key, p.token)
	prm.InvokePrmOptional = p.InvokePrmOptional

	// no magic bugs with notary requests anymore, this operation should
	// _always_ be notary signed so make it one more time even if it is
	// a repeated flag setting
	prm.RequireAlphabetSignature()

	err := c.client.Invoke(prm)
	if err != nil {
		if isMethodNotFoundError(err, fschaincontracts.RemoveContainerMethod) {
			prm.SetMethod(deleteMethod)
			prm.SetArgs(p.cnr, p.signature, p.token)
			if err = c.client.Invoke(prm); err != nil {
				if e := err.Error(); strings.Contains(e, containerrpc.ErrorLocked) {
					return apistatus.NewContainerLocked(e)
				}
				return fmt.Errorf("could not invoke method (%s): %w", deleteMethod, err)
			}
			return nil
		}
		if e := err.Error(); strings.Contains(e, containerrpc.ErrorLocked) {
			return apistatus.NewContainerLocked(e)
		}
		return fmt.Errorf("could not invoke method (%s): %w", fschaincontracts.RemoveContainerMethod, err)
	}
	return nil
}
