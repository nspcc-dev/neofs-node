package container

import (
	"context"
	"fmt"
	"strings"

	containerrpc "github.com/nspcc-dev/neofs-contract/rpc/container"
	fschaincontracts "github.com/nspcc-dev/neofs-node/pkg/morph/contracts"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
)

// DeletePrm groups parameters of Delete client operation.
type DeletePrm struct {
	cnr       []byte
	signature []byte
	key       []byte
	token     []byte

	origReq []byte
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

// SetOriginalRequest sets original API request that triggered contract call.
func (d *DeletePrm) SetOriginalRequest(req []byte) {
	d.origReq = req
}

// Delete calls Container contract to delete container with parameterized
// credentials. If transaction is accepted for processing, Delete waits for it
// to be successfully executed. Waiting is performed within ctx,
// [client.ErrTxAwaitTimeout] is returned when it is done.
//
// Returns any error encountered that caused
// the removal to interrupt.
//
// Returns [apistatus.ContainerLocked] if container is locked.
func (c *Client) Delete(ctx context.Context, p DeletePrm) error {
	if len(p.signature) == 0 && len(p.origReq) == 0 {
		return errNilArgument
	}

	var args []any
	if p.origReq == nil {
		args = []any{p.cnr, p.signature, p.key, p.token}
	} else {
		args = []any{p.cnr, p.origReq}
	}

	err := c.client.CallWithAlphabetWitness(ctx, fschaincontracts.RemoveContainerMethod, args)
	if err != nil {
		if e := err.Error(); strings.Contains(e, containerrpc.ErrorLocked) {
			return apistatus.NewContainerLocked(e)
		}
		return fmt.Errorf("could not invoke method (%s): %w", fschaincontracts.RemoveContainerMethod, err)
	}
	return nil
}
