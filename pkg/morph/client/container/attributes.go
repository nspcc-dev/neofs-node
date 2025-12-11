package container

import (
	"context"
	"fmt"
	"strings"

	containerrpc "github.com/nspcc-dev/neofs-contract/rpc/container"
	fschaincontracts "github.com/nspcc-dev/neofs-node/pkg/morph/contracts"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// SetAttribute calls Container contract to set container attribute with
// parameterized credentials. If transaction is accepted for processing,
// SetAttribute waits for it to be successfully executed. Waiting is done within
// ctx, [client.ErrTxAwaitTimeout] is returned when it is done.
//
// Returns [apistatus.ErrContainerNotFound] if requested container is missing.
//
// Returns any error encountered that caused the saving to interrupt.
func (c *Client) SetAttribute(ctx context.Context, cnr cid.ID, attr, val string, validUntil uint64, pub, sig, token []byte) error {
	err := c.client.CallWithAlphabetWitness(ctx, fschaincontracts.SetContainerAttributeMethod, []any{
		cnr[:], attr, val, validUntil, sig, pub, token,
	})
	if err != nil {
		if strings.Contains(err.Error(), containerrpc.NotFoundError) {
			return apistatus.ErrContainerNotFound
		}
		return fmt.Errorf("could not invoke method (%s): %w", fschaincontracts.SetContainerAttributeMethod, err)
	}
	return nil
}

// RemoveAttribute calls Container contract to remove container attribute with
// parameterized credentials. If transaction is accepted for processing,
// RemoveAttribute waits for it to be successfully executed. Waiting is done
// within ctx, [client.ErrTxAwaitTimeout] is returned when it is done.
//
// Returns [apistatus.ErrContainerNotFound] if requested container is missing.
func (c *Client) RemoveAttribute(ctx context.Context, cnr cid.ID, attr string, validUntil uint64, pub, sig, token []byte) error {
	err := c.client.CallWithAlphabetWitness(ctx, fschaincontracts.RemoveContainerAttributeMethod, []any{
		cnr[:], attr, validUntil, sig, pub, token,
	})
	if err != nil {
		if strings.Contains(err.Error(), containerrpc.NotFoundError) {
			return apistatus.ErrContainerNotFound
		}
		return fmt.Errorf("could not invoke method (%s): %w", fschaincontracts.RemoveContainerAttributeMethod, err)
	}
	return nil
}
