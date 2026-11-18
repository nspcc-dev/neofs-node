package container

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	containerrpc "github.com/nspcc-dev/neofs-contract/rpc/container"
	fschaincontracts "github.com/nspcc-dev/neofs-node/pkg/morph/contracts"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// SetAttributeWithOrigReq is a temporary copy of [Client.SetAttribute] with
// original request.
func (c *Client) SetAttributeWithOrigReq(ctx context.Context, cnr cid.ID, attr, val string, validUntil uint64, pub, sig, token, origReq []byte) error {
	if origReq == nil {
		return c.SetAttribute(ctx, cnr, attr, val, validUntil, pub, sig, token)
	}

	err := c.client.CallWithAlphabetWitness(ctx, fschaincontracts.SetContainerAttributeMethod, []any{
		cnr[:], attr, val, origReq,
	})
	if err != nil {
		if isMethodNotFoundError(err, fschaincontracts.SetContainerAttributeMethod) {
			return c.SetAttribute(ctx, cnr, attr, val, validUntil, pub, sig, token)
		}
		if strings.Contains(err.Error(), containerrpc.NotFoundError) {
			return apistatus.ErrContainerNotFound
		}
		return fmt.Errorf("could not invoke method (%s): %w", fschaincontracts.SetContainerAttributeMethod, err)
	}
	return nil
}

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

// RemoveAttributeWithOrigReq is a temporary copy of [Client.RemoveAttribute] with
// original request.
func (c *Client) RemoveAttributeWithOrigReq(ctx context.Context, cnr cid.ID, attr string, validUntil uint64, pub, sig, token, origReq []byte) error {
	if origReq == nil {
		return c.RemoveAttribute(ctx, cnr, attr, validUntil, pub, sig, token)
	}

	err := c.client.CallWithAlphabetWitness(ctx, fschaincontracts.RemoveContainerAttributeMethod, []any{
		cnr[:], attr, origReq,
	})
	if err != nil {
		if isMethodNotFoundError(err, fschaincontracts.RemoveContainerAttributeMethod) {
			return c.RemoveAttribute(ctx, cnr, attr, validUntil, pub, sig, token)
		}
		if strings.Contains(err.Error(), containerrpc.NotFoundError) {
			return apistatus.ErrContainerNotFound
		}
		return fmt.Errorf("could not invoke method (%s): %w", fschaincontracts.RemoveContainerAttributeMethod, err)
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

func isMethodNotFoundError(err error, mtd string) bool {
	if exc, ok := errors.AsType[unwrap.Exception](err); ok {
		return isMethodNotFoundException(string(exc), mtd)
	}
	return isMethodNotFoundException(err.Error(), mtd)
}

func isMethodNotFoundException(msg, mtd string) bool {
	return strings.Contains(msg, "method not found: "+mtd)
}
