package container

import (
	"context"
	"fmt"

	fschaincontracts "github.com/nspcc-dev/neofs-node/pkg/morph/contracts"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// TODO: docs.
func (c *Client) SetAttribute(ctx context.Context, cnr cid.ID, attr, val string, pub, sig, token []byte) error {
	err := c.client.CallWithAlphabetWitness(ctx, fschaincontracts.SetContainerAttributeMethod, []any{
		cnr[:], attr, val, sig, pub, token,
	})
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", fschaincontracts.SetContainerAttributeMethod, err)
	}
	return nil
}

// TODO: docs.
func (c *Client) RemoveAttribute(ctx context.Context, cnr cid.ID, attr string, pub, sig, token []byte) error {
	err := c.client.CallWithAlphabetWitness(ctx, fschaincontracts.RemoveContainerAttributeMethod, []any{
		cnr[:], attr, sig, pub, token,
	})
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", fschaincontracts.RemoveContainerAttributeMethod, err)
	}
	return nil
}
