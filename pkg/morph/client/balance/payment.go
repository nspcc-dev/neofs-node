package balance

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	fschaincontracts "github.com/nspcc-dev/neofs-node/pkg/morph/contracts"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// SettleContainerPayment invokes transfers from container's owner to storage
// nodes that maintain objects inside the container. Always sends a notary
// request with Alphabet multi-signature. Always awaits transaction inclusion.
func (c *Client) SettleContainerPayment(cID cid.ID) error {
	prm := client.InvokePrm{}
	prm.SetMethod(fschaincontracts.PayBalanceMethod)
	prm.SetArgs(cID[:])
	prm.RequireAlphabetSignature()
	prm.Await()

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", fschaincontracts.PayBalanceMethod, err)
	}

	return nil
}

// GetUnpaidContainerEpoch checks if container has been marked as unpaid.
// Returns epoch mark was put. A negative epoch means container has not been
// marked as unpaid.
func (c *Client) GetUnpaidContainerEpoch(cID cid.ID) (int64, error) {
	prm := client.TestInvokePrm{}
	prm.SetMethod(fschaincontracts.UnpaidBalanceMethod)
	prm.SetArgs(cID[:])

	res, err := c.client.TestInvoke(prm)
	if err != nil {
		return 0, fmt.Errorf("could not invoke method (%s): %w", fschaincontracts.UnpaidBalanceMethod, err)
	}
	if len(res) != 1 {
		return 0, fmt.Errorf("stack has unexpected items: %d (want %d)", len(res), 1)
	}
	unpaidEpoch, err := client.IntFromStackItem(res[0])
	if err != nil {
		return 0, fmt.Errorf("not an int on stack: %w", err)
	}

	return unpaidEpoch, nil
}
