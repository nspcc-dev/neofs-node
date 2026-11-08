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
//
// vub pins the notary main transaction's ValidUntilBlock. Every Alphabet
// node settles payments for the same epoch independently, on its own local
// timer, with no shared triggering transaction to derive a nonce/VUB pair
// from (unlike most other notary calls in this package) - so the caller
// must compute vub from something every node already agrees on (e.g. a
// chain-recorded block height), not from its own observed current height,
// or different nodes' calls build different main transactions that can
// never combine into one 2/3+1-signed notary request.
func (c *Client) SettleContainerPayment(epoch uint64, cID cid.ID, vub uint32) error {
	prm := client.InvokePrm{}
	prm.SetMethod(fschaincontracts.PayBalanceMethod)
	prm.SetArgs(cID[:])
	prm.RequireAlphabetSignature()
	prm.SetNonce(uint32(epoch))
	prm.SetVUB(vub)

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
