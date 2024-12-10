package container

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// AddNextEpochNodes registers public keys as a container's placement vector
// with specified index. Registration must be finished with final
// [Client.CommitContainerListUpdate] call. Always sends a notary request with
// Alphabet multi-signature.
func (c *Client) AddNextEpochNodes(cid cid.ID, placementIndex int, nodesKeys [][]byte) error {
	if len(nodesKeys) == 0 {
		return errNilArgument
	}

	prm := client.InvokePrm{}
	prm.SetMethod(addNextEpochNodes)
	prm.SetArgs(cid, placementIndex, nodesKeys)
	prm.RequireAlphabetSignature()

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", addNextEpochNodes, err)
	}

	return nil
}

// CommitContainerListUpdate finishes container placement updates for the current
// epoch made by former [Client.AddNextEpochNodes] calls. Always sends a notary
// request with Alphabet multi-signature.
func (c *Client) CommitContainerListUpdate(cid cid.ID, replicas []uint32) error {
	if len(replicas) == 0 {
		return errNilArgument
	}

	prm := client.InvokePrm{}
	prm.SetMethod(commitContainerListUpdate)
	prm.SetArgs(cid, replicas)
	prm.RequireAlphabetSignature()

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", commitContainerListUpdate, err)
	}

	return nil
}
