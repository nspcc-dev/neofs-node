package container

import (
	"fmt"
	"slices"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// UpdateContainerPlacement registers public keys as a container's placement
// vectors. Always sends a notary request with Alphabet multi-signature.
// Number of vectors must equal number of replicas. Empty vectors removes
// container placement from the contract.
func (c *Client) UpdateContainerPlacement(cid cid.ID, vectors [][]netmap.NodeInfo, replicas []uint32) error {
	if len(vectors) == 0 {
		return c.dropPlacement(cid[:])
	}

	cnrHash := c.client.ContractAddress()
	b := smartcontract.NewBuilder()

	for i, vector := range vectors {
		b.InvokeMethod(cnrHash, addNextEpochNodes, cid[:], i, toAnySlice(pubKeys(vector)))
	}
	b.InvokeMethod(cnrHash, commitContainerListUpdate, cid[:], toAnySlice(replicas))

	script, err := b.Script()
	if err != nil {
		return fmt.Errorf("building TX script: %w", err)
	}

	err = c.client.RunAlphabetNotaryScript(script)
	if err != nil {
		return fmt.Errorf("could not invoke alphabet script: %w", err)
	}

	return nil
}

func (c *Client) dropPlacement(cid []byte) error {
	prm := client.InvokePrm{}
	prm.SetMethod(commitContainerListUpdate)
	prm.SetArgs(cid, nil)
	prm.RequireAlphabetSignature()

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", commitContainerListUpdate, err)
	}

	return nil
}

func pubKeys(nodes []netmap.NodeInfo) [][]byte {
	res := make([][]byte, 0, len(nodes))
	for _, node := range nodes {
		res = append(res, node.PublicKey())
	}

	// arrays take parts in transaction that should be multi-singed, so order
	// is important to be the same
	slices.SortFunc(res, func(a, b []byte) int {
		return strings.Compare(string(a), string(b))
	})

	return res
}

func toAnySlice[T any](vv []T) []any {
	res := make([]any, 0, len(vv))
	for _, v := range vv {
		res = append(res, v)
	}

	return res
}
