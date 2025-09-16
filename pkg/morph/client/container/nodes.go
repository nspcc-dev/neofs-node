package container

import (
	"bytes"
	"crypto/elliptic"
	"fmt"
	"math"
	"slices"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
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
	slices.SortFunc(res, bytes.Compare)

	return res
}

func toAnySlice[T any](vv []T) []any {
	res := make([]any, 0, len(vv))
	for _, v := range vv {
		res = append(res, v)
	}

	return res
}

// Nodes returns container nodes placement from the Container contract.
// Result is a two-dimensional array corresponding to placement vectors.
// Returns (nil, nil) if there is no information from the contract.
func (c *Client) Nodes(cid cid.ID) ([][]*keys.PublicKey, error) {
	items, err := c.client.TestInvokeIterator(replicasListMethod, iteratorPrefetchNumber, cid[:])
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", replicasListMethod, err)
	}
	if len(items) > math.MaxUint8 {
		return nil, fmt.Errorf("too many replicas returned from the contract: %d (max expected: %d)", len(items), math.MaxUint8)
	}

	res := make([][]*keys.PublicKey, 0, len(items))
	for i := range items {
		vector, err := c.placementVector(cid, uint8(i))
		if err != nil {
			return nil, fmt.Errorf("could not get placement vector at index %d: %w", i, err)
		}

		res = append(res, vector)
	}

	return res, nil
}

func (c *Client) placementVector(cid cid.ID, vector uint8) ([]*keys.PublicKey, error) {
	items, err := c.client.TestInvokeIterator(nodesListMethod, iteratorPrefetchNumber, cid[:], vector)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", nodesListMethod, err)
	}
	if len(items) == 0 {
		return nil, nil
	}

	res := make([]*keys.PublicKey, 0, len(items))
	for i, item := range items {
		pkRaw, err := item.TryBytes()
		if err != nil {
			return nil, fmt.Errorf("reading %d stackitem as raw public key: %w", i, err)
		}
		pk, err := keys.NewPublicKeyFromBytes(pkRaw, elliptic.P256())
		if err != nil {
			return nil, fmt.Errorf("reading %d stackitem as public key: %w", i, err)
		}

		res = append(res, pk)
	}

	return res, nil
}
