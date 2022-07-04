package reputation

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-sdk-go/reputation"
)

type (
	// GetPrm groups the arguments of "get reputation value" test invocation.
	GetPrm struct {
		epoch  uint64
		peerID reputation.PeerID
	}

	// GetByIDPrm groups the arguments of "get reputation value by
	// reputation id" test invocation.
	GetByIDPrm struct {
		id ID
	}
)

// SetEpoch sets epoch of expected reputation value.
func (g *GetPrm) SetEpoch(v uint64) {
	g.epoch = v
}

// SetPeerID sets peer id of expected reputation value.
func (g *GetPrm) SetPeerID(v reputation.PeerID) {
	g.peerID = v
}

// SetID sets id of expected reputation value in reputation contract.
func (g *GetByIDPrm) SetID(v ID) {
	g.id = v
}

// Get invokes the call of "get reputation value" method of reputation contract.
func (c *Client) Get(p GetPrm) ([]reputation.GlobalTrust, error) {
	invokePrm := client.TestInvokePrm{}
	invokePrm.SetMethod(getMethod)
	invokePrm.SetArgs(p.epoch, p.peerID.PublicKey())

	res, err := c.client.TestInvoke(invokePrm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", getMethod, err)
	}

	return parseReputations(res, getMethod)
}

// GetByID invokes the call of "get reputation value by reputation id" method
// of reputation contract.
func (c *Client) GetByID(p GetByIDPrm) ([]reputation.GlobalTrust, error) {
	invokePrm := client.TestInvokePrm{}
	invokePrm.SetMethod(getByIDMethod)
	invokePrm.SetArgs([]byte(p.id))

	prms, err := c.client.TestInvoke(invokePrm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", getByIDMethod, err)
	}

	return parseReputations(prms, getByIDMethod)
}

func parseGetResult(rawReputations [][]byte, method string) ([]reputation.GlobalTrust, error) {
	reputations := make([]reputation.GlobalTrust, 0, len(rawReputations))

	for i := range rawReputations {
		r := reputation.GlobalTrust{}

		err := r.Unmarshal(rawReputations[i])
		if err != nil {
			return nil, fmt.Errorf("can't unmarshal global trust value (%s): %w", method, err)
		}

		reputations = append(reputations, r)
	}

	return reputations, nil
}

func parseReputations(items []stackitem.Item, method string) ([]reputation.GlobalTrust, error) {
	if ln := len(items); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d", method, ln)
	}

	items, err := client.ArrayFromStackItem(items[0])
	if err != nil {
		return nil, fmt.Errorf("could not get stack item array from stack item (%s): %w", method, err)
	}

	res := make([][]byte, 0, len(items))

	for i := range items {
		rawReputation, err := client.BytesFromStackItem(items[i])
		if err != nil {
			return nil, fmt.Errorf("could not get byte array from stack item (%s): %w", method, err)
		}

		res = append(res, rawReputation)
	}

	return parseGetResult(res, method)
}
