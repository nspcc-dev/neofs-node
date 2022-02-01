package reputation

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// GetArgs groups the arguments of "get reputation value" test invocation.
type GetArgs struct {
	epoch  uint64
	peerID []byte // object of reputation evaluation
}

// GetByIDArgs groups the arguments of "get reputation value by reputation id"
// test invocation.
type GetByIDArgs struct {
	id []byte // id of reputation value in reputation contract
}

// GetResult groups the stack parameters returned by
// "get" and "get by id" test invocations.
type GetResult struct {
	reputations [][]byte
}

// SetEpoch sets epoch of expected reputation value.
func (g *GetArgs) SetEpoch(v uint64) {
	g.epoch = v
}

// SetPeerID sets peer id of expected reputation value.
func (g *GetArgs) SetPeerID(v []byte) {
	g.peerID = v
}

// SetID sets id of expected reputation value in reputation contract.
func (g *GetByIDArgs) SetID(v []byte) {
	g.id = v
}

// Reputations returns slice of marshalled reputation values.
func (g GetResult) Reputations() [][]byte {
	return g.reputations
}

// Get invokes the call of "get reputation value" method of reputation contract.
func (c *Client) Get(args GetArgs) (*GetResult, error) {
	invokePrm := client.TestInvokePrm{}

	invokePrm.SetMethod(getMethod)
	invokePrm.SetArgs(int64(args.epoch), args.peerID)

	prms, err := c.client.TestInvoke(invokePrm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", getMethod, err)
	}

	return parseReputations(prms, getMethod)
}

// GetByID invokes the call of "get reputation value by reputation id" method
// of reputation contract.
func (c *Client) GetByID(args GetByIDArgs) (*GetResult, error) {
	invokePrm := client.TestInvokePrm{}

	invokePrm.SetMethod(getByIDMethod)
	invokePrm.SetArgs(args.id)

	prms, err := c.client.TestInvoke(invokePrm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", getByIDMethod, err)
	}

	return parseReputations(prms, getByIDMethod)
}

func parseReputations(items []stackitem.Item, method string) (*GetResult, error) {
	if ln := len(items); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d", method, ln)
	}

	items, err := client.ArrayFromStackItem(items[0])
	if err != nil {
		return nil, fmt.Errorf("could not get stack item array from stack item (%s): %w", method, err)
	}

	res := &GetResult{
		reputations: make([][]byte, 0, len(items)),
	}

	for i := range items {
		rawReputation, err := client.BytesFromStackItem(items[i])
		if err != nil {
			return nil, fmt.Errorf("could not get byte array from stack item (%s): %w", method, err)
		}

		res.reputations = append(res.reputations, rawReputation)
	}

	return res, nil
}
