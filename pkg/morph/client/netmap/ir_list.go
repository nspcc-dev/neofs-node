package netmap

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/pkg/errors"
)

// InnerRingListArgs groups the arguments
// of inner ring list test invoke call.
type InnerRingListArgs struct {
}

// InnerRingListValues groups the stack parameters
// returned by inner ring list test invoke.
type InnerRingListValues struct {
	keys [][]byte // list of keys of IR nodes in a binary format
}

// KeyList return the list of IR node keys
// in a binary format.
func (g InnerRingListValues) KeyList() [][]byte {
	return g.keys
}

// InnerRingList performs the test invoke of inner ring list
// method of NeoFS Netmap contract.
func (c *Client) InnerRingList(args InnerRingListArgs) (*InnerRingListValues, error) {
	prms, err := c.client.TestInvoke(
		c.innerRingListMethod,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "could not perform test invocation (%s)", c.innerRingListMethod)
	} else if ln := len(prms); ln != 1 {
		return nil, errors.Errorf("unexpected stack item count (%s): %d", c.innerRingListMethod, ln)
	}

	prms, err = client.ArrayFromStackItem(prms[0])
	if err != nil {
		return nil, errors.Wrapf(err, "could not get stack item array from stack item (%s)", c.innerRingListMethod)
	}

	res := &InnerRingListValues{
		keys: make([][]byte, 0, len(prms)),
	}

	for i := range prms {
		nodePrms, err := client.ArrayFromStackItem(prms[i])
		if err != nil {
			return nil, errors.Wrap(err, "could not get stack item array (Node #%d)")
		}

		key, err := client.BytesFromStackItem(nodePrms[0])
		if err != nil {
			return nil, errors.Wrapf(err, "could not parse stack item (Key #%d)", i)
		}

		res.keys = append(res.keys, key)
	}

	return res, nil
}
