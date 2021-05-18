package container

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// ListArgs groups the arguments
// of list containers test invoke call.
type ListArgs struct {
	ownerID []byte // container owner identifier
}

// ListValues groups the stack parameters
// returned by list containers test invoke.
type ListValues struct {
	cidList [][]byte // list of container identifiers
}

// SetOwnerID sets the container owner identifier
// in a binary format.
func (l *ListArgs) SetOwnerID(v []byte) {
	l.ownerID = v
}

// CIDList returns the list of container
// identifiers in a binary format.
func (l *ListValues) CIDList() [][]byte {
	return l.cidList
}

// List performs the test invoke of list container
// method of NeoFS Container contract.
func (c *Client) List(args ListArgs) (*ListValues, error) {
	invokeArgs := make([]interface{}, 0, 1)

	invokeArgs = append(invokeArgs, args.ownerID)

	prms, err := c.client.TestInvoke(
		c.listMethod,
		invokeArgs...,
	)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", c.listMethod, err)
	} else if ln := len(prms); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d", c.listMethod, ln)
	}

	prms, err = client.ArrayFromStackItem(prms[0])
	if err != nil {
		return nil, fmt.Errorf("could not get stack item array from stack item (%s): %w", c.listMethod, err)
	}

	res := &ListValues{
		cidList: make([][]byte, 0, len(prms)),
	}

	for i := range prms {
		cid, err := client.BytesFromStackItem(prms[i])
		if err != nil {
			return nil, fmt.Errorf("could not get byte array from stack item (%s): %w", c.listMethod, err)
		}

		res.cidList = append(res.cidList, cid)
	}

	return res, nil
}
