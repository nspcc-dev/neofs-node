package container

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// GetArgs groups the arguments
// of get container test invoke call.
type GetArgs struct {
	cid []byte // container identifier
}

// GetValues groups the stack parameters
// returned by get container test invoke.
type GetValues struct {
	cnr []byte // container in a binary form
}

// SetCID sets the container identifier
// in a binary format.
func (g *GetArgs) SetCID(v []byte) {
	g.cid = v
}

// Container returns the container
// in a binary format.
func (g *GetValues) Container() []byte {
	return g.cnr
}

// Get performs the test invoke of get container
// method of NeoFS Container contract.
func (c *Client) Get(args GetArgs) (*GetValues, error) {
	prms, err := c.client.TestInvoke(
		c.getMethod,
		args.cid,
	)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", c.getMethod, err)
	} else if ln := len(prms); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d", c.getMethod, ln)
	}

	arr, err := client.ArrayFromStackItem(prms[0])
	if err != nil {
		return nil, fmt.Errorf("could not get item array of container (%s): %w", c.getMethod, err)
	}

	if len(arr) != 4 {
		return nil, fmt.Errorf("unexpected container stack item count (%s): %d", c.getMethod, len(arr))
	}

	cnrBytes, err := client.BytesFromStackItem(arr[0])
	if err != nil {
		return nil, fmt.Errorf("could not get byte array of container (%s): %w", c.getMethod, err)
	}

	return &GetValues{
		cnr: cnrBytes,
	}, nil
}
