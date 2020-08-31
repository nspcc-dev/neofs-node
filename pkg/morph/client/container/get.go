package container

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/pkg/errors"
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
		return nil, errors.Wrapf(err, "could not perform test invocation (%s)", c.getMethod)
	} else if ln := len(prms); ln != 1 {
		return nil, errors.Errorf("unexpected stack item count (%s): %d", c.getMethod, ln)
	}

	cnrBytes, err := client.BytesFromStackItem(prms[0])
	if err != nil {
		return nil, errors.Wrapf(err, "could not get byte array from stack item (%s)", c.getMethod)
	}

	return &GetValues{
		cnr: cnrBytes,
	}, nil
}
