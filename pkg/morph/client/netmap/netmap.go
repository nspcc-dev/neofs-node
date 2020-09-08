package netmap

import (
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/pkg/errors"
)

// GetNetMapArgs groups the arguments
// of get network map test invoke call.
type GetNetMapArgs struct {
}

// GetNetMapValues groups the stack parameters
// returned by get network map test invoke.
type GetNetMapValues struct {
	peers [][]byte
}

const nodeInfoFixedPrmNumber = 1

// Peers return the list of peers from
// network map in a binary format.
func (g GetNetMapValues) Peers() [][]byte {
	return g.peers
}

// NetMap performs the test invoke of get network map
// method of NeoFS Netmap contract.
func (c *Client) NetMap(_ GetNetMapArgs) (*GetNetMapValues, error) {
	prms, err := c.client.TestInvoke(
		c.netMapMethod,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "could not perform test invocation (%s)", c.netMapMethod)
	} else if ln := len(prms); ln != 1 {
		return nil, errors.Errorf("unexpected stack item count (%s): %d", c.netMapMethod, ln)
	}

	prms, err = client.ArrayFromStackItem(prms[0])
	if err != nil {
		return nil, errors.Wrapf(err, "could not get stack item array from stack item (%s)", c.netMapMethod)
	}

	res := &GetNetMapValues{
		peers: make([][]byte, 0, len(prms)),
	}

	for i := range prms {
		peer, err := peerInfoFromStackItem(prms[i])
		if err != nil {
			return nil, errors.Wrapf(err, "could not parse stack item (Peer #%d)", i)
		}

		res.peers = append(res.peers, peer)
	}

	return res, nil
}

func peerInfoFromStackItem(prm stackitem.Item) ([]byte, error) {
	prms, err := client.ArrayFromStackItem(prm)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get stack item array (PeerInfo)")
	} else if ln := len(prms); ln != nodeInfoFixedPrmNumber {
		return nil, errors.Errorf("unexpected stack item count (PeerInfo): expected %d, has %d", 1, ln)
	}

	return client.BytesFromStackItem(prms[0])
}
