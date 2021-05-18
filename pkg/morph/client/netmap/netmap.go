package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// GetNetMapArgs groups the arguments
// of get network map test invoke call.
type GetNetMapArgs struct {
}

// GetSnapshotArgs groups the arguments
// of get netmap snapshot test invoke call.
type GetSnapshotArgs struct {
	diff uint64
}

// GetNetMapValues groups the stack parameters
// returned by get network map test invoke.
type GetNetMapValues struct {
	peers [][]byte
}

// EpochSnapshotArgs groups the arguments
// of snapshot by epoch test invoke call.
type EpochSnapshotArgs struct {
	epoch uint64
}

// EpochSnapshotValues groups the stack parameters
// returned by snapshot by epoch test invoke.
type EpochSnapshotValues struct {
	*GetNetMapValues
}

const nodeInfoFixedPrmNumber = 1

// SetDiff sets argument for snapshot method of
// netmap contract.
func (g *GetSnapshotArgs) SetDiff(d uint64) {
	g.diff = d
}

// SetEpoch sets epoch number to get snapshot.
func (a *EpochSnapshotArgs) SetEpoch(d uint64) {
	a.epoch = d
}

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
		return nil, fmt.Errorf("could not perform test invocation (%s): %w",
			c.netMapMethod, err)
	}

	return peersFromStackItems(prms, c.netMapMethod)
}

// NetMap performs the test invoke of get snapshot of network map
// from NeoFS Netmap contract. Contract saves only one previous epoch,
// so all invokes with diff > 1 return error.
func (c *Client) Snapshot(a GetSnapshotArgs) (*GetNetMapValues, error) {
	prms, err := c.client.TestInvoke(
		c.snapshotMethod,
		int64(a.diff),
	)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w",
			c.netMapMethod, err)
	}

	return peersFromStackItems(prms, c.snapshotMethod)
}

// EpochSnapshot performs the test invoke of get snapshot of network map by epoch
// from NeoFS Netmap contract.
func (c *Client) EpochSnapshot(args EpochSnapshotArgs) (*EpochSnapshotValues, error) {
	prms, err := c.client.TestInvoke(
		c.epochSnapshotMethod,
		int64(args.epoch),
	)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w",
			c.epochSnapshotMethod, err)
	}

	nmVals, err := peersFromStackItems(prms, c.epochSnapshotMethod)
	if err != nil {
		return nil, err
	}

	return &EpochSnapshotValues{
		GetNetMapValues: nmVals,
	}, nil
}

func peersFromStackItems(stack []stackitem.Item, method string) (*GetNetMapValues, error) {
	if ln := len(stack); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d",
			method, ln)
	}

	peers, err := client.ArrayFromStackItem(stack[0])
	if err != nil {
		return nil, fmt.Errorf("could not get stack item array from stack item (%s): %w",
			method, err)
	}

	res := &GetNetMapValues{
		peers: make([][]byte, 0, len(peers)),
	}

	for i := range peers {
		peer, err := peerInfoFromStackItem(peers[i])
		if err != nil {
			return nil, fmt.Errorf("could not parse stack item (Peer #%d): %w", i, err)
		}

		res.peers = append(res.peers, peer)
	}

	return res, nil
}

func peerInfoFromStackItem(prm stackitem.Item) ([]byte, error) {
	prms, err := client.ArrayFromStackItem(prm)
	if err != nil {
		return nil, fmt.Errorf("could not get stack item array (PeerInfo): %w", err)
	} else if ln := len(prms); ln != nodeInfoFixedPrmNumber {
		return nil, fmt.Errorf("unexpected stack item count (PeerInfo): expected %d, has %d", 1, ln)
	}

	return client.BytesFromStackItem(prms[0])
}
