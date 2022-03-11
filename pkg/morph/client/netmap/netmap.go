package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// State is an enumeration of various states of the NeoFS node.
type State int64

const (
	// Undefined is unknown state.
	Undefined State = iota

	// Online is network unavailable state.
	Online

	// Offline is an active state in the network.
	Offline
)

const (
	nodeInfoFixedPrmNumber = 1

	peerWithStateFixedPrmNumber = 2
)

// GetNetMapByEpoch receives information list about storage nodes
// through the Netmap contract call, composes network map
// from them and returns it. Returns snapshot of the specified epoch number.
func (c *Client) GetNetMapByEpoch(epoch uint64) (*netmap.Netmap, error) {
	invokePrm := client.TestInvokePrm{}
	invokePrm.SetMethod(epochSnapshotMethod)
	invokePrm.SetArgs(epoch)

	res, err := c.client.TestInvoke(invokePrm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w",
			epochSnapshotMethod, err)
	}

	return unmarshalNetmap(res, epochSnapshotMethod)
}

// GetCandidates receives information list about candidates
// for the next epoch network map through the Netmap contract
// call, composes network map from them and returns it.
func (c *Client) GetCandidates() (*netmap.Netmap, error) {
	invokePrm := client.TestInvokePrm{}
	invokePrm.SetMethod(netMapCandidatesMethod)

	prms, err := c.client.TestInvoke(invokePrm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", netMapCandidatesMethod, err)
	}

	candVals, err := nodeInfosFromStackItems(prms, netMapCandidatesMethod)
	if err != nil {
		return nil, fmt.Errorf("could not parse contract response: %w", err)
	}

	return netmap.NewNetmap(netmap.NodesFromInfo(candVals))
}

// NetMap performs the test invoke of get network map
// method of NeoFS Netmap contract.
func (c *Client) NetMap() ([][]byte, error) {
	invokePrm := client.TestInvokePrm{}
	invokePrm.SetMethod(netMapMethod)

	prms, err := c.client.TestInvoke(invokePrm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w",
			netMapMethod, err)
	}

	return peersFromStackItems(prms, netMapMethod)
}

func nodeInfosFromStackItems(stack []stackitem.Item, method string) ([]netmap.NodeInfo, error) {
	if ln := len(stack); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d", method, ln)
	}

	netmapNodes, err := client.ArrayFromStackItem(stack[0])
	if err != nil {
		return nil, fmt.Errorf("could not get stack item array from stack item (%s): %w", method, err)
	}

	res := make([]netmap.NodeInfo, len(netmapNodes))
	for i := range netmapNodes {
		err := stackItemToNodeInfo(netmapNodes[i], &res[i])
		if err != nil {
			return nil, fmt.Errorf("could not parse stack item (Peer #%d): %w", i, err)
		}
	}

	return res, nil
}

func stackItemToNodeInfo(prm stackitem.Item, res *netmap.NodeInfo) error {
	prms, err := client.ArrayFromStackItem(prm)
	if err != nil {
		return fmt.Errorf("could not get stack item array (PeerWithState): %w", err)
	} else if ln := len(prms); ln != peerWithStateFixedPrmNumber {
		return fmt.Errorf(
			"unexpected stack item count (PeerWithState): expected %d, has %d",
			peerWithStateFixedPrmNumber,
			ln,
		)
	}

	peer, err := peerInfoFromStackItem(prms[0])
	if err != nil {
		return fmt.Errorf("could not get bytes from 'node' field of PeerWithState: %w", err)
	} else if err = res.Unmarshal(peer); err != nil {
		return fmt.Errorf("can't unmarshal peer info: %w", err)
	}

	// state
	state, err := client.IntFromStackItem(prms[1])
	if err != nil {
		return fmt.Errorf("could not get int from 'state' field of PeerWithState: %w", err)
	}

	switch state {
	case 1:
		res.SetState(netmap.NodeStateOnline)
	case 2:
		res.SetState(netmap.NodeStateOffline)
	default:
		res.SetState(0)
	}

	return nil
}

func peersFromStackItems(stack []stackitem.Item, method string) ([][]byte, error) {
	if ln := len(stack); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d",
			method, ln)
	}

	peers, err := client.ArrayFromStackItem(stack[0])
	if err != nil {
		return nil, fmt.Errorf("could not get stack item array from stack item (%s): %w",
			method, err)
	}

	res := make([][]byte, 0, len(peers))

	for i := range peers {
		peer, err := peerInfoFromStackItem(peers[i])
		if err != nil {
			return nil, fmt.Errorf("could not parse stack item (Peer #%d): %w", i, err)
		}

		res = append(res, peer)
	}

	return res, nil
}

func peerInfoFromStackItem(prm stackitem.Item) ([]byte, error) {
	prms, err := client.ArrayFromStackItem(prm)
	if err != nil {
		return nil, fmt.Errorf("could not get stack item array (PeerInfo): %w", err)
	} else if ln := len(prms); ln != nodeInfoFixedPrmNumber {
		return nil, fmt.Errorf(
			"unexpected stack item count (PeerInfo): expected %d, has %d",
			nodeInfoFixedPrmNumber,
			ln,
		)
	}

	return client.BytesFromStackItem(prms[0])
}
