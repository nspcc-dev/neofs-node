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

// PeerWithState groups information about peer
// and its state in network map.
type PeerWithState struct {
	peer  []byte
	state State
}

func (ps PeerWithState) State() State {
	return ps.state
}

func (ps PeerWithState) Peer() []byte {
	return ps.peer
}

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
	invokePrm.SetArgs(int64(epoch))

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

	candVals, err := peersWithStateFromStackItems(prms, netMapCandidatesMethod)
	if err != nil {
		return nil, fmt.Errorf("could not parse contract response: %w", err)
	}

	return unmarshalCandidates(candVals)
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

func unmarshalCandidates(rawCandidate []*PeerWithState) (*netmap.Netmap, error) {
	candidates := make([]netmap.NodeInfo, 0, len(rawCandidate))

	for _, candidate := range rawCandidate {
		nodeInfo := netmap.NewNodeInfo()
		if err := nodeInfo.Unmarshal(candidate.Peer()); err != nil {
			return nil, fmt.Errorf("can't unmarshal peer info: %w", err)
		}

		switch candidate.State() {
		case Online:
			nodeInfo.SetState(netmap.NodeStateOnline)
		case Offline:
			nodeInfo.SetState(netmap.NodeStateOffline)
		default:
			nodeInfo.SetState(0)
		}

		candidates = append(candidates, *nodeInfo)
	}

	return netmap.NewNetmap(netmap.NodesFromInfo(candidates))
}

func peersWithStateFromStackItems(stack []stackitem.Item, method string) ([]*PeerWithState, error) {
	if ln := len(stack); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d", method, ln)
	}

	netmapNodes, err := client.ArrayFromStackItem(stack[0])
	if err != nil {
		return nil, fmt.Errorf("could not get stack item array from stack item (%s): %w", method, err)
	}

	res := make([]*PeerWithState, 0, len(netmapNodes))
	for i := range netmapNodes {
		node, err := peerWithStateFromStackItem(netmapNodes[i])
		if err != nil {
			return nil, fmt.Errorf("could not parse stack item (Peer #%d): %w", i, err)
		}

		res = append(res, node)
	}

	return res, nil
}

func peerWithStateFromStackItem(prm stackitem.Item) (*PeerWithState, error) {
	prms, err := client.ArrayFromStackItem(prm)
	if err != nil {
		return nil, fmt.Errorf("could not get stack item array (PeerWithState): %w", err)
	} else if ln := len(prms); ln != peerWithStateFixedPrmNumber {
		return nil, fmt.Errorf(
			"unexpected stack item count (PeerWithState): expected %d, has %d",
			peerWithStateFixedPrmNumber,
			ln,
		)
	}

	var res PeerWithState

	// peer
	if res.peer, err = peerInfoFromStackItem(prms[0]); err != nil {
		return nil, fmt.Errorf("could not get bytes from 'node' field of PeerWithState: %w", err)
	}

	// state
	state, err := client.IntFromStackItem(prms[1])
	if err != nil {
		return nil, fmt.Errorf("could not get int from 'state' field of PeerWithState: %w", err)
	}

	switch state {
	case 1:
		res.state = Online
	case 2:
		res.state = Offline
	default:
		res.state = Undefined
	}

	return &res, nil
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
