package wrapper

import (
	"fmt"

	client "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// GetNetMap receives information list about storage nodes
// through the Netmap contract call, composes network map
// from them and returns it. With diff == 0 returns current
// network map, else return snapshot of previous network map.
func (w Wrapper) GetNetMap(diff uint64) (*netmap.Netmap, error) {
	args := client.GetSnapshotArgs{}
	args.SetDiff(diff)

	vals, err := w.client.Snapshot(args)
	if err != nil {
		return nil, err
	}

	return unmarshalNetmap(vals.Peers())
}

// GetNetMapByEpoch receives information list about storage nodes
// through the Netmap contract call, composes network map
// from them and returns it. Returns snapshot of the specified epoch number.
func (w Wrapper) GetNetMapByEpoch(epoch uint64) (*netmap.Netmap, error) {
	args := client.EpochSnapshotArgs{}
	args.SetEpoch(epoch)

	vals, err := w.client.EpochSnapshot(args)
	if err != nil {
		return nil, err
	}

	return unmarshalNetmap(vals.Peers())
}

// GetCandidates receives information list about candidates
// for the next epoch network map through the Netmap contract
// call, composes network map from them and returns it.
func (w Wrapper) GetCandidates() (*netmap.Netmap, error) {
	args := client.GetNetMapCandidatesArgs{}

	vals, err := w.client.Candidates(args)
	if err != nil {
		return nil, err
	}

	return unmarshalCandidates(vals.NetmapNodes())
}

func unmarshalNetmap(rawPeers [][]byte) (*netmap.Netmap, error) {
	infos := make([]netmap.NodeInfo, 0, len(rawPeers))

	for _, peer := range rawPeers {
		nodeInfo := netmap.NewNodeInfo()
		if err := nodeInfo.Unmarshal(peer); err != nil {
			return nil, fmt.Errorf("can't unmarshal peer info: %w", err)
		}

		infos = append(infos, *nodeInfo)
	}

	nodes := netmap.NodesFromInfo(infos)

	return netmap.NewNetmap(nodes)
}

func unmarshalCandidates(rawCandidate []*client.PeerWithState) (*netmap.Netmap, error) {
	candidates := make([]netmap.NodeInfo, 0, len(rawCandidate))

	for _, candidate := range rawCandidate {
		nodeInfo := netmap.NewNodeInfo()
		if err := nodeInfo.Unmarshal(candidate.Peer()); err != nil {
			return nil, fmt.Errorf("can't unmarshal peer info: %w", err)
		}

		switch candidate.State() {
		case client.Online:
			nodeInfo.SetState(netmap.NodeStateOnline)
		case client.Offline:
			nodeInfo.SetState(netmap.NodeStateOffline)
		default:
			nodeInfo.SetState(0)
		}

		candidates = append(candidates, *nodeInfo)
	}

	nodes := netmap.NodesFromInfo(candidates)

	return netmap.NewNetmap(nodes)
}
