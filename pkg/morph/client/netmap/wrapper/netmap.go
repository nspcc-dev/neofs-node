package wrapper

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	client "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/pkg/errors"
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

func unmarshalNetmap(rawPeers [][]byte) (*netmap.Netmap, error) {
	infos := make([]netmap.NodeInfo, 0, len(rawPeers))

	for _, peer := range rawPeers {
		nodeInfo := netmap.NewNodeInfo()
		if err := nodeInfo.Unmarshal(peer); err != nil {
			return nil, errors.Wrap(err, "can't unmarshal peer info")
		}

		infos = append(infos, *nodeInfo)
	}

	nodes := netmap.NodesFromInfo(infos)

	return netmap.NewNetmap(nodes)
}
