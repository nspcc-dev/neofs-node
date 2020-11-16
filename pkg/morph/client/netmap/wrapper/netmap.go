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

	peers, err := w.client.Snapshot(args)
	if err != nil {
		return nil, err
	}

	rawPeers := peers.Peers() // slice of serialized node infos
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
