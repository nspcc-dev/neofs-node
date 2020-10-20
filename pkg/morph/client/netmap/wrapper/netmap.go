package wrapper

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	v2netmap "github.com/nspcc-dev/neofs-api-go/v2/netmap"
	grpcNetmap "github.com/nspcc-dev/neofs-api-go/v2/netmap/grpc"
	client "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
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
	infos := make([]v2netmap.NodeInfo, 0, len(rawPeers))

	for _, peer := range rawPeers {
		grpcNodeInfo := new(grpcNetmap.NodeInfo) // transport representation of struct
		err = proto.Unmarshal(peer, grpcNodeInfo)
		if err != nil {
			// consider unmarshalling into different major versions
			// of NodeInfo structure, if there any
			return nil, errors.Wrap(err, "can't unmarshal peer info")
		}

		v2 := v2netmap.NodeInfoFromGRPCMessage(grpcNodeInfo)
		infos = append(infos, *v2)
	}

	nodes := netmap.NodesFromV2(infos)

	return netmap.NewNetmap(nodes)
}
