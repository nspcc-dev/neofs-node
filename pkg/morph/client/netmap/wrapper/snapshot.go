package wrapper

import (
	"fmt"

	netmap2 "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// Snapshot returns current netmap node infos.
// Consider using pkg/morph/client/netmap for this.
func (w *Wrapper) Snapshot() (*netmap.Netmap, error) {
	res, err := w.client.Snapshot(netmap2.GetSnapshotArgs{})
	if err != nil {
		return nil, err
	}

	peers := res.Peers()
	result := make([]netmap.NodeInfo, len(peers))

	for i := range peers {
		if err := result[i].Unmarshal(peers[i]); err != nil {
			return nil, fmt.Errorf("can't unmarshal node info: %w", err)
		}
	}

	return netmap.NewNetmap(netmap.NodesFromInfo(result))
}
