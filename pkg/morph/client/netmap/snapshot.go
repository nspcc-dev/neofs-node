package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// GetNetMap receives information list about storage nodes
// through the Netmap contract call, composes network map
// from them and returns it. With diff == 0 returns current
// network map, else return snapshot of previous network map.
func (c *Client) GetNetMap(diff uint64) (*netmap.NetMap, error) {
	return c.getNetMap(diff)
}

// Snapshot returns current netmap node infos.
// Consider using pkg/morph/client/netmap for this.
func (c *Client) Snapshot() (*netmap.NetMap, error) {
	return c.getNetMap(0)
}

func (c *Client) getNetMap(diff uint64) (*netmap.NetMap, error) {
	prm := client.TestInvokePrm{}
	prm.SetMethod(snapshotMethod)
	prm.SetArgs(diff)

	res, err := c.client.TestInvoke(prm)
	if err != nil {
		return nil, err
	}

	return unmarshalNetmap(res, snapshotMethod)
}

func unmarshalNetmap(items []stackitem.Item, method string) (*netmap.NetMap, error) {
	rawPeers, err := peersFromStackItems(items, method)
	if err != nil {
		return nil, err
	}

	result := make([]netmap.NodeInfo, len(rawPeers))
	for i := range rawPeers {
		if err := result[i].Unmarshal(rawPeers[i]); err != nil {
			return nil, fmt.Errorf("can't unmarshal node info (%s): %w", method, err)
		}
	}

	var nm netmap.NetMap
	nm.SetNodes(result)

	return &nm, nil
}
