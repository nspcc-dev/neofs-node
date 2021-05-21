package snapshot

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

const getNetmapSnapshotMethod = "netmap"

// Fetch returns current netmap node infos.
// Consider using pkg/morph/client/netmap for this.
func Fetch(cli *client.Client, contract util.Uint160) (*netmap.Netmap, error) {
	rawNetmapStack, err := cli.TestInvoke(contract, getNetmapSnapshotMethod)
	if err != nil {
		return nil, err
	}

	if ln := len(rawNetmapStack); ln != 1 {
		return nil, errors.New("invalid RPC response")
	}

	rawNodeInfos, err := client.ArrayFromStackItem(rawNetmapStack[0])
	if err != nil {
		return nil, err
	}

	result := make([]netmap.NodeInfo, 0, len(rawNodeInfos))

	for i := range rawNodeInfos {
		nodeInfo, err := peerInfoFromStackItem(rawNodeInfos[i])
		if err != nil {
			return nil, fmt.Errorf("invalid RPC response: %w", err)
		}

		result = append(result, *nodeInfo)
	}

	return netmap.NewNetmap(netmap.NodesFromInfo(result))
}

func peerInfoFromStackItem(prm stackitem.Item) (*netmap.NodeInfo, error) {
	node := netmap.NewNodeInfo()

	subItems, err := client.ArrayFromStackItem(prm)
	if err != nil {
		return nil, err
	} else if ln := len(subItems); ln != 1 {
		return nil, errors.New("invalid RPC response")
	} else if rawNodeInfo, err := client.BytesFromStackItem(subItems[0]); err != nil {
		return nil, err
	} else if err = node.Unmarshal(rawNodeInfo); err != nil {
		return nil, err
	}

	return node, nil
}
