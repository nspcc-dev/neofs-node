package processors

import (
	"fmt"

	cnrcli "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// UpdatePlacementVectors updates placement vectors after a container placement
// change in Container contract. Empty vectors drops container vectors from the
// contract.
func UpdatePlacementVectors(cID cid.ID, cnrCli *cnrcli.Client, vectors [][]netmap.NodeInfo, replicas []uint32) error {
	for i, vector := range vectors {
		err := cnrCli.AddNextEpochNodes(cID, i, pubKeys(vector))
		if err != nil {
			return fmt.Errorf("can't add %d placement vector to Container contract: %w", i, err)
		}
	}

	err := cnrCli.CommitContainerListUpdate(cID, replicas)
	if err != nil {
		return fmt.Errorf("can't commit container list to Container contract: %w", err)
	}

	return nil
}

func pubKeys(nodes []netmap.NodeInfo) [][]byte {
	res := make([][]byte, 0, len(nodes))
	for _, node := range nodes {
		res = append(res, node.PublicKey())
	}

	return res
}
