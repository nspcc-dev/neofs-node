package object

import (
	"math/rand/v2"
	"slices"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

func iterateSearchableContainerNodes(nodeSets [][]netmap.NodeInfo, repRules []uint, ecRules []iec.Rule, allNodes bool, f func(netmap.NodeInfo) bool) {
	for i := range nodeSets {
		var (
			nodeSet = nodeSets[i]
			ecIndex = i - len(repRules)
		)

		if !allNodes && ecIndex >= 0 {
			var (
				partsN    = int(ecRules[ecIndex].ParityPartNum + ecRules[ecIndex].DataPartNum)
				requiredN = int(ecRules[ecIndex].ParityPartNum + 1)
				searchN   = max(requiredN, len(nodeSet)-partsN+requiredN) // CBF 2 and alike.
			)

			if searchN < len(nodeSet) { // Stay safe in case of missing nodes.
				nodeSet = slices.Clone(nodeSet)
				rand.Shuffle(len(nodeSet), func(i, j int) {
					nodeSet[i], nodeSet[j] = nodeSet[j], nodeSet[i]
				})
				nodeSet = nodeSet[:requiredN]
			}
		}
		for _, node := range nodeSet {
			if !f(node) {
				return
			}
		}
	}
}
