package placement

import (
	"math"

	"github.com/nspcc-dev/hrw"
	"github.com/nspcc-dev/neofs-node/lib/netmap"
	"github.com/nspcc-dev/neofs-node/lib/peers"
	"go.uber.org/zap"
)

func calculateCount(n int) int {
	if n < 30 {
		return n
	}

	return int(1.4*math.Log(float64(n))+9) + 1
}

// Neighbours peers that which are distributed by hrw(seed)
// If full flag is set, all set of peers returns.
// Otherwise, result size depends on calculateCount function.
func (p *placement) Neighbours(seed, epoch uint64, full bool) []peers.ID {
	nm := p.nmStore.get(epoch)
	if nm == nil {
		p.log.Error("could not receive network state",
			zap.Uint64("epoch", epoch),
		)

		return nil
	}

	rPeers := p.listPeers(nm.ItemsCopy(), !full)

	hrw.SortSliceByValue(rPeers, seed)

	if full {
		return rPeers
	}

	var (
		ln  = len(rPeers)
		cut = calculateCount(ln)
	)

	if cut > ln {
		cut = ln
	}

	return rPeers[:cut]
}

func (p *placement) listPeers(nodes netmap.Nodes, exclSelf bool) []peers.ID {
	var (
		id     = p.ps.SelfID()
		result = make([]peers.ID, 0, len(nodes))
	)

	for i := range nodes {
		key := peers.IDFromBinary(nodes[i].PubKey)
		if exclSelf && id.Equal(key) {
			continue
		}

		result = append(result, key)
	}

	return result
}
