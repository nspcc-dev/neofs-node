package policer

import (
	"context"
	"strings"

	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	"go.uber.org/zap"
)

func (p *Policer) processObject(ctx context.Context, addr *object.Address) {
	cnr, err := p.cnrSrc.Get(addr.GetContainerID())
	if err != nil {
		p.log.Error("could not get container",
			zap.String("error", err.Error()),
		)

		return
	}

	policy := cnr.GetPlacementPolicy()

	nn, err := p.placementBuilder.BuildPlacement(addr, policy)
	if err != nil {
		p.log.Error("could not build placement vector for object",
			zap.String("error", err.Error()),
		)

		return
	}

	replicas := policy.GetReplicas()

	for i := range nn {
		select {
		case <-ctx.Done():
			return
		default:
		}

		p.processNodes(ctx, addr, nn[i], replicas[i].GetCount())
	}
}

func (p *Policer) processNodes(ctx context.Context, addr *object.Address, nodes netmap.Nodes, shortage uint32) {
	prm := new(headsvc.RemoteHeadPrm).WithObjectAddress(addr)

	for i := 0; shortage > 0 && i < len(nodes); i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		netAddr := nodes[i].NetworkAddress()

		log := p.log.With(zap.String("node", netAddr))

		node, err := network.AddressFromString(netAddr)
		if err != nil {
			log.Error("could not parse network address")

			continue
		}

		if network.IsLocalAddress(p.localAddrSrc, node) {
			shortage--
		} else {
			callCtx, cancel := context.WithTimeout(ctx, p.headTimeout)

			_, err = p.remoteHeader.Head(callCtx, prm.WithNodeAddress(node))

			cancel()

			if err != nil {
				// FIXME: this is a temporary solution to resolve 404 response from remote node
				// We need to distinguish problem nodes from nodes without an object.
				if strings.Contains(err.Error(), headsvc.ErrNotFound.Error()) {
					continue
				} else {
					log.Error("could not receive object header",
						zap.String("error", err.Error()),
					)
				}
			} else {
				shortage--
			}
		}

		nodes = append(nodes[:i], nodes[i+1:]...)
		i--
	}

	if shortage > 0 {
		p.log.Info("shortage of object copies detected",
			zap.Uint32("shortage", shortage),
		)

		p.replicator.AddTask(new(replicator.Task).
			WithObjectAddress(addr).
			WithNodes(nodes).
			WithCopiesNumber(shortage),
		)
	}
}
