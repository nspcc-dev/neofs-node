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
	cnr, err := p.cnrSrc.Get(addr.ContainerID())
	if err != nil {
		p.log.Error("could not get container",
			zap.String("error", err.Error()),
		)

		return
	}

	policy := cnr.PlacementPolicy()

	nn, err := p.placementBuilder.BuildPlacement(addr, policy)
	if err != nil {
		p.log.Error("could not build placement vector for object",
			zap.String("error", err.Error()),
		)

		return
	}

	replicas := policy.Replicas()

	for i := range nn {
		select {
		case <-ctx.Done():
			return
		default:
		}

		p.processNodes(ctx, addr, nn[i], replicas[i].Count())
	}
}

func (p *Policer) processNodes(ctx context.Context, addr *object.Address, nodes netmap.Nodes, shortage uint32) {
	log := p.log.With(
		zap.Stringer("object", addr),
	)

	prm := new(headsvc.RemoteHeadPrm).WithObjectAddress(addr)
	redundantLocalCopy := false

	for i := 0; i < len(nodes); i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var node network.AddressGroup

		err := node.FromIterator(nodes[i])
		if err != nil {
			log.Error("could not parse network address",
				zap.String("error", err.Error()),
			)

			continue
		}

		if p.netmapKeys.IsLocalKey(nodes[i].PublicKey()) {
			if shortage == 0 {
				// we can call the redundant copy callback
				// here to slightly improve the performance
				// instead of readability.
				redundantLocalCopy = true
				break
			} else {
				shortage--
			}
		} else if shortage > 0 {
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

					continue
				}
			} else {
				shortage--
			}
		}

		nodes = append(nodes[:i], nodes[i+1:]...)
		i--
	}

	if shortage > 0 {
		log.Info("shortage of object copies detected",
			zap.Uint32("shortage", shortage),
		)

		p.replicator.AddTask(new(replicator.Task).
			WithObjectAddress(addr).
			WithNodes(nodes).
			WithCopiesNumber(shortage),
		)
	} else if redundantLocalCopy {
		p.cbRedundantCopy(addr)
	}
}
