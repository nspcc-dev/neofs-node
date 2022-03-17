package policer

import (
	"context"
	"strings"

	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"go.uber.org/zap"
)

func (p *Policer) processObject(ctx context.Context, addr *addressSDK.Address) {
	cnr, err := p.cnrSrc.Get(addr.ContainerID())
	if err != nil {
		p.log.Error("could not get container",
			zap.Stringer("cid", addr.ContainerID()),
			zap.String("error", err.Error()),
		)
		if container.IsErrNotFound(err) {
			prm := new(engine.InhumePrm)
			prm.MarkAsGarbage(addr)
			_, err := p.jobQueue.localStorage.Inhume(prm)
			if err != nil {
				p.log.Error("could not inhume object with missing container",
					zap.Stringer("cid", addr.ContainerID()),
					zap.Stringer("oid", addr.ObjectID()),
					zap.String("error", err.Error()))
			}
		}

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

func (p *Policer) processNodes(ctx context.Context, addr *addressSDK.Address, nodes netmap.Nodes, shortage uint32) {
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

			_, err := p.remoteHeader.Head(callCtx, prm.WithNodeInfo(nodes[i].NodeInfo))

			cancel()

			if err != nil {
				if strings.Contains(err.Error(), headsvc.ErrNotFound.Error()) {
					continue
				} else {
					log.Debug("could not receive object header",
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
		log.Debug("shortage of object copies detected",
			zap.Uint32("shortage", shortage),
		)

		p.replicator.HandleTask(ctx, new(replicator.Task).
			WithObjectAddress(addr).
			WithNodes(nodes).
			WithCopiesNumber(shortage),
		)
	} else if redundantLocalCopy {
		log.Info("redundant local object copy detected")

		p.cbRedundantCopy(addr)
	}
}
