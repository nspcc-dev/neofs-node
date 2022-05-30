package policer

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"go.uber.org/zap"
)

func (p *Policer) processObject(ctx context.Context, addr *addressSDK.Address) {
	idCnr, _ := addr.ContainerID()

	cnr, err := p.cnrSrc.Get(&idCnr)
	if err != nil {
		p.log.Error("could not get container",
			zap.Stringer("cid", idCnr),
			zap.String("error", err.Error()),
		)
		if container.IsErrNotFound(err) {
			prm := new(engine.InhumePrm)
			prm.MarkAsGarbage(addr)
			_, err := p.jobQueue.localStorage.Inhume(prm)
			if err != nil {
				id, _ := addr.ObjectID()
				p.log.Error("could not inhume object with missing container",
					zap.Stringer("cid", idCnr),
					zap.Stringer("oid", id),
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
	c := &processPlacementContext{
		Context: ctx,
	}

	for i := range nn {
		select {
		case <-ctx.Done():
			return
		default:
		}

		p.processNodes(c, addr, nn[i], replicas[i].Count())
	}

	if !c.needLocalCopy {
		p.log.Info("redundant local object copy detected",
			zap.Stringer("object", addr),
		)

		p.cbRedundantCopy(addr)
	}
}

type processPlacementContext struct {
	context.Context

	needLocalCopy bool
}

func (p *Policer) processNodes(ctx *processPlacementContext, addr *addressSDK.Address, nodes netmap.Nodes, shortage uint32) {
	log := p.log.With(
		zap.Stringer("object", addr),
	)

	prm := new(headsvc.RemoteHeadPrm).WithObjectAddress(addr)

	for i := 0; shortage > 0 && i < len(nodes); i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if p.netmapKeys.IsLocalKey(nodes[i].PublicKey()) {
			ctx.needLocalCopy = true

			shortage--
		} else {
			callCtx, cancel := context.WithTimeout(ctx, p.headTimeout)

			_, err := p.remoteHeader.Head(callCtx, prm.WithNodeInfo(nodes[i].NodeInfo))

			cancel()

			if client.IsErrObjectNotFound(err) {
				continue
			}

			if err != nil {
				log.Error("receive object header to check policy compliance",
					zap.String("error", err.Error()),
				)
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

		// TODO(@cthulhu-rider): replace to processObject in order to prevent repetitions
		//  in nodes for single object, see #1410
		p.replicator.HandleTask(ctx, new(replicator.Task).
			WithObjectAddress(addr).
			WithNodes(nodes).
			WithCopiesNumber(shortage),
		)
	}
}
