package policer

import (
	"context"
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

type nodeCache map[uint64]bool

func (n nodeCache) SubmitSuccessfulReplication(id uint64) {
	n[id] = true
}

func (p *Policer) processObject(ctx context.Context, addr oid.Address) {
	idCnr := addr.Container()

	cnr, err := p.cnrSrc.Get(idCnr)
	if err != nil {
		p.log.Error("could not get container",
			zap.Stringer("cid", idCnr),
			zap.String("error", err.Error()),
		)
		if container.IsErrNotFound(err) {
			var prm engine.InhumePrm
			prm.MarkAsGarbage(addr)
			_, err := p.jobQueue.localStorage.Inhume(prm)
			if err != nil {
				p.log.Error("could not inhume object with missing container",
					zap.Stringer("cid", idCnr),
					zap.Stringer("oid", addr.Object()),
					zap.String("error", err.Error()))
			}
		}

		return
	}

	policy := cnr.Value.PlacementPolicy()
	if policy == nil {
		p.log.Error("missing placement policy in container",
			zap.Stringer("cid", idCnr),
		)

		return
	}

	obj := addr.Object()

	nn, err := p.placementBuilder.BuildPlacement(idCnr, &obj, *policy)
	if err != nil {
		p.log.Error("could not build placement vector for object",
			zap.String("error", err.Error()),
		)

		return
	}

	c := &processPlacementContext{
		Context: ctx,
	}

	var numOfContainerNodes int
	for i := range nn {
		numOfContainerNodes += len(nn[i])
	}

	// cached info about already checked nodes
	var checkedNodes nodeCache = make(map[uint64]bool, numOfContainerNodes)

	for i := range nn {
		select {
		case <-ctx.Done():
			return
		default:
		}

		p.processNodes(c, addr, nn[i], policy.ReplicaNumberByIndex(i), checkedNodes)
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

func (p *Policer) processNodes(ctx *processPlacementContext, addr oid.Address,
	nodes []netmap.NodeInfo, shortage uint32, checkedNodes nodeCache) {
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
			nodeID := nodes[i].Hash()
			if hasReplica, checked := checkedNodes[nodeID]; checked {
				if hasReplica {
					// node already contains replica, no need to replicate
					nodes = append(nodes[:i], nodes[i+1:]...)
					i--
					shortage--
				}

				continue
			}

			callCtx, cancel := context.WithTimeout(ctx, p.headTimeout)

			_, err := p.remoteHeader.Head(callCtx, prm.WithNodeInfo(nodes[i]))

			cancel()

			// client.IsErrObjectNotFound doesn't support wrapped errors, so unwrap it
			for wErr := errors.Unwrap(err); wErr != nil; wErr = errors.Unwrap(err) {
				err = wErr
			}

			if client.IsErrObjectNotFound(err) {
				checkedNodes[nodeID] = false
				continue
			}

			if err != nil {
				p.log.Error("receive object header to check policy compliance",
					zap.Stringer("object", addr),
					zap.String("error", err.Error()),
				)
			} else {
				shortage--
				checkedNodes[nodeID] = true
			}
		}

		nodes = append(nodes[:i], nodes[i+1:]...)
		i--
	}

	if shortage > 0 {
		p.log.Debug("shortage of object copies detected",
			zap.Stringer("object", addr),
			zap.Uint32("shortage", shortage),
		)

		p.replicator.HandleTask(ctx, new(replicator.Task).
			WithObjectAddress(addr).
			WithNodes(nodes).
			WithCopiesNumber(shortage),
			checkedNodes,
		)
	}
}
