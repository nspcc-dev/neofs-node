package policer

import (
	"context"
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// tracks Policer's check progress.
type nodeCache map[uint64]bool

func newNodeCache() *nodeCache {
	m := make(map[uint64]bool)
	return (*nodeCache)(&m)
}

func (n *nodeCache) set(node netmap.NodeInfo, val bool) {
	(*n)[node.Hash()] = val
}

// submits storage node as a candidate to store the object replica in case of
// shortage.
func (n *nodeCache) submitReplicaCandidate(node netmap.NodeInfo) {
	n.set(node, false)
}

// submits storage node as a current object replica holder.
func (n *nodeCache) submitReplicaHolder(node netmap.NodeInfo) {
	n.set(node, true)
}

// processStatus returns current processing status of the storage node
//
//	>0 if node does not currently hold the object
//	 0 if node already holds the object
//	<0 if node has not been processed yet
func (n *nodeCache) processStatus(node netmap.NodeInfo) int8 {
	val, ok := (*n)[node.Hash()]
	if !ok {
		return -1
	}

	if val {
		return 0
	}

	return 1
}

// SubmitSuccessfulReplication marks given storage node as a current object
// replica holder.
//
// SubmitSuccessfulReplication implements replicator.TaskResult.
func (n *nodeCache) SubmitSuccessfulReplication(node netmap.NodeInfo) {
	n.submitReplicaHolder(node)
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
			prm.WithForceRemoval()

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
	obj := addr.Object()

	nn, err := p.placementBuilder.BuildPlacement(idCnr, &obj, policy)
	if err != nil {
		p.log.Error("could not build placement vector for object",
			zap.Stringer("cid", idCnr),
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
	checkedNodes := newNodeCache()

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
	nodes []netmap.NodeInfo, shortage uint32, checkedNodes *nodeCache) {
	prm := new(headsvc.RemoteHeadPrm).WithObjectAddress(addr)

	handleMaintenance := func(node netmap.NodeInfo) {
		// consider remote nodes under maintenance as problem OK. Such
		// nodes MAY not respond with object, however, this is how we
		// prevent spam with new replicas.
		checkedNodes.submitReplicaHolder(node)
		shortage--

		p.log.Debug("consider node under maintenance as OK",
			zap.String("node", netmap.StringifyPublicKey(node)),
		)
	}

	for i := 0; shortage > 0 && i < len(nodes); i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if p.netmapKeys.IsLocalKey(nodes[i].PublicKey()) {
			ctx.needLocalCopy = true

			shortage--
		} else if nodes[i].IsMaintenance() {
			handleMaintenance(nodes[i])
		} else {
			if status := checkedNodes.processStatus(nodes[i]); status >= 0 {
				if status == 0 {
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

			if client.IsErrObjectNotFound(err) {
				checkedNodes.submitReplicaCandidate(nodes[i])
				continue
			}

			if isClientErrMaintenance(err) {
				handleMaintenance(nodes[i])
			} else if err != nil {
				p.log.Error("receive object header to check policy compliance",
					zap.Stringer("object", addr),
					zap.String("error", err.Error()),
				)
			} else {
				shortage--
				checkedNodes.submitReplicaHolder(nodes[i])
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

		var task replicator.Task
		task.SetObjectAddress(addr)
		task.SetNodes(nodes)
		task.SetCopiesNumber(shortage)

		p.replicator.HandleTask(ctx, task, checkedNodes)
	}
}

// isClientErrMaintenance checks if err corresponds to NeoFS status return
// which tells that node is currently under maintenance. Supports wrapped
// errors.
//
// Similar to client.IsErr___ errors, consider replacing to NeoFS SDK.
func isClientErrMaintenance(err error) bool {
	switch unwrapErr(err).(type) {
	default:
		return false
	case
		apistatus.NodeUnderMaintenance,
		*apistatus.NodeUnderMaintenance:
		return true
	}
}

// unwrapErr unwraps error using errors.Unwrap.
func unwrapErr(err error) error {
	for e := errors.Unwrap(err); e != nil; e = errors.Unwrap(err) {
		err = e
	}

	return err
}
