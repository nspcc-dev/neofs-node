package policer

import (
	"context"
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
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

func (p *Policer) processObject(ctx context.Context, addrWithType objectcore.AddressWithType) {
	addr := addrWithType.Address
	idCnr := addr.Container()
	idObj := addr.Object()

	cnr, err := p.cnrSrc.Get(idCnr)
	if err != nil {
		p.log.Error("could not get container",
			zap.Stringer("cid", idCnr),
			zap.String("error", err.Error()),
		)
		if container.IsErrNotFound(err) {
			var prm engine.InhumePrm
			prm.MarkAsGarbage(addrWithType.Address)
			prm.WithForceRemoval()

			_, err := p.jobQueue.localStorage.Inhume(prm)
			if err != nil {
				p.log.Error("could not inhume object with missing container",
					zap.Stringer("cid", idCnr),
					zap.Stringer("oid", idObj),
					zap.String("error", err.Error()))
			}
		}

		return
	}

	policy := cnr.Value.PlacementPolicy()

	nn, err := p.placementBuilder.BuildPlacement(idCnr, &idObj, policy)
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

		p.processNodes(c, addrWithType, nn[i], policy.ReplicaNumberByIndex(i), checkedNodes)
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

func (p *Policer) processNodes(ctx *processPlacementContext, addrWithType objectcore.AddressWithType,
	nodes []netmap.NodeInfo, shortage uint32, checkedNodes *nodeCache) {
	addr := addrWithType.Address
	typ := addrWithType.Type
	prm := new(headsvc.RemoteHeadPrm).WithObjectAddress(addr)

	// Number of copies that are stored on maintenance nodes.
	var uncheckedCopies int

	handleMaintenance := func(node netmap.NodeInfo) {
		// consider remote nodes under maintenance as problem OK. Such
		// nodes MAY not respond with object, however, this is how we
		// prevent spam with new replicas.
		// However, additional copies should not be removed in this case,
		// because we can remove the only copy this way.
		checkedNodes.submitReplicaHolder(node)
		shortage--
		uncheckedCopies++

		p.log.Debug("consider node under maintenance as OK",
			zap.String("node", netmap.StringifyPublicKey(node)),
		)
	}

	if typ == object.TypeLock {
		// all nodes of a container must store the `LOCK` objects
		// for correct object removal protection:
		//   - `LOCK` objects are broadcast on their PUT requests;
		//   - `LOCK` object removal is a prohibited action in the GC.
		shortage = uint32(len(nodes))
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
	} else if uncheckedCopies > 0 {
		// If we have more copies than needed, but some of them are from the maintenance nodes,
		// save the local copy.
		ctx.needLocalCopy = true
		p.log.Debug("some of the copies are stored on nodes under maintenance, save local copy",
			zap.Int("count", uncheckedCopies))
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
