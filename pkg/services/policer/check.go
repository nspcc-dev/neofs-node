package policer

import (
	"context"
	"errors"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
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

// checks whether at least one remote container node holds particular object
// replica (including as a result of successful replication).
func (n nodeCache) atLeastOneHolder() bool {
	for _, v := range n {
		if v {
			return true
		}
	}

	return false
}

func (p *Policer) processObject(ctx context.Context, addrWithAttrs objectcore.AddressWithAttributes) {
	addr := addrWithAttrs.Address
	idCnr := addr.Container()
	idObj := addr.Object()

	ecp, err := iec.DecodePartInfoFromAttributes(addrWithAttrs.Attributes[0], addrWithAttrs.Attributes[1])
	if err != nil {
		p.log.Error("failed to decode EC part info from attributes, skip object",
			zap.Stringer("object", addr), zap.Error(err))
		return
	}

	nn, repRules, ecRules, err := p.network.GetNodesForObject(addr)
	if err != nil {
		p.log.Error("could not build placement vector for object",
			zap.Stringer("cid", idCnr),
			zap.Error(err),
		)
		if container.IsErrNotFound(err) {
			err = p.localStorage.Delete(addrWithAttrs.Address)
			if err != nil {
				p.log.Error("could not inhume object with missing container",
					zap.Stringer("cid", idCnr),
					zap.Stringer("oid", idObj),
					zap.Error(err))
			}
		}

		return
	}

	if ecp.RuleIndex >= 0 {
		if len(ecRules) > 0 {
			p.processECPart(ctx, addr, ecp, ecRules, nn[len(repRules):])
			return
		}
		// TODO: forbid to PUT such objects and drop this one?
		p.log.Info("object with EC attributes in container without EC rules detected, process according to REP rules",
			zap.Stringer("object", addr), zap.Int("ruleIdx", ecp.RuleIndex), zap.Int("partIdx", ecp.Index))
	} else if len(ecRules) > 0 && len(repRules) == 0 {
		p.log.Info("object with lacking EC attributes detected, deleting",
			zap.Stringer("object", addr))
		if err := p.localStorage.Delete(addr); err != nil {
			p.log.Error("failed to delete local object with lacking EC attributes",
				zap.Stringer("object", addr), zap.Error(err))
		}
		return
	}

	c := &processPlacementContext{
		object:       addrWithAttrs,
		checkedNodes: newNodeCache(),
	}

	for i := range repRules {
		select {
		case <-ctx.Done():
			return
		default:
		}

		p.processNodes(ctx, c, nn[i], uint32(repRules[i]))
	}

	// if context is done, needLocalCopy might not be able to calculate
	select {
	case <-ctx.Done():
		return
	default:
	}

	if !c.needLocalCopy {
		if !c.localNodeInContainer {
			// Here we may encounter a special case where the node is not in the network
			// map. In this scenario, it is impossible to determine whether the local node
			// will enter the container in the future or not. At the same time, the rest of
			// the network will perceive local peer as a 3rd party which will cause possible
			// replication problems. Iin order to avoid the potential loss of a single
			// replica, it is held.
			if !p.network.IsLocalNodeInNetmap() {
				p.log.Info("node is outside the network map, holding the replica...",
					zap.Stringer("object", addr),
				)

				return
			}

			// If local node is outside the object container and at least one correct
			// replica exists, then the node must not hold object replica. Otherwise, the
			// node violates the container storage policy declared by its owner. On the
			// other hand, in the complete absence of object replicas, the node must hold
			// the replica to prevent data loss.
			if !c.checkedNodes.atLeastOneHolder() {
				p.log.Info("node outside the container, but nobody stores the object, holding the replica...",
					zap.Stringer("object", addr),
				)

				return
			}

			p.log.Info("node outside the container, removing the replica so as not to violate the storage policy...",
				zap.Stringer("object", addr),
			)
		} else {
			p.log.Info("local replica of the object is redundant in the container, removing...",
				zap.Stringer("object", addr),
			)
		}

		p.dropRedundantLocalObject(addr)
	}
}

type processPlacementContext struct {
	// whether the local node is in the object container
	localNodeInContainer bool

	// whether the local node must store a meaningful replica of the object
	// according to the container's storage policy (e.g. as a primary placement node
	// or when such nodes fail replica check). Can be true only along with
	// localNodeInContainer.
	needLocalCopy bool

	// descriptor of the object for which the policy is being checked
	object objectcore.AddressWithAttributes

	// caches nodes which has been already processed in previous iterations
	checkedNodes *nodeCache
}

func (p *Policer) processNodes(ctx context.Context, plc *processPlacementContext, nodes []netmap.NodeInfo, shortage uint32) {
	headTimeout := p.getHeadTimeout()

	// Number of copies that are stored on maintenance nodes.
	var uncheckedCopies int

	handleMaintenance := func(node netmap.NodeInfo) {
		// consider remote nodes under maintenance as problem OK. Such
		// nodes MAY not respond with object, however, this is how we
		// prevent spam with new replicas.
		// However, additional copies should not be removed in this case,
		// because we can remove the only copy this way.
		plc.checkedNodes.submitReplicaHolder(node)
		shortage--
		uncheckedCopies++

		p.log.Debug("consider node under maintenance as OK",
			zap.String("node", netmap.StringifyPublicKey(node)),
		)
	}

	if plc.object.Type == object.TypeLock || plc.object.Type == object.TypeLink {
		// all nodes of a container must store the `LOCK` and `LINK` objects
		// for correct object relations handling:
		//   - `LINK` objects allows treating all children as root object;
		//   - `LOCK` and `LINK` objects are broadcast on their PUT requests;
		//   - `LOCK` object removal is a prohibited action in the GC.
		shortage = uint32(len(nodes))
	}

	var candidates []netmap.NodeInfo
	for i := 0; (!plc.localNodeInContainer || shortage > 0) && i < len(nodes); i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		isLocalNode := p.network.IsLocalNodePublicKey(nodes[i].PublicKey())

		if !plc.localNodeInContainer {
			plc.localNodeInContainer = isLocalNode
		}

		if shortage == 0 {
			candidates = append(candidates, nodes[i])
			continue
		} else if isLocalNode {
			plc.needLocalCopy = true

			shortage--
		} else if nodes[i].IsMaintenance() {
			handleMaintenance(nodes[i])
		} else {
			if status := plc.checkedNodes.processStatus(nodes[i]); status >= 0 {
				if status > 0 {
					candidates = append(candidates, nodes[i])
				}

				continue
			}

			callCtx, cancel := context.WithTimeout(ctx, headTimeout)

			_, err := p.apiConns.headObject(callCtx, nodes[i], plc.object.Address)

			cancel()

			if errors.Is(err, apistatus.ErrObjectNotFound) {
				plc.checkedNodes.submitReplicaCandidate(nodes[i])
				candidates = append(candidates, nodes[i])
				continue
			}

			if errors.Is(err, apistatus.ErrNodeUnderMaintenance) {
				handleMaintenance(nodes[i])
			} else if err != nil {
				p.log.Error("receive object header to check policy compliance",
					zap.Stringer("object", plc.object.Address),
					zap.Error(err),
				)
			} else {
				shortage--
				plc.checkedNodes.submitReplicaHolder(nodes[i])
			}
		}
	}

	if shortage > 0 {
		p.log.Debug("shortage of object copies detected",
			zap.Stringer("object", plc.object.Address),
			zap.Uint32("shortage", shortage),
		)

		p.tryToReplicate(ctx, plc.object.Address, shortage, candidates, plc.checkedNodes)
	} else if uncheckedCopies > 0 {
		// If we have more copies than needed, but some of them are from the maintenance nodes,
		// save the local copy.
		plc.needLocalCopy = true
		p.log.Debug("some of the copies are stored on nodes under maintenance, save local copy",
			zap.Int("count", uncheckedCopies))
	}
}

func (p *Policer) dropRedundantLocalObject(addr oid.Address) {
	err := p.localStorage.Delete(addr)
	if err != nil {
		p.log.Warn("could not inhume mark redundant copy as garbage",
			zap.Error(err))
	}
}

func (p *Policer) tryToReplicate(ctx context.Context, addr oid.Address, shortage uint32, candidates []netmap.NodeInfo, res replicator.TaskResult) {
	var task replicator.Task
	task.SetObjectAddress(addr)
	task.SetNodes(candidates)
	task.SetCopiesNumber(shortage)

	p.replicator.HandleTask(ctx, task, res)
}
