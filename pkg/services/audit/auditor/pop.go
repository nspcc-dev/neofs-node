package auditor

import (
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/tzhash/tz"
	"go.uber.org/zap"
)

const (
	hashRangeNumber    = 4
	minGamePayloadSize = hashRangeNumber * tz.Size
)

func (c *Context) executePoP() {
	c.buildCoverage()

	c.report.SetPlacementCounters(
		c.counters.hit,
		c.counters.miss,
		c.counters.fail,
	)
}

func (c *Context) buildCoverage() {
	policy := c.task.ContainerStructure().PlacementPolicy()

	// select random member from another storage group
	// and process all placement vectors
	c.iterateSGMembersPlacementRand(func(id oid.ID, ind int, nodes []netmap.NodeInfo) bool {
		c.processObjectPlacement(id, nodes, policy.ReplicaNumberByIndex(ind))
		return c.containerCovered()
	})
}

func (c *Context) containerCovered() bool {
	// number of container nodes can be calculated once
	return c.cnrNodesNum <= len(c.pairedNodes)
}

func (c *Context) processObjectPlacement(id oid.ID, nodes []netmap.NodeInfo, replicas uint32) {
	var (
		ok      uint32
		optimal bool

		unpairedCandidate1, unpairedCandidate2 = -1, -1

		pairedCandidate = -1
	)

	var getHeaderPrm GetHeaderPrm
	getHeaderPrm.Context = c.task.AuditContext()
	getHeaderPrm.OID = id
	getHeaderPrm.CID = c.task.ContainerID()
	getHeaderPrm.NodeIsRelay = false

	for i := 0; ok < replicas && i < len(nodes); i++ {
		getHeaderPrm.Node = nodes[i]

		// try to get object header from node
		hdr, err := c.cnrCom.GetHeader(getHeaderPrm)
		if err != nil {
			c.log.Debug("could not get object header from candidate",
				zap.Stringer("id", id),
				zap.Error(err),
			)

			continue
		}

		c.updateHeadResponses(hdr)

		// increment success counter
		ok++

		// update optimal flag
		optimal = ok == replicas && uint32(i) < replicas

		// exclude small objects from coverage
		if c.objectSize(id) < minGamePayloadSize {
			continue
		}

		// update potential candidates to be paired
		if _, ok := c.pairedNodes[nodes[i].Hash()]; !ok {
			if unpairedCandidate1 < 0 {
				unpairedCandidate1 = i
			} else if unpairedCandidate2 < 0 {
				unpairedCandidate2 = i
			}
		} else if pairedCandidate < 0 {
			pairedCandidate = i
		}
	}

	if optimal {
		c.counters.hit++
	} else if ok == replicas {
		c.counters.miss++
	} else {
		c.counters.fail++
	}

	if unpairedCandidate1 >= 0 {
		if unpairedCandidate2 >= 0 {
			c.composePair(id, nodes[unpairedCandidate1], nodes[unpairedCandidate2])
		} else if pairedCandidate >= 0 {
			c.composePair(id, nodes[unpairedCandidate1], nodes[pairedCandidate])
		}
	}
}

func (c *Context) composePair(id oid.ID, n1, n2 netmap.NodeInfo) {
	c.pairs = append(c.pairs, gamePair{
		n1: n1,
		n2: n2,
		id: id,
	})

	c.pairedNodes[n1.Hash()] = &pairMemberInfo{
		node: n1,
	}
	c.pairedNodes[n2.Hash()] = &pairMemberInfo{
		node: n2,
	}
}

func (c *Context) iterateSGMembersPlacementRand(f func(oid.ID, int, []netmap.NodeInfo) bool) {
	// iterate over storage groups members for all storage groups (one by one)
	// with randomly shuffled members
	c.iterateSGMembersRand(func(id oid.ID) bool {
		// build placement vector for the current object
		nn, err := c.buildPlacement(id)
		if err != nil {
			c.log.Debug("could not build placement for object",
				zap.Stringer("id", id),
				zap.Error(err),
			)

			return false
		}

		for i, nodes := range nn {
			if f(id, i, nodes) {
				return true
			}
		}

		return false
	})
}

func (c *Context) iterateSGMembersRand(f func(oid.ID) bool) {
	c.iterateSGInfo(func(members []oid.ID) bool {
		ln := len(members)

		processed := make(map[uint64]struct{}, ln-1)

		for len(processed) < ln {
			ind := nextRandUint64(uint64(ln), processed)
			processed[ind] = struct{}{}

			if f(members[ind]) {
				return true
			}
		}

		return false
	})
}

func (c *Context) iterateSGInfo(f func([]oid.ID) bool) {
	c.sgMembersMtx.RLock()
	defer c.sgMembersMtx.RUnlock()

	// we can add randomization like for SG members,
	// but list of storage groups is already expected
	// to be shuffled since it is a Search response
	// with unpredictable order
	for i := range c.sgMembersCache {
		if f(c.sgMembersCache[i]) {
			return
		}
	}
}
