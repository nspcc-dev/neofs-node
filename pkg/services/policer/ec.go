package policer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"time"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func (p *Policer) processECPart(ctx context.Context, obj objectcore.AddressWithAttributes, parent oid.ID, pi iec.PartInfo, ecRules []iec.Rule, nodeLists [][]netmap.NodeInfo) {
	addr := obj.Address
	if pi.RuleIndex >= len(ecRules) {
		p.log.Warn("local object with invalid EC rule index detected, deleting",
			zap.Stringer("object", addr), zap.Int("ruleIdx", pi.RuleIndex), zap.Int("totalRules", len(ecRules)))
		if err := p.deleteLocalObject(ctx, addr, true); err != nil {
			p.log.Error("failed to delete local object with invalid EC rule index",
				zap.Stringer("object", addr), zap.Error(err))
		}
		return
	}

	rule := ecRules[pi.RuleIndex]
	if pi.Index >= int(rule.DataPartNum+rule.ParityPartNum) {
		p.log.Warn("local object with invalid EC part index detected, deleting",
			zap.Stringer("object", addr), zap.Stringer("rule", rule), zap.Int("partIdx", pi.Index))
		if err := p.deleteLocalObject(ctx, addr, true); err != nil {
			p.log.Error("failed to delete local object with invalid EC part index",
				zap.Stringer("object", addr), zap.Error(err))
		}
		return
	}

	for ruleIdx := range ecRules {
		if ruleIdx == pi.RuleIndex {
			p.checkECParts(ctx, addr.Container(), parent, rule, pi.RuleIndex, pi.Index, addr.Object())
			continue
		}

		p.checkECParts(ctx, addr.Container(), parent, ecRules[ruleIdx], ruleIdx, -1, oid.ID{})
	}
	if p.processECPartByRule(ctx, rule, addr, pi.Index, nodeLists[pi.RuleIndex]) {
		p.optimizeLocalShardLocation(ctx, obj, parent)
	}
}

// processECPartByRule returns true if the local part has no confirmed
// more-optimal remote holder and should have its local shard location optimized.
func (p *Policer) processECPartByRule(ctx context.Context, rule iec.Rule, addr oid.Address, partIdx int, nodes []netmap.NodeInfo) bool {
	var candidates []netmap.NodeInfo
	var maintenance bool
	headTimeout := time.Duration(-1)

	for i := range iec.NodeSequenceForPart(partIdx, int(rule.DataPartNum+rule.ParityPartNum), len(nodes)) {
		if p.network.IsLocalNodePublicKey(nodes[i].PublicKey()) {
			if len(candidates) == 0 {
				p.log.Debug("local node is optimal for EC part, hold",
					zap.Stringer("cid", addr.Container()), zap.Stringer("partOID", addr.Object()),
					zap.Stringer("rule", rule), zap.Int("partIdx", partIdx))
				return true
			}
			break
		}

		if headTimeout < 0 {
			headTimeout = p.getHeadTimeout()
		}

		callCtx, cancel := context.WithTimeout(ctx, headTimeout)
		_, err := p.apiConns.headObject(callCtx, nodes[i], addr, true, iec.PartInfo{RuleIndex: -1})
		cancel()

		if err == nil {
			p.log.Info("EC part header successfully received from more optimal node, drop",
				zap.Stringer("cid", addr.Container()), zap.Stringer("partOID", addr.Object()),
				zap.Stringer("rule", rule), zap.Int("partIdx", partIdx),
				zap.Strings("node", slices.Collect(nodes[i].NetworkEndpoints())))
			p.dropRedundantLocalObject(ctx, addr, true)
			return false
		}

		switch {
		default:
			p.log.Info("failed to receive EC part header from more optimal node, exclude",
				zap.Stringer("cid", addr.Container()), zap.Stringer("partOID", addr.Object()),
				zap.Stringer("rule", rule), zap.Int("partIdx", partIdx), zap.Error(err)) // error includes network addresses
		case errors.Is(err, apistatus.ErrNodeUnderMaintenance): // same as for REP rules
			p.log.Info("failed to receive EC part header from more optimal node due to its maintenance, continue",
				zap.Stringer("cid", addr.Container()), zap.Stringer("partOID", addr.Object()),
				zap.Stringer("rule", rule), zap.Int("partIdx", partIdx),
				zap.Strings("node", slices.Collect(nodes[i].NetworkEndpoints())))
			maintenance = true
		case errors.Is(err, apistatus.ErrObjectNotFound):
			candidates = append(candidates, nodes[i])
		}
	}

	if maintenance {
		// same as for REP rules
		p.log.Info("more optimal node for EC part is under maintenance, hold",
			zap.Stringer("cid", addr.Container()), zap.Stringer("partOID", addr.Object()),
			zap.Stringer("rule", rule), zap.Int("partIdx", partIdx))
		return true
	}

	if len(candidates) == 0 {
		p.log.Info("local node is suboptimal for EC part but now there are no other candidates, hold",
			zap.Stringer("cid", addr.Container()), zap.Stringer("partOID", addr.Object()),
			zap.Stringer("rule", rule), zap.Int("partIdx", partIdx))
		return true
	}

	p.log.Info("local node is suboptimal for EC part, moving to more optimal node...",
		zap.Stringer("cid", addr.Container()), zap.Stringer("partOID", addr.Object()),
		zap.Stringer("rule", rule), zap.Int("partIdx", partIdx), zap.Int("candidateNum", len(candidates)))

	var repRes singleReplication
	p.tryToReplicate(ctx, addr, 1, candidates, &repRes)
	if repRes.done {
		p.metrics.IncPolicerObjectReplicated(true)
		p.log.Info("EC part successfully moved to more optimal node, drop",
			zap.Stringer("cid", addr.Container()), zap.Stringer("partOID", addr.Object()),
			zap.Stringer("rule", rule), zap.Int("partIdx", partIdx), zap.Strings("newHolder", repRes.netAddresses))
		p.dropRedundantLocalObject(ctx, addr, true)
		return false
	}

	p.log.Info("failed to move EC part to more optimal node, hold",
		zap.Stringer("cid", addr.Container()), zap.Stringer("partOID", addr.Object()),
		zap.Stringer("rule", rule), zap.Int("partIdx", partIdx), zap.Int("candidateNum", len(candidates)))
	return true
}

type singleReplication struct {
	done         bool
	netAddresses []string
}

func (x *singleReplication) SubmitSuccessfulReplication(node netmap.NodeInfo) {
	if x.done {
		panic("recall")
	}
	x.done = true
	x.netAddresses = slices.Collect(node.NetworkEndpoints())
}

func (p *Policer) checkECParts(ctx context.Context, cnr cid.ID, parent oid.ID, rule iec.Rule, ruleIdx, localPartIdx int, localPartID oid.ID) {
	parentAddr := oid.NewAddress(cnr, parent)

	sortedNodeLists, repRules, ecRules, err := p.network.GetNodesForObject(parentAddr)
	if err != nil {
		p.log.Warn("failed to select nodes for EC parent to check its parts",
			zap.Stringer("container", cnr), zap.Stringer("parent", parent),
			zap.Stringer("rule", rule), zap.Error(err))
		return
	}

	if ruleIdx >= len(ecRules) {
		p.log.Error("rule index overflows total number of EC rules in policy",
			zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Stringer("rule", rule),
			zap.Int("rule_idx", ruleIdx), zap.Int("total_rules", len(ecRules)))
		return
	}

	totalParts := int(rule.DataPartNum + rule.ParityPartNum)
	if localPartIdx >= totalParts {
		p.log.Error("part index overflows total number of parts in the EC rule",
			zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Stringer("rule", rule),
			zap.Int("rule_idx", ruleIdx), zap.Int("total_parts", totalParts))
		return
	}

	var missingIdx, skipIdx []int
	var parentHdr object.Object
	var partLen uint64
	mPartID := make(map[int]oid.ID, totalParts)
	headTimeout := p.getHeadTimeout()
	sortedNodes := sortedNodeLists[len(repRules)+ruleIdx]

headNextPart:
	for partIdx := range totalParts {
		if partIdx == localPartIdx {
			mPartID[partIdx] = localPartID
			continue
		}

		for nodeIdx := range iec.NodeSequenceForPart(partIdx, totalParts, len(sortedNodes)) {
			var hdr object.Object
			local := p.network.IsLocalNodePublicKey(sortedNodes[nodeIdx].PublicKey())
			if local {
				hdr, err = p.localStorage.HeadECPart(ctx, cnr, parent, iec.PartInfo{RuleIndex: ruleIdx, Index: partIdx})
			} else {
				hdr, err = p.headECPart(ctx, headTimeout, sortedNodes[nodeIdx], cnr, parent, ruleIdx, partIdx)
			}
			if err == nil {
				if parentHdr.GetID().IsZero() {
					ph := hdr.Parent()
					if ph == nil {
						p.log.Error("missing parent header in received EC part object",
							zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Stringer("rule", rule),
							zap.Int("ruleIdx", ruleIdx), zap.Int("partIdx", partIdx), zap.Bool("local", local),
							zap.String("node", netmap.StringifyPublicKey(sortedNodes[nodeIdx])))

						return
					}

					parentHdr = *ph
					partLen = (parentHdr.PayloadSize() + uint64(rule.DataPartNum) - 1) / uint64(rule.DataPartNum)
				}

				if got := hdr.PayloadSize(); got != partLen {
					p.log.Error("unexpected payload len of EC part object received",
						zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Stringer("rule", rule),
						zap.Int("ruleIdx", ruleIdx), zap.Int("partIdx", partIdx), zap.Bool("local", local),
						zap.String("node", netmap.StringifyPublicKey(sortedNodes[nodeIdx])), zap.Uint64("expected", partLen),
						zap.Uint64("got", got))
					return
				}

				mPartID[partIdx] = hdr.GetID()
				continue headNextPart
			}

			switch {
			case errors.Is(err, apistatus.ErrObjectAlreadyRemoved):
				return
			case errors.Is(err, apistatus.ErrNodeUnderMaintenance):
				// Server may store the part. We consider it unavailable, but we don't attempt to recreate it.
				// Once SN finishes maintenance, the part will likely become available.
				if len(missingIdx)+len(skipIdx) >= int(rule.ParityPartNum) && len(ecRules) == 1 {
					p.log.Warn("too many EC parts unavailable, recreation is impossible",
						zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Stringer("rule", rule),
						zap.Int("unavailable", len(missingIdx)+len(skipIdx)))
					return
				}

				skipIdx = append(skipIdx, partIdx)
				continue headNextPart
			case errors.Is(err, apistatus.ErrObjectNotFound):
			default:
				p.log.Info("failed to get EC part header",
					zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Stringer("rule", rule),
					zap.Int("ruleIdx", ruleIdx), zap.Int("partIdx", partIdx), zap.Bool("local", local), zap.Error(err))
			}
		}

		if len(missingIdx)+len(skipIdx) >= int(rule.ParityPartNum) && len(ecRules) == 1 {
			p.log.Warn("too many EC parts unavailable, recreation is impossible",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Stringer("rule", rule),
				zap.Int("unavailable", len(missingIdx)+len(skipIdx)))
			return
		}

		missingIdx = append(missingIdx, partIdx)
	}

	if len(missingIdx) == 0 {
		return
	}

	p.metrics.SetPolicerConsistency(false)
	p.hadReplicaShortage.Store(true)

	if parentHdr.GetID().IsZero() {
		// can only happen for 1/1 rule: local part is never HEADed in for-loop above and remote one is unreachable
		hdr, err := p.localStorage.Head(ctx, parentAddr, false)
		if err != nil {
			p.log.Info("failed to get EC parent header locally",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Stringer("rule", rule),
				zap.Int("ruleIdx", ruleIdx), zap.Error(err))
			return
		}

		parentHdr = *hdr
		partLen = (parentHdr.PayloadSize() + uint64(rule.DataPartNum) - 1) / uint64(rule.DataPartNum)
	}

	buf := make([]byte, int(partLen)*totalParts)

	if len(missingIdx)+len(skipIdx) > int(rule.ParityPartNum) {
		if len(ecRules) == 1 {
			// code should never be here, it must be checked in every
			// parts restoration branch above
			panic("trying to cross restore EC parts with single EC rule")
		}

		var nextRule = ruleIdx + 1
		if nextRule == len(ecRules) {
			nextRule = 0
		}
		obj, err := p.fetchObjectFromECRule(ctx, buf, sortedNodeLists[len(repRules):], parentHdr, ecRules, ruleIdx, nextRule)
		if err != nil {
			p.log.Warn("fetching EC parts from other EC rules failed",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Stringer("rule", rule),
				zap.Int("unavailable", len(missingIdx)+len(skipIdx)), zap.Error(err))
			return
		}

		parts, err := fillParityParts(obj, rule, int(partLen))
		if err != nil {
			p.log.Warn("restoring EC parity parts from other EC rules failed",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Stringer("rule", rule),
				zap.Int("localPartIdx", localPartIdx), zap.Int("unavailable", len(missingIdx)+len(skipIdx)),
				zap.Error(err))
			return
		}
		err = p.recreateECParts(ctx, parentHdr, rule, ruleIdx, parts, missingIdx)
		if err != nil {
			p.log.Warn("restoring EC parts from other EC rules failed",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Stringer("rule", rule),
				zap.Int("localPartIdx", localPartIdx), zap.Int("unavailable", len(missingIdx)+len(skipIdx)),
				zap.Error(err))
			return
		}

		p.log.Info("restored EC objects from other EC rules successfully",
			zap.Stringer("container", cnr), zap.Stringer("parent", parent),
			zap.Int("localPartIdx", localPartIdx), zap.Stringer("rule", rule),
			zap.Int("ruleIdx", ruleIdx),
		)

		return
	}

	// fetchObjectFromECRule restores the object inside if parts are missing.
	_, err = p.fetchObjectFromECRule(ctx, buf, sortedNodeLists[len(repRules):], parentHdr, ecRules, ruleIdx, ruleIdx)
	if err != nil {
		p.log.Warn("fetching other EC parts failed, while restoring EC object",
			zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Stringer("rule", rule),
			zap.Int("localPartIdx", localPartIdx), zap.Int("unavailable", len(missingIdx)+len(skipIdx)),
			zap.Error(err))
		return
	}
}

// fetchObjectFromECRule returns requested object WITH parity parts concatenated.
func (p *Policer) fetchObjectFromECRule(ctx context.Context, buf []byte, nodes [][]netmap.NodeInfo, parentHeader object.Object, ecRules []iec.Rule, initialRuleIdx, ruleIdx int) ([]byte, error) {
	var (
		cnr          = parentHeader.GetContainerID()
		parentID     = parentHeader.GetID()
		rule         = ecRules[ruleIdx]
		partLen      = (parentHeader.PayloadSize() + uint64(rule.DataPartNum) - 1) / uint64(rule.DataPartNum)
		totalParts   = int(rule.DataPartNum + rule.ParityPartNum)
		requiredNum  = int(rule.DataPartNum)
		missingParts []int
		sortedNodes  = nodes[ruleIdx]

		err error
	)
	needLen := int(partLen) * totalParts
	if cap(buf) < needLen {
		buf = make([]byte, 0, needLen)
	}
	buf = buf[:needLen]

getNextPart:
	for partIdx := range totalParts {
		var (
			part        = buf[partIdx*int(partLen) : (partIdx+1)*int(partLen)]
			partIdxAttr string
			off, ln     uint64
		)
		for nodeIdx := range iec.NodeSequenceForPart(partIdx, totalParts, len(sortedNodes)) {
			var (
				rc   io.ReadCloser
				node zap.Field
			)
			if p.network.IsLocalNodePublicKey(sortedNodes[nodeIdx].PublicKey()) {
				_, _, rc, err = p.localStorage.GetECPartRange(ctx, cnr, parentID, iec.PartInfo{RuleIndex: ruleIdx, Index: partIdx}, blobstor.NewPayloadRange(off, ln), false)
				if err != nil {
					p.log.Info("failed to open RANGE stream for EC part from local storage",
						zap.Stringer("container", cnr), zap.Stringer("parent", parentID), zap.Stringer("rule", rule),
						zap.Int("ruleIdx", ruleIdx), zap.Int("partIdx", partIdx),
						zap.Uint64("off", off), zap.Uint64("ln", ln), zap.Error(err))
					continue
				}

				node = zap.String("node", "local")
			} else {
				node = zap.Strings("nodeEndpoints", slices.Collect(sortedNodes[nodeIdx].NetworkEndpoints()))
				if partIdxAttr == "" {
					partIdxAttr = strconv.Itoa(partIdx)
				}

				// TODO: this is the 1st place where we known IDs of EC parts in advance.
				//  Consider supporting direct requests of EC parts by ID, they are more lightweight.
				rc, err = p.apiConns.GetRange(ctx, sortedNodes[nodeIdx], cnr, parentID, off, ln, iec.PartInfo{
					RuleIndex: ruleIdx,
					Index:     partIdx,
				})
				if err != nil {
					p.log.Info("failed to open RANGE stream for EC part from remote node",
						zap.Stringer("container", cnr), zap.Stringer("parent", parentID), zap.Stringer("rule", rule),
						zap.Int("ruleIdx", ruleIdx), zap.Int("partIdx", partIdx), node,
						zap.Uint64("off", off), zap.Uint64("ln", ln), zap.Error(err))
					continue
				}
			}
			n, err := io.ReadFull(rc, part[off:])
			rc.Close()
			if err == nil {
				continue getNextPart
			}
			if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) {
				return nil, err
			}
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			p.log.Info("failed to RANGE EC part",
				zap.Stringer("container", cnr), zap.Stringer("parent", parentID), zap.Stringer("rule", rule),
				zap.Int("ruleIdx", ruleIdx), zap.Int("partIdx", partIdx), node,
				zap.Uint64("off", off), zap.Uint64("len", ln), zap.Error(err))

			if n > 0 {
				off += uint64(n)
				ln = partLen - off
			}
		}

		missingParts = append(missingParts, partIdx)
		if len(missingParts) > requiredNum {
			break getNextPart
		}
	}

	if missingParts == nil {
		return buf, nil
	}

	p.log.Info("could not get full object by EC rule, trying to restore EC rule implicitly...",
		zap.Stringer("cnr", cnr), zap.Stringer("parent", parentID), zap.Int("ruleIdx", ruleIdx),
		zap.Stringer("rule", rule))

	if totalParts-len(missingParts) >= requiredNum {
		p.log.Info("restoring EC rule by survived EC parts", zap.Stringer("cnr", cnr),
			zap.Stringer("parent", parentID), zap.Int("partsMissing", len(missingParts)),
			zap.Int("ruleIdx", ruleIdx), zap.Stringer("rule", rule))
		err = p.recreateECParts(ctx, parentHeader, rule, ruleIdx, sliceECParts(buf, int(partLen), rule, missingParts), missingParts)
		if err != nil {
			return nil, fmt.Errorf("part re-endcoding: %w", err)
		}

		return buf, nil
	}

	nextRule := ruleIdx + 1
	if nextRule == len(ecRules) {
		nextRule = 0
	}
	if nextRule == initialRuleIdx {
		return nil, fmt.Errorf("all EC rules were tried, there is not enough data for restoration, last rule tried: %d", ruleIdx)
	}
	p.log.Info("not enought EC parts for rule restoration, trying another EC rule...",
		zap.Stringer("cnr", cnr), zap.Stringer("parent", parentID), zap.Int("partsMissing", len(missingParts)),
		zap.Int("failedRuleIdx", ruleIdx), zap.Int("nextRuleIdx", nextRule))

	obj, err := p.fetchObjectFromECRule(ctx, buf, nodes, parentHeader, ecRules, ruleIdx, nextRule)
	if err != nil {
		return nil, fmt.Errorf("could not find enough (%d) EC parts but restoring from %d EC rule also failed: %w", requiredNum, nextRule, err)
	}
	if len(obj) < needLen {
		obj = obj[:needLen]
	}

	parts, err := fillParityParts(obj, rule, int(partLen))
	if err != nil {
		return nil, fmt.Errorf("missing parity EC parts cannot be re-calculated: %w", err)
	}

	return obj, p.recreateECParts(ctx, parentHeader, rule, ruleIdx, parts, missingParts)
}

func parityIndexes(rule iec.Rule) []int {
	res := make([]int, 0, rule.ParityPartNum)
	for i := int(rule.DataPartNum); i < int(rule.ParityPartNum+rule.DataPartNum); i++ {
		res = append(res, i)
	}
	return res
}

func fillParityParts(obj []byte, rule iec.Rule, partLen int) ([][]byte, error) {
	var (
		parityIdxs = parityIndexes(rule)
		parts      = sliceECParts(obj, partLen, rule, parityIdxs)
	)
	if err := iec.DecodeIndexes(rule, parts, parityIdxs); err != nil {
		return nil, fmt.Errorf("missing EC parts cannot be re-calculated: %w", err)
	}

	return parts, nil
}

// sliceECParts uses the buffer and slice it according to the EC rule.
// Parity parts point to the right buffer places but have zero length.
func sliceECParts(buf []byte, partLn int, rule iec.Rule, missingParts []int) [][]byte {
	var (
		totalParts = int(rule.DataPartNum + rule.ParityPartNum)
		res        = make([][]byte, totalParts)
	)
	for part := range totalParts {
		res[part] = buf[partLn*part : partLn*(part+1) : partLn*(part+1)]
	}
	for _, missing := range missingParts {
		res[missing] = res[missing][:0:partLn]
	}

	return res
}

// returns part ID and parent header.
func (p *Policer) headECPart(ctx context.Context, timeout time.Duration, node netmap.NodeInfo, cnr cid.ID, parent oid.ID,
	ruleIdx, partIdx int) (object.Object, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	hdr, err := p.apiConns.headObject(ctx, node, oid.NewAddress(cnr, parent), false, iec.PartInfo{
		RuleIndex: ruleIdx,
		Index:     partIdx,
	})
	if err != nil {
		return object.Object{}, err
	}

	if got := hdr.GetParentID(); got != parent {
		return object.Object{}, fmt.Errorf("wrong parent ID in received object %s", got)
	}

	if err = checkECAttributesInReceivedObject(hdr, ruleIdx, partIdx); err != nil {
		return object.Object{}, err
	}

	return hdr, nil
}

func (p *Policer) recreateECParts(ctx context.Context, parent object.Object, rule iec.Rule, ruleIdx int, parts [][]byte, missingIdx []int) error {
	sortedNodeLists, _, ecRules, err := p.network.GetNodesForObject(oid.NewAddress(parent.GetContainerID(), parent.GetID()))
	if err != nil {
		p.log.Error("failed to select nodes for EC parent to recreate its parts",
			zap.Stringer("container", parent.GetContainerID()), zap.Stringer("parent", parent.GetID()),
			zap.Stringer("rule", rule), zap.Error(err))
		return err
	}

	if ruleIdx >= len(ecRules) {
		return fmt.Errorf("rule index overflows total number of EC rules in policy: ruleIdx: %d, rules length: %d", ruleIdx, len(ecRules))
	}

	return p.recreateECPartsIdx(ctx, parent, rule, ruleIdx, sortedNodeLists[ruleIdx], parts, missingIdx)
}

func (p *Policer) recreateECPartsIdx(ctx context.Context, parent object.Object, rule iec.Rule, ruleIdx int, sortedNodes []netmap.NodeInfo,
	parts [][]byte, missingIdx []int) error {
	if err := iec.DecodeIndexes(rule, parts, missingIdx); err != nil { // should never happen if we count parts correctly
		return fmt.Errorf("missing EC parts cannot be re-calculated: %w", err)
	}

	var wg errgroup.Group

	for _, partIdx := range missingIdx {
		wg.Go(func() error {
			return p.recreateECPart(ctx, parent, rule, ruleIdx, partIdx, parts[partIdx], sortedNodes)
		})
	}

	return wg.Wait()
}

func (p *Policer) recreateECPart(ctx context.Context, parent object.Object, rule iec.Rule, ruleIdx, partIdx int, part []byte, sortedNodes []netmap.NodeInfo) error {
	partObj, err := iec.FormObjectForECPart(p.signer, parent, part, iec.PartInfo{
		RuleIndex: ruleIdx,
		Index:     partIdx,
	})
	if err != nil {
		return fmt.Errorf("failed to form object with %d restored EC part: %w", partIdx, err)
	}

	totalParts := int(rule.DataPartNum) + int(rule.ParityPartNum)
	seq := slices.Collect(iec.NodeSequenceForPart(partIdx, totalParts, len(sortedNodes)))
	nodes := make([]netmap.NodeInfo, len(seq))
	for i, idx := range seq {
		nodes[i] = sortedNodes[idx]
	}

	var repRes singleReplication
	var task replicator.Task
	task.SetObject(&partObj)
	task.SetObjectAddress(oid.NewAddress(parent.GetContainerID(), partObj.GetID()))
	task.SetCopiesNumber(1)
	task.SetNodes(nodes)
	p.replicator.HandleTask(ctx, task, &repRes)

	if !repRes.done {
		p.log.Info("failed to put recreated EC part on any node",
			zap.Stringer("container", parent.GetContainerID()), zap.Stringer("parent", parent.GetID()),
			zap.Stringer("rule", rule), zap.Int("part_idx", partIdx), zap.Stringer("part_id", partObj.GetID()))

		return fmt.Errorf("failed to put recreated %d EC part on any node", partIdx)
	}

	p.metrics.IncPolicerObjectReplicated(true)
	p.log.Info("EC part successfully recreated",
		zap.Stringer("container", parent.GetContainerID()), zap.Stringer("parent", parent.GetID()),
		zap.Stringer("rule", rule), zap.Int("part_idx", partIdx), zap.Stringer("part_id", partObj.GetID()),
		zap.Strings("node", repRes.netAddresses))

	return nil
}

func checkECAttributesInReceivedObject(hdr object.Object, ruleIdx, partIdx int) error {
	// copy-paste from GET service
	var found uint8
	const expected = 2

	attrs := hdr.Attributes()
	for i := range attrs {
		switch attrs[i].Key() {
		default:
			continue
		case object.AttributeECRuleIndex:
			if attrs[i].Value() != strconv.Itoa(ruleIdx) {
				return fmt.Errorf("wrong EC rule index attribute in received object for part: requested %v, got %q", ruleIdx, attrs[i].Value())
			}
		case object.AttributeECPartIndex:
			if attrs[i].Value() != strconv.Itoa(partIdx) {
				return fmt.Errorf("wrong EC part index attribute in received object for part: requested %v, got %q", partIdx, attrs[i].Value())
			}
		}

		found++
		if found == expected {
			return nil
		}
	}

	return fmt.Errorf("not all EC attributes received: requested %d, got %d", expected, found)
}
