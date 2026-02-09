package policer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"sync"
	"time"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

func (p *Policer) processECPart(ctx context.Context, addr oid.Address, parent oid.ID, pi iec.PartInfo, ecRules []iec.Rule, nodeLists [][]netmap.NodeInfo) {
	if pi.RuleIndex >= len(ecRules) {
		p.log.Warn("local object with invalid EC rule index detected, deleting",
			zap.Stringer("object", addr), zap.Int("ruleIdx", pi.RuleIndex), zap.Int("totalRules", len(ecRules)))
		if err := p.localStorage.Delete(addr); err != nil {
			p.log.Error("failed to delete local object with invalid EC rule index",
				zap.Stringer("object", addr), zap.Error(err))
		}
		return
	}

	rule := ecRules[pi.RuleIndex]
	if pi.Index >= int(rule.DataPartNum+rule.ParityPartNum) {
		p.log.Warn("local object with invalid EC part index detected, deleting",
			zap.Stringer("object", addr), zap.Stringer("rule", rule), zap.Int("partIdx", pi.Index))
		if err := p.localStorage.Delete(addr); err != nil {
			p.log.Error("failed to delete local object with invalid EC part index",
				zap.Stringer("object", addr), zap.Error(err))
		}
		return
	}

	p.tryScheduleCheckECPartsTask(ctx, addr.Container(), parent, rule, addr.Object(), pi)

	p.processECPartByRule(ctx, rule, addr, pi.Index, nodeLists[pi.RuleIndex])
}

func (p *Policer) processECPartByRule(ctx context.Context, rule iec.Rule, addr oid.Address, partIdx int, nodes []netmap.NodeInfo) {
	var candidates []netmap.NodeInfo
	var maintenance bool
	headTimeout := time.Duration(-1)

	for i := range iec.NodeSequenceForPart(partIdx, int(rule.DataPartNum+rule.ParityPartNum), len(nodes)) {
		if p.network.IsLocalNodePublicKey(nodes[i].PublicKey()) {
			if len(candidates) == 0 {
				p.log.Debug("local node is optimal for EC part, hold",
					zap.Stringer("cid", addr.Container()), zap.Stringer("partOID", addr.Object()),
					zap.Stringer("rule", rule), zap.Int("partIdx", partIdx))
				return
			}
			break
		}

		if headTimeout < 0 {
			headTimeout = p.getHeadTimeout()
		}

		callCtx, cancel := context.WithTimeout(ctx, headTimeout)
		_, err := p.apiConns.headObject(callCtx, nodes[i], addr, true, nil)
		cancel()

		if err == nil {
			p.log.Info("EC part header successfully received from more optimal node, drop",
				zap.Stringer("cid", addr.Container()), zap.Stringer("partOID", addr.Object()),
				zap.Stringer("rule", rule), zap.Int("partIdx", partIdx),
				zap.Strings("node", slices.Collect(nodes[i].NetworkEndpoints())))
			p.dropRedundantLocalObject(addr)
			return
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
		return
	}

	if len(candidates) == 0 {
		p.log.Info("local node is suboptimal for EC part but now there are no other candidates, hold",
			zap.Stringer("cid", addr.Container()), zap.Stringer("partOID", addr.Object()),
			zap.Stringer("rule", rule), zap.Int("partIdx", partIdx))
		return
	}

	p.log.Info("local node is suboptimal for EC part, moving to more optimal node...",
		zap.Stringer("cid", addr.Container()), zap.Stringer("partOID", addr.Object()),
		zap.Stringer("rule", rule), zap.Int("partIdx", partIdx), zap.Int("candidateNum", len(candidates)))

	var repRes singleReplication
	p.tryToReplicate(ctx, addr, 1, candidates, &repRes)
	if repRes.done {
		p.log.Info("EC part successfully moved to more optimal node, drop",
			zap.Stringer("cid", addr.Container()), zap.Stringer("partOID", addr.Object()),
			zap.Stringer("rule", rule), zap.Int("partIdx", partIdx), zap.Strings("newHolder", repRes.netAddresses))
		p.dropRedundantLocalObject(addr)
		return
	}

	p.log.Info("failed to move EC part to more optimal node, hold",
		zap.Stringer("cid", addr.Container()), zap.Stringer("partOID", addr.Object()),
		zap.Stringer("rule", rule), zap.Int("partIdx", partIdx), zap.Int("candidateNum", len(candidates)))
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

func (p *Policer) tryScheduleCheckECPartsTask(ctx context.Context, cnr cid.ID, parent oid.ID, rule iec.Rule, localPartID oid.ID, localPartInfo iec.PartInfo) {
	p.checkECPartsProgressMtx.Lock()
	defer p.checkECPartsProgressMtx.Unlock()

	addr := oid.NewAddress(cnr, parent)
	if _, ok := p.checkECPartsProgressMap[addr]; ok {
		return
	}

	err := p.checkECPartsWorkerPool.Submit(func() {
		defer func() {
			p.checkECPartsProgressMtx.Lock()
			delete(p.checkECPartsProgressMap, addr)
			p.checkECPartsProgressMtx.Unlock()
		}()

		p.checkECParts(ctx, cnr, parent, rule, localPartInfo.RuleIndex, localPartInfo.Index, localPartID)
	})
	if err != nil {
		if errors.Is(err, ants.ErrPoolOverload) {
			p.log.Info("pool of workers for EC parts checking is full, skip the task",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Stringer("local_part_id", localPartID))
			return
		}

		p.log.Warn("unexpected error returned from pool of workers for EC part checking", zap.Error(err))
		return
	}

	p.checkECPartsProgressMap[addr] = struct{}{}
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
	ruleIdxAttr := strconv.Itoa(ruleIdx)
	headTimeout := p.getHeadTimeout()
	sortedNodes := sortedNodeLists[len(repRules)+ruleIdx]

headNextPart:
	for partIdx := range totalParts {
		if partIdx == localPartIdx {
			mPartID[partIdx] = localPartID
			continue
		}

		var partIdxAttr string

		for nodeIdx := range iec.NodeSequenceForPart(partIdx, totalParts, len(sortedNodes)) {
			var hdr object.Object
			local := p.network.IsLocalNodePublicKey(sortedNodes[nodeIdx].PublicKey())
			if local {
				hdr, err = p.localStorage.HeadECPart(cnr, parent, iec.PartInfo{RuleIndex: ruleIdx, Index: partIdx})
			} else {
				if partIdxAttr == "" {
					partIdxAttr = strconv.Itoa(partIdx)
				}
				hdr, err = p.headECPart(ctx, headTimeout, sortedNodes[nodeIdx], cnr, parent, ruleIdxAttr, partIdxAttr)
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
				if len(missingIdx)+len(skipIdx) >= int(rule.ParityPartNum) {
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

		if len(missingIdx)+len(skipIdx) >= int(rule.ParityPartNum) {
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
	p.hadToReplicate.Store(true)

	if parentHdr.GetID().IsZero() {
		// can only happen for 1/1 rule: local part is never HEADed in for-loop above and remote one is unreachable
		hdr, err := p.localStorage.Head(parentAddr, false)
		if err != nil {
			p.log.Info("failed to get EC parent header locally",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Stringer("rule", rule),
				zap.Int("ruleIdx", ruleIdx), zap.Error(err))
			return
		}

		parentHdr = *hdr
		partLen = (parentHdr.PayloadSize() + uint64(rule.DataPartNum) - 1) / uint64(rule.DataPartNum)
	}

	parts := make([][]byte, totalParts)
	required := make([]bool, totalParts)

getNextPart:
	for partIdx := range totalParts {
		if slices.Contains(skipIdx, partIdx) {
			continue
		}
		if slices.Contains(missingIdx, partIdx) {
			required[partIdx] = true
			continue
		}

		partID, ok := mPartID[partIdx]
		if !ok {
			panic(fmt.Sprintf("missing ID of part#%d after successful HEAD", partIdx))
		}

		if partIdx == localPartIdx {
			b, err := p.localStorage.GetRange(oid.NewAddress(cnr, partID), 0, 0)
			if err == nil {
				parts[partIdx] = b
				continue
			}

			p.log.Info("failed to RANGE EC part from local storage",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Stringer("rule", rule),
				zap.Int("ruleIdx", ruleIdx), zap.Int("partIdx", partIdx), zap.Stringer("partID", partID), zap.Error(err))
		}

		var partIdxAttr string
		var off, ln uint64
		for nodeIdx := range iec.NodeSequenceForPart(partIdx, totalParts, len(sortedNodes)) {
			if p.network.IsLocalNodePublicKey(sortedNodes[nodeIdx].PublicKey()) {
				if partIdx == localPartIdx { // done above
					continue
				}

				b, err := p.localStorage.GetRange(oid.NewAddress(cnr, partID), off, ln)
				if err != nil {
					p.log.Info("failed to RANGE EC part from local storage",
						zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Stringer("rule", rule),
						zap.Int("ruleIdx", ruleIdx), zap.Int("partIdx", partIdx), zap.Stringer("partID", partID), zap.Error(err))
					continue
				}

				if parts[partIdx] == nil {
					parts[partIdx] = make([]byte, partLen)
				}

				copy(parts[partIdx][off:], b)
				continue getNextPart
			}

			if partIdxAttr == "" {
				partIdxAttr = strconv.Itoa(partIdx)
			}

			// TODO: this is the 1st place where we known IDs of EC parts in advance.
			//  Consider supporting direct requests of EC parts by ID, they are more lightweight.
			rc, err := p.apiConns.GetRange(ctx, sortedNodes[nodeIdx], cnr, parent, off, ln, []string{
				iec.AttributeRuleIdx, ruleIdxAttr,
				iec.AttributePartIdx, partIdxAttr,
			})
			if err != nil {
				p.log.Info("failed to open RANGE stream for EC part from remote node",
					zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Stringer("rule", rule),
					zap.Int("ruleIdx", ruleIdx), zap.Int("partIdx", partIdx), zap.Stringer("partID", partID), zap.Error(err))
				continue
			}

			defer rc.Close()

			if parts[partIdx] == nil {
				parts[partIdx] = make([]byte, partLen)
			}

			n, err := io.ReadFull(rc, parts[partIdx][off:])
			if err == nil {
				continue getNextPart
			}
			if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) {
				return
			}

			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}

			p.log.Info("failed to RANGE EC part from remote node",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Stringer("rule", rule),
				zap.Int("ruleIdx", ruleIdx), zap.Int("partIdx", partIdx),
				zap.Stringer("partID", partID), zap.Uint64("off", off), zap.Uint64("len", ln), zap.Error(err))

			if n > 0 {
				off += uint64(n)
				ln = partLen - off
			}
		}

		if len(missingIdx)+len(skipIdx) >= int(rule.ParityPartNum) {
			p.log.Warn("too many EC parts unavailable, recreation is impossible",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Stringer("rule", rule),
				zap.Int("unavailable", len(missingIdx)+len(skipIdx)))
			return
		}

		parts[partIdx] = nil

		// Exists and may be OK later, so do not recreate.
		skipIdx = append(skipIdx, partIdx)
	}

	p.recreateECParts(ctx, parentHdr, rule, ruleIdx, parts, missingIdx)
}

// returns part ID and parent header.
func (p *Policer) headECPart(ctx context.Context, timeout time.Duration, node netmap.NodeInfo, cnr cid.ID, parent oid.ID,
	ruleIdx, partIdx string) (object.Object, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	hdr, err := p.apiConns.headObject(ctx, node, oid.NewAddress(cnr, parent), false, []string{
		iec.AttributeRuleIdx, ruleIdx,
		iec.AttributePartIdx, partIdx,
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

func (p *Policer) recreateECParts(ctx context.Context, parent object.Object, rule iec.Rule, ruleIdx int, parts [][]byte, missingIdx []int) {
	sortedNodeLists, _, ecRules, err := p.network.GetNodesForObject(oid.NewAddress(parent.GetContainerID(), parent.GetID()))
	if err != nil {
		p.log.Error("failed to select nodes for EC parent to recreate its parts",
			zap.Stringer("container", parent.GetContainerID()), zap.Stringer("parent", parent.GetID()),
			zap.Stringer("rule", rule), zap.Error(err))
		return
	}

	if ruleIdx >= len(ecRules) {
		p.log.Error("rule index overflows total number of EC rules in policy",
			zap.Stringer("container", parent.GetContainerID()), zap.Stringer("parent", parent.GetID()),
			zap.Stringer("rule", rule), zap.Int("rule_idx", ruleIdx), zap.Int("total_rules", len(ecRules)))
		return
	}

	p.recreateECPartsIdx(ctx, parent, rule, ruleIdx, sortedNodeLists[ruleIdx], parts, missingIdx)
}

func (p *Policer) recreateECPartsIdx(ctx context.Context, parent object.Object, rule iec.Rule, ruleIdx int, sortedNodes []netmap.NodeInfo,
	parts [][]byte, missingIdx []int) {
	if err := iec.DecodeIndexes(rule, parts, missingIdx); err != nil { // should never happen if we count parts correctly
		p.log.Error("missing EC parts cannot be re-calculated",
			zap.Stringer("container", parent.GetContainerID()), zap.Stringer("parent", parent.GetID()),
			zap.Stringer("rule", rule), zap.Error(err))
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(missingIdx))

	for _, partIdx := range missingIdx {
		go func(partIdx int) {
			defer wg.Done()

			p.recreateECPart(ctx, parent, rule, ruleIdx, partIdx, parts[partIdx], sortedNodes)
		}(partIdx)
	}

	wg.Wait()
}

func (p *Policer) recreateECPart(ctx context.Context, parent object.Object, rule iec.Rule, ruleIdx, partIdx int, part []byte, sortedNodes []netmap.NodeInfo) {
	partObj, err := iec.FormObjectForECPart(p.signer, parent, part, iec.PartInfo{
		RuleIndex: ruleIdx,
		Index:     partIdx,
	})
	if err != nil {
		p.log.Info("failed to form object with restored EC part",
			zap.Stringer("container", parent.GetContainerID()), zap.Stringer("parent", parent.GetID()),
			zap.Stringer("rule", rule), zap.Int("part_idx", partIdx), zap.Error(err))
		return
	}

	for i := range iec.NodeSequenceForPart(partIdx, int(rule.DataPartNum)+int(rule.ParityPartNum), len(sortedNodes)) {
		if p.network.IsLocalNodePublicKey(sortedNodes[i].PublicKey()) {
			err = p.localStorage.Put(&partObj, nil)
			if err == nil {
				p.log.Info("EC part successfully recreated and put to local storage",
					zap.Stringer("container", parent.GetContainerID()), zap.Stringer("parent", parent.GetID()),
					zap.Stringer("rule", rule), zap.Int("part_idx", partIdx), zap.Stringer("part_id", partObj.GetID()))
				return
			}

			p.log.Info("failed to put recreated EC part to local storage",
				zap.Stringer("container", parent.GetContainerID()), zap.Stringer("parent", parent.GetID()),
				zap.Stringer("rule", rule), zap.Int("part_idx", partIdx), zap.Stringer("part_id", partObj.GetID()),
				zap.Error(err))
			continue
		}

		err = p.replicator.PutObjectToNode(ctx, partObj, sortedNodes[i])
		if err == nil {
			p.log.Info("EC part successfully recreated and put to remote node",
				zap.Stringer("container", parent.GetContainerID()), zap.Stringer("parent", parent.GetID()),
				zap.Stringer("rule", rule), zap.Int("part_idx", partIdx), zap.Stringer("part_id", partObj.GetID()),
				zap.String("node_pub", netmap.StringifyPublicKey(sortedNodes[i])))
			return
		}

		if errors.Is(err, ctx.Err()) {
			return
		}

		p.log.Info("failed to put recreated EC part to remote node",
			zap.Stringer("container", parent.GetContainerID()), zap.Stringer("parent", parent.GetID()),
			zap.Stringer("rule", rule), zap.Int("part_idx", partIdx), zap.Stringer("part_id", partObj.GetID()),
			zap.String("node_pub", netmap.StringifyPublicKey(sortedNodes[i])), zap.Error(err))
	}
}

func checkECAttributesInReceivedObject(hdr object.Object, ruleIdx, partIdx string) error {
	// copy-paste from GET service
	var found uint8
	const expected = 2

	attrs := hdr.Attributes()
	for i := range attrs {
		switch attrs[i].Key() {
		default:
			continue
		case iec.AttributeRuleIdx:
			if attrs[i].Value() != ruleIdx {
				return fmt.Errorf("wrong EC rule index attribute in received object for part: requested %q, got %q", ruleIdx, attrs[i].Value())
			}
		case iec.AttributePartIdx:
			if attrs[i].Value() != partIdx {
				return fmt.Errorf("wrong EC part index attribute in received object for part: requested %q, got %q", partIdx, attrs[i].Value())
			}
		}

		found++
		if found == expected {
			return nil
		}
	}

	return fmt.Errorf("not all EC attributes received: requested %d, got %d", expected, found)
}
