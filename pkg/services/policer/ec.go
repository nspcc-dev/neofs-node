package policer

import (
	"context"
	"errors"
	"slices"
	"time"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

func (p *Policer) processECPart(ctx context.Context, addr oid.Address, pi iec.PartInfo, ecRules []iec.Rule, nodeLists [][]netmap.NodeInfo) {
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
		_, err := p.apiConns.headObject(callCtx, nodes[i], addr)
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
