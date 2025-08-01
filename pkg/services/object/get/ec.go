package getsvc

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/klauspost/reedsolomon"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

func (s *Service) restoreFromECParts(ctx context.Context, cnr cid.ID, id oid.ID, ecRules []ECRule, sortedNodeLists [][]netmap.NodeInfo) (object.Object, error) {
	// TODO: sort EC rules by complexity and try simpler ones first
	for i := range ecRules {
		obj, err := s.restoreFromECPartsByRule(ctx, cnr, id, ecRules[i], i, sortedNodeLists[i])
		if err == nil {
			return obj, nil
		}
		s.log.Info("failed to restore object by EC rule", zap.Stringer("rule", ecRules[i]), zap.Int("ruleIdx", i), zap.Error(err))
	}

	return object.Object{}, errors.New("all EC rules failed") // TODO: return first or all errors
}

func (s *Service) restoreFromECPartsByRule(ctx context.Context, cnr cid.ID, parent oid.ID, rule ECRule, ruleIdx int, sortedNodes []netmap.NodeInfo) (object.Object, error) {
	var hdr object.Object
	var gotHdr atomic.Bool
	var wg sync.WaitGroup
	payloadParts := make([][]byte, rule.DataParts+rule.ParityParts)

	for i := range int(rule.DataParts) {
		wg.Add(1)
		go func(partIdx int) {
			defer wg.Done()

			obj, err := s.getECPartByIdx(ctx, cnr, parent, ruleIdx, partIdx, sortedNodes)
			if err != nil {
				s.log.Info("failed to get EC part by idx", zap.Int("partIdx", partIdx), zap.Error(err)) // TODO: more fields
				return
			}

			payloadParts[partIdx] = obj.Payload()

			if !gotHdr.Swap(true) {
				hdr = obj
			}
		}(i)
	}
	// TODO: abort when more than rule.ParityParts failures happen
	// TODO: if GET in routine fails, and it makes sense to get parity parts => do this insta

	wg.Wait()

	if !gotHdr.Load() {
		return object.Object{}, errors.New("failed to get any part")
	}

	if hdr.PayloadSize() == 0 { // TODO: enough to get any single part in this case
		return hdr, nil
	}

	var rem uint8
	for i := range rule.DataParts {
		if payloadParts[i] == nil {
			rem++
		}
	}
	if rem > rule.ParityParts {
		return object.Object{}, fmt.Errorf("%d parts unavailable", rem)
	}
	if rem == 0 {
		// TODO: if response is streamed, concatenation may be avoided
		hdr.SetPayload(slices.Concat(payloadParts[:rule.DataParts]...))
		return hdr, nil
	}

	// FIXME: get parity parts here

	// TODO: Explore reedsolomon.Option for performance improvement.
	dec, err := reedsolomon.New(int(rule.DataParts), int(rule.ParityParts))
	if err != nil { // should never happen
		return object.Object{}, fmt.Errorf("init EC decoder: %w", err)
	}
	required := make([]bool, rule.DataParts+rule.ParityParts)
	for i := range rule.DataParts {
		required[i] = true
	}
	if err := dec.ReconstructSome(payloadParts, required); err != nil {
		return object.Object{}, fmt.Errorf("failed to restore payload from EC parts: %w", err)
	}

	// TODO: if response is streamed, concatenation may be avoided
	hdr.SetPayload(slices.Concat(payloadParts[:rule.DataParts]...))
	return hdr, nil
}

func (s *Service) getECPartByIdx(ctx context.Context, cnr cid.ID, parent oid.ID, ruleIdx, partIdx int, sortedNodes []netmap.NodeInfo) (object.Object, error) {
	panic("unimplemented")
}

func (s *Service) getECPartByIdxLocally(cnr cid.ID, parent oid.ID, ruleIdx, partIdx int) (object.Object, error) {
	return s.localObjects.GetECPartByIdx(cnr, parent, ruleIdx, partIdx)
}
