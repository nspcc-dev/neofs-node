package getsvc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	islices "github.com/nspcc-dev/neofs-node/internal/slices"
	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
)

// TODO: abort all iterations when external context is done

// looks up for local object that carries EC part produced within cnr for parent
// object and indexed by pi, and writes it into dst.
//
// Returns [apistatus.ErrObjectAlreadyRemoved] if the object was marked for
// removal. Returns [apistatus.ErrObjectNotFound] if the object is missing.
func (s *Service) copyLocalECPart(cnr cid.ID, parent oid.ID, pi iec.PartInfo, dst ObjectWriter) error {
	obj, err := s.localObjects.GetECPart(cnr, parent, pi)
	if err != nil {
		return fmt.Errorf("get object from local storage: %w", err)
	}

	if err := dst.WriteHeader(&obj); err != nil {
		return fmt.Errorf("write object header to destination stream: %w", err)
	}

	if err := dst.WriteChunk(obj.Payload()); err != nil {
		return fmt.Errorf("write object payload to destination stream: %w", err)
	}

	return nil
}

// calls restoreFromECParts and writes the result into dst.
func (s *Service) copyECObject(ctx context.Context, cnr cid.ID, id oid.ID, ecRules []iec.Rule, sortedNodeLists [][]netmap.NodeInfo, dst ObjectWriter) error {
	obj, err := s.restoreFromECParts(ctx, cnr, id, ecRules, sortedNodeLists)
	if err != nil {
		return err
	}

	if err := dst.WriteHeader(&obj); err != nil {
		return fmt.Errorf("write object header to destination stream: %w", err)
	}

	if err := dst.WriteChunk(obj.Payload()); err != nil {
		return fmt.Errorf("write object payload to destination stream: %w", err)
	}

	return nil
}

// reads object by (cnr, id) which should be partitioned across specified nodes
// according to EC rules from cnr policy, and writes it into dst.
//
// The ecRules and sortedNodeLists correspond to each other. All sortedNodeLists
// are sorted by id. Each node list has at least [iec.Rule.DataPartsNum] +
// [iec.Rule.ParityPartsNum] elements.
//
// Returns [apistatus.ErrObjectAlreadyRemoved] if the object was marked for
// removal. Returns [apistatus.ErrObjectNotFound] otherwise.
func (s *Service) restoreFromECParts(ctx context.Context, cnr cid.ID, id oid.ID, rules []iec.Rule, sortedNodeLists [][]netmap.NodeInfo) (object.Object, error) {
	// TODO: sort EC rules by complexity and try simpler ones first
	for i := range rules {
		obj, err := s.restoreFromECPartsByRule(ctx, cnr, id, rules[i], i, sortedNodeLists[i])
		if err == nil {
			return obj, nil
		}
		if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) {
			return object.Object{}, err
		}

		s.log.Info("failed to restore object by EC rule", zap.Stringer("rule", rules[i]), zap.Int("ruleIdx", i), zap.Error(err)) // TODO: more fields
	}

	return object.Object{}, fmt.Errorf("%w: processing of all EC rules failed", apistatus.ErrObjectNotFound)
}

// reads object by (cnr, id) which should be partitioned across specified nodes
// according to EC rule with index = ruleIdx from cnr policy.
//
// The sortedNodes are sorted by parent. It has at least [iec.Rule.DataPartsNum]
// + [iec.Rule.ParityPartsNum] elements.
//
// Returns [apistatus.ErrObjectAlreadyRemoved] if the object was marked for
// removal.
func (s *Service) restoreFromECPartsByRule(ctx context.Context, cnr cid.ID, id oid.ID, rule iec.Rule, ruleIdx int, sortedNodes []netmap.NodeInfo) (object.Object, error) {
	var hdr object.Object
	var gotHdr atomic.Bool
	var wg sync.WaitGroup
	parts := make([][]byte, rule.DataPartNum+rule.ParityPartNum)

	// FIXME: if one of the servers hangs, it eats up the entire context
	// FIXME: if 'already removed' error => abort and forward it
	for i := range int(rule.DataPartNum) {
		wg.Add(1)
		go func(partIdx int) {
			defer wg.Done()

			par, part, err := s.getECPart(ctx, cnr, id, rule, ruleIdx, sortedNodes, partIdx)
			if err != nil {
				s.log.Info("failed to get EC data part", zap.Int("partIdx", partIdx), zap.Error(err)) // TODO: more fields
				return
			}

			parts[partIdx] = part

			if !gotHdr.Swap(true) {
				hdr = par
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

	rem := islices.CountNilsInTwoDimSlice(parts[:rule.DataPartNum])
	if rem > int(rule.ParityPartNum) {
		return object.Object{}, fmt.Errorf("%d data parts unavailable", rem)
	}
	if rem == 0 {
		// TODO: if response is streamed, concatenation may be avoided.
		//  Keep in sync with https://github.com/nspcc-dev/neofs-node/pull/3466.
		payload := iec.ConcatDataParts(rule, hdr.PayloadSize(), parts)
		hdr.SetPayload(payload)
		return hdr, nil
	}

	// TODO: getting rem parts is most likely faster
	for i := range rule.ParityPartNum {
		wg.Add(1)
		go func(partIdx int) {
			defer wg.Done()

			_, pldPart, err := s.getECPart(ctx, cnr, id, rule, ruleIdx, sortedNodes, partIdx)
			if err != nil {
				s.log.Info("failed to get EC parity part", zap.Int("partIdx", partIdx), zap.Error(err)) // TODO: more fields
				return
			}

			parts[partIdx] = pldPart
		}(int(rule.DataPartNum + i))
	}
	wg.Wait()
	// TODO: abort when more than rule.ParityParts failures happen (same as for data parts stage)

	if rem = islices.CountNilsInTwoDimSlice(parts); rem > int(rule.ParityPartNum) {
		return object.Object{}, fmt.Errorf("%d parts unavailable", rem)
	}

	payload, err := iec.Decode(rule, hdr.PayloadSize(), parts)
	if err != nil {
		return object.Object{}, fmt.Errorf("decode payload from parts: %w", err)
	}

	hdr.SetPayload(payload)
	return hdr, nil
}

// reads object for EC part which should produced for parent object and
// distributed sortedNodes according to EC rule with index = ruleIdx in cnr
// policy. Returns parent object header and part payload.
//
// Returns [apistatus.ErrObjectAlreadyRemoved] if the object was marked for
// removal.
func (s *Service) getECPart(ctx context.Context, cnr cid.ID, parent oid.ID, rule iec.Rule, ruleIdx int, sortedNodes []netmap.NodeInfo, partIdx int) (object.Object, []byte, error) {
	var part object.Object

	pi := iec.PartInfo{
		RuleIndex: ruleIdx,
		Index:     partIdx,
	}

	var err error
	var local, localDone bool
	for i := range iec.NodeSequenceForPart(partIdx, int(rule.DataPartNum+rule.ParityPartNum), len(sortedNodes)) {
		local = !localDone && s.neoFSNet.IsLocalNodePublicKey(sortedNodes[i].PublicKey())
		if local {
			localDone = true
		}

		if local {
			part, err = s.localObjects.GetECPart(cnr, parent, pi)
		} else {
			part, err = s.getECPartFromNode(ctx, cnr, parent, pi, sortedNodes[i])
		}
		if err == nil {
			break
		}
		if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) {
			return object.Object{}, nil, err
		}

		if !errors.Is(err, apistatus.ErrObjectNotFound) {
			s.log.Info("failed to get EC part from node, continue...", zap.Bool("local", local), zap.Stringer("container", cnr),
				zap.Stringer("parent", parent), zap.Error(err))
		}
	}
	if err != nil {
		return object.Object{}, nil, errors.New("all nodes failed")
	}

	parHdr := part.Parent()
	if parHdr == nil {
		return object.Object{}, nil, errors.New("missing parent header in object for part")
	}

	return *parHdr, part.Payload(), nil
}

// queries object that carries EC part produced within cnr for parent object and
// indexed by pi from node.
//
// Returns [apistatus.ErrObjectAlreadyRemoved] if the object was marked for
// removal. Returns [apistatus.ErrObjectNotFound] if the object is missing.
//
// RPC errors include network addresses.
func (s *Service) getECPartFromNode(ctx context.Context, cnr cid.ID, parent oid.ID, pi iec.PartInfo, node netmap.NodeInfo) (object.Object, error) {
	// TODO: code is copied from pkg/services/object/get/container.go:63. Worth sharing?
	// TODO: we may waste resources doing this per request. Make once on network map change instead.
	var ag network.AddressGroup
	if err := ag.FromIterator(network.NodeEndpointsIterator(node)); err != nil {
		return object.Object{}, fmt.Errorf("decode SN network addresses: %w", err)
	}

	localNodeKey, err := s.keyStore.GetKey(nil)
	if err != nil {
		return object.Object{}, fmt.Errorf("get local SN private key: %w", err)
	}

	var ni clientcore.NodeInfo
	ni.SetAddressGroup(ag)
	ni.SetPublicKey(node.PublicKey())
	conn, err := s.conns.Get(ni)
	if err != nil {
		return object.Object{}, fmt.Errorf("get connection to remote SN: %w", err)
	}

	ruleIdxAttr := strconv.Itoa(pi.RuleIndex)
	partIdxAttr := strconv.Itoa(pi.Index)

	var opts client.PrmObjectGet
	opts.MarkLocal()
	opts.SkipChecksumVerification() // TODO: only OID equality check should be skipped
	opts.WithXHeaders(              // TODO: this must be stated in https://github.com/nspcc-dev/neofs-api
		iec.AttributeRuleIdx, ruleIdxAttr,
		iec.AttributePartIdx, partIdxAttr,
	)
	// FIXME: access tokens
	hdr, pr, err := conn.ObjectGetInit(ctx, cnr, parent, user.NewAutoIDSigner(*localNodeKey), opts)
	if err != nil {
		return object.Object{}, fmt.Errorf("get object API (header): %w", err)
	}

	if got := hdr.GetParentID(); got != parent {
		return object.Object{}, fmt.Errorf("wrong parent ID in received object for part: requested %s, got %s", got, parent)
	}

	if err := checkECAttributesInReceivedObject(hdr, ruleIdxAttr, partIdxAttr); err != nil {
		return object.Object{}, err
	}

	if hdr.PayloadSize() == 0 {
		return hdr, nil
	}

	pld := make([]byte, hdr.PayloadSize())
	if _, err := io.ReadFull(pr, pld); err != nil {
		return object.Object{}, fmt.Errorf("get object API (payload): %w", err)
	}

	hdr.SetPayload(pld)
	return hdr, nil
}

// returns [iec.PartInfo.RuleIndex] = -1 if request is not for particular EC part.
func checkECPartInfoRequest(xHdrs []string) (iec.PartInfo, error) {
	var res iec.PartInfo

	var ruleIdxStr, partIdxStr string
	for i := 0; i < len(xHdrs); i += 2 {
		if xHdrs[i] == iec.AttributeRuleIdx {
			ruleIdxStr = xHdrs[i+1]
		}
		if xHdrs[i] == iec.AttributePartIdx {
			partIdxStr = xHdrs[i+1]
		}
		if ruleIdxStr != "" && partIdxStr != "" {
			break
		}
	}

	if ruleIdxStr == "" && partIdxStr == "" {
		res.RuleIndex = -1
		return res, nil
	}

	if (ruleIdxStr == "") != (partIdxStr == "") {
		return res, fmt.Errorf("%s and %s X-headers must be set together", iec.AttributeRuleIdx, iec.AttributePartIdx)
	}

	// TODO: state limits in https://github.com/nspcc-dev/neofs-api. Share consts for them.
	ruleIdx, err := strconv.ParseUint(ruleIdxStr, 10, 8)
	if err != nil {
		return res, fmt.Errorf("invalid %s X-header: %w", iec.AttributeRuleIdx, err)
	}

	partIdx, err := strconv.ParseUint(partIdxStr, 10, 8)
	if err != nil {
		return res, fmt.Errorf("invalid %s X-header: %w", iec.AttributePartIdx, err)
	}

	res.RuleIndex = int(ruleIdx)
	res.Index = int(partIdx)

	return res, nil
}

func checkECAttributesInReceivedObject(hdr object.Object, ruleIdx, partIdx string) error {
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
