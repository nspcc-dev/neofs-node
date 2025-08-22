package getsvc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync/atomic"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	islices "github.com/nspcc-dev/neofs-node/internal/slices"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// looks up for local object that carries EC part produced within cnr for parent
// object and indexed by pi, and writes it into dst.
//
// Returns [apistatus.ErrObjectAlreadyRemoved] if the object was marked for
// removal. Returns [apistatus.ErrObjectNotFound] if the object is missing.
func (s *Service) copyLocalECPart(dst ObjectWriter, cnr cid.ID, parent oid.ID, pi iec.PartInfo) error {
	hdr, rc, err := s.localObjects.GetECPart(cnr, parent, pi)
	if err != nil {
		return fmt.Errorf("get object from local storage: %w", err)
	}
	defer rc.Close()

	// TODO: sync with https://github.com/nspcc-dev/neofs-node/pull/3466
	if err := copyObjectStream(dst, hdr, rc); err != nil {
		return fmt.Errorf("copy object: %w", err)
	}

	return nil
}

func (s *Service) copyECObject(ctx context.Context, cnr cid.ID, parent oid.ID, sTok *session.Object, bTok *bearer.Token,
	ecRules []iec.Rule, sortedNodeLists [][]netmap.NodeInfo, dst ObjectWriter) error {
	obj, err := s.restoreFromECParts(ctx, cnr, parent, sTok, bTok, ecRules, sortedNodeLists)
	if err != nil {
		return err
	}

	// TODO: sync with https://github.com/nspcc-dev/neofs-node/pull/3466
	if err := copyObject(dst, obj); err != nil {
		return fmt.Errorf("copy object: %w", err)
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
func (s *Service) restoreFromECParts(ctx context.Context, cnr cid.ID, parent oid.ID, sTok *session.Object, bTok *bearer.Token,
	rules []iec.Rule, sortedNodeLists [][]netmap.NodeInfo) (object.Object, error) {
	// TODO: sort EC rules by complexity and try simpler ones first
	for i := range rules {
		obj, err := s.restoreFromECPartsByRule(ctx, cnr, parent, sTok, bTok, rules[i], i, sortedNodeLists[i])
		if err == nil {
			return obj, nil
		}
		if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) {
			return object.Object{}, err
		}

		s.log.Info("failed to restore object by EC rule", zap.Stringer("rule", rules[i]), zap.Int("ruleIdx", i), zap.Error(err)) // TODO: more fields
	}

	return object.Object{}, apistatus.ErrObjectNotFound
}

// reads object by (cnr, id) which should be partitioned across specified nodes
// according to EC rule with index = ruleIdx from cnr policy.
//
// The sortedNodes are sorted by parent. It has at least [iec.Rule.DataPartsNum]
// + [iec.Rule.ParityPartsNum] elements.
//
// Returns [apistatus.ErrObjectAlreadyRemoved] if the object was marked for
// removal.
func (s *Service) restoreFromECPartsByRule(ctx context.Context, cnr cid.ID, parent oid.ID, sTok *session.Object, bTok *bearer.Token,
	rule iec.Rule, ruleIdx int, sortedNodes []netmap.NodeInfo) (object.Object, error) {
	var hdr object.Object
	var gotHdr atomic.Bool
	parts := make([][]byte, rule.DataPartNum+rule.ParityPartNum)

	// TODO: If some servers hang, they can waste the entire context. If there are no more than rule.ParityPartNum,
	//  and parity servers work fast, availability can still be provided. Right now, for example, if one server
	//  responds after the context deadline, whole operation fails. Think how this can be accurately improved.
	eg, gCtx := errgroup.WithContext(ctx)

	// TODO: explore streaming options. See reedsolomon.NewStream. Cmp performance. https://github.com/nspcc-dev/neofs-node/issues/3501
	// TODO: abort insta when more than rule.ParityParts failures happen
	// TODO: if routine in the loop fails, we may already be in a situation when we have to get parity parts.
	//   If so, it's better to do it in the same routine.
	for i := range int(rule.DataPartNum) {
		partIdx := i
		eg.Go(func() error {
			par, part, err := s.getECPart(gCtx, cnr, parent, sTok, bTok, rule, ruleIdx, sortedNodes, partIdx)
			if err != nil {
				if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) || errors.Is(err, context.Canceled) {
					return err
				}

				s.log.Info("failed to get EC data part", zap.Int("partIdx", partIdx), zap.Error(err)) // TODO: more fields

				return nil
			}

			parts[partIdx] = part

			if !gotHdr.Swap(true) {
				hdr = par
			}

			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return object.Object{}, err
	}

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

	eg, gCtx = errgroup.WithContext(ctx)

	// TODO: when rem < rule.ParityParts, getting rem parity parts is most likely faster
	// TODO: abort when more than rule.ParityParts failures happen (same as for data parts stage)
	// TODO: loop is very similar. Try share code
	for i := range rule.ParityPartNum {
		partIdx := int(rule.DataPartNum + i)
		eg.Go(func() error {
			_, part, err := s.getECPart(gCtx, cnr, parent, sTok, bTok, rule, ruleIdx, sortedNodes, partIdx)
			if err != nil {
				if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) {
					return err
				}
				if errors.Is(err, context.Canceled) {
					// either original context is cancelled or other routine returned an error
					return nil
				}

				s.log.Info("failed to get EC parity part", zap.Int("partIdx", partIdx), zap.Error(err)) // TODO: more fields

				return nil
			}

			parts[partIdx] = part

			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return object.Object{}, err
	}

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
//
// Can return [context.Canceled] from the passed ctx only.
func (s *Service) getECPart(ctx context.Context, cnr cid.ID, parent oid.ID, sTok *session.Object, bTok *bearer.Token,
	rule iec.Rule, ruleIdx int, sortedNodes []netmap.NodeInfo, partIdx int) (object.Object, []byte, error) {
	var hdr object.Object
	var rc io.ReadCloser

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
			hdr, rc, err = s.localObjects.GetECPart(cnr, parent, pi)
		} else {
			hdr, rc, err = s.getECPartFromNode(ctx, cnr, parent, sTok, bTok, pi, sortedNodes[i])
		}
		if err == nil {
			break
		}
		if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) {
			return object.Object{}, nil, err
		}
		if errors.Is(err, context.Canceled) {
			return object.Object{}, nil, err
		}

		if !errors.Is(err, apistatus.ErrObjectNotFound) {
			s.log.Info("failed to get EC part from node, continue...", zap.Bool("local", local), zap.Stringer("container", cnr),
				zap.Stringer("parent", parent), zap.Error(err)) // TODO: more fields
		}
	}
	if err != nil {
		return object.Object{}, nil, errors.New("all nodes failed")
	}

	defer rc.Close()

	parHdr := hdr.Parent()
	if parHdr == nil {
		return object.Object{}, nil, errors.New("missing parent header in object for part")
	}

	ln := hdr.PayloadSize()
	if ln == 0 {
		return hdr, nil, nil
	}

	buf := make([]byte, ln)
	if _, err := io.ReadFull(rc, buf); err != nil {
		if errors.Is(err, io.EOF) { // empty payload is caught above
			err = io.ErrUnexpectedEOF
		}
		return object.Object{}, nil, fmt.Errorf("read full payload: %w", err)
	}

	return *parHdr, buf, nil
}

// queries object that carries EC part produced within cnr for parent object and
// indexed by pi from node. On success, returns header and payload reader.
//
// Returns [apistatus.ErrObjectAlreadyRemoved] if the object was marked for
// removal. Returns [apistatus.ErrObjectNotFound] if the object is missing.
//
// Can return [context.Canceled] from the passed ctx only.
//
// RPC errors include network addresses.
func (s *Service) getECPartFromNode(ctx context.Context, cnr cid.ID, parent oid.ID, sTok *session.Object, bTok *bearer.Token,
	pi iec.PartInfo, node netmap.NodeInfo) (object.Object, io.ReadCloser, error) {
	localNodeKey, err := s.keyStore.GetKey(nil)
	if err != nil {
		return object.Object{}, nil, fmt.Errorf("get local SN private key: %w", err)
	}

	ruleIdxAttr := strconv.Itoa(pi.RuleIndex)
	partIdxAttr := strconv.Itoa(pi.Index)

	// TODO: this must be stated in https://github.com/nspcc-dev/neofs-api
	hdr, rc, err := s.conns.InitGetObjectStream(ctx, node, *localNodeKey, cnr, parent, sTok, bTok, true, false, []string{
		iec.AttributeRuleIdx, ruleIdxAttr,
		iec.AttributePartIdx, partIdxAttr,
	})
	if err != nil {
		return object.Object{}, nil, fmt.Errorf("get object from remote SN: %w", err)
	}

	if got := hdr.GetParentID(); got != parent {
		return object.Object{}, nil, fmt.Errorf("wrong parent ID in received object for part: requested %s, got %s", got, parent)
	}

	if err := checkECAttributesInReceivedObject(hdr, ruleIdxAttr, partIdxAttr); err != nil {
		return object.Object{}, nil, err
	}

	return hdr, rc, nil
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
