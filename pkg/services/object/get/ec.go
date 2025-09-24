package getsvc

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io"
	"math"
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

type tooManyPartsUnavailableError int

func (x tooManyPartsUnavailableError) Error() string {
	return strconv.Itoa(int(x)) + " data parts unavailable"
}

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
	// TODO: sort EC rules by complexity and try simpler ones first. Note that rule idxs passed as arguments must be kept.
	for i := range rules {
		obj, err := s.restoreFromECPartsByRule(ctx, cnr, parent, sTok, bTok, rules[i], i, sortedNodeLists[i])
		if err == nil {
			return obj, nil
		}
		if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) {
			return object.Object{}, err
		}

		s.log.Info("failed to restore object by EC rule",
			zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Stringer("rule", rules[i]),
			zap.Int("ruleIdx", i), zap.Error(err),
		)
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
	// TODO: explore streaming options. See reedsolomon.NewStream. Cmp performance. https://github.com/nspcc-dev/neofs-node/issues/3501
	// TODO: if routine in the loop fails, we may already be in a situation when we have to get parity parts.
	//   If so, it's better to do it in the same routine.
	eg, gCtx := errgroup.WithContext(ctx)
	var failCounter atomic.Uint32
	for i := range int(rule.DataPartNum) {
		partIdx := i
		eg.Go(func() error {
			parentHdr, partPayload, err := s.getECPart(gCtx, cnr, parent, sTok, bTok, rule, ruleIdx, sortedNodes, partIdx)
			if err != nil {
				if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) || errors.Is(err, gCtx.Err()) {
					return err
				}
				if failed := failCounter.Add(1); failed > uint32(rule.ParityPartNum) {
					return tooManyPartsUnavailableError(failed)
				}
				s.log.Info("failed to get EC data part",
					zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Stringer("rule", rule),
					zap.Int("ruleIdx", ruleIdx), zap.Int("partIdx", partIdx), zap.Error(err),
				)
				return nil
			}

			if !gotHdr.Swap(true) {
				hdr = parentHdr
			}
			if parentHdr.PayloadSize() == 0 {
				return errInterrupt
			}

			parts[partIdx] = partPayload

			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		if errors.Is(err, errInterrupt) {
			return hdr, nil
		}
		return object.Object{}, err
	}

	rem := islices.CountNilsInTwoDimSlice(parts[:rule.DataPartNum])
	if rem > int(rule.ParityPartNum) {
		return object.Object{}, tooManyPartsUnavailableError(rem)
	}

	pldLen := hdr.PayloadSize()

	if rem == 0 {
		if got := islices.TwoDimSliceElementCount(parts[:rule.DataPartNum]); uint64(got) < pldLen {
			return object.Object{}, fmt.Errorf("sum len of received data parts is less than full len: %d < %d", got, pldLen)
		}
		// TODO: if response is streamed, concatenation may be avoided.
		payload := iec.ConcatDataParts(rule, pldLen, parts)
		hdr.SetPayload(payload)
		return hdr, nil
	}

	eg, gCtx = errgroup.WithContext(ctx)
	failCounter.Store(0)
	var okCounter atomic.Uint32
	for i := range rule.ParityPartNum {
		partIdx := int(rule.DataPartNum + i)
		eg.Go(func() error {
			_, part, err := s.getECPart(gCtx, cnr, parent, sTok, bTok, rule, ruleIdx, sortedNodes, partIdx)
			if err != nil {
				if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) || errors.Is(err, gCtx.Err()) {
					return err
				}
				if failed := failCounter.Add(1); failed+uint32(rem) > uint32(rule.ParityPartNum) {
					return tooManyPartsUnavailableError(failed)
				}
				s.log.Info("failed to get EC parity part",
					zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Stringer("rule", rule),
					zap.Int("ruleIdx", ruleIdx), zap.Int("partIdx", partIdx), zap.Error(err),
				)
				return nil
			}

			parts[partIdx] = part
			if okCounter.Add(1) >= uint32(rem) {
				return errInterrupt
			}

			return nil
		})
	}
	if err := eg.Wait(); err != nil && !errors.Is(err, errInterrupt) {
		return object.Object{}, err
	}

	if rem = islices.CountNilsInTwoDimSlice(parts); rem > int(rule.ParityPartNum) {
		return object.Object{}, tooManyPartsUnavailableError(rem)
	}

	payload, err := iec.Decode(rule, pldLen, parts)
	if err != nil {
		return object.Object{}, fmt.Errorf("decode payload from parts: %w", err)
	}

	hdr.SetPayload(payload)
	return hdr, nil
}

// reads object for EC part which should produced for parent object and
// distributed sortedNodes according to EC rule with index = ruleIdx in cnr
// policy. Returns parent object header and part payload. Payload slice is nil
// iff parent payload is empty.
//
// Returns [apistatus.ErrObjectAlreadyRemoved] if the object was marked for
// removal.
//
// Can return [context.Canceled] from the passed ctx only.
func (s *Service) getECPart(ctx context.Context, cnr cid.ID, parent oid.ID, sTok *session.Object, bTok *bearer.Token,
	rule iec.Rule, ruleIdx int, sortedNodes []netmap.NodeInfo, partIdx int) (object.Object, []byte, error) {
	var partHdr object.Object
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
			partHdr, rc, err = s.localObjects.GetECPart(cnr, parent, pi)
		} else {
			partHdr, rc, err = s.getECPartFromNode(ctx, cnr, parent, sTok, bTok, pi, sortedNodes[i])
		}
		if err == nil {
			break
		}
		if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) {
			return object.Object{}, nil, err
		}
		if errors.Is(err, ctx.Err()) {
			return object.Object{}, nil, err
		}

		if !errors.Is(err, apistatus.ErrObjectNotFound) {
			s.log.Info("failed to get EC part from node, continue...",
				zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Stringer("rule", rule),
				zap.Int("ruleIdx", pi.RuleIndex), zap.Int("partIdx", pi.Index), zap.Bool("local", local), zap.Error(err))
		}
	}
	if err != nil {
		return object.Object{}, nil, errors.New("all nodes failed")
	}

	defer rc.Close()

	parentHdr := partHdr.Parent()
	if parentHdr == nil {
		return object.Object{}, nil, errors.New("missing parent header in object for part")
	}

	if partHdr.PayloadSize() > parentHdr.PayloadSize() {
		return object.Object{}, nil, errors.New("part object payload is bigger than the parent one")
	}

	if parentHdr.PayloadSize() == 0 {
		return *parentHdr, nil, nil
	}

	buf := make([]byte, partHdr.PayloadSize())
	if _, err := io.ReadFull(rc, buf); err != nil {
		if errors.Is(err, io.EOF) { // empty payload is caught above
			err = io.ErrUnexpectedEOF
		}
		return object.Object{}, nil, fmt.Errorf("read full payload: %w", err)
	}

	return *parentHdr, buf, nil
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
		err = convertContextCanceledStatus(err)
		return object.Object{}, nil, fmt.Errorf("get object from remote SN: %w", err)
	}

	defer func() {
		if err != nil {
			_ = rc.Close()
		}
	}()

	if got := hdr.GetParentID(); got != parent {
		err = fmt.Errorf("wrong parent ID in received object for part: requested %s, got %s", got, parent) // for defer
		return object.Object{}, nil, err
	}

	if err = checkECAttributesInReceivedObject(hdr, ruleIdxAttr, partIdxAttr); err != nil {
		return object.Object{}, nil, err // notice defer
	}

	return hdr, rc, nil
}

// looks up for local object that carries EC part produced within cnr for parent
// object and indexed by pi, and writes its payload range into dst. Both zero
// off and ln correspond to full payload.
//
// Returns [apistatus.ErrObjectAlreadyRemoved] if the object was marked for
// removal. Returns [apistatus.ErrObjectNotFound] if the object is missing.
// Returns [apistatus.ErrObjectOutOfRange] if the range is out of payload range.
func (s *Service) copyLocalECPartRange(dst ChunkWriter, cnr cid.ID, parent oid.ID, pi iec.PartInfo, off, ln uint64) error {
	pldLen, rc, err := s.localObjects.GetECPartRange(cnr, parent, pi, off, ln)
	if err != nil {
		return fmt.Errorf("get object payload range from local storage: %w", err)
	}
	if pldLen == 0 {
		// TODO: highlight (nil, nil) return for this case in interface and storage docs
		return nil
	}
	defer rc.Close()

	var bufLen uint64
	if ln == 0 && off == 0 {
		bufLen = min(pldLen, streamChunkSize)
	} else {
		bufLen = min(ln, streamChunkSize)
	}
	if err := copyPayloadStream(dst, rc, bufLen); err != nil {
		return fmt.Errorf("copy payload: %w", err)
	}

	return nil
}

func (s *Service) copyECObjectRange(ctx context.Context, dst ChunkWriter, cnr cid.ID, parent oid.ID, sTok *session.Object, bTok *bearer.Token,
	ecRules []iec.Rule, sortedNodeLists [][]netmap.NodeInfo, off, ln uint64) error {
	return s.copyECObjectRangeByRule(ctx, dst, cnr, parent, sTok, bTok, ecRules[0], 0, sortedNodeLists[0], off, ln)
	// // TODO: sort EC rules by complexity and try simpler ones first. Note that rule idxs passed as arguments must be kept.
	// //  https://github.com/nspcc-dev/neofs-node/issues/3563
	// for i := range ecRules {
	// 	full, written, err := s.copyECObjectRangeByRule(ctx, cnr, parent, sTok, bTok, ecRules[i], i, sortedNodeLists[i], off, ln, dst)
	// 	if err == nil || errors.Is(err, apistatus.ErrObjectAlreadyRemoved) || errors.Is(err, apistatus.ErrObjectOutOfRange) {
	// 		return err
	// 	}
	// 	if i == len(ecRules)-1 {
	// 		return fmt.Errorf("%w: all rules failed (last rule error: %v)", apistatus.ErrObjectNotFound, err)
	// 	}
	// 	if written >= ln {
	// 		panic(fmt.Sprintf("did not succeeded after writing %d bytes out of %d", n, ln))
	// 	}
	//
	// 	s.log.Info("failed to copy object by EC rule",
	// 		zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Stringer("rule", rules[i]),
	// 		zap.Int("ruleIdx", i), zap.Error(err),
	// 	)
	//
	// 	ln -= n
	// }
	// panic("unreachable")
}

// TODO: docs.
func (s *Service) copyECObjectRangeByRule(ctx context.Context, dst ChunkWriter, cnr cid.ID, parent oid.ID, sTok *session.Object,
	bTok *bearer.Token, rule iec.Rule, ruleIdx int, sortedNodes []netmap.NodeInfo, off, ln uint64) error {
	// TODO: too big func, try to split
	// TODO: limit context (https://github.com/nspcc-dev/neofs-node/issues/3560)
	localNodeKey, err := s.keyStore.GetKey(nil)
	if err != nil {
		return fmt.Errorf("get local SN private key: %w", err)
	}

	totalParts := int(rule.DataPartNum + rule.ParityPartNum)

	// resolve full payload and part len
	var pldLen uint64
	for i := range sortedNodes {
		if s.neoFSNet.IsLocalNodePublicKey(sortedNodes[i].PublicKey()) {
			var hdr *object.Object
			hdr, err = s.localStorage.(*storageEngineWrapper).engine.Head(oid.NewAddress(cnr, parent), false)
			if err == nil {
				pldLen = hdr.PayloadSize()
				break
			}
			if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) {
				return err
			}
			if !errors.Is(err, apistatus.ErrObjectNotFound) {
				s.log.Info("failed to HEAD EC parent locally, continue...",
					zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Error(err))
			}
			continue
		}

		var hdr object.Object
		hdr, err = s.conns.Head(ctx, sortedNodes[i], *localNodeKey, cnr, parent, sTok, bTok)
		if err == nil {
			pldLen = hdr.PayloadSize()
			break
		}
		if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) || errors.Is(err, apistatus.ErrObjectAccessDenied) {
			return err
		}
		// TODO: consider caching failed nodes to not call them twice
		if !errors.Is(err, apistatus.ErrObjectNotFound) {
			s.log.Info("failed to HEAD EC parent remotely, continue...",
				zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Error(err))
		}
	}
	if err != nil {
		return apistatus.ErrObjectNotFound
	}

	fullPartLen := (pldLen + uint64(rule.DataPartNum) - 1) / uint64(rule.DataPartNum)

	// mark part ranges
	var firstIdx, lastIdx int
	var firstOff, lastTo uint64
	if ln == 0 && off == 0 { // full range request
		if pldLen == 0 {
			return nil
		}

		lastIdx = int(rule.DataPartNum) - 1
		if lastTo = pldLen % fullPartLen; lastTo == 0 {
			lastTo = fullPartLen
		}
	} else {
		if off >= pldLen || pldLen-off < ln {
			return apistatus.ErrObjectOutOfRange
		}

		firstIdx, firstOff, lastIdx, lastTo = requiredChildrenIter(off, ln, nEqualSizeIter(totalParts, fullPartLen))
	}

	// calc optimal buffer len for copying
	var bufLen uint64
	switch lastIdx - firstIdx {
	case 0:
		bufLen = lastTo - firstOff
	case 1:
		bufLen = max(fullPartLen-firstOff, lastTo)
	default:
		bufLen = fullPartLen
	}
	bufLen = min(bufLen, streamChunkSize)

	// copy payload while no failure
	var buf []byte
	var curIdx int
	for curIdx = firstIdx; curIdx <= lastIdx; curIdx++ {
		var partOff, partLen uint64
		if curIdx == firstIdx {
			partOff = firstOff
			if curIdx == lastIdx {
				partLen = lastTo - firstOff
			} else {
				partLen = fullPartLen - firstOff
			}
		} else if curIdx < lastIdx {
			partOff, partLen = 0, 0 // full
		} else {
			partOff, partLen = 0, lastTo
		}

		// We could open as many streams in parallel as we need. However, since GetRange
		// stream is server-side, so the server sends data without processing ACK (this
		// is only possible in bidirectional streams). This way, background buffering of
		// data from all streams will start immediately.
		rc, err := s.getECPartRangeStream(ctx, cnr, parent, partOff, partLen, sTok, bTok, rule, ruleIdx, sortedNodes, curIdx, *localNodeKey)
		if err != nil {
			if curIdx != firstIdx {
				firstIdx = curIdx
				firstOff = 0
			}
			break
		}

		if buf == nil {
			buf = make([]byte, bufLen)
		}

		n, err := copyPayloadStreamBuffer(dst, rc, buf)
		rc.Close()
		if err != nil {
			if errors.Is(err, errWriteChunk) {
				return err
			}

			if curIdx == firstIdx {
				firstOff += n
			} else {
				firstIdx = curIdx
				firstOff = n
			}
			break
		}

		if curIdx == lastIdx {
			return nil
		}
	}

	panic("unimplemented")
	// // here one required data part is unavailable (its index is curIdx). Try recovery
	// parts := make([][]byte, totalParts)
	//
	// eg, gCtx := errgroup.WithContext(ctx)
	// var failCounter atomic.Uint32
	// for i := range int(rule.DataPartNum) {
	// 	partIdx := i
	// 	eg.Go(func() error {
	// 		parentHdr, partPayload, err := s.getECPart(gCtx, cnr, parent, sTok, bTok, rule, ruleIdx, sortedNodes, partIdx)
	// 		if err != nil {
	// 			if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) || errors.Is(err, gCtx.Err()) {
	// 				return err
	// 			}
	// 			if failed := failCounter.Add(1); failed > uint32(rule.ParityPartNum) {
	// 				return tooManyPartsUnavailableError(failed)
	// 			}
	// 			s.log.Info("failed to get EC data part",
	// 				zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Stringer("rule", rule),
	// 				zap.Int("ruleIdx", ruleIdx), zap.Int("partIdx", partIdx), zap.Error(err),
	// 			)
	// 			return nil
	// 		}
	//
	// 		if !gotHdr.Swap(true) {
	// 			hdr = parentHdr
	// 		}
	// 		if parentHdr.PayloadSize() == 0 {
	// 			return errInterrupt
	// 		}
	//
	// 		parts[partIdx] = partPayload
	//
	// 		return nil
	// 	})
	// }
	// if err := eg.Wait(); err != nil {
	// 	if errors.Is(err, errInterrupt) {
	// 		return hdr, nil
	// 	}
	// 	return object.Object{}, err
	// }
}

// TODO: docs.
// Similar to getECPart.
func (s *Service) getECPartRangeStream(ctx context.Context, cnr cid.ID, parent oid.ID, off, ln uint64, sTok *session.Object, bTok *bearer.Token,
	rule iec.Rule, ruleIdx int, sortedNodes []netmap.NodeInfo, partIdx int, localNodeKey ecdsa.PrivateKey) (io.ReadCloser, error) {
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
			_, rc, err = s.localObjects.GetECPartRange(cnr, parent, pi, off, ln)
			if err == nil || errors.Is(err, apistatus.ErrObjectAlreadyRemoved) || errors.Is(err, apistatus.ErrObjectOutOfRange) {
				return rc, err
			}
		}

		rc, err = s.getECPartRangeFromNode(ctx, cnr, parent, off, ln, sTok, bTok, pi, localNodeKey, sortedNodes[i])
		if err != nil {
			if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) || errors.Is(err, apistatus.ErrObjectOutOfRange) {
				return nil, err
			}
			if errors.Is(err, ctx.Err()) {
				return nil, err
			}
			if !errors.Is(err, apistatus.ErrObjectNotFound) {
				s.log.Info("failed to get EC part payload range from node, continue...",
					zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Uint64("off", off), zap.Uint64("len", ln),
					zap.Stringer("rule", rule), zap.Int("ruleIdx", pi.RuleIndex), zap.Int("partIdx", pi.Index), zap.Bool("local", local),
					zap.Error(err))
			}
			continue
		}

		// Fallback to GET similar to fallbackRangeReader. Track https://github.com/nspcc-dev/neofs-node/issues/3547.
		b := []byte{0}
		_, err = io.ReadFull(rc, b)
		if err == nil {
			// TODO: used in several places, share.
			return struct {
				io.Reader
				io.Closer
			}{
				Reader: io.MultiReader(io.MultiReader(bytes.NewReader(b), rc), rc),
				Closer: rc,
			}, nil
		}

		if !errors.Is(err, apistatus.ErrObjectAccessDenied) {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return nil, fmt.Errorf("read GetRange response stream: %w", err)
		}

		_, rc, err = s.getECPartFromNode(ctx, cnr, parent, sTok, bTok, pi, sortedNodes[i])
		if err != nil {
			s.log.Info("range->get fallback attempt for EC part failed on stream init",
				zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Stringer("rule", rule),
				zap.Int("ruleIdx", pi.RuleIndex), zap.Int("partIdx", pi.Index), zap.Error(err))
			continue
		}

		if off > math.MaxInt64 && ln > math.MaxInt64 {
			// underlying io functions behave specifically with negative input. 8EB+ are ranges from the future.
			s.log.Warn("too big object range for this server",
				zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Uint64("off", off), zap.Uint64("len", ln))
			continue
		}

		if _, err = io.CopyN(io.Discard, rc, int64(off)); err != nil {
			s.log.Info("range->get fallback attempt for EC part failed on stream seek",
				zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Stringer("rule", rule),
				zap.Int("ruleIdx", pi.RuleIndex), zap.Int("partIdx", pi.Index), zap.Uint64("skipBytes", off),
				zap.Error(err))
			continue
		}

		var r io.Reader
		if ln > 0 {
			r = io.LimitReader(rc, int64(ln))
		} else { // full range request
			r = rc
		}

		return struct {
			io.Reader
			io.Closer
		}{
			Reader: r,
			Closer: rc,
		}, nil
	}

	return nil, errors.New("all nodes failed")
}

// TODO: docs.
// Similar to getECPartFromNode.
func (s *Service) getECPartRangeFromNode(ctx context.Context, cnr cid.ID, parent oid.ID, off, ln uint64, sTok *session.Object,
	bTok *bearer.Token, pi iec.PartInfo, localNodeKey ecdsa.PrivateKey, node netmap.NodeInfo) (io.ReadCloser, error) {
	ruleIdxAttr := strconv.Itoa(pi.RuleIndex)
	partIdxAttr := strconv.Itoa(pi.Index)

	// TODO: this must be stated in https://github.com/nspcc-dev/neofs-api
	rc, err := s.conns.InitGetObjectRangeStream(ctx, node, localNodeKey, cnr, parent, off, ln, sTok, bTok, []string{
		iec.AttributeRuleIdx, ruleIdxAttr,
		iec.AttributePartIdx, partIdxAttr,
	})
	if err != nil {
		err = convertContextCanceledStatus(err)
		return nil, fmt.Errorf("get object from remote SN: %w", err)
	}

	return rc, nil
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
