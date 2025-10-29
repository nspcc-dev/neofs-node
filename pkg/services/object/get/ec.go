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
	"time"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	islices "github.com/nspcc-dev/neofs-node/internal/slices"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
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

func (s *Service) copyECObjectHeader(ctx context.Context, dst internal.HeaderWriter, cnr cid.ID, parent oid.ID,
	sTok *session.Object, ecRules []iec.Rule, sortedNodeLists [][]netmap.NodeInfo) error {
	hdr, err := s.getECObjectHeader(ctx, cnr, parent, sTok, ecRules, sortedNodeLists)
	if err != nil {
		return err
	}

	if err := dst.WriteHeader(&hdr); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	return nil
}

func (s *Service) getECObjectHeader(ctx context.Context, cnr cid.ID, id oid.ID, sTok *session.Object,
	ecRules []iec.Rule, sortedNodeLists [][]netmap.NodeInfo) (object.Object, error) {
	localNodeKey, err := s.keyStore.GetKey(nil)
	if err != nil {
		return object.Object{}, fmt.Errorf("get local SN private key: %w", err)
	}

	// TODO: limit per-rule context? https://github.com/nspcc-dev/neofs-node/issues/3560
	var firstErr error
	for i := range ecRules {
		hdr, err := s.getECObjectHeaderByRule(ctx, *localNodeKey, cnr, id, sTok, sortedNodeLists[i])
		if err == nil || errors.Is(err, apistatus.ErrObjectAlreadyRemoved) || errors.Is(err, ctx.Err()) {
			return hdr, err
		}

		if i == 0 {
			firstErr = err
		}

		s.log.Debug("failed to fetch object header by EC rule",
			zap.Stringer("container", cnr), zap.Stringer("object", id), zap.Stringer("rule", ecRules[i]),
			zap.Int("rulesLeft", len(ecRules)-i-1), zap.Error(err))
	}

	return object.Object{}, fmt.Errorf("%w: failed processing of all %d EC rules, first error: %w",
		apistatus.ErrObjectNotFound, len(ecRules), firstErr)
}

func (s *Service) getECObjectHeaderByRule(ctx context.Context, localNodeKey ecdsa.PrivateKey, cnr cid.ID, id oid.ID, sTok *session.Object,
	sortedNodes []netmap.NodeInfo) (object.Object, error) {
	var firstErr error

	for i := range sortedNodes {
		if s.neoFSNet.IsLocalNodePublicKey(sortedNodes[i].PublicKey()) {
			hdr, err := s.localStorage.(*storageEngineWrapper).engine.Head(oid.NewAddress(cnr, id), false)
			if err == nil {
				return *hdr, nil
			}

			if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) {
				return object.Object{}, err
			}

			if errors.Is(err, apistatus.ErrObjectNotFound) {
				continue
			}

			if firstErr == nil {
				firstErr = err
			}

			s.log.Info("failed to HEAD object from local storage",
				zap.Stringer("container", cnr), zap.Stringer("object", id), zap.Error(err))

			continue
		}

		hdr, err := s.conns.Head(ctx, sortedNodes[i], localNodeKey, cnr, id, sTok)
		if err == nil {
			return hdr, nil
		}

		err = convertContextStatus(err)

		if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) || errors.Is(err, ctx.Err()) {
			return object.Object{}, err
		}

		if errors.Is(err, apistatus.ErrObjectNotFound) {
			continue
		}

		if firstErr == nil {
			firstErr = err
		}

		s.log.Info("failed to HEAD object from remote node",
			zap.Stringer("container", cnr), zap.Stringer("object", id), zap.Error(err))
	}

	if firstErr != nil {
		return object.Object{}, fmt.Errorf("all %d nodes failed, one of errors: %w", len(sortedNodes), firstErr)
	}

	return object.Object{}, fmt.Errorf("not found on all %d nodes", len(sortedNodes))
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
func (s *Service) copyECObject(ctx context.Context, cnr cid.ID, parent oid.ID, sTok *session.Object,
	rules []iec.Rule, sortedNodeLists [][]netmap.NodeInfo, dst ObjectWriter) error {
	// TODO: sort EC rules by complexity and try simpler ones first. Note that rule idxs passed as arguments must be kept.
	for i := range rules {
		obj, err := s.restoreFromECPartsByRule(ctx, cnr, parent, sTok, rules[i], i, sortedNodeLists[i])
		if err == nil {
			if obj.Type() == object.TypeLink {
				return s.copySplitECObject(ctx, dst, cnr, parent, sTok, rules, sortedNodeLists, i, obj)
			}
			if err := copyObject(dst, obj); err != nil {
				return fmt.Errorf("copy object: %w", err)
			}
			return nil
		}
		if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) || errors.Is(err, ctx.Err()) {
			return err
		}

		s.log.Info("failed to restore object by EC rule",
			zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Stringer("rule", rules[i]),
			zap.Int("ruleIdx", i), zap.Error(err),
		)
	}

	return apistatus.ErrObjectNotFound
}

func (s *Service) copySplitECObject(ctx context.Context, dst ObjectWriter, cnr cid.ID, parent oid.ID, sTok *session.Object,
	rules []iec.Rule, sortedNodeLists [][]netmap.NodeInfo, fromRule int, linker object.Object) error {
	parentHdr := linker.Parent()
	if parentHdr == nil {
		return fmt.Errorf("%w: missing parent header", errInvalidSizeSplitLinker)
	}

	var l object.Link
	if err := linker.ReadLink(&l); err != nil {
		return fmt.Errorf("%w: %w", errInvalidSizeSplitLinker, err)
	}
	sizeSplitParts := l.Objects()
	if len(sizeSplitParts) == 0 {
		return fmt.Errorf("%w: empty part list", errInvalidSizeSplitLinker)
	}

	if err := dst.WriteHeader(parentHdr); err != nil {
		return fmt.Errorf("%w: write parent header: %w", errStreamFailure, err)
	}

nextPart:
	for partIdx := range sizeSplitParts {
		partID := sizeSplitParts[partIdx].ObjectID()

		// TODO: limit per-rule context? https://github.com/nspcc-dev/neofs-node/issues/3560
		for ; fromRule < len(rules); fromRule++ {
			obj, err := s.restoreFromECPartsByRule(ctx, cnr, partID, sTok, rules[fromRule], fromRule, sortedNodeLists[fromRule])
			if err == nil {
				if obj.Type() == object.TypeLink { // prevents recursion, i.e. size-split of size-split object
					return fmt.Errorf("get size-split part #%d=%s: unexpected linker", partIdx, partID)
				}
				if err := dst.WriteChunk(obj.Payload()); err != nil {
					return fmt.Errorf("failed to write size-split part #%d=%s: %w: %w", partIdx, partID, errStreamFailure, err)
				}
				continue nextPart
			}

			if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) || errors.Is(err, ctx.Err()) {
				return err
			}

			s.log.Info("failed to restore size-split part object by EC rule",
				zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Stringer("rule", rules[fromRule]),
				zap.Int("ruleIdx", fromRule), zap.Stringer("partID", partID), zap.Int("rulesLeft", len(rules)-1-fromRule),
				zap.Error(err))
		}

		if fromRule >= len(rules) {
			return fmt.Errorf("%w: failed to get size-split part #%d=%s by any EC rule",
				apistatus.ErrObjectNotFound, partIdx, sizeSplitParts[partIdx].ObjectID())
		}
	}

	return nil
}

// reads object by (cnr, id) which should be partitioned across specified nodes
// according to EC rule with index = ruleIdx from cnr policy.
//
// The sortedNodes are sorted by parent. It has at least [iec.Rule.DataPartsNum]
// + [iec.Rule.ParityPartsNum] elements.
//
// Returns [apistatus.ErrObjectAlreadyRemoved] if the object was marked for
// removal.
func (s *Service) restoreFromECPartsByRule(ctx context.Context, cnr cid.ID, parent oid.ID, sTok *session.Object,
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
			parentHdr, partPayload, err := s.getECPart(gCtx, cnr, parent, sTok, rule, ruleIdx, sortedNodes, partIdx)
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

			linker := parentHdr.Type() == object.TypeLink

			if !gotHdr.Swap(true) {
				if linker {
					parentHdr.SetPayload(partPayload)
				}
				hdr = parentHdr
			}
			if linker || parentHdr.PayloadSize() == 0 {
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
			_, part, err := s.getECPart(gCtx, cnr, parent, sTok, rule, ruleIdx, sortedNodes, partIdx)
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
// If the object is not EC part but of [object.TypeTombstone] or
// [object.TypeLock] type, getECPart returns this object without an error.
//
// Returns [apistatus.ErrObjectAlreadyRemoved] if the object was marked for
// removal.
//
// Can return [context.Canceled] from the passed ctx only.
func (s *Service) getECPartStream(ctx context.Context, cnr cid.ID, parent oid.ID, sTok *session.Object, rule iec.Rule,
	ruleIdx int, sortedNodes []netmap.NodeInfo, partIdx int) (object.Object, io.ReadCloser, error) {
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
			partHdr, rc, err = s.getECPartFromNode(ctx, cnr, parent, sTok, pi, sortedNodes[i])
		}
		if err == nil {
			return partHdr, rc, nil
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

	return object.Object{}, nil, errors.New("all nodes failed")
}

// reads object for EC part which should produced for parent object and
// distributed sortedNodes according to EC rule with index = ruleIdx in cnr
// policy. Returns parent object header and part payload. Payload slice is nil
// iff parent payload is empty.
//
// If the object is not EC part but of [object.TypeTombstone] or
// [object.TypeLock] type, getECPart returns this object without an error.
//
// Returns [apistatus.ErrObjectAlreadyRemoved] if the object was marked for
// removal.
//
// Can return [context.Canceled] from the passed ctx only.
func (s *Service) getECPart(ctx context.Context, cnr cid.ID, parent oid.ID, sTok *session.Object,
	rule iec.Rule, ruleIdx int, sortedNodes []netmap.NodeInfo, partIdx int) (object.Object, []byte, error) {
	partHdr, rc, err := s.getECPartStream(ctx, cnr, parent, sTok, rule, ruleIdx, sortedNodes, partIdx)
	if err != nil {
		return object.Object{}, nil, err
	}

	defer rc.Close()

	typ := partHdr.Type()
	if typ == object.TypeTombstone || typ == object.TypeLock {
		if partHdr.PayloadSize() != 0 {
			return object.Object{}, nil, fmt.Errorf("received %s object with non-empty payload", typ)
		}
		return partHdr, nil, nil
	}

	parentHdr := partHdr.Parent()
	if parentHdr == nil {
		return object.Object{}, nil, errors.New("missing parent header in object for part")
	}

	if typ == object.TypeLink {
		buf := make([]byte, partHdr.PayloadSize())
		if _, err := io.ReadFull(rc, buf); err != nil {
			if errors.Is(err, io.EOF) { // empty payload is caught above
				err = io.ErrUnexpectedEOF
			} else {
				err = convertContextStatus(err)
			}
			return object.Object{}, nil, fmt.Errorf("read full payload: %w", err)
		}

		return partHdr, buf, nil
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
		} else {
			err = convertContextStatus(err)
		}
		return object.Object{}, nil, fmt.Errorf("read full payload: %w", err)
	}

	return *parentHdr, buf, nil
}

// queries object that carries EC part produced within cnr for parent object and
// indexed by pi from node. On success, returns header and payload reader.
//
// If the object is not EC part but of [object.TypeTombstone] or
// [object.TypeLock] type, getECPartFromNode returns this object without an
// error.
//
// Returns [apistatus.ErrObjectAlreadyRemoved] if the object was marked for
// removal. Returns [apistatus.ErrObjectNotFound] if the object is missing.
//
// Can return [context.Canceled] from the passed ctx only.
//
// RPC errors include network addresses.
func (s *Service) getECPartFromNode(ctx context.Context, cnr cid.ID, parent oid.ID, sTok *session.Object,
	pi iec.PartInfo, node netmap.NodeInfo) (object.Object, io.ReadCloser, error) {
	localNodeKey, err := s.keyStore.GetKey(nil)
	if err != nil {
		return object.Object{}, nil, fmt.Errorf("get local SN private key: %w", err)
	}

	ruleIdxAttr := strconv.Itoa(pi.RuleIndex)
	partIdxAttr := strconv.Itoa(pi.Index)

	// TODO: this must be stated in https://github.com/nspcc-dev/neofs-api
	hdr, rc, err := s.conns.InitGetObjectStream(ctx, node, *localNodeKey, cnr, parent, sTok, true, false, []string{
		iec.AttributeRuleIdx, ruleIdxAttr,
		iec.AttributePartIdx, partIdxAttr,
	})
	if err != nil {
		err = convertContextStatus(err)
		return object.Object{}, nil, fmt.Errorf("get object from remote SN: %w", err)
	}

	defer func() {
		if err != nil {
			_ = rc.Close()
		}
	}()

	typ := hdr.Type()
	if typ == object.TypeTombstone || typ == object.TypeLock {
		return hdr, rc, nil
	}

	if got := hdr.GetParentID(); got != parent {
		err = fmt.Errorf("wrong parent ID in received object for part: requested %s, got %s", got, parent) // for defer
		return object.Object{}, nil, err
	}

	if typ == object.TypeLink {
		return hdr, rc, nil
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

func (s *Service) copyECObjectRange(ctx context.Context, dst ChunkWriter, cnr cid.ID, parent oid.ID, sTok *session.Object,
	ecRules []iec.Rule, sortedNodeLists [][]netmap.NodeInfo, off, ln uint64) error {
	localNodeKey, err := s.keyStore.GetKey(nil)
	if err != nil {
		return fmt.Errorf("get local SN private key: %w", err)
	}

	// TODO: sort EC rules by complexity and try simpler ones first. Note that rule idxs passed as arguments must be kept.
	//  https://github.com/nspcc-dev/neofs-node/issues/3563
	// TODO: limit per-rule context? https://github.com/nspcc-dev/neofs-node/issues/3560
	var firstErr error
	for i := range ecRules {
		written, err := s.copyECObjectRangeByRule(ctx, dst, *localNodeKey, cnr, parent, sTok, ecRules[i], i, sortedNodeLists[i], off, ln)
		if err == nil || errors.Is(err, apistatus.ErrObjectAlreadyRemoved) || errors.Is(err, apistatus.ErrObjectOutOfRange) ||
			errors.Is(err, errStreamFailure) || errors.Is(err, ctx.Err()) {
			return err
		}

		var linker sizeSplitinkerError
		if errors.As(err, &linker) {
			return s.copySplitECObjectRange(ctx, dst, *localNodeKey, cnr, parent, sTok, ecRules, sortedNodeLists, off, ln, i, object.Object(linker))
		}

		if i == 0 {
			firstErr = err
		}

		s.log.Info("failed to fetch payload range by EC rule",
			zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Stringer("rule", ecRules[i]),
			zap.Int("ruleIdx", i), zap.Int("rulesLeft", len(ecRules)-i-1), zap.Uint64("off", off),
			zap.Uint64("len", ln), zap.Uint64("written", written), zap.Error(err))

		off += written
	}

	return fmt.Errorf("%w: failed processing of all %d EC rules, first error: %w", apistatus.ErrObjectNotFound, len(ecRules), firstErr)
}

func (s *Service) copySplitECObjectRange(ctx context.Context, dst ChunkWriter, localNodeKey ecdsa.PrivateKey, cnr cid.ID,
	parent oid.ID, sTok *session.Object, rules []iec.Rule, sortedNodeLists [][]netmap.NodeInfo,
	off, ln uint64, fromRule int, linker object.Object) error {
	parentHdr := linker.Parent()
	if parentHdr == nil {
		return fmt.Errorf("%w: missing parent header", errInvalidSizeSplitLinker)
	}

	var l object.Link
	if err := linker.ReadLink(&l); err != nil {
		return fmt.Errorf("%w: %w", errInvalidSizeSplitLinker, err)
	}
	sizeSplitParts := l.Objects()
	if len(sizeSplitParts) == 0 {
		return fmt.Errorf("%w: empty part list", errInvalidSizeSplitLinker)
	}

	pldLen := parentHdr.PayloadSize()

	// Mark ranges of size-split parts.
	var firstPartIdx, lastPartIdx int
	var firstPartOff, lastPartTo uint64
	if ln == 0 && off == 0 { // full range request
		if pldLen == 0 { // should never happen
			return nil
		}

		lastPartIdx = len(sizeSplitParts) - 1
		lastPartTo = uint64(sizeSplitParts[lastPartIdx].ObjectSize())
	} else {
		if off >= pldLen || pldLen-off < ln {
			return apistatus.ErrObjectOutOfRange
		}

		firstPartIdx, firstPartOff, lastPartIdx, lastPartTo = requiredChildren(off, ln, sizeSplitParts)
	}

	var partTimeout time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		// don't take into account that first/last range may be tiny
		if partTimeout = time.Until(deadline) / time.Duration(lastPartIdx-firstPartIdx+1); partTimeout <= 0 {
			return context.DeadlineExceeded
		}
	} else {
		partTimeout = time.Minute
	}

nextPart:
	for curIdx := firstPartIdx; curIdx <= lastPartIdx; curIdx++ {
		partID := sizeSplitParts[curIdx].ObjectID()

		fullPartLen := uint64(sizeSplitParts[curIdx].ObjectSize())
		partOff, partLen := uint64(0), fullPartLen
		if curIdx == firstPartIdx {
			partOff = firstPartOff
		}
		if curIdx == lastPartIdx {
			partLen = lastPartTo
		}
		partLen -= partOff

		partCtx, cancel := context.WithTimeout(ctx, partTimeout)
		defer cancel()

		// TODO: limit per-rule context? https://github.com/nspcc-dev/neofs-node/issues/3560
		for ; fromRule < len(rules); fromRule++ {
			written, err := s.copyECObjectRangeByParts(partCtx, dst, localNodeKey, cnr, partID, sTok, rules[fromRule], fromRule,
				sortedNodeLists[fromRule], fullPartLen, partOff, partLen, nil)
			if err == nil {
				continue nextPart
			}

			if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) || errors.Is(err, apistatus.ErrObjectOutOfRange) ||
				errors.Is(err, errStreamFailure) || errors.Is(err, partCtx.Err()) {
				return err
			}

			s.log.Info("failed to copy range of size-split part object by EC rule",
				zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Stringer("rule", rules[fromRule]),
				zap.Int("ruleIdx", fromRule), zap.Stringer("partID", partID), zap.Int("rulesLeft", len(rules)-1-fromRule),
				zap.Uint64("partOff", partOff), zap.Uint64("partLen", partLen), zap.Uint64("written", written), zap.Error(err))

			partOff += written
		}

		if fromRule >= len(rules) {
			return fmt.Errorf("%w: failed to get size-split part #%d=%s by any EC rule",
				apistatus.ErrObjectNotFound, curIdx, partID)
		}
	}

	return nil
}

func (s *Service) copyECObjectRangeByRule(ctx context.Context, dst ChunkWriter, localNodeKey ecdsa.PrivateKey, cnr cid.ID,
	parent oid.ID, sTok *session.Object, rule iec.Rule, ruleIdx int, sortedNodes []netmap.NodeInfo,
	off, ln uint64) (uint64, error) {
	deadline, deadlineSet := ctx.Deadline()
	var stageTimeout time.Duration

	// Resolve full parent and all parts' len. Since this op should go fast, limit it to 10% of remaining context.
	if deadlineSet {
		if stageTimeout = time.Until(deadline) / 10; stageTimeout <= 0 {
			return 0, context.DeadlineExceeded
		}
	} else {
		stageTimeout = 10 * time.Second
	}
	stageCtx, cancel := context.WithTimeout(ctx, stageTimeout)
	defer cancel()

	partHdr, firstPartStream, err := s.getECPartStream(stageCtx, cnr, parent, sTok, rule, ruleIdx, sortedNodes, 0)
	if err != nil {
		return 0, fmt.Errorf("resolve parent payload length: %w", err)
	}
	if firstPartStream != nil {
		defer firstPartStream.Close()
	}

	if partHdr.Type() == object.TypeLink {
		buf := make([]byte, partHdr.PayloadSize())
		if _, err := io.ReadFull(firstPartStream, buf); err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			} else {
				err = convertContextStatus(err)
			}
			return 0, fmt.Errorf("read size-split linker payload stream: %w", err)
		}
		partHdr.SetPayload(buf)
		return 0, sizeSplitinkerError(partHdr)
	}

	parentHdr := partHdr.Parent()
	if parentHdr == nil {
		return 0, errors.New("missing parent header in object for part") // TODO: copied, share?
	}

	return s.copyECObjectRangeByParts(ctx, dst, localNodeKey, cnr, parent, sTok, rule, ruleIdx, sortedNodes,
		parentHdr.PayloadSize(), off, ln, firstPartStream)
}

func (s *Service) copyECObjectRangeByParts(ctx context.Context, dst ChunkWriter, localNodeKey ecdsa.PrivateKey, cnr cid.ID,
	parent oid.ID, sTok *session.Object, rule iec.Rule, ruleIdx int, sortedNodes []netmap.NodeInfo,
	pldLen, off, ln uint64, firstPartStream io.ReadCloser) (uint64, error) {
	totalParts := int(rule.DataPartNum + rule.ParityPartNum)
	fullPartLen := (pldLen + uint64(rule.DataPartNum) - 1) / uint64(rule.DataPartNum)

	// Mark ranges of EC parts.
	var firstPartIdx, lastPartIdx int
	var firstPartOff, lastPartTo uint64
	if ln == 0 && off == 0 { // full range request
		if pldLen == 0 {
			return 0, nil
		}

		lastPartIdx = int(rule.DataPartNum) - 1
		if lastPartTo = pldLen % fullPartLen; lastPartTo == 0 {
			lastPartTo = fullPartLen
		}
	} else {
		if off >= pldLen || pldLen-off < ln {
			return 0, apistatus.ErrObjectOutOfRange
		}

		firstPartIdx, firstPartOff, lastPartIdx, lastPartTo = requiredChildrenIter(off, ln, nEqualSizeIter(totalParts, fullPartLen))
	}

	deadline, deadlineSet := ctx.Deadline()
	var stageTimeout time.Duration

	// Fetch requested data parts one-by-one. Main part of the entire operation, but
	// 20% of the remaining context is reserved for possible necessary recovery.
	// The recovery consumes more and is slower, but happen quire less frequently.
	if deadlineSet {
		if stageTimeout = time.Until(deadline) * 4 / 5; stageTimeout <= 0 {
			return 0, context.DeadlineExceeded
		}
	} else {
		stageTimeout = 30 * time.Second
	}
	stageCtx, cancel := context.WithTimeout(ctx, stageTimeout)
	defer cancel()

	// TODO: We could open as many streams in parallel as we need. However, since GetRange
	//  stream is server-side, so the server sends data without processing ACK (this
	//  is only possible in bidirectional streams). This way, background buffering of
	//  data from all streams will start immediately. Explore gRPC abilities of
	//  buffer control for parallelism potential.

	failedPartIdx, failedPartWritten, written, err := s.copyECPartsRanges(stageCtx, dst, localNodeKey, cnr, parent, sTok, rule, ruleIdx, sortedNodes,
		fullPartLen, firstPartIdx, firstPartOff, lastPartIdx, lastPartTo, firstPartStream)
	if err == nil || errors.Is(err, apistatus.ErrObjectAlreadyRemoved) || errors.Is(err, apistatus.ErrObjectOutOfRange) ||
		errors.Is(err, errStreamFailure) || errors.Is(err, ctx.Err()) {
		return written, err
	}

	s.log.Info("copying ranges of EC data parts failed, trying recovery...",
		zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Stringer("rule", rule), zap.Uint64("off", off),
		zap.Uint64("len", ln), zap.Int("failedPartIdx", failedPartIdx), zap.Uint64("failedPartWritten", failedPartWritten),
		zap.Uint64("written", written), zap.Error(err))

	if failedPartIdx != firstPartIdx {
		firstPartOff = 0
	}
	firstPartIdx = failedPartIdx
	firstPartOff += failedPartWritten

	// Recover failed part. To do this, we need to fetch all other data parts (some
	// of them may be requested too) and at least 1 parity part. Also, reserve 10%
	// of the remaining context for maths.
	//
	// Note: we reread the parts that were successfully read at the previous stage. This is done to favor best case
	// (as most expected), so as not to retain too much data in memory. Anyway, recovery requires obtaining
	// ranges that were not originally requested.
	if deadlineSet {
		if stageTimeout = time.Until(deadline) * 9 / 10; stageTimeout <= 0 {
			return written, context.DeadlineExceeded
		}
	} // else keep the same
	stageCtx, cancel = context.WithTimeout(ctx, stageTimeout)
	defer cancel()

	parts, err := s.getRecoveryECPartRanges(stageCtx, localNodeKey, cnr, parent, sTok, rule, ruleIdx, sortedNodes, fullPartLen, failedPartIdx)
	if err != nil {
		return written, fmt.Errorf("get recovery EC part ranges: %w", err)
	}

	if err := iec.DecodeRange(rule, firstPartIdx, lastPartIdx, parts); err != nil { // should never happen if we count parts correctly
		return written, fmt.Errorf("recover EC part range from=%d to=%d using single parity part: %w", firstPartIdx, lastPartIdx, err)
	}

	for partIdx := firstPartIdx; partIdx <= lastPartIdx; partIdx++ {
		from, to := uint64(0), fullPartLen
		if partIdx == firstPartIdx {
			from = firstPartOff
		}
		if partIdx == lastPartIdx {
			to = lastPartTo
		}

		if err := dst.WriteChunk(parts[partIdx][from:to]); err != nil {
			return written, fmt.Errorf("%w: write [%d:%d] range of EC part #%d: %w", errStreamFailure, from, to, partIdx, err)
		}

		written += to - from
	}

	return written, nil
}

func (s *Service) copyECPartsRanges(ctx context.Context, dst ChunkWriter, localNodeKey ecdsa.PrivateKey, cnr cid.ID, parent oid.ID,
	sTok *session.Object, rule iec.Rule, ruleIdx int, sortedNodes []netmap.NodeInfo,
	fullPartLen uint64, firstIdx int, firstOff uint64, lastIdx int, lastTo uint64, fullFirstPart io.ReadCloser) (int, uint64, uint64, error) {
	var callTimeout time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		// don't take into account that first/last range may be tiny
		if callTimeout = time.Until(deadline) / time.Duration(lastIdx-firstIdx+1); callTimeout <= 0 {
			return 0, 0, 0, context.DeadlineExceeded
		}
	} else {
		callTimeout = 30 * time.Second
	}

	var buf []byte
	var written uint64
	for curIdx := firstIdx; curIdx <= lastIdx; curIdx++ {
		partOff, partLen := uint64(0), fullPartLen
		if curIdx == firstIdx {
			partOff = firstOff
		}
		if curIdx == lastIdx {
			partLen = lastTo
		}
		partLen -= partOff

		var rc io.ReadCloser
		if curIdx == 0 && fullFirstPart != nil {
			if partOff > math.MaxInt64 {
				// underlying io functions behave specifically with negative input. 8EB+ are ranges from the future.
				return 0, 0, 0, fmt.Errorf("too big object range for this server: off=%d,ln=%d", partOff, partLen)
			}
			if _, err := io.CopyN(io.Discard, fullFirstPart, int64(partOff)); err != nil {
				err = convertContextStatus(err)
				return 0, 0, 0, fmt.Errorf("discard first %d bytes from first part payload stream: %w", partOff, err)
			}

			if lastIdx == 0 && partOff+partLen < fullPartLen {
				rc = struct {
					io.Reader
					io.Closer
				}{
					Reader: io.LimitReader(fullFirstPart, int64(partLen)),
					Closer: fullFirstPart,
				}
			} else {
				rc = fullFirstPart
			}
		} else {
			callCtx, cancel := context.WithTimeout(ctx, callTimeout)
			defer cancel()
			var err error
			rc, err = s.getECPartRangeStream(callCtx, cnr, parent, partOff, partLen, sTok, rule, ruleIdx, sortedNodes, curIdx, localNodeKey)
			if err != nil {
				return curIdx, 0, written, fmt.Errorf("open stream: %w", err)
			}
		}

		if buf == nil {
			bufLen := calcECRangeBufferLen(fullPartLen, firstIdx, firstOff, lastIdx, lastTo)
			buf = make([]byte, bufLen)
		}

		n, err := copyPayloadStreamBuffer(dst, rc, buf)
		written += n
		rc.Close()
		if err != nil {
			if n == partLen { // should never happen
				err = fmt.Errorf("%w: received needed range with error for EC part #%d: %w", errStreamFailure, curIdx, err)
			}
			return curIdx, n, written, err
		}
	}

	return 0, 0, written, nil
}

func (s *Service) getRecoveryECPartRanges(ctx context.Context, localNodeKey ecdsa.PrivateKey, cnr cid.ID, parent oid.ID,
	sTok *session.Object, rule iec.Rule, ruleIdx int, sortedNodes []netmap.NodeInfo, fullPartLen uint64, failedPartIdx int) ([][]byte, error) {
	totalParts := int(rule.DataPartNum + rule.ParityPartNum)
	parts := make([][]byte, totalParts)

	rem := uint32(rule.DataPartNum)
	var failCounter, okCounter atomic.Uint32
	failCounter.Store(1)

	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(int(rem))
	for i := range totalParts {
		if i == failedPartIdx {
			continue
		}

		partIdx := i
		eg.Go(func() error {
			part, err := s.readFullECPartRange(egCtx, cnr, parent, sTok, rule, ruleIdx, sortedNodes, partIdx, localNodeKey, fullPartLen)
			if err != nil {
				if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) || errors.Is(err, apistatus.ErrObjectOutOfRange) || errors.Is(err, egCtx.Err()) {
					return err
				}

				s.log.Info("failed to get full EC part payload",
					zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Stringer("rule", rule),
					zap.Int("ruleIdx", ruleIdx), zap.Int("partIdx", partIdx), zap.Error(err))

				if failed := failCounter.Add(1); failed > uint32(rule.ParityPartNum) {
					return tooManyPartsUnavailableError(failed)
				}
				return nil
			}

			parts[partIdx] = part
			if okCounter.Add(1) >= rem {
				return errInterrupt
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil && !errors.Is(err, errInterrupt) {
		return nil, err
	}

	return parts, nil
}

func (s *Service) readFullECPartRange(ctx context.Context, cnr cid.ID, parent oid.ID, sTok *session.Object, rule iec.Rule,
	ruleIdx int, sortedNodes []netmap.NodeInfo, partIdx int, localNodeKey ecdsa.PrivateKey, fullPartLen uint64) ([]byte, error) {
	rc, err := s.getECPartRangeStream(ctx, cnr, parent, 0, 0, sTok, rule, ruleIdx, sortedNodes, partIdx, localNodeKey)
	if err != nil {
		return nil, fmt.Errorf("open stream: %w", err)
	}

	defer rc.Close()

	buf := make([]byte, fullPartLen)
	if _, err := io.ReadFull(rc, buf); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		} else {
			err = convertContextStatus(err)
		}
		return nil, fmt.Errorf("read stream: %w", err)
	}

	return buf, nil
}

func (s *Service) getECPartRangeStream(ctx context.Context, cnr cid.ID, parent oid.ID, off, ln uint64, sTok *session.Object,
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
			if !errors.Is(err, apistatus.ErrObjectNotFound) {
				s.log.Info("failed to get EC part payload range from local storage",
					zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Uint64("off", off), zap.Uint64("len", ln),
					zap.Stringer("rule", rule), zap.Int("ruleIdx", pi.RuleIndex), zap.Int("partIdx", pi.Index), zap.Error(err))
			}
			continue
		}

		rc, err = s.getECPartRangeFromNode(ctx, cnr, parent, off, ln, sTok, pi, localNodeKey, sortedNodes[i])
		if err != nil {
			if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) || errors.Is(err, apistatus.ErrObjectOutOfRange) || errors.Is(err, ctx.Err()) {
				return nil, err
			}
			if !errors.Is(err, apistatus.ErrObjectNotFound) {
				s.log.Info("failed to get EC part payload range from remote node",
					zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Uint64("off", off), zap.Uint64("len", ln),
					zap.Stringer("rule", rule), zap.Int("ruleIdx", pi.RuleIndex), zap.Int("partIdx", pi.Index), zap.Error(err))
			}
			continue
		}

		// Fallback to GET similar to fallbackRangeReader. Track https://github.com/nspcc-dev/neofs-node/issues/3547.
		b := []byte{0}
		_, err = io.ReadFull(rc, b)
		if err == nil {
			// TODO: consider reader that switches to another node to continue the stream
			return struct {
				io.Reader
				io.Closer
			}{
				Reader: io.MultiReader(bytes.NewReader(b), rc),
				Closer: rc,
			}, nil
		}

		err = convertContextStatus(err)

		if !errors.Is(err, apistatus.ErrObjectAccessDenied) {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return nil, fmt.Errorf("read GetRange response stream: %w", err)
		}

		_, rc, err = s.getECPartFromNode(ctx, cnr, parent, sTok, pi, sortedNodes[i])
		if err != nil {
			err = convertContextStatus(err)
			if errors.Is(err, ctx.Err()) {
				return nil, err
			}
			if !errors.Is(err, apistatus.ErrObjectNotFound) {
				s.log.Info("range->get fallback attempt for EC part failed on stream init, trying another node...",
					zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Stringer("rule", rule),
					zap.Int("ruleIdx", pi.RuleIndex), zap.Int("partIdx", pi.Index), zap.Error(err))
			}
			continue
		}

		if off > math.MaxInt64 && ln > math.MaxInt64 {
			// underlying io functions behave specifically with negative input. 8EB+ are ranges from the future.
			s.log.Warn("too big object range for this server",
				zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Uint64("off", off), zap.Uint64("len", ln))
			continue
		}

		if _, err = io.CopyN(io.Discard, rc, int64(off)); err != nil {
			err = convertContextStatus(err)
			if errors.Is(err, ctx.Err()) {
				return nil, err
			}
			s.log.Info("range->get fallback attempt for EC part failed on stream seek",
				zap.Stringer("container", cnr), zap.Stringer("object", parent), zap.Stringer("rule", rule),
				zap.Int("ruleIdx", pi.RuleIndex), zap.Int("partIdx", pi.Index), zap.Uint64("skipBytes", off),
				zap.Error(err))
			continue
		}

		if ln == 0 { // full range request
			return rc, nil
		}

		return struct {
			io.Reader
			io.Closer
		}{
			Reader: io.LimitReader(rc, int64(ln)),
			Closer: rc,
		}, nil
	}

	return nil, errors.New("all nodes failed")
}

func (s *Service) getECPartRangeFromNode(ctx context.Context, cnr cid.ID, parent oid.ID, off, ln uint64, sTok *session.Object,
	pi iec.PartInfo, localNodeKey ecdsa.PrivateKey, node netmap.NodeInfo) (io.ReadCloser, error) {
	ruleIdxAttr := strconv.Itoa(pi.RuleIndex)
	partIdxAttr := strconv.Itoa(pi.Index)

	rc, err := s.conns.InitGetObjectRangeStream(ctx, node, localNodeKey, cnr, parent, off, ln, sTok, []string{
		iec.AttributeRuleIdx, ruleIdxAttr,
		iec.AttributePartIdx, partIdxAttr,
	})
	if err != nil {
		err = convertContextStatus(err)
		return nil, fmt.Errorf("get range from remote node: %w", err)
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

func checkPartRequestAgainstPolicy(ecRules []iec.Rule, pi iec.PartInfo) error {
	if len(ecRules) == 0 {
		return errors.New("EC part requested in container without EC policy")
	}

	if pi.RuleIndex >= len(ecRules) {
		return fmt.Errorf("EC rule index overflows container policy: idx=%d,rules=%d", pi.RuleIndex, len(ecRules))
	}

	if total := ecRules[pi.RuleIndex].DataPartNum + ecRules[pi.RuleIndex].ParityPartNum; pi.Index >= int(total) {
		return fmt.Errorf("EC part index overflows container policy: idx=%d,parts=%d", pi.Index, total)
	}

	return nil
}

func calcECRangeBufferLen(fullPartLen uint64, firstIdx int, firstOff uint64, lastIdx int, lastTo uint64) uint64 {
	var bufLen uint64
	switch lastIdx - firstIdx {
	case 0:
		bufLen = lastTo - firstOff
	case 1:
		bufLen = max(fullPartLen-firstOff, lastTo)
	default:
		bufLen = fullPartLen
	}

	return min(bufLen, streamChunkSize)
}
