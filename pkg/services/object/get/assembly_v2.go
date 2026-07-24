package getsvc

import (
	"errors"
	"iter"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

var (
	errNoLinkNoLastPart = errors.New("no link and no last part in split info")
	errWrongChildHeader = errors.New("wrong child header")
)

func (exec *execCtx) processV2Split(si *object.SplitInfo) {
	if si.GetFirstPart().IsZero() {
		exec.log.Debug("no first ID found in V2 split")
		exec.err = errors.New("v2 split without first object ID")

		return
	}

	linkID := si.GetLink()
	if !linkID.IsZero() && exec.processV2Link(linkID) {
		return
	}

	// fallback to the full chain assembly from the last part
	prev := si.GetLastPart()
	if prev.IsZero() {
		exec.log.Debug("neither link, not last part is set in the v2 split information")
		exec.err = errNoLinkNoLastPart
		return
	}

	exec.processV2Last(prev)
}

func (exec *execCtx) processV2Last(lastID oid.ID) {
	lastHead, ok := exec.headChild(lastID)
	if !ok {
		exec.log.Debug("failed to read last object")
		return
	}

	exec.collectedHeader = lastHead.Parent()
	if !exec.resolvePayloadRange() {
		return
	}
	if r := exec.ctxRange(); r != nil && r.GetLength() == 0 {
		r.SetLength(exec.collectedHeader.PayloadSize())
	}

	if ok := exec.writeCollectedHeader(); ok {
		exec.overtakePayloadInReverse(lastID)
	}
}

func (exec *execCtx) processV2Link(linkID oid.ID) bool {
	// need full payload of link object to parse link structure
	w := NewSimpleObjectWriter()

	p := exec.prm
	p.common = p.common.WithLocalOnly(false)
	p.objWriter = w
	p.SetRange(nil)
	p.addr.SetContainer(exec.containerID())
	p.addr.SetObject(linkID)

	exec.statusError = exec.svc.get(exec.context(), p.commonPrm, withLogger(exec.log))

	if exec.status != statusOK {
		exec.log.Debug("failed to read link object")
		return false
	}

	linkObj := w.Object()
	if !exec.isChild(linkObj) {
		exec.status = statusUndefined
		exec.err = errWrongChildHeader
		exec.log.Debug("parent address in link object differs")
		return false
	}

	exec.collectedHeader = linkObj.Parent()

	var link object.Link
	err := linkObj.ReadLink(&link)
	if err != nil {
		exec.log.Debug("failed to parse link object", zap.Error(err))
		return false
	}

	if !exec.resolvePayloadRange() {
		return true
	}
	rng := exec.ctxRange()
	if rng != nil && rng.GetLength() == 0 {
		rng.SetLength(exec.collectedHeader.PayloadSize())
	}

	if rng == nil {
		// GET case

		if exec.writeCollectedHeader() {
			exec.overtakePayloadDirectly(measuredObjsToIDs(link.Objects()), nil, true)
			return true
		}

		exec.log.Debug("failed to write parent header")

		// we failed to write the header, no need to try more
		return true
	}

	// RANGE case
	seekOff := rng.GetOffset()
	seekLen := rng.GetLength()
	parSize := linkObj.Parent().PayloadSize()
	if seekLen == 0 {
		seekLen = parSize
	}
	seekTo := seekOff + seekLen

	if seekTo < seekOff || parSize < seekOff || parSize < seekTo {
		exec.err = apistatus.ErrObjectOutOfRange
		exec.status = statusAPIResponse

		// the operation has failed but no need to continue so `true` here
		return true
	}

	if !exec.writeCollectedHeader() {
		exec.log.Debug("failed to write parent header")
		return true
	}

	return exec.rangeFromLink(link)
}

func (exec *execCtx) rangeFromLink(link object.Link) bool {
	children := link.Objects()
	rng := exec.ctxRange()
	first, firstOffset, last, lastBound := requiredChildren(rng.GetOffset(), rng.GetLength(), children)
	n := last - first + 1

	at := func(i int) (oid.ID, *object.Range, bool) {
		if i >= n {
			return oid.ID{}, nil, false
		}

		idx := first + i
		child := children[idx]

		var rngPerChild object.Range
		if idx == first || idx == last {
			if idx == first {
				rngPerChild.SetOffset(firstOffset)
				rngPerChild.SetLength(uint64(child.ObjectSize()) - firstOffset)
			}
			if idx == last {
				rngPerChild.SetLength(lastBound - rngPerChild.GetOffset())
			}
			return child.ObjectID(), &rngPerChild, true
		}

		return child.ObjectID(), nil, true
	}

	if n > 1 && prefetchWindow > 1 {
		exec.statusError = exec.streamChildrenPipelined(at, false)
		return true
	}

	for i := range n {
		id, r, _ := at(i)
		if !exec.copyChild(id, r, false) {
			return true
		}
	}

	exec.status = statusOK
	exec.err = nil

	return true
}

// it is required for ranges to be in the bounds of the all objects' payload;
// it must be checked on higher levels; returns (firstObject, firstObjectOffset,
// lastObject, lastObjectRightBound).
func requiredChildren(off, ln uint64, children []object.MeasuredObject) (int, uint64, int, uint64) {
	return requiredChildrenIter(off, ln, func(yield func(int, uint64) bool) {
		for i := range children {
			if !yield(i, uint64(children[i].ObjectSize())) {
				return
			}
		}
	})
}

func nEqualSizeIter(n int, sz uint64) iter.Seq2[int, uint64] {
	return func(yield func(int, uint64) bool) {
		for i := range n {
			if !yield(i, sz) {
				return
			}
		}
	}
}

func requiredChildrenIter(off, ln uint64, children iter.Seq2[int, uint64]) (int, uint64, int, uint64) {
	var firstChildIndex = -1
	var firstChildOffset uint64
	var lastChildIndex int
	var lastChildRightBound uint64

	leftBound := off
	rightBound := leftBound + ln

	var bytesSeen uint64

	for i, size := range children {
		bytesSeen += size

		if bytesSeen <= leftBound {
			continue
		}

		if firstChildIndex == -1 {
			firstChildIndex = i
			firstChildOffset = size - (bytesSeen - leftBound)
		}

		if rightBound <= bytesSeen {
			lastChildIndex = i
			lastChildRightBound = size - (bytesSeen - rightBound)
			break
		}
	}

	return firstChildIndex, firstChildOffset, lastChildIndex, lastChildRightBound
}

func measuredObjsToIDs(mm []object.MeasuredObject) []oid.ID {
	res := make([]oid.ID, 0, len(mm))
	for i := range mm {
		res = append(res, mm[i].ObjectID())
	}

	return res
}
