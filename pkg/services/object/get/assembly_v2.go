package getsvc

import (
	"errors"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

func (exec *execCtx) processV2Split(si *objectSDK.SplitInfo) {
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
		return
	}

	exec.processV2Last(prev)
}

func (exec *execCtx) processV2Last(lastID oid.ID) {
	lastObj, ok := exec.getChild(lastID, nil, true)
	if !ok {
		exec.log.Debug("failed to read last object")
		return
	}

	exec.collectedObject = lastObj.Parent()
	if r := exec.ctxRange(); r != nil && r.GetLength() == 0 {
		r.SetLength(exec.collectedObject.PayloadSize())
	}

	// copied from V1, and it has the same problems as V1;
	// see it for comments and optimization suggestions
	if ok := exec.writeCollectedHeader(); ok {
		if ok := exec.overtakePayloadInReverse(lastID); ok {
			exec.writeObjectPayload(exec.collectedObject)
		}
	}
}

func (exec *execCtx) processV2Link(linkID oid.ID) bool {
	linkObj, ok := exec.getChild(linkID, nil, true)
	if !ok {
		exec.log.Debug("failed to read link object")
		return false
	}

	exec.collectedObject = linkObj.Parent()
	rng := exec.ctxRange()
	if rng != nil && rng.GetLength() == 0 {
		rng.SetLength(exec.collectedObject.PayloadSize())
	}

	var link objectSDK.Link
	err := linkObj.ReadLink(&link)
	if err != nil {
		exec.log.Debug("failed to parse link object", zap.Error(err))
		return false
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
		var errOutOfRange apistatus.ObjectOutOfRange

		exec.err = &errOutOfRange
		exec.status = statusAPIResponse

		// the operation has failed but no need to continue so `true` here
		return true
	}

	return exec.rangeFromLink(link)
}

func (exec *execCtx) rangeFromLink(link objectSDK.Link) bool {
	children := link.Objects()
	first, firstOffset, last, lastBound := requiredChildren(exec.ctxRange(), children)

	for i := first; i <= last; i++ {
		child := children[i]

		var rngPerChild *objectSDK.Range
		if i == first || i == last {
			rngPerChild = new(objectSDK.Range)

			if i == first {
				rngPerChild.SetOffset(uint64(firstOffset))
				rngPerChild.SetLength(uint64(child.ObjectSize()) - uint64(firstOffset))
			}
			if i == last {
				rngPerChild.SetLength(uint64(lastBound) - rngPerChild.GetOffset())
			}
		}

		part, ok := exec.getChild(child.ObjectID(), rngPerChild, false)
		if !ok {
			return false
		}

		if !exec.writeObjectPayload(part) {
			// we have payload, we want to send it but can't so stop here
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
func requiredChildren(rng *objectSDK.Range, children []objectSDK.MeasuredObject) (int, int, int, int) {
	var firstChildIndex = -1
	var firstChildOffset int
	var lastChildIndex int
	var lastChildRightBound int

	leftBound := rng.GetOffset()
	rightBound := leftBound + rng.GetLength()

	var bytesSeen uint64

	for i, child := range children {
		size := uint64(child.ObjectSize())
		bytesSeen += size

		if bytesSeen <= leftBound {
			continue
		}

		if firstChildIndex == -1 {
			firstChildIndex = i
			firstChildOffset = int(size - (bytesSeen - leftBound))
		}

		if rightBound <= bytesSeen {
			lastChildIndex = i
			lastChildRightBound = int(size - (bytesSeen - rightBound))
			break
		}
	}

	return firstChildIndex, firstChildOffset, lastChildIndex, lastChildRightBound
}

func measuredObjsToIDs(mm []objectSDK.MeasuredObject) []oid.ID {
	res := make([]oid.ID, 0, len(mm))
	for i := range mm {
		res = append(res, mm[i].ObjectID())
	}

	return res
}
