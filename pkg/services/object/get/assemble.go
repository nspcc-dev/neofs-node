package getsvc

import (
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

func (exec *execCtx) assemble() {
	if !exec.canAssemble() {
		exec.log.Debug("can not assemble the object")
		return
	}

	// Do not use forwarding during assembly stage.
	// Request forwarding closure inherited in produced
	// `execCtx` so it should be disabled there.
	exec.disableForwarding()

	exec.log.Debug("trying to assemble the object...")

	splitInfo := exec.splitInfo()

	childID, ok := splitInfo.Link()
	if !ok {
		childID, ok = splitInfo.LastPart()
		if !ok {
			exec.log.Debug("neither linking nor last part of split-chain is presented in split info")
			return
		}
	}

	prev, children := exec.initFromChild(childID)

	if len(children) > 0 {
		if exec.ctxRange() == nil {
			if ok := exec.writeCollectedHeader(); ok {
				exec.overtakePayloadDirectly(children, nil, true)
			}
		} else {
			// TODO: #1155 choose one-by-one restoring algorithm according to size
			//  * if size > MAX => go right-to-left with HEAD and back with GET
			//  * else go right-to-left with GET and compose in single object before writing

			if ok := exec.overtakePayloadInReverse(children[len(children)-1]); ok {
				// payload of all children except the last are written, write last payload
				exec.writeObjectPayload(exec.collectedObject)
			}
		}
	} else if prev != nil {
		if ok := exec.writeCollectedHeader(); ok {
			// TODO: #1155 choose one-by-one restoring algorithm according to size
			//  * if size > MAX => go right-to-left with HEAD and back with GET
			//  * else go right-to-left with GET and compose in single object before writing

			if ok := exec.overtakePayloadInReverse(*prev); ok {
				// payload of all children except the last are written, write last payloa
				exec.writeObjectPayload(exec.collectedObject)
			}
		}
	} else {
		exec.log.Debug("could not init parent from child")
	}
}

func (exec *execCtx) initFromChild(obj oid.ID) (prev *oid.ID, children []oid.ID) {
	log := exec.log.With(zap.Stringer("child ID", obj))

	log.Debug("starting assembling from child")

	child, ok := exec.getChild(obj, nil, true)
	if !ok {
		return
	}

	par := child.Parent()
	if par == nil {
		exec.status = statusUndefined

		log.Debug("received child with empty parent")

		return
	}

	exec.collectedObject = par

	var payload []byte

	if rng := exec.ctxRange(); rng != nil {
		seekOff := rng.GetOffset()
		seekLen := rng.GetLength()
		seekTo := seekOff + seekLen
		parSize := par.PayloadSize()

		if seekTo < seekOff || parSize < seekOff || parSize < seekTo {
			var errOutOfRange apistatus.ObjectOutOfRange

			exec.err = &errOutOfRange
			exec.status = statusOutOfRange

			return
		}

		childSize := child.PayloadSize()

		exec.curOff = parSize - childSize

		from := uint64(0)
		if exec.curOff < seekOff {
			from = seekOff - exec.curOff
		}

		to := uint64(0)
		if seekOff+seekLen > exec.curOff+from {
			to = seekOff + seekLen - exec.curOff
		}

		payload = child.Payload()[from:to]
		rng.SetLength(rng.GetLength() - to + from)
	} else {
		payload = child.Payload()
	}

	exec.collectedObject.SetPayload(payload)

	idPrev, ok := child.PreviousID()
	if ok {
		return &idPrev, child.Children()
	}

	return nil, child.Children()
}

func (exec *execCtx) overtakePayloadDirectly(children []oid.ID, rngs []objectSDK.Range, checkRight bool) {
	withRng := len(rngs) > 0 && exec.ctxRange() != nil

	for i := range children {
		var r *objectSDK.Range
		if withRng {
			r = &rngs[i]
		}

		child, ok := exec.getChild(children[i], r, !withRng && checkRight)
		if !ok {
			return
		}

		if ok := exec.writeObjectPayload(child); !ok {
			return
		}
	}

	exec.status = statusOK
	exec.err = nil
}

func (exec *execCtx) overtakePayloadInReverse(prev oid.ID) bool {
	chain, rngs, ok := exec.buildChainInReverse(prev)
	if !ok {
		return false
	}

	reverseRngs := len(rngs) > 0

	// reverse chain
	for left, right := 0, len(chain)-1; left < right; left, right = left+1, right-1 {
		chain[left], chain[right] = chain[right], chain[left]

		if reverseRngs {
			rngs[left], rngs[right] = rngs[right], rngs[left]
		}
	}

	exec.overtakePayloadDirectly(chain, rngs, false)

	return exec.status == statusOK
}

func (exec *execCtx) buildChainInReverse(prev oid.ID) ([]oid.ID, []objectSDK.Range, bool) {
	var (
		chain   = make([]oid.ID, 0)
		rngs    = make([]objectSDK.Range, 0)
		seekRng = exec.ctxRange()
		from    = seekRng.GetOffset()
		to      = from + seekRng.GetLength()

		withPrev = true
	)

	// fill the chain end-to-start
	for withPrev {
		if exec.curOff < from {
			break
		}

		head, ok := exec.headChild(prev)
		if !ok {
			return nil, nil, false
		}

		if seekRng != nil {
			sz := head.PayloadSize()

			exec.curOff -= sz

			if exec.curOff < to {
				off := uint64(0)
				if from > exec.curOff {
					off = from - exec.curOff
					sz -= from - exec.curOff
				}

				if to < exec.curOff+off+sz {
					sz = to - off - exec.curOff
				}

				index := len(rngs)
				rngs = append(rngs, objectSDK.Range{})
				rngs[index].SetOffset(off)
				rngs[index].SetLength(sz)

				id, _ := head.ID()
				chain = append(chain, id)
			}
		} else {
			id, _ := head.ID()
			chain = append(chain, id)
		}

		prev, withPrev = head.PreviousID()
	}

	return chain, rngs, true
}

func equalAddresses(a, b oid.Address) bool {
	return a.Container().Equals(b.Container()) && a.Object().Equals(b.Object())
}
