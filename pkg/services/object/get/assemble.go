package getsvc

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
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

	childID := splitInfo.Link()
	if childID == nil {
		childID = splitInfo.LastPart()
	}

	prev, children := exec.initFromChild(childID)

	if len(children) > 0 {
		if exec.ctxRange() == nil {
			if ok := exec.writeCollectedHeader(); ok {
				exec.overtakePayloadDirectly(children, nil, true)
			}
		} else {
			// TODO: choose one-by-one restoring algorithm according to size
			//  * if size > MAX => go right-to-left with HEAD and back with GET
			//  * else go right-to-left with GET and compose in single object before writing

			if ok := exec.overtakePayloadInReverse(children[len(children)-1]); ok {
				// payload of all children except the last are written, write last payload
				exec.writeObjectPayload(exec.collectedObject)
			}
		}
	} else if prev != nil {
		if ok := exec.writeCollectedHeader(); ok {
			// TODO: choose one-by-one restoring algorithm according to size
			//  * if size > MAX => go right-to-left with HEAD and back with GET
			//  * else go right-to-left with GET and compose in single object before writing

			if ok := exec.overtakePayloadInReverse(prev); ok {
				// payload of all children except the last are written, write last payloa
				exec.writeObjectPayload(exec.collectedObject)
			}
		}
	} else {
		exec.log.Debug("could not init parent from child")
	}
}

func (exec *execCtx) initFromChild(id *objectSDK.ID) (prev *objectSDK.ID, children []*objectSDK.ID) {
	log := exec.log.With(zap.Stringer("child ID", id))

	log.Debug("starting assembling from child")

	child, ok := exec.getChild(id, nil, true)
	if !ok {
		return
	}

	par := child.GetParent()
	if par == nil {
		exec.status = statusUndefined

		log.Debug("received child with empty parent")

		return
	}

	exec.collectedObject = par

	var payload []byte

	if rng := exec.ctxRange(); rng != nil {
		seekLen := rng.GetLength()
		seekOff := rng.GetOffset()
		parSize := par.PayloadSize()

		if seekOff+seekLen > parSize {
			exec.status = statusOutOfRange
			exec.err = object.ErrRangeOutOfBounds
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

	object.NewRawFromObject(exec.collectedObject).SetPayload(payload)

	return child.PreviousID(), child.Children()
}

func (exec *execCtx) overtakePayloadDirectly(children []*objectSDK.ID, rngs []*objectSDK.Range, checkRight bool) {
	withRng := len(rngs) > 0 && exec.ctxRange() != nil

	for i := range children {
		var r *objectSDK.Range
		if withRng {
			r = rngs[i]
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

func (exec *execCtx) overtakePayloadInReverse(prev *objectSDK.ID) bool {
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

	exec.status = statusOK
	exec.err = nil

	return true
}

func (exec *execCtx) buildChainInReverse(prev *objectSDK.ID) ([]*objectSDK.ID, []*objectSDK.Range, bool) {
	var (
		chain   = make([]*objectSDK.ID, 0)
		rngs    = make([]*objectSDK.Range, 0)
		seekRng = exec.ctxRange()
		from    = seekRng.GetOffset()
		to      = from + seekRng.GetLength()
	)

	// fill the chain end-to-start
	for prev != nil {
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

				r := objectSDK.NewRange()
				r.SetOffset(off)
				r.SetLength(sz)

				rngs = append(rngs, r)
				chain = append(chain, head.ID())
			}
		} else {
			chain = append(chain, head.ID())
		}

		prev = head.PreviousID()
	}

	return chain, rngs, true
}

func equalAddresses(a, b *objectSDK.Address) bool {
	return a.ContainerID().Equal(b.ContainerID()) &&
		a.ObjectID().Equal(b.ObjectID())
}
