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

	exec.log.Debug("trying to assemble the object...")

	splitInfo := exec.splitInfo()

	childID := splitInfo.Link()
	if childID == nil {
		childID = splitInfo.LastPart()
	}

	prev, children := exec.initFromChild(childID)

	if len(children) > 0 {
		if ok := exec.writeCollectedHeader(); ok {
			exec.overtakePayloadDirectly(children)
		}
	} else if prev != nil {
		if ok := exec.writeCollectedHeader(); ok {
			// TODO: choose one-by-one restoring algorithm according to size
			//  * if size > MAX => go right-to-left with HEAD and back with GET
			//  * else go right-to-left with GET and compose in single object before writing

			if ok := exec.overtakePayloadInReverse(prev); ok {
				// payload of all children except the last are written, write last payload
				exec.writeObjectPayload(exec.collectedObject)
			}
		}
	} else {
		exec.status = statusUndefined
		exec.err = object.ErrNotFound

		exec.log.Debug("could not init parent from child")
	}
}

func (exec *execCtx) initFromChild(id *objectSDK.ID) (prev *objectSDK.ID, children []*objectSDK.ID) {
	log := exec.log.With(zap.Stringer("child ID", id))

	log.Debug("starting assembling from child")

	child, ok := exec.getChild(id)
	if !ok {
		return
	}

	par := child.GetParent()
	if par == nil {
		exec.status = statusUndefined

		log.Debug("received child with empty parent")

		return
	} else if !equalAddresses(par.Address(), exec.address()) {
		exec.status = statusUndefined

		log.Debug("parent address in child object differs",
			zap.Stringer("expected", exec.address()),
			zap.Stringer("received", par.Address()),
		)

		return
	}

	exec.collectedObject = par
	object.NewRawFromObject(exec.collectedObject).SetPayload(child.Payload())

	return child.PreviousID(), child.Children()
}

func (exec *execCtx) overtakePayloadDirectly(children []*objectSDK.ID) {
	for i := range children {
		child, ok := exec.getChild(children[i])
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
	chain := make([]*objectSDK.ID, 0)

	// fill the chain end-to-start
	for prev != nil {
		head, ok := exec.headChild(prev)
		if !ok {
			return false
		}

		chain = append(chain, head.ID())

		prev = head.PreviousID()
	}

	// reverse chain
	for left, right := 0, len(chain)-1; left < right; left, right = left+1, right-1 {
		chain[left], chain[right] = chain[right], chain[left]
	}

	exec.overtakePayloadDirectly(chain)

	exec.status = statusOK
	exec.err = nil

	return true
}

func equalAddresses(a, b *objectSDK.Address) bool {
	return a.ContainerID().Equal(b.ContainerID()) &&
		a.ObjectID().Equal(b.ObjectID())
}
