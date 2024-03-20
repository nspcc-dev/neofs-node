package split

import (
	"context"
	"fmt"

	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"golang.org/x/sync/errgroup"
)

// maxConcurrentObjects defines now many objects in the chain are checked simultaneously.
const maxConcurrentObjects = 16

// NewVerifier returns Verifier that ready to Verifier.VerifySplit.
// Get service must be non-nil, otherwise stable work is not
// guaranteed.
func NewVerifier(get *getsvc.Service) *Verifier {
	return &Verifier{
		get: get,
	}
}

// Verifier implements [object.SplitVerifier] interface.
type Verifier struct {
	get *getsvc.Service
}

// VerifySplit verifies split chains:
//  1. Children should be ordered correctly (in the sense of a linked list child objects compose)
//  2. NeoFS should answer to the HEAD requests with children whose object size corresponds to the link's info
//  3. Every child should have a provided first object in its split header (the first object itself is an exception)
func (v *Verifier) VerifySplit(ctx context.Context, cnr cid.ID, firstID oid.ID, childrenFromLink []object.MeasuredObject) error {
	uncheckedChildren := childrenFromLink
	for len(uncheckedChildren) > 0 {
		if len(uncheckedChildren) > maxConcurrentObjects {
			bound := len(uncheckedChildren) - maxConcurrentObjects
			group := uncheckedChildren[bound:]

			leftChild, err := v.verifyChildGroup(ctx, cnr, firstID, false, group)
			if err != nil {
				return err
			}

			uncheckedChildren = uncheckedChildren[:bound]

			if leftChild != nil {
				leftChildID, _ := leftChild.ID()
				prevRead, _ := leftChild.PreviousID()
				prevGot := uncheckedChildren[len(uncheckedChildren)-1].ObjectID()

				if prevRead != prevGot {
					return fmt.Errorf("link: object %s has wrong previous object: got %s, want: %s", leftChildID, prevGot, prevRead)
				}
			}
		} else {
			_, err := v.verifyChildGroup(ctx, cnr, firstID, true, childrenFromLink)
			if err != nil {
				return err
			}

			break
		}
	}

	return nil
}

type headerWriter struct {
	h *object.Object
}

func (w *headerWriter) WriteHeader(o *object.Object) error {
	w.h = o
	return nil
}

func (v *Verifier) verifyChildGroup(ctx context.Context, cnr cid.ID, firstID oid.ID, firstObjIncluded bool, children []object.MeasuredObject) (*object.Object, error) {
	var wg errgroup.Group
	receivedObjects := make([]*object.Object, len(children))

	for i := range children {
		iCopy := i
		var shouldHaveFirstObject *oid.ID
		if firstObjIncluded && iCopy != 0 {
			shouldHaveFirstObject = &firstID
		}

		wg.Go(func() error {
			var err error
			receivedObjects[iCopy], err = v.verifySinglePart(ctx, cnr, shouldHaveFirstObject, children[iCopy])

			return err
		})
	}

	err := wg.Wait()
	if err != nil {
		return nil, err
	}

	// check children order
	for i, o := range receivedObjects {
		id, _ := o.ID()

		if firstObjIncluded && i == 0 {
			// first object check only

			_, firstSet := o.FirstID()
			if firstSet {
				return nil, fmt.Errorf("link: first object has split firstID: %s", id)
			}

			_, prevSet := o.PreviousID()
			if prevSet {
				return nil, fmt.Errorf("link: first object has split previousID: %s", id)
			}
		} else {
			prevRead, prevSet := o.PreviousID()
			if !prevSet {
				return nil, fmt.Errorf("link: non-first object does not have previous ID: %s", id)
			}

			if i == 0 {
				// does not have the prev here, will be checked one level above
				return o, nil
			}

			prevGot, _ := receivedObjects[i-1].ID()
			if prevRead != prevGot {
				return nil, fmt.Errorf("link: object %s has wrong previous object: got %s, want: %s", id, prevGot, prevRead)
			}
		}
	}

	return nil, nil
}

func (v *Verifier) verifySinglePart(ctx context.Context, cnr cid.ID, firstID *oid.ID, objToCheck object.MeasuredObject) (*object.Object, error) {
	var childAddr oid.Address
	childAddr.SetContainer(cnr)
	childAddr.SetObject(objToCheck.ObjectID())

	var hw headerWriter

	// no custom common prms since a caller is expected to be a container
	// participant so no additional headers, access tokens, etc
	var prm getsvc.HeadPrm
	prm.SetHeaderWriter(&hw)
	prm.WithAddress(childAddr)
	prm.WithRawFlag(true)

	err := v.get.Head(ctx, prm)
	if err != nil {
		return nil, fmt.Errorf("reading %s header: %w", childAddr, err)
	}

	if firstID != nil {
		idRead, has := hw.h.FirstID()
		if !has {
			return nil, readObjectErr(childAddr, "object that does not have first object's ID")
		}
		if idRead != *firstID {
			return nil, readObjectErr(childAddr, fmt.Sprintf("its first object is unknown: got: %s, want: %s", idRead, firstID))
		}
	}

	if sizeRead := uint32(hw.h.PayloadSize()); sizeRead != objToCheck.ObjectSize() {
		return nil, readObjectErr(childAddr, fmt.Sprintf("its size differs: got: %d, want: %d", sizeRead, objToCheck.ObjectSize()))
	}

	return hw.h, nil
}

func readObjectErr(a oid.Address, text string) error {
	return fmt.Errorf("read %s object: %s", a, text)
}
