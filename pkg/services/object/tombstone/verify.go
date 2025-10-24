package tombstone

import (
	"context"
	"errors"
	"fmt"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/core/version"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"golang.org/x/sync/errgroup"
)

// maxConcurrentChecks defines now many tombstone members can be checked simultaneously.
const maxConcurrentChecks = 16

// ObjectSource describes objects available in the NeoFS.
type ObjectSource interface {
	// Head returns object by its address and any error that does not
	// allow processing operation. Must return *object.SplitInfoError
	// if an object is split, NOT the original parent header.
	// Returns [apistatus.ErrObjectAlreadyRemoved] if object exists but marked
	// for removal.
	Head(ctx context.Context, addr oid.Address) (*object.Object, error)

	// SearchOne returns objects ID that satisfy provided search filters with limit 1 and
	// any error that does not allow processing operation.
	// If an object is not found, it returns zero ID.
	SearchOne(ctx context.Context, cnr cid.ID, filter object.SearchFilters) (oid.ID, error)
}

// Verifier implements [object.TombVerifier] interface.
type Verifier struct {
	objs ObjectSource
}

// NewVerifier returns Verifier that ready to Verifier.VerifyTomb.
// Get and Search services must be non-nil, otherwise stable work is not
// guaranteed.
func NewVerifier(objSource ObjectSource) *Verifier {
	return &Verifier{
		objs: objSource,
	}
}

// VerifyTombStoneWithoutPayload verifies API v2.18+ tombstones. Checks that it
// does not store child object of a finished root (user's) object. Only child
// objects without a link object are acceptable for partial removal (as a
// garbage collection routine). Return any error that does not allow verification.
func (v *Verifier) VerifyTombStoneWithoutPayload(ctx context.Context, t object.Object) error {
	if t.Type() != object.TypeTombstone {
		return fmt.Errorf("not a tombstone object: %s", t.Type())
	}
	if !version.SysObjTargetShouldBeInHeader(t.Version()) {
		return fmt.Errorf("old tombstone with payload expected (version: %s)", t.Version())
	}
	deleted := t.AssociatedObject()
	if deleted.IsZero() {
		return errors.New("tombstone object with no target object")
	}

	return v.verifyMember(ctx, t.GetContainerID(), deleted)
}

// VerifyTomb verifies tombstone. Checks that it does not store child object of a
// finished root (user's) object. Only child objects without a link object are acceptable
// for partial removal (as a garbage collection routine).
// Return any error that does not allow verification.
func (v *Verifier) VerifyTomb(ctx context.Context, cnr cid.ID, t object.Tombstone) error {
	// this code is written when there is the V2 split scheme already,
	// so no split ID is expected, moreover, what is it for tombs
	// is not cleat at all
	if t.SplitID() != nil {
		return fmt.Errorf("unexpected split ID: %s", t.SplitID())
	}

	var wg errgroup.Group
	wg.SetLimit(maxConcurrentChecks)

	for _, member := range t.Members() {
		wg.Go(func() error {
			err := v.verifyMember(ctx, cnr, member)
			if err != nil {
				err = fmt.Errorf("verifying %s member: %w", member, err)
			}

			return err
		})
	}

	return wg.Wait()
}

func (v *Verifier) verifyMember(ctx context.Context, cnr cid.ID, member oid.ID) error {
	var addr oid.Address
	addr.SetContainer(cnr)
	addr.SetObject(member)

	header, err := v.objs.Head(ctx, addr)
	if err != nil {
		if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) {
			// may occur if member's tombstone (same or another) has already landed on other
			// nodes. The header is not available in this case, but the tombstone was
			// accepted by another node, we trust it.
			return nil
		}
		var siErr *object.SplitInfoError
		if errors.As(err, &siErr) {
			// inhuming parent object, that is ok
			return nil
		}

		return fmt.Errorf("heading object: %w", err)
	}

	firstChild, firstSet := header.FirstID()
	parent := header.Parent()
	sID := header.SplitID()

	if sID == nil /* V1 split */ && (!firstSet && parent == nil) /* V2 split */ {
		// regular small object removal
		return nil
	}

	// it is a child object removal somehow; only allowed for incomplete object put
	// (garbage collecting) so check if there is a correct link object for the chain

	if sID != nil {
		err = v.verifyV1Child(ctx, cnr, *sID)
		if err != nil {
			return fmt.Errorf("verify V1 split: %w", err)
		}
	} else {
		if iec.ObjectWithAttributes(*header) {
			return errors.New("object has EC attributes")
		}

		if !firstSet {
			// checks above say that this is a (non-V1) split object and also
			// a first object is the only part that does not have a first object
			// field set, obviously
			firstChild = addr.Object()
		}

		err = v.verifyV2Child(ctx, cnr, firstChild)
		if err != nil {
			return fmt.Errorf("verify V2 split: %w", err)
		}
	}

	return nil
}

func (v *Verifier) verifyV1Child(ctx context.Context, cnr cid.ID, sID object.SplitID) error {
	filters := object.SearchFilters{}
	filters.AddSplitIDFilter(object.MatchStringEqual, sID)
	filters.AddPayloadSizeFilter(object.MatchStringEqual, 0)

	id, err := v.objs.SearchOne(ctx, cnr, filters)
	if err != nil {
		return fmt.Errorf("searching objects: %w", err)
	}

	if !id.IsZero() {
		return fmt.Errorf("found link object %s", id)
	}

	return nil
}

func (v *Verifier) verifyV2Child(ctx context.Context, cnr cid.ID, firstObject oid.ID) error {
	filters := object.SearchFilters{}
	filters.AddFirstSplitObjectFilter(object.MatchStringEqual, firstObject)
	filters.AddTypeFilter(object.MatchStringEqual, object.TypeLink)

	id, err := v.objs.SearchOne(ctx, cnr, filters)
	if err != nil {
		return fmt.Errorf("searching objects: %w", err)
	}

	if !id.IsZero() {
		return fmt.Errorf("found link object %s", id)
	}
	return nil
}
