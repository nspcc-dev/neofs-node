package tsourse

import (
	"context"
	"errors"
	"fmt"

	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Source represents wrapper over the object service that
// allows checking if a tombstone is available in NeoFS
// network.
//
// Must be created via NewSource function. `var` and `Source{}`
// declarations leads to undefined behaviour and may lead
// to panics.
type Source struct {
	s *getsvc.Service
}

// TombstoneSourcePrm groups required parameters for Source creation.
type TombstoneSourcePrm struct {
	s *getsvc.Service
}

// SetGetService sets object service.
func (s *TombstoneSourcePrm) SetGetService(v *getsvc.Service) {
	s.s = v
}

// NewSource creates, initialize and returns local tombstone Source.
// The returned structure is ready to use.
//
// Panics if any of the provided options does not allow
// constructing a valid tombstone local Source.
func NewSource(p TombstoneSourcePrm) Source {
	if p.s == nil {
		panic("Tombstone source: nil object service")
	}

	return Source(p)
}

type headerWriter struct {
	o *object.Object
}

func (h *headerWriter) WriteHeader(o *object.Object) error {
	h.o = o
	return nil
}

// Tombstone checks if the engine stores tombstone.
// Returns nil, nil if the tombstone has been removed
// or marked for removal.
func (s Source) Tombstone(ctx context.Context, a oid.Address, _ uint64) (*object.Object, error) {
	var hr headerWriter

	var headPrm getsvc.HeadPrm
	headPrm.WithAddress(a)
	headPrm.SetHeaderWriter(&hr)
	headPrm.SetCommonParameters(&util.CommonPrm{}) // default values are ok for that operation

	err := s.s.Head(ctx, headPrm)
	switch {
	case errors.As(err, new(apistatus.ObjectNotFound)) || errors.As(err, new(apistatus.ObjectAlreadyRemoved)):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("could not get tombstone from the source: %w", err)
	default:
	}

	if hr.o.Type() != object.TypeTombstone {
		return nil, fmt.Errorf("returned %s object is not a tombstone", a)
	}

	return hr.o, nil
}
