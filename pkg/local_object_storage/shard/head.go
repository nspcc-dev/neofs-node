package shard

import (
	"errors"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Head reads header of the object from the shard. raw flag controls split
// object handling, if unset, then virtual object header is returned, otherwise
// SplitInfo of this object.
//
// Returns any error encountered.
//
// Returns an error of type apistatus.ObjectNotFound if object is missing in Shard.
// Returns an error of type apistatus.ObjectAlreadyRemoved if the requested object has been marked as removed in shard.
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
func (s *Shard) Head(addr oid.Address, raw bool) (*object.Object, error) {
	var (
		errSplitInfo *object.SplitInfoError
		children     []oid.Address
	)
	if !s.GetMode().NoMetabase() {
		available, err := s.metaBase.Exists(addr, false)
		if err != nil {
			var errECParts iec.ErrParts
			switch {
			default:
				return nil, err
			case errors.As(err, &errSplitInfo):
				if raw {
					return nil, err
				}
				var si = errSplitInfo.SplitInfo()

				children = []oid.Address{oid.NewAddress(addr.Container(), si.GetLastPart()),
					oid.NewAddress(addr.Container(), si.GetLink())}
			case errors.As(err, &errECParts):
				if len(errECParts) == 0 {
					panic(errors.New("empty EC part set"))
				}

				children = make([]oid.Address, len(errECParts))
				for i := range errECParts {
					children[i] = oid.NewAddress(addr.Container(), errECParts[i])
				}
			}
		} else if !available {
			return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
		}
	}

	for _, child := range children {
		if child.Object().IsZero() {
			continue
		}

		if s.hasWriteCache() {
			childHead, err := s.writeCache.Head(child)
			if err == nil {
				return childHead.Parent(), nil
			}
		}

		childHead, err := s.blobStor.Head(child)
		if err == nil {
			return childHead.Parent(), nil
		}
	}

	if len(children) != 0 {
		if errSplitInfo == nil {
			return nil, logicerr.Wrap(apistatus.ErrObjectNotFound)
		}
		// SI present, but no objects found -> let caller handle SI.
		return nil, errSplitInfo
	}

	if s.hasWriteCache() {
		obj, err := s.writeCache.Head(addr)
		if err == nil {
			return obj, err
		}
	}

	return s.blobStor.Head(addr)
}

func (s *Shard) HeadBuffered(buf []byte, addr oid.Address, raw bool) (int, error) {
	var (
		errSplitInfo *object.SplitInfoError
		children     []oid.Address
	)
	if !s.GetMode().NoMetabase() {
		available, err := s.metaBase.Exists(addr, false)
		if err != nil {
			var errECParts iec.ErrParts
			switch {
			default:
				return 0, err
			case errors.As(err, &errSplitInfo):
				if raw {
					return 0, err
				}
				var si = errSplitInfo.SplitInfo()

				children = []oid.Address{oid.NewAddress(addr.Container(), si.GetLastPart()),
					oid.NewAddress(addr.Container(), si.GetLink())}
			case errors.As(err, &errECParts):
				if len(errECParts) == 0 {
					panic(errors.New("empty EC part set"))
				}

				children = make([]oid.Address, len(errECParts))
				for i := range errECParts {
					children[i] = oid.NewAddress(addr.Container(), errECParts[i])
				}
			}
		} else if !available {
			return 0, logicerr.Wrap(apistatus.ObjectNotFound{})
		}
	}

	for _, child := range children {
		if child.Object().IsZero() {
			continue
		}

		if s.hasWriteCache() {
			childHead, err := s.writeCache.Head(child)
			if err == nil {
				return copy(buf, childHead.Parent().Marshal()), nil
			}
		}

		n, err := s.blobStor.HeadBuffered(buf, child)
		if err == nil {
			return n, nil
		}
	}

	if len(children) != 0 {
		if errSplitInfo == nil {
			return 0, logicerr.Wrap(apistatus.ErrObjectNotFound)
		}
		// SI present, but no objects found -> let caller handle SI.
		return 0, errSplitInfo
	}

	if s.hasWriteCache() {
		obj, err := s.writeCache.Head(addr)
		if err == nil {
			return copy(buf, obj.Marshal()), nil
		}
	}

	return s.blobStor.HeadBuffered(buf, addr)
}
