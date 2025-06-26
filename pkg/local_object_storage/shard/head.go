package shard

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
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
func (s *Shard) Head(addr oid.Address, raw bool) (*objectSDK.Object, error) {
	var (
		errSplitInfo *objectSDK.SplitInfoError
		children     = make([]oid.Address, 0, 2)
	)
	if !s.GetMode().NoMetabase() {
		available, err := s.metaBase.Exists(addr, false)
		if err != nil && !errors.As(err, &errSplitInfo) {
			return nil, err
		}
		if errSplitInfo != nil {
			if raw {
				return nil, err
			}
			var si = errSplitInfo.SplitInfo()

			children = append(children, oid.NewAddress(addr.Container(), si.GetLastPart()))
			children = append(children, oid.NewAddress(addr.Container(), si.GetLink()))
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

	// SI present, but no objects found -> let caller handle SI.
	if len(children) != 0 {
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
