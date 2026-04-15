package shard

import (
	"errors"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

func (s *Shard) checkExistence(addr oid.Address, raw bool) (*object.SplitInfoError, []oid.Address, error) {
	if s.GetMode().NoMetabase() {
		return nil, nil, nil
	}

	available, err := s.metaBase.Exists(addr, false)
	if err != nil {
		var errECParts iec.ErrParts
		var errSplitInfo *object.SplitInfoError
		switch {
		default:
			return nil, nil, err
		case errors.As(err, &errSplitInfo):
			if raw {
				return nil, nil, err
			}
			var si = errSplitInfo.SplitInfo()

			return errSplitInfo, []oid.Address{
				oid.NewAddress(addr.Container(), si.GetLastPart()),
				oid.NewAddress(addr.Container(), si.GetLink()),
			}, nil
		case errors.As(err, &errECParts):
			if len(errECParts) == 0 {
				panic(errors.New("empty EC part set"))
			}

			children := make([]oid.Address, len(errECParts))
			for i := range errECParts {
				children[i] = oid.NewAddress(addr.Container(), errECParts[i])
			}

			return nil, children, nil
		}
	}

	if !available {
		return nil, nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	return nil, nil, nil
}

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
	debugLogger := s.log.With(zap.String("component", "DEBUG TS panic, shard"), zap.Stringer("addr", addr))

	errSplitInfo, children, err := s.checkExistence(addr, raw)
	if errSplitInfo != nil {
		debugLogger.Info("DEBUG: `checkExistence`", zap.String("errSplitInfo", errSplitInfo.Error()), zap.Stringers("children", children), zap.Error(err))
	} else {
		debugLogger.Info("DEBUG: `checkExistence`", zap.Stringers("children", children), zap.Error(err))
	}
	if err != nil {
		return nil, err
	}

	for _, child := range children {
		if child.Object().IsZero() {
			continue
		}

		if s.hasWriteCache() {
			childHead, err := s.writeCache.Head(child)
			if err == nil {
				par := childHead.Parent()
				debugLogger.Info("DEBUG: returning object from write-cache by a child", zap.Stringer("childAddr", childHead.GetID()), zap.Bool("parIsNil", par == nil))

				return childHead.Parent(), nil
			}
		}

		childHead, err := s.blobStor.Head(child)
		if err == nil {
			par := childHead.Parent()
			debugLogger.Info("DEBUG: returning object from blobStor by a child", zap.Stringer("childAddr", childHead.GetID()), zap.Bool("parIsNil", par == nil))

			return childHead.Parent(), nil
		}
	}

	if len(children) != 0 {
		if errSplitInfo == nil {
			debugLogger.Info("DEBUG: returning 404 cause children not found and `errSplitInfo` is empty", zap.Stringers("children", children))

			return nil, logicerr.Wrap(apistatus.ErrObjectNotFound)
		}
		// SI present, but no objects found -> let caller handle SI.

		debugLogger.Info("DEBUG: returning errSplitInfo cause no children found", zap.Error(errSplitInfo))

		return nil, errSplitInfo
	}

	if s.hasWriteCache() {
		obj, err := s.writeCache.Head(addr)
		if err == nil {
			debugLogger.Info("DEBUG: returning direct writecache's HEAD", zap.Bool("objIsNil", obj == nil), zap.Error(err))
			return obj, err
		}
	}

	obj, err := s.blobStor.Head(addr)
	debugLogger.Info("DEBUG: returning direct blobStor's HEAD", zap.Bool("objIsNil", obj == nil), zap.Error(err))

	return obj, err
}

// ReadHeader reads first bytes of the referenced object's binary containing its
// full header from s into buf. Returns number of bytes read.
//
// If write-cache is enabled, ReadHeader looks up there first. Its errors are
// logged and never returned.
//
// If object is missing, ReadHeader returns [apistatus.ErrObjectNotFound].
//
// If object is known but removed, ReadHeader returns
// [apistatus.ErrObjectAlreadyRemoved].
//
// If object is known but expired, ReadHeader returns [meta.ErrObjectIsExpired].
//
// If object is a split-parent, behavior depends on raw flag. If set, ReadHeader
// returns [object.SplitInfoError] with all relations recorded in s. If unset,
// ReadHeader reads header of the requested object from the child one into buf.
func (s *Shard) ReadHeader(addr oid.Address, raw bool, buf []byte) (int, error) {
	errSplitInfo, children, err := s.checkExistence(addr, raw)
	if err != nil {
		return 0, err
	}

	for _, child := range children {
		if child.Object().IsZero() {
			continue
		}

		if s.hasWriteCache() {
			n, err := s.writeCache.ReadHeader(child, buf)
			if err == nil {
				return shiftParentHeader(buf[:n])
			}
		}

		n, err := s.blobStor.ReadHeader(child, buf)
		if err == nil {
			return shiftParentHeader(buf[:n])
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
		n, err := s.writeCache.ReadHeader(addr, buf)
		if err == nil {
			return n, nil
		}
	}

	return s.blobStor.ReadHeader(addr, buf)
}

func shiftParentHeader(b []byte) (int, error) {
	idf, sigf, hdrf, err := iobject.GetParentNonPayloadFieldBounds(b)
	if err != nil {
		return 0, err
	}

	var n int

	if !idf.IsMissing() {
		// ID has same tag in header and split header
		n = copy(b, b[idf.From:idf.To])
	}

	if !sigf.IsMissing() {
		b[sigf.From] = iprotobuf.TagBytes2
		n += copy(b[n:], b[sigf.From:sigf.To])
	}

	if !hdrf.IsMissing() {
		b[hdrf.From] = iprotobuf.TagBytes3
		n += copy(b[n:], b[hdrf.From:hdrf.To])
	}

	return n, nil
}
