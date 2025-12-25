package engine

import (
	"errors"
	"fmt"
	"io"
	"math"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// GetECPart looks up for object that carries EC part produced within cnr for
// parent object and indexed by pi in the underlying metabase, checks its
// availability and reads it from the underlying BLOB storage. The result is a
// header and a payload stream that must be closed by caller after processing.
//
// If the object is not EC part but of [object.TypeTombstone], [object.TypeLock]
// or [object.TypeLink] type, GetECPart returns this object instead.
//
// If write-cache is enabled, GetECPart tries to get the object from it first.
//
// If object has expired, GetECPart returns [meta.ErrObjectIsExpired].
//
// If object exists but tombstoned (e.g. via [StorageEngine.Inhume] or stored
// tombstone object), GetECPart returns [apistatus.ErrObjectAlreadyRemoved].
//
// If object is marked as garbage (e.g. via [StorageEngine.MarkGarbage]),
// GetECPart returns [apistatus.ErrObjectNotFound].
//
// If object is locked (e.g. via [StorageEngine.Lock] or stored locker object),
// GetECPart ignores expiration, tombstone and garbage marks.
func (e *StorageEngine) GetECPart(cnr cid.ID, parent oid.ID, pi iec.PartInfo) (object.Object, io.ReadCloser, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddGetECPartDuration)()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()
	if e.blockErr != nil {
		return object.Object{}, nil, e.blockErr
	}

	// TODO: sync placement with PUT. They should sort shards equally, but now PUT sorts by part ID.
	//  https://github.com/nspcc-dev/neofs-node/issues/3537
	s := e.sortShardsFn(e, oid.NewAddress(cnr, parent))

	var partID oid.ID
loop:
	for i := range s {
		obj, rdr, err := s[i].shardIface.GetECPart(cnr, parent, pi)
		switch {
		case err == nil:
			return obj, rdr, nil
		case errors.Is(err, apistatus.ErrObjectAlreadyRemoved):
			return object.Object{}, nil, err
		case errors.Is(err, meta.ErrObjectIsExpired):
			return object.Object{}, nil, apistatus.ErrObjectNotFound // like Get
		case errors.As(err, (*ierrors.ObjectID)(&partID)):
			if partID.IsZero() {
				panic("zero object ID returned as error")
			}

			e.log.Info("EC part's object ID resolved in shard but reading failed, continue bypassing metabase",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent),
				zap.Int("ecRule", pi.RuleIndex), zap.Int("partIdx", pi.Index),
				zap.Stringer("shardID", s[i].shardIface.ID()), zap.Error(err))
			// TODO: need report error? Same for other places. https://github.com/nspcc-dev/neofs-node/issues/3538

			s = s[i+1:]
			break loop
		case errors.Is(err, apistatus.ErrObjectNotFound):
		case errors.As(err, new(*object.SplitInfoError)):
			return object.Object{}, nil, err
		default:
			e.log.Info("failed to get EC part from shard, ignore error",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent),
				zap.Int("ecRule", pi.RuleIndex), zap.Int("partIdx", pi.Index),
				zap.Stringer("shardID", s[i].shardIface.ID()), zap.Error(err))
		}
	}

	if partID.IsZero() {
		return object.Object{}, nil, apistatus.ErrObjectNotFound
	}

	for i := range s {
		// get an object bypassing the metabase. We can miss deletion or expiration mark. Get behaves like this, so here too.
		obj, rdr, err := s[i].shardIface.GetStream(oid.NewAddress(cnr, partID), true)
		switch {
		case err == nil:
			return *obj, rdr, nil
		case errors.Is(err, apistatus.ErrObjectNotFound):
		default:
			e.log.Info("failed to get EC part from shard bypassing metabase, ignore error",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent),
				zap.Int("ecRule", pi.RuleIndex), zap.Int("partIdx", pi.Index),
				zap.Stringer("partID", partID),
				zap.Stringer("shardID", s[i].shardIface.ID()), zap.Error(err))
		}
	}

	return object.Object{}, nil, apistatus.ErrObjectNotFound
}

// GetECPartRange looks up for object that carries EC part produced within cnr
// for parent object and indexed by pi in the underlying shards, checks its
// availability and, if available, reads it. Returns full payload len. If zero,
// GetECPartRange returns (0, nil, nil). Otherwise, range-cut payload stream is
// also returned. In this case, the stream must be finally closed by the caller.
//
// If write-cache is enabled, GetECPartRange tries to get the object from it
// first.
//
// If object has expired, GetECPartRange returns [meta.ErrObjectIsExpired].
//
// If object exists but tombstoned (e.g. via [StorageEngine.Inhume] or stored
// tombstone object), GetECPartRange returns
// [apistatus.ErrObjectAlreadyRemoved].
//
// If object is marked as garbage (e.g. via [StorageEngine.Delete]),
// GetECPartRange returns [apistatus.ErrObjectNotFound].
//
// If object is locked (e.g. via [StorageEngine.Lock] or stored locker object),
// GetECPartRange ignores expiration, tombstone and garbage marks.
//
// If the range is out of payload bounds, GetECPartRange returns
// [apistatus.ErrObjectOutOfRange].
//
// Range bounds are limited by int64.
func (e *StorageEngine) GetECPartRange(cnr cid.ID, parent oid.ID, pi iec.PartInfo, off, ln uint64) (uint64, io.ReadCloser, error) {
	if off > math.MaxInt64 || ln > math.MaxInt64 { // 8 exabytes, amply
		return 0, nil, fmt.Errorf("range overflowing int64 is not supported by this server: off=%d,len=%d", off, ln)
	}

	if e.metrics != nil {
		defer elapsed(e.metrics.AddGetECPartRangeDuration)()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()
	if e.blockErr != nil {
		return 0, nil, e.blockErr
	}

	s := e.sortShardsFn(e, oid.NewAddress(cnr, parent))

	var partID oid.ID
loop:
	for i := range s {
		pldLen, rc, err := s[i].shardIface.GetECPartRange(cnr, parent, pi, int64(off), int64(ln))
		switch {
		case err == nil:
			return pldLen, rc, nil
		case errors.Is(err, apistatus.ErrObjectAlreadyRemoved), errors.Is(err, apistatus.ErrObjectOutOfRange):
			return 0, nil, err
		case errors.Is(err, meta.ErrObjectIsExpired):
			return 0, nil, apistatus.ErrObjectNotFound
		case errors.As(err, (*ierrors.ObjectID)(&partID)):
			if partID.IsZero() {
				panic("zero object ID returned as error")
			}

			e.log.Info("EC part's object ID and payload len resolved in shard but reading failed, continue bypassing metabase",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Int("ecRule", pi.RuleIndex),
				zap.Int("partIdx", pi.Index), zap.Stringer("partID", partID), zap.Stringer("shardID", s[i].shardIface.ID()),
				zap.Error(err))

			s = s[i+1:]
			break loop
		case errors.Is(err, apistatus.ErrObjectNotFound):
		default:
			e.log.Info("failed to RANGE EC part in shard, ignore error",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Int("ecRule", pi.RuleIndex),
				zap.Int("partIdx", pi.Index), zap.Stringer("shardID", s[i].shardIface.ID()), zap.Error(err))
		}
	}

	if partID.IsZero() {
		return 0, nil, apistatus.ErrObjectNotFound
	}

	for i := range s {
		// get an object bypassing the metabase. We can miss deletion or expiration mark. GetECPart behaves like this, so here too.
		pldLen, rc, err := s[i].shardIface.GetRangeStream(cnr, partID, int64(off), int64(ln))
		switch {
		case err == nil:
			return pldLen, rc, nil
		case errors.Is(err, apistatus.ErrObjectOutOfRange):
			return 0, nil, err
		case errors.Is(err, apistatus.ErrObjectNotFound):
		default:
			e.log.Info("failed to RANGE EC part in shard bypassing metabase, ignore error",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Int("ecRule", pi.RuleIndex),
				zap.Int("partIdx", pi.Index), zap.Stringer("partID", partID),
				zap.Stringer("shardID", s[i].shardIface.ID()), zap.Error(err))
		}
	}

	return 0, nil, apistatus.ErrObjectNotFound
}

// HeadECPart is similar to [StorageEngine.GetECPart] but returns only the header.
func (e *StorageEngine) HeadECPart(cnr cid.ID, parent oid.ID, pi iec.PartInfo) (object.Object, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddHeadECPartDuration)()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()
	if e.blockErr != nil {
		return object.Object{}, e.blockErr
	}

	// TODO: sync placement with PUT. They should sort shards equally, but now PUT sorts by part ID.
	//  https://github.com/nspcc-dev/neofs-node/issues/3537
	s := e.sortShardsFn(e, oid.NewAddress(cnr, parent))

	var partID oid.ID
loop:
	for i := range s {
		hdr, err := s[i].shardIface.HeadECPart(cnr, parent, pi)
		switch {
		case err == nil:
			return hdr, nil
		case errors.Is(err, apistatus.ErrObjectAlreadyRemoved):
			return object.Object{}, err
		case errors.Is(err, meta.ErrObjectIsExpired):
			return object.Object{}, apistatus.ErrObjectNotFound // like Get
		case errors.As(err, (*ierrors.ObjectID)(&partID)):
			if partID.IsZero() {
				panic("zero object ID returned as error")
			}

			e.log.Info("EC part's object ID resolved in shard but reading failed, continue by ID",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent),
				zap.Int("ecRule", pi.RuleIndex), zap.Int("partIdx", pi.Index),
				zap.Stringer("shardID", s[i].shardIface.ID()), zap.Error(err))
			// TODO: need report error? Same for other places. https://github.com/nspcc-dev/neofs-node/issues/3538

			s = s[i+1:]
			break loop
		case errors.Is(err, apistatus.ErrObjectNotFound):
		default:
			e.log.Info("failed to get EC part header from shard, ignore error",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent),
				zap.Int("ecRule", pi.RuleIndex), zap.Int("partIdx", pi.Index),
				zap.Stringer("shardID", s[i].shardIface.ID()), zap.Error(err))
		}
	}

	if partID.IsZero() {
		return object.Object{}, apistatus.ErrObjectNotFound
	}

	for i := range s {
		// get an object bypassing the metabase. We can miss deletion or expiration mark. Get behaves like this, so here too.
		hdr, err := s[i].shardIface.Head(oid.NewAddress(cnr, partID), true)
		switch {
		case err == nil:
			return *hdr, nil
		case errors.Is(err, apistatus.ErrObjectNotFound):
		default:
			e.log.Info("failed to get EC part header from shard bypassing metabase, ignore error",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent),
				zap.Int("ecRule", pi.RuleIndex), zap.Int("partIdx", pi.Index),
				zap.Stringer("partID", partID),
				zap.Stringer("shardID", s[i].shardIface.ID()), zap.Error(err))
		}
	}

	return object.Object{}, apistatus.ErrObjectNotFound
}
