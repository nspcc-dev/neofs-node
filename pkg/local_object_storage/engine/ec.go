package engine

import (
	"errors"
	"io"
	"time"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// ReadECPart is a buffered alternative for [StorageEngine.GetECPart] similar to
// [StorageEngine.ReadObject].
func (e *StorageEngine) ReadECPart(cnr cid.ID, parent oid.ID, pi iec.PartInfo, buf []byte) (int, io.ReadCloser, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddReadECPartDuration)()
	}

	var n int
	var stream io.ReadCloser
	return n, stream, e.getECPartFunc(cnr, parent, pi, func(s shardInterface, cnr cid.ID, parent oid.ID, pi iec.PartInfo) error {
		var err error
		n, stream, err = s.ReadECPart(cnr, parent, pi, buf)
		return err
	}, func(s shardInterface, partAddr oid.Address) error {
		var err error
		n, stream, err = s.ReadObject(partAddr, true, buf)
		return err
	})
}

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

	var hdr object.Object
	var stream io.ReadCloser
	return hdr, stream, e.getECPartFunc(cnr, parent, pi, func(s shardInterface, cnr cid.ID, parent oid.ID, pi iec.PartInfo) error {
		var err error
		hdr, stream, err = s.GetECPart(cnr, parent, pi)
		return err
	}, func(s shardInterface, partAddr oid.Address) error {
		h, str, err := s.GetStream(partAddr, true)
		if err == nil {
			hdr, stream = *h, str
		}
		return err
	})
}

func (e *StorageEngine) getECPartFunc(cnr cid.ID, parent oid.ID, pi iec.PartInfo, resolveFn func(shardInterface, cid.ID, oid.ID, iec.PartInfo) error,
	getFn func(shardInterface, oid.Address) error) error {
	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()
	if e.blockErr != nil {
		return e.blockErr
	}

	s := e.sortShardsFn(e, parent)

	var partID oid.ID
loop:
	for i := range s {
		err := resolveFn(s[i].shardIface, cnr, parent, pi)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, apistatus.ErrObjectAlreadyRemoved):
			return err
		case errors.Is(err, meta.ErrObjectIsExpired):
			return apistatus.ErrObjectNotFound // like Get
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
			return err
		default:
			e.log.Info("failed to get EC part from shard, ignore error",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent),
				zap.Int("ecRule", pi.RuleIndex), zap.Int("partIdx", pi.Index),
				zap.Stringer("shardID", s[i].shardIface.ID()), zap.Error(err))
		}
	}

	if partID.IsZero() {
		return apistatus.ErrObjectNotFound
	}

	for i := range s {
		// get an object bypassing the metabase. We can miss deletion or expiration mark. Get behaves like this, so here too.
		err := getFn(s[i].shardIface, oid.NewAddress(cnr, partID))
		switch {
		case err == nil:
			return nil
		case errors.Is(err, apistatus.ErrObjectNotFound):
		default:
			e.log.Info("failed to get EC part from shard bypassing metabase, ignore error",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent),
				zap.Int("ecRule", pi.RuleIndex), zap.Int("partIdx", pi.Index),
				zap.Stringer("partID", partID),
				zap.Stringer("shardID", s[i].shardIface.ID()), zap.Error(err))
		}
	}

	return apistatus.ErrObjectNotFound
}

// ReadECPartRange is a buffered alternative for [StorageEngine.GetECPartRange]
// similar to [StorageEngine.ReadECPart].
func (e *StorageEngine) ReadECPartRange(cnr cid.ID, parent oid.ID, pi iec.PartInfo, off, ln uint64, buf []byte) (io.ReadCloser, error) {
	var stream io.ReadCloser
	return stream, e.getECPartRangeFunc(cnr, parent, pi, off, ln, MetricRegister.AddReadECPartRangeDuration, func(s shardInterface, cnr cid.ID, parent oid.ID, pi iec.PartInfo, off, ln uint64) error {
		var err error
		stream, err = s.ReadECPartRange(cnr, parent, pi, off, ln, buf)
		return err
	}, func(s shardInterface, cnr cid.ID, partID oid.ID, off, ln uint64) error {
		var err error
		stream, err = s.ReadRange(cnr, partID, off, ln, buf)
		return err
	})
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
	var pldLen uint64
	var stream io.ReadCloser
	return pldLen, stream, e.getECPartRangeFunc(cnr, parent, pi, off, ln, MetricRegister.AddGetECPartRangeDuration, func(s shardInterface, cnr cid.ID, parent oid.ID, pi iec.PartInfo, off, ln uint64) error {
		var err error
		pldLen, stream, err = s.GetECPartRange(cnr, parent, pi, off, ln)
		return err
	}, func(s shardInterface, cnr cid.ID, partID oid.ID, off, ln uint64) error {
		var err error
		pldLen, stream, err = s.GetRangeStream(cnr, partID, off, ln)
		return err
	})
}

func (e *StorageEngine) getECPartRangeFunc(cnr cid.ID, parent oid.ID, pi iec.PartInfo, off, ln uint64, metricFn func(MetricRegister, time.Duration),
	resolveFn func(shardInterface, cid.ID, oid.ID, iec.PartInfo, uint64, uint64) error,
	rangeFn func(shardInterface, cid.ID, oid.ID, uint64, uint64) error,
) error {
	if e.metrics != nil {
		defer elapsed(func(d time.Duration) { metricFn(e.metrics, d) })()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()
	if e.blockErr != nil {
		return e.blockErr
	}

	s := e.sortShardsFn(e, parent)

	var partID oid.ID
loop:
	for i := range s {
		err := resolveFn(s[i].shardIface, cnr, parent, pi, off, ln)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, apistatus.ErrObjectAlreadyRemoved), errors.Is(err, apistatus.ErrObjectOutOfRange), errors.As(err, new(*object.SplitInfoError)):
			return err
		case errors.Is(err, meta.ErrObjectIsExpired):
			return apistatus.ErrObjectNotFound
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
		return apistatus.ErrObjectNotFound
	}

	for i := range s {
		// get an object bypassing the metabase. We can miss deletion or expiration mark. GetECPart behaves like this, so here too.
		err := rangeFn(s[i].shardIface, cnr, partID, off, ln)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, apistatus.ErrObjectOutOfRange):
			return err
		case errors.Is(err, apistatus.ErrObjectNotFound):
		default:
			e.log.Info("failed to RANGE EC part in shard bypassing metabase, ignore error",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Int("ecRule", pi.RuleIndex),
				zap.Int("partIdx", pi.Index), zap.Stringer("partID", partID),
				zap.Stringer("shardID", s[i].shardIface.ID()), zap.Error(err))
		}
	}

	return apistatus.ErrObjectNotFound
}

// ReadECPartHeader is a buffered alternative for [StorageEngine.HeadECPart]
// similar to [StorageEngine.ReadHeader].
func (e *StorageEngine) ReadECPartHeader(cnr cid.ID, parent oid.ID, pi iec.PartInfo, buf []byte) (int, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddReadECPartHeaderDuration)()
	}

	var n int
	return n, e.headECPartFunc(cnr, parent, pi, func(s shardInterface, cnr cid.ID, parent oid.ID, pi iec.PartInfo) error {
		var err error
		n, err = s.ReadECPartHeader(cnr, parent, pi, buf)
		return err
	}, func(s shardInterface, partAddr oid.Address) error {
		var err error
		n, err = s.ReadHeader(partAddr, true, buf)
		return err
	})
}

// HeadECPart is similar to [StorageEngine.GetECPart] but returns only the header.
func (e *StorageEngine) HeadECPart(cnr cid.ID, parent oid.ID, pi iec.PartInfo) (object.Object, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddHeadECPartDuration)()
	}

	var hdr object.Object
	return hdr, e.headECPartFunc(cnr, parent, pi, func(s shardInterface, cnr cid.ID, parent oid.ID, pi iec.PartInfo) error {
		var err error
		hdr, err = s.HeadECPart(cnr, parent, pi)
		return err
	}, func(s shardInterface, partAddr oid.Address) error {
		h, err := s.Head(partAddr, true)
		if err == nil {
			hdr = *h
		}
		return err
	})
}

func (e *StorageEngine) headECPartFunc(cnr cid.ID, parent oid.ID, pi iec.PartInfo, resolveFn func(shardInterface, cid.ID, oid.ID, iec.PartInfo) error,
	headFn func(shardInterface, oid.Address) error) error {
	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()
	if e.blockErr != nil {
		return e.blockErr
	}

	s := e.sortShardsFn(e, parent)

	var partID oid.ID
loop:
	for i := range s {
		err := resolveFn(s[i].shardIface, cnr, parent, pi)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, apistatus.ErrObjectAlreadyRemoved):
			return err
		case errors.Is(err, meta.ErrObjectIsExpired):
			return apistatus.ErrObjectNotFound // like Get
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
		return apistatus.ErrObjectNotFound
	}

	for i := range s {
		// get an object bypassing the metabase. We can miss deletion or expiration mark. Get behaves like this, so here too.
		err := headFn(s[i].shardIface, oid.NewAddress(cnr, partID))
		switch {
		case err == nil:
			return nil
		case errors.Is(err, apistatus.ErrObjectNotFound):
		default:
			e.log.Info("failed to get EC part header from shard bypassing metabase, ignore error",
				zap.Stringer("container", cnr), zap.Stringer("parent", parent),
				zap.Int("ecRule", pi.RuleIndex), zap.Int("partIdx", pi.Index),
				zap.Stringer("partID", partID),
				zap.Stringer("shardID", s[i].shardIface.ID()), zap.Error(err))
		}
	}

	return apistatus.ErrObjectNotFound
}
