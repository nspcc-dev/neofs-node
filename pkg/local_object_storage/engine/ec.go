package engine

import (
	"errors"
	"io"

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
// If the object is not EC part but of [object.TypeTombstone] or
// [object.TypeLock] type, GetECPart returns this object instead.
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
