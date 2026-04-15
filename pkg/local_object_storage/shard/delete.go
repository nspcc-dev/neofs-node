package shard

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Delete removes data from the shard's writeCache, metaBase and
// blobStor.
func (s *Shard) Delete(cnr cid.ID, addrs []oid.ID) error {
	s.m.RLock()
	defer s.m.RUnlock()

	return s.deleteObjs(cnr, addrs)
}

func (s *Shard) deleteObjs(cnr cid.ID, addrs []oid.ID) error {
	if s.info.Mode.ReadOnly() {
		return ErrReadOnlyMode
	} else if s.info.Mode.NoMetabase() {
		return ErrDegradedMode
	}

	if len(addrs) == 0 {
		return nil
	}

	hasWriteCache := s.hasWriteCache()
	if hasWriteCache {
		for _, addr := range addrs {
			err := s.writeCache.Delete(oid.NewAddress(cnr, addr))
			if err != nil && !IsErrNotFound(err) && !errors.Is(err, writecache.ErrReadOnly) {
				s.log.Warn("can't delete object from write cache", zap.Error(err))
			}
		}
	}

	res, diff, err := s.metaBase.Delete(cnr, addrs)
	if err != nil {
		return err // stop on metabase error ?
	}

	if hasWriteCache {
		for _, id := range res[len(addrs):] { // the rest are addrs, removed above
			err := s.writeCache.Delete(oid.NewAddress(cnr, id))
			if err != nil && !IsErrNotFound(err) && !errors.Is(err, writecache.ErrReadOnly) {
				s.log.Warn("can't delete object from write cache", zap.Error(err))
			}
		}
	}

	s.addObjectCounter(physicalObjType, diff.Phy)
	s.addObjectCounter(rootObjType, diff.Root)
	s.addObjectCounter(tsObjType, diff.TS)
	s.addObjectCounter(lockObjType, diff.Lock)
	s.addObjectCounter(linkObjType, diff.Link)
	s.addObjectCounter(gcObjType, diff.GC)
	s.addToContainerSize(cnr.EncodeToString(), diff.Payload)
	s.addToPayloadCounter(diff.Payload)

	for _, id := range res {
		var addr = oid.NewAddress(cnr, id)
		err = s.blobStor.Delete(addr)
		if err == nil {
			logOp(s.log, deleteOp, addr)
		} else {
			if IsErrNotFound(err) {
				continue
			}

			s.log.Debug("can't remove object from blobStor",
				zap.Stringer("object_address", addr),
				zap.Error(err))
		}
	}

	return nil
}
