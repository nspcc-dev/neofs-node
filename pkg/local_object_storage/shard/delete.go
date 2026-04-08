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

	res, err := s.metaBase.Delete(cnr, addrs)
	if err != nil {
		return err // stop on metabase error ?
	}

	if hasWriteCache {
		for i := range res.RemovedObjects[len(addrs):] { // the rest are addrs, removed above
			err := s.writeCache.Delete(oid.NewAddress(cnr, res.RemovedObjects[i].ID))
			if err != nil && !IsErrNotFound(err) && !errors.Is(err, writecache.ErrReadOnly) {
				s.log.Warn("can't delete object from write cache", zap.Error(err))
			}
		}
	}

	s.addObjectCounter(physicalObjType, res.Counters.Phy)
	s.addObjectCounter(rootObjType, res.Counters.Root)
	s.addObjectCounter(tsObjType, res.Counters.TS)
	s.addObjectCounter(lockObjType, res.Counters.Lock)
	s.addObjectCounter(linkObjType, res.Counters.Link)
	s.addObjectCounter(gcObjType, res.Counters.GC)

	var totalRemovedPayload uint64

	for i := range res.RemovedObjects {
		totalRemovedPayload += res.RemovedObjects[i].PayloadLen

		var addr = oid.NewAddress(cnr, res.RemovedObjects[i].ID)
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

	s.addToContainerSize(cnr.EncodeToString(), -int64(totalRemovedPayload))
	s.addToPayloadCounter(-int64(totalRemovedPayload))

	return nil
}
