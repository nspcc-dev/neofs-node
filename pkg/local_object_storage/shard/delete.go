package shard

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Delete removes data from the shard's writeCache, metaBase and
// blobStor.
func (s *Shard) Delete(addrs []oid.Address) error {
	s.m.RLock()
	defer s.m.RUnlock()

	return s.deleteObjs(addrs)
}

func (s *Shard) deleteObjs(addrs []oid.Address) error {
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
			err := s.writeCache.Delete(addr)
			if err != nil && !IsErrNotFound(err) && !errors.Is(err, writecache.ErrReadOnly) {
				s.log.Warn("can't delete object from write cache", zap.Error(err))
			}
		}
	}

	res, err := s.metaBase.Delete(addrs)
	if err != nil {
		return err // stop on metabase error ?
	}

	if hasWriteCache {
		for i := range res.RemovedObjects[len(addrs):] { // the rest are addrs, removed above
			err := s.writeCache.Delete(res.RemovedObjects[i].Address)
			if err != nil && !IsErrNotFound(err) && !errors.Is(err, writecache.ErrReadOnly) {
				s.log.Warn("can't delete object from write cache", zap.Error(err))
			}
		}
	}

	s.decObjectCounterBy(physical, res.RawRemoved)

	var totalRemovedPayload uint64

	for i := range res.RemovedObjects {
		removedPayload := res.RemovedObjects[i].PayloadLen
		totalRemovedPayload += removedPayload
		s.addToContainerSize(res.RemovedObjects[i].Address.Container().EncodeToString(), -int64(removedPayload))

		err = s.blobStor.Delete(res.RemovedObjects[i].Address)
		if err == nil {
			logOp(s.log, deleteOp, res.RemovedObjects[i].Address)
		} else {
			if IsErrNotFound(err) {
				continue
			}

			s.log.Debug("can't remove object from blobStor",
				zap.Stringer("object_address", res.RemovedObjects[i].Address),
				zap.Error(err))
		}
	}

	s.addToPayloadCounter(-int64(totalRemovedPayload))

	return nil
}
