package shard

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Delete removes data from the shard's writeCache, metaBase and
// blobStor.
func (s *Shard) Delete(addrs []oid.Address) error {
	s.m.RLock()
	defer s.m.RUnlock()

	return s.deleteObjs(addrs, false)
}

func (s *Shard) deleteObjs(addrs []oid.Address, skipNotFoundError bool) error {
	if s.info.Mode.ReadOnly() {
		return ErrReadOnlyMode
	} else if s.info.Mode.NoMetabase() {
		return ErrDegradedMode
	}

	smalls := make(map[oid.Address][]byte, len(addrs))

	for _, addr := range addrs {
		if s.hasWriteCache() {
			err := s.writeCache.Delete(addr)
			if err != nil && !IsErrNotFound(err) && !errors.Is(err, writecache.ErrReadOnly) {
				s.log.Warn("can't delete object from write cache", zap.Error(err))
			}
		}

		sid, err := s.metaBase.StorageID(addr)
		if err != nil {
			s.log.Debug("can't get storage ID from metabase",
				zap.Stringer("object", addr),
				zap.Error(err))

			continue
		}

		if sid != nil {
			smalls[addr] = sid
		}
	}

	res, err := s.metaBase.Delete(addrs)
	if err != nil {
		return err // stop on metabase error ?
	}

	s.decObjectCounterBy(physical, res.RawRemoved)
	s.decObjectCounterBy(logical, res.AvailableRemoved)

	var totalRemovedPayload uint64

	for i := range addrs {
		removedPayload := res.Sizes[i]
		totalRemovedPayload += removedPayload
		s.addToContainerSize(addrs[i].Container().EncodeToString(), -int64(removedPayload))
	}

	s.addToPayloadCounter(-int64(totalRemovedPayload))

	for _, addr := range addrs {
		var delPrm common.DeletePrm
		delPrm.Address = addr
		id := smalls[addr]
		delPrm.StorageID = id

		_, err = s.blobStor.Delete(delPrm)
		if err != nil {
			if IsErrNotFound(err) && skipNotFoundError {
				continue
			}

			s.log.Debug("can't remove object from blobStor",
				zap.Stringer("object_address", addr),
				zap.Error(err))
		}
	}

	return nil
}
