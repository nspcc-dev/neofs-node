package shard

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

func (s *Shard) deleteObjs(addrs []oid.Address, skipNotFoundError bool) error {
	if s.info.Mode.ReadOnly() {
		return ErrReadOnlyMode
	} else if s.info.Mode.NoMetabase() {
		return ErrDegradedMode
	}

	for _, addr := range addrs {
		if s.hasWriteCache() {
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
		err = s.blobStor.Delete(addr)
		if err == nil {
			logOp(s.log, deleteOp, addr)
		} else {
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
