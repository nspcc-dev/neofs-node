package shard

import (
	"errors"
	"iter"
	"slices"

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

	s.deleteFromWriteCache(slices.Values(addrs))

	res, err := s.metaBase.Delete(addrs)
	if err != nil {
		return err // stop on metabase error ?
	}

	additionalAddrs := func(yield func(oid.Address) bool) {
		for parent, ids := range res.AdditionalObjects {
			for i := range ids {
				if !yield(oid.NewAddress(parent.Container(), ids[i])) {
					return
				}
			}
		}
	}

	s.deleteFromWriteCache(additionalAddrs)

	s.decObjectCounterBy(physical, res.RawRemoved)
	s.decObjectCounterBy(logical, res.AvailableRemoved)

	allAddrs := func(yield func(oid.Address) bool) {
		for _, addr := range addrs {
			if !yield(addr) {
				return
			}
		}
		for addr := range additionalAddrs {
			if !yield(addr) {
				return
			}
		}
	}

	var totalRemovedPayload uint64

	for addr := range allAddrs {
		removedPayload := res.Sizes[addr]
		totalRemovedPayload += removedPayload
		s.addToContainerSize(addr.Container().EncodeToString(), -int64(removedPayload))
	}

	s.addToPayloadCounter(-int64(totalRemovedPayload))

	for addr := range allAddrs {
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

func (s *Shard) deleteFromWriteCache(addrs iter.Seq[oid.Address]) {
	for addr := range addrs {
		if s.hasWriteCache() {
			err := s.writeCache.Delete(addr)
			if err != nil && !IsErrNotFound(err) && !errors.Is(err, writecache.ErrReadOnly) {
				s.log.Warn("can't delete object from write cache", zap.Error(err))
			}
		}
	}
}
