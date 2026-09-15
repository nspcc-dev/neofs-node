package shard

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Put saves the object in shard. objBin parameter is  optional and used
// to optimize out object marshaling.
//
// Returns any error encountered that
// did not allow to completely save the object.
//
// Returns ErrReadOnlyMode error if shard is in "read-only" mode.
//
// Returns [apistatus.ErrObjectAlreadyRemoved] if obj is of [object.TypeLock]
// type and there is an object of [object.TypeTombstone] type associated with
// the same target.
func (s *Shard) Put(obj *object.Object, objBin []byte) error {
	s.m.RLock()
	defer s.m.RUnlock()

	m := s.info.Mode
	if m.ReadOnly() {
		return ErrReadOnlyMode
	}

	if objBin == nil {
		objBin = obj.Marshal()
	}

	var addr = obj.Address()

	writeCacheFn := func(writeCache writecache.Cache) error {
		return s.writeCache.Put(addr, obj, objBin)
	}

	blobStorageFn := func(blobStorage common.Storage) error {
		return s.blobStor.Put(addr, objBin)
	}

	cachedPut, err := s.putFunc(writeCacheFn, blobStorageFn)
	if err != nil {
		return err
	}

	if !cachedPut {
		logOp(s.log, putOp, addr)
	}

	if m.NoMetabase() {
		return nil
	}

	return s.putToMetabaseLocked(addr, *obj, cachedPut)
}

func (s *Shard) putFunc(writeCacheFn func(writecache.Cache) error, blobStorageFn func(common.Storage) error) (bool, error) {
	var cachedPut bool

	// exist check are not performed there, these checks should be executed
	// ahead of `Put` by storage engine
	if s.hasWriteCache() {
		var err = writeCacheFn(s.writeCache)
		cachedPut = err == nil
		if !cachedPut {
			s.log.Debug("can't put object to the write-cache, trying blobstor",
				zap.Error(err))
			// Consider returning an error if cache is full.
		}
	}
	if !cachedPut {
		var err = blobStorageFn(s.blobStor)
		if err != nil {
			return false, fmt.Errorf("could not put object to BLOB storage: %w", err)
		}
	}

	return cachedPut, nil
}

func (s *Shard) putToMetabaseLocked(addr oid.Address, hdr object.Object, cachedPut bool) error {
	diff, metaErr := s.metaBase.PutCounted(&hdr)
	if metaErr != nil {
		if cachedPut {
			var err = s.writeCache.Delete(addr)
			if err != nil && !errors.Is(err, apistatus.ErrObjectNotFound) {
				s.log.Warn("can't drop object from write cache on meta put failure",
					zap.Stringer("addr", addr), zap.Error(err))
			}
		}
		// Always delete from blobstor, write cache
		// might have flushed it already.
		var err = s.blobStor.Delete(addr)
		if err != nil && !errors.Is(err, apistatus.ErrObjectNotFound) {
			s.log.Warn("can't drop object from blobstor on meta put failure",
				zap.Stringer("addr", addr), zap.Error(err))
		}

		// may we need to handle this case in a special way
		// since the object has been successfully written to BlobStor
		return fmt.Errorf("could not put object to metabase: %w", metaErr)
	}

	s.addObjectCounter(physicalObjType, diff.Phy)
	s.addObjectCounter(rootObjType, diff.Root)
	s.addObjectCounter(tsObjType, diff.TS)
	s.addObjectCounter(lockObjType, diff.Lock)
	s.addObjectCounter(linkObjType, diff.Link)
	s.addObjectCounter(gcObjType, diff.GC)
	s.addToContainerSize(addr.Container().EncodeToString(), diff.Payload)

	return nil
}
