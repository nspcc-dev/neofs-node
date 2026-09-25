package shard

import (
	"errors"
	"fmt"
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
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
		return writeCache.Put(addr, obj, objBin)
	}

	blobStorageFn := func(blobStorage blobstor.Storage) error {
		return blobStorage.Put(addr, objBin)
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

// InitPut calls [blobstor.Storage.InitPut] on the underlying BLOB storage. If the
// write-cache is enabled, InitPut attempts to write to it using
// [writecache.Cache.InitPut]. In this case, if InitPut or resulting stream
// fails, fallback to the main storage is performed if possible.
//
// If s is in read-only mode, InitPut instantly returns [ErrReadOnlyMode]. If
// underlying [blobstor.Storage] is in read-only mode, InitPut returns
// [blobstor.ErrReadOnly].
//
// If underlying device runs out of space, InitPut or resulting stream calls
// return [blobstor.ErrNoSpace].
func (s *Shard) InitPut(hdr object.Object, hdrLen uint64, hdrW io.WriterTo) (io.WriteCloser, func(), error) {
	s.m.RLock()

	m := s.info.Mode
	if m.ReadOnly() {
		s.m.RUnlock()
		return nil, nil, ErrReadOnlyMode
	}

	var (
		addr    = hdr.Address()
		stream  io.WriteCloser
		abortFn func()
	)

	writeCacheFn := func(writeCache writecache.Cache) error {
		var err error
		stream, abortFn, err = writeCache.InitPut(addr, hdrLen, hdr.PayloadSize(), hdrW)
		return err
	}

	blobStorageFn := func(blobStorage blobstor.Storage) error {
		var err error
		stream, abortFn, err = blobStorage.InitPut(addr, hdrLen, hdr.PayloadSize(), hdrW)
		return err
	}

	cachedPut, err := s.putFunc(writeCacheFn, blobStorageFn)
	if err != nil {
		s.m.RUnlock()
		return nil, nil, err
	}

	res := newPayloadWriteStream(s, hdr, stream, abortFn, cachedPut)
	return res, res.abort, nil
}

type payloadWriteStream struct {
	shard     *Shard
	header    object.Object
	stream    io.WriteCloser
	abortFn   func()
	cachedPut bool
	aborted   bool
}

func newPayloadWriteStream(s *Shard, hdr object.Object, stream io.WriteCloser, abortFn func(), cachedPut bool) *payloadWriteStream {
	return &payloadWriteStream{
		shard:     s,
		header:    hdr,
		stream:    stream,
		abortFn:   abortFn,
		cachedPut: cachedPut,
	}
}

func (x *payloadWriteStream) Write(p []byte) (int, error) {
	if x.aborted {
		return 0, logicerr.ErrStreamAborted
	}

	n, err := x.stream.Write(p)
	if err != nil {
		x.finish()
		if x.cachedPut {
			x.shard.logPutWriteCacheError(err)
		} else {
			err = newPutToBLOBStorageError(err)
		}
		return n, err
	}

	return n, nil
}

func (x *payloadWriteStream) Close() error {
	if x.aborted {
		return logicerr.ErrStreamAborted
	}

	defer x.finish()

	err := x.stream.Close()
	if err != nil {
		if x.cachedPut {
			x.shard.logPutWriteCacheError(err)
		} else {
			err = newPutToBLOBStorageError(err)
		}
		return err
	}

	return x.putToMetabase()
}

func (x *payloadWriteStream) putToMetabase() error {
	if !x.cachedPut {
		logOp(x.shard.log, putOp, x.header.Address())
	}

	return x.shard.putToMetabaseLocked(x.header.Address(), x.header, x.cachedPut)
}

func (x *payloadWriteStream) abort() {
	if x.aborted {
		return
	}
	x.abortFn()
	x.finish()
}

func (x *payloadWriteStream) finish() {
	x.shard.m.RUnlock()
	x.aborted = true
}

func (s *Shard) putFunc(writeCacheFn func(writecache.Cache) error, blobStorageFn func(blobstor.Storage) error) (bool, error) {
	var cachedPut bool

	// exist check are not performed there, these checks should be executed
	// ahead of `Put` by storage engine
	if s.hasWriteCache() {
		var err = writeCacheFn(s.writeCache)
		cachedPut = err == nil
		if !cachedPut {
			s.logPutWriteCacheError(err)
			// Consider returning an error if cache is full.
		}
	}
	if !cachedPut {
		var err = blobStorageFn(s.blobStor)
		if err != nil {
			return false, newPutToBLOBStorageError(err)
		}
	}

	return cachedPut, nil
}

func (s *Shard) putToMetabaseLocked(addr oid.Address, hdr object.Object, cachedPut bool) error {
	diff, metaErr := s.metaBaseIface.PutCounted(&hdr)
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

func (s *Shard) logPutWriteCacheError(err error) {
	s.log.Debug("can't put object to the write-cache, trying blobstor", zap.Error(err))
}

func newPutToBLOBStorageError(err error) error {
	return fmt.Errorf("could not put object to BLOB storage: %w", err)
}
