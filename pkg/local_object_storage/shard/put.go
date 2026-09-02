package shard

import (
	"errors"
	"fmt"
	"io"
	"math"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	protoencoding "github.com/nspcc-dev/neofs-sdk-go/proto/encoding"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protowire"
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

	var (
		addr      = obj.Address()
		cachedPut bool
	)

	// exist check are not performed there, these checks should be executed
	// ahead of `Put` by storage engine
	if s.hasWriteCache() {
		var err = s.writeCache.Put(addr, obj, objBin)
		cachedPut = err == nil
		if !cachedPut {
			s.log.Debug("can't put object to the write-cache, trying blobstor",
				zap.Error(err))
			// Consider returning an error if cache is full.
		}
	}
	if !cachedPut {
		var err = s.blobStor.Put(addr, objBin)
		if err != nil {
			return fmt.Errorf("could not put object to BLOB storage: %w", err)
		}
		logOp(s.log, putOp, addr)
	}

	return s.recordObjectSaveInMetabaseLocked(*obj, cachedPut)
}

func (s *Shard) recordObjectSaveInMetabaseLocked(hdr object.Object, writeCached bool) error {
	if s.info.Mode.NoMetabase() {
		return nil
	}

	addr := hdr.Address()

	diff, metaErr := s.metaBase.PutCounted(&hdr)
	if metaErr != nil {
		if writeCached {
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

// TODO: docs.
func (s *Shard) InitPut(hdr object.Object) (io.WriteCloser, func(), error) {
	// TODO: shareable with Put?
	s.m.RLock()
	defer s.m.RUnlock()

	m := s.info.Mode
	if m.ReadOnly() {
		return nil, nil, ErrReadOnlyMode
	}

	payloadLen := hdr.PayloadSize()

	// TODO: https://github.com/nspcc-dev/neofs-sdk-go/issues/846
	hdrMsg := hdr.ProtoMessage()
	hdrMsg.Payload = nil

	hdrLen := hdrMsg.MarshaledSize()

	// TODO: add utility function
	fullObjLen := uint64(hdrLen)
	fullObjLen += uint64(protoencoding.SizeVarint(protoobject.FieldObjectPayload, payloadLen)) + payloadLen
	if fullObjLen > math.MaxInt {
		return nil, nil, fmt.Errorf("full object length %d overflows int", fullObjLen)
	}

	dataPrefix := make([]byte, fullObjLen)

	hdrMsg.MarshalStable(dataPrefix)
	off := hdrLen
	off += protoencoding.WriteTagAndVarint(dataPrefix[off:], protoobject.FieldObjectPayload, protowire.BytesType, payloadLen)
	copy(dataPrefix[off:], hdr.Payload())

	var (
		addr      = hdr.Address()
		cachedPut bool
	)

	var blobStream io.WriteCloser
	var abortFn func()
	var err error

	// exist check are not performed there, these checks should be executed
	// ahead of `Put` by storage engine
	if s.hasWriteCache() {
		blobStream, abortFn, err = s.writeCache.InitPut(addr, int(fullObjLen), dataPrefix)
		cachedPut = err == nil
		if !cachedPut {
			s.log.Debug("can't put object to the write-cache, trying blobstor",
				zap.Error(err))
			// Consider returning an error if cache is full.
		}
	}
	if !cachedPut {
		blobStream, abortFn, err = s.blobStor.InitPut(addr, int(fullObjLen), dataPrefix)
		if err != nil {
			return nil, nil, fmt.Errorf("could not put object to BLOB storage: %w", err)
		}
	}

	return objectPayloadWriteStream{
		shard:       s,
		savedHeader: hdr,
		writeCached: cachedPut,
		blobStream:  blobStream,
	}, abortFn, nil
}

type objectPayloadWriteStream struct {
	shard       *Shard
	savedHeader object.Object
	writeCached bool
	blobStream  io.WriteCloser
}

func (x objectPayloadWriteStream) Write(p []byte) (int, error) {
	// TODO: wrap error with component context (Close too)
	return x.blobStream.Write(p)
}

func (x objectPayloadWriteStream) Close() error {
	err := x.blobStream.Close()
	if err != nil {
		return err
	}

	if !x.writeCached {
		logOp(x.shard.log, putOp, x.savedHeader.Address())
	}

	x.shard.m.RLock()
	defer x.shard.m.RUnlock()

	return x.shard.recordObjectSaveInMetabaseLocked(x.savedHeader, x.writeCached)
}
