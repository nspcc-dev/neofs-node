package shard

import (
	"bytes"
	"errors"
	"io"
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

func newSimpleTestShard(_ testing.TB, bs common.Storage, mb metabase, wc writecache.Cache) *Shard {
	return &Shard{
		cfg: &cfg{
			useWriteCache: wc != nil,
			blobStor:      bs,
		},
		writeCache:    wc,
		metaBaseIface: mb,
	}
}

type resolveECPartKey struct {
	cnr    cid.ID
	parent oid.ID
	pi     iec.PartInfo
}

type resolveECPartValue struct {
	id  oid.ID
	err error
}

type resolveECPartWithLenValue struct {
	id  oid.ID
	ln  uint64
	err error
}

type mockMetabase struct {
	resolveECPart        map[resolveECPartKey]resolveECPartValue
	resolveECPartWithLen map[resolveECPartKey]resolveECPartWithLenValue
}

func (x *mockMetabase) ResolveECPart(cnr cid.ID, parent oid.ID, pi iec.PartInfo) (oid.ID, error) {
	val, ok := x.resolveECPart[resolveECPartKey{
		cnr:    cnr,
		parent: parent,
		pi:     pi,
	}]
	if !ok {
		return oid.ID{}, errors.New("[test] unexpected part requested")
	}
	return val.id, val.err
}

func (x *mockMetabase) ResolveECPartWithPayloadLen(cnr cid.ID, parent oid.ID, pi iec.PartInfo) (oid.ID, uint64, error) {
	val, ok := x.resolveECPartWithLen[resolveECPartKey{
		cnr:    cnr,
		parent: parent,
		pi:     pi,
	}]
	if !ok {
		return oid.ID{}, 0, errors.New("[test] unexpected part requested")
	}
	return val.id, val.ln, val.err
}

type getStreamValue struct {
	obj object.Object
	rc  io.ReadCloser
	err error
}

type headValue struct {
	hdr object.Object
	err error
}

type mockBLOBStore struct {
	unimplementedBLOBStore
	getStream map[oid.Address]getStreamValue
	head      map[oid.Address]headValue
}

func (x *mockBLOBStore) GetStream(addr oid.Address) (*object.Object, io.ReadCloser, error) {
	val, ok := x.getStream[addr]
	if !ok {
		return nil, nil, errors.New("[test] unexpected object requested")
	}
	if val.rc != nil {
		return val.obj.CutPayload(), val.rc, nil
	}
	return val.obj.CutPayload(), io.NopCloser(bytes.NewReader(val.obj.Payload())), val.err
}

func (x *mockBLOBStore) Head(addr oid.Address) (*object.Object, error) {
	val, ok := x.head[addr]
	if !ok {
		return nil, errors.New("[test] unexpected object requested")
	}
	return &val.hdr, val.err
}

type mockWriteCache struct {
	unimplementedWriteCache
	getStream map[oid.Address]getStreamValue
	head      map[oid.Address]headValue
}

func (x *mockWriteCache) GetStream(addr oid.Address) (*object.Object, io.ReadCloser, error) {
	val, ok := x.getStream[addr]
	if !ok {
		return nil, nil, errors.New("[test] unexpected object requested")
	}
	if val.rc != nil {
		return val.obj.CutPayload(), val.rc, nil
	}
	return val.obj.CutPayload(), io.NopCloser(bytes.NewReader(val.obj.Payload())), val.err
}

func (x *mockWriteCache) Head(addr oid.Address) (*object.Object, error) {
	val, ok := x.head[addr]
	if !ok {
		return nil, errors.New("[test] unexpected object requested")
	}
	return &val.hdr, val.err
}

type unimplementedBLOBStore struct{}

func (unimplementedBLOBStore) Open(bool) error {
	panic("unimplemented")
}

func (unimplementedBLOBStore) Init() error {
	panic("unimplemented")
}

func (unimplementedBLOBStore) Close() error {
	panic("unimplemented")
}

func (unimplementedBLOBStore) Type() string {
	panic("unimplemented")
}

func (unimplementedBLOBStore) Path() string {
	panic("unimplemented")
}

func (unimplementedBLOBStore) SetLogger(*zap.Logger) {
	panic("unimplemented")
}

func (unimplementedBLOBStore) SetCompressor(*compression.Config) {
	panic("unimplemented")
}

func (unimplementedBLOBStore) GetBytes(oid.Address) ([]byte, error) {
	panic("unimplemented")
}

func (unimplementedBLOBStore) Get(oid.Address) (*object.Object, error) {
	panic("unimplemented")
}

func (unimplementedBLOBStore) GetRange(oid.Address, uint64, uint64) ([]byte, error) {
	panic("unimplemented")
}

func (unimplementedBLOBStore) GetStream(oid.Address) (*object.Object, io.ReadCloser, error) {
	panic("unimplemented")
}

func (unimplementedBLOBStore) Head(oid.Address) (*object.Object, error) {
	panic("unimplemented")
}

func (unimplementedBLOBStore) Exists(oid.Address) (bool, error) {
	panic("unimplemented")
}

func (unimplementedBLOBStore) Put(oid.Address, []byte) error {
	panic("unimplemented")
}

func (unimplementedBLOBStore) PutBatch(map[oid.Address][]byte) error {
	panic("unimplemented")
}

func (unimplementedBLOBStore) Delete(oid.Address) error {
	panic("unimplemented")
}

func (unimplementedBLOBStore) Iterate(func(oid.Address, []byte) error, func(oid.Address, error) error) error {
	panic("unimplemented")
}

func (unimplementedBLOBStore) IterateAddresses(func(oid.Address) error, bool) error {
	panic("unimplemented")
}

type unimplementedWriteCache struct{}

func (unimplementedWriteCache) Get(oid.Address) (*object.Object, error) {
	panic("unimplemented")
}

func (unimplementedWriteCache) GetBytes(oid.Address) ([]byte, error) {
	panic("unimplemented")
}

func (unimplementedWriteCache) GetStream(oid.Address) (*object.Object, io.ReadCloser, error) {
	panic("unimplemented")
}

func (unimplementedWriteCache) Head(oid.Address) (*object.Object, error) {
	panic("unimplemented")
}

func (unimplementedWriteCache) Delete(oid.Address) error {
	panic("unimplemented")
}

func (unimplementedWriteCache) Iterate(func(oid.Address, []byte) error, bool) error {
	panic("unimplemented")
}

func (unimplementedWriteCache) Put(oid.Address, *object.Object, []byte) error {
	panic("unimplemented")
}

func (unimplementedWriteCache) SetMode(mode.Mode) error {
	panic("unimplemented")
}

func (unimplementedWriteCache) SetLogger(*zap.Logger) {
	panic("unimplemented")
}

func (unimplementedWriteCache) SetShardIDMetrics(string) {
	panic("unimplemented")
}

func (unimplementedWriteCache) DumpInfo() writecache.Info {
	panic("unimplemented")
}

func (unimplementedWriteCache) Flush(bool) error {
	panic("unimplemented")
}

func (unimplementedWriteCache) Init() error {
	panic("unimplemented")
}

func (unimplementedWriteCache) Open(bool) error {
	panic("unimplemented")
}

func (unimplementedWriteCache) Close() error {
	panic("unimplemented")
}

func (unimplementedWriteCache) ObjectStatus(oid.Address) (writecache.ObjectStatus, error) {
	panic("unimplemented")
}

type unimplementedMetabase struct{}

func (unimplementedMetabase) ResolveECPart(cid.ID, oid.ID, iec.PartInfo) (oid.ID, error) {
	panic("unimplemented")
}

func (unimplementedMetabase) ResolveECPartWithPayloadLen(cid.ID, oid.ID, iec.PartInfo) (oid.ID, uint64, error) {
	panic("unimplemented")
}

type closeFlag bool

func (x *closeFlag) Close() error {
	*x = true
	return nil
}

// [iotest.ErrReader] analogue.
type errSeeker struct {
	io.ReadCloser
	err error
}

func (x errSeeker) Seek(int64, int) (int64, error) {
	return 0, x.err
}
