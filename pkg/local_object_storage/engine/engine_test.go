package engine

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	checksumtest "github.com/nspcc-dev/neofs-sdk-go/checksum/test"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type epochState struct {
	e uint64
}

func (s epochState) CurrentEpoch() uint64 {
	return s.e
}

func BenchmarkExists(b *testing.B) {
	b.Run("2 shards", func(b *testing.B) {
		benchmarkExists(b, 2)
	})
	b.Run("4 shards", func(b *testing.B) {
		benchmarkExists(b, 4)
	})
	b.Run("8 shards", func(b *testing.B) {
		benchmarkExists(b, 8)
	})
}

func benchmarkExists(b *testing.B, shardNum int) {
	shards := make([]*shard.Shard, shardNum)
	for i := range shardNum {
		shards[i] = testNewShard(b, i)
	}

	e := testNewEngineWithShards(shards...)
	b.Cleanup(func() {
		_ = e.Close()
		_ = os.RemoveAll(b.Name())
	})

	addr := oidtest.Address()
	for range 100 {
		obj := generateObjectWithCID(cidtest.ID())
		err := e.Put(obj, nil)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		ok, err := e.exists(addr)
		if err != nil || ok {
			b.Fatalf("%t %v", ok, err)
		}
	}
}

func testNewEngineWithShards(shards ...*shard.Shard) *StorageEngine {
	engine := New()

	for _, s := range shards {
		pool, err := ants.NewPool(10, ants.WithNonblocking(true))
		if err != nil {
			panic(err)
		}

		engine.shards[s.ID().String()] = shardWrapper{
			errorCount: new(atomic.Uint32),
			Shard:      s,
		}
		engine.shardPools[s.ID().String()] = pool
	}

	return engine
}

func newStorage(root string) common.Storage {
	return fstree.New(
		fstree.WithPath(root),
		fstree.WithDepth(1))
}

func testNewShard(t testing.TB, id int) *shard.Shard {
	sid, err := generateShardID()
	require.NoError(t, err)

	dir := t.TempDir()

	s := shard.New(
		shard.WithID(sid),
		shard.WithLogger(zap.L()),
		shard.WithBlobstor(
			newStorage(filepath.Join(dir, fmt.Sprintf("%d.fstree", id)))),
		shard.WithMetaBaseOptions(
			meta.WithPath(filepath.Join(dir, fmt.Sprintf("%d.metabase", id))),
			meta.WithPermissions(0700),
			meta.WithEpochState(epochState{}),
		),
		shard.WithGCWorkerPoolInitializer(func(sz int) util.WorkerPool {
			pool, err := ants.NewPool(sz)
			if err != nil {
				panic(err)
			}

			return pool
		}))

	require.NoError(t, s.Open())
	require.NoError(t, s.Init())

	return s
}

func testEngineFromShardOpts(t *testing.T, num int, extraOpts []shard.Option) *StorageEngine {
	engine := New()
	for i := range num {
		_, err := engine.AddShard(append([]shard.Option{
			shard.WithBlobstor(
				newStorage(filepath.Join(t.Name(), fmt.Sprintf("fstree%d", i))),
			),
			shard.WithMetaBaseOptions(
				meta.WithPath(filepath.Join(t.Name(), fmt.Sprintf("metabase%d", i))),
				meta.WithPermissions(0700),
				meta.WithEpochState(epochState{}),
			),
			shard.WithExpiredObjectsCallback(engine.processExpiredObjects),
		}, extraOpts...)...)
		require.NoError(t, err)
	}

	require.NoError(t, engine.Open())
	require.NoError(t, engine.Init())

	return engine
}

func generateObjectWithCID(cnr cid.ID) *object.Object {
	var ver version.Version
	ver.SetMajor(2)
	ver.SetMinor(1)

	csum := checksumtest.Checksum()

	csumTZ := checksum.NewTillichZemor(tz.Sum(csum.Value()))

	obj := object.New()
	obj.SetID(oidtest.ID())
	owner := usertest.ID()
	obj.SetOwner(owner)
	obj.SetContainerID(cnr)
	obj.SetVersion(&ver)
	obj.SetPayloadChecksum(csum)
	obj.SetPayloadHomomorphicHash(csumTZ)
	obj.SetPayload([]byte{1, 2, 3, 4, 5})

	return obj
}

func addAttribute(obj *object.Object, key, val string) {
	var attr object.Attribute
	attr.SetKey(key)
	attr.SetValue(val)

	attrs := obj.Attributes()
	attrs = append(attrs, attr)
	obj.SetAttributes(attrs...)
}

func testNewEngineWithShardNum(t *testing.T, num int) *StorageEngine {
	shards := make([]*shard.Shard, 0, num)

	for i := range num {
		shards = append(shards, testNewShard(t, i))
	}

	return testNewEngineWithShards(shards...)
}

func newEngineWithFixedShardOrder(ss []shardInterface) *StorageEngine {
	e := New()

	ws := make([]shardWrapper, len(ss))

	for i := range ss {
		ws[i] = shardWrapper{
			shardIface: ss[i],
		}
	}

	e.sortShardsFn = func(*StorageEngine, interface{ EncodeToString() string }) []shardWrapper {
		return ws
	}

	return e
}

type unimplementedShard struct{}

func (unimplementedShard) ID() *shard.ID {
	panic("unimplemented")
}

func (unimplementedShard) GetStream(oid.Address, bool) (*object.Object, io.ReadCloser, error) {
	panic("unimplemented")
}

func (unimplementedShard) GetECPart(cid.ID, oid.ID, iec.PartInfo) (object.Object, io.ReadCloser, error) {
	panic("unimplemented")
}

type getECPartKey struct {
	cnr    cid.ID
	parent oid.ID
	pi     iec.PartInfo
}

type getECPartValue struct {
	obj object.Object
	err error
}

type getStreamKey struct {
	addr     oid.Address
	skipMeta bool
}

type getStreamValue struct {
	obj object.Object
	err error
}

type mockShard struct {
	i              int
	getECPartSleep time.Duration
	getECPart      map[getECPartKey]getECPartValue
	getStream      map[getStreamKey]getStreamValue
}

func (x *mockShard) ID() *shard.ID {
	si := strconv.Itoa(x.i)
	return shard.NewIDFromBytes([]byte(si))
}

func (x *mockShard) GetECPart(cnr cid.ID, parent oid.ID, pi iec.PartInfo) (object.Object, io.ReadCloser, error) {
	time.Sleep(x.getECPartSleep)
	val, ok := x.getECPart[getECPartKey{
		cnr:    cnr,
		parent: parent,
		pi:     pi,
	}]
	if !ok {
		return object.Object{}, nil, errors.New("[test] unexpected object requested")
	}
	return *val.obj.CutPayload(), io.NopCloser(bytes.NewReader(val.obj.Payload())), val.err
}

func (x *mockShard) GetStream(addr oid.Address, skipMeta bool) (*object.Object, io.ReadCloser, error) {
	val, ok := x.getStream[getStreamKey{
		addr:     addr,
		skipMeta: skipMeta,
	}]
	if !ok {
		return nil, nil, errors.New("[test] unexpected object requested")
	}
	return val.obj.CutPayload(), io.NopCloser(bytes.NewReader(val.obj.Payload())), val.err
}

type unimplementedMetrics struct{}

func (x unimplementedMetrics) AddListContainersDuration(time.Duration) {
	panic("unimplemented")
}

func (x unimplementedMetrics) AddEstimateContainerSizeDuration(time.Duration) {
	panic("unimplemented")
}

func (x unimplementedMetrics) AddDeleteDuration(time.Duration) {
	panic("unimplemented")
}

func (x unimplementedMetrics) AddExistsDuration(time.Duration) {
	panic("unimplemented")
}

func (x unimplementedMetrics) AddGetDuration(time.Duration) {
	panic("unimplemented")
}

func (x unimplementedMetrics) AddHeadDuration(time.Duration) {
	panic("unimplemented")
}

func (x unimplementedMetrics) AddGetStreamDuration(time.Duration) {
	panic("unimplemented")
}

func (x unimplementedMetrics) AddInhumeDuration(time.Duration) {
	panic("unimplemented")
}

func (x unimplementedMetrics) AddPutDuration(time.Duration) {
	panic("unimplemented")
}

func (x unimplementedMetrics) AddRangeDuration(time.Duration) {
	panic("unimplemented")
}

func (x unimplementedMetrics) AddSearchDuration(time.Duration) {
	panic("unimplemented")
}

func (x unimplementedMetrics) AddListObjectsDuration(time.Duration) {
	panic("unimplemented")
}

func (x unimplementedMetrics) AddGetECPartDuration(time.Duration) {
	panic("unimplemented")
}

func (x unimplementedMetrics) SetObjectCounter(string, string, uint64) {
	panic("unimplemented")
}

func (x unimplementedMetrics) AddToObjectCounter(string, string, int) {
	panic("unimplemented")
}

func (x unimplementedMetrics) SetReadonly(string, bool) {
	panic("unimplemented")
}

func (x unimplementedMetrics) AddToContainerSize(string, int64) {
	panic("unimplemented")
}

func (x unimplementedMetrics) AddToPayloadCounter(string, int64) {
	panic("unimplemented")
}

type testMetrics struct {
	unimplementedMetrics
	getECPart atomic.Int64
}

func (x *testMetrics) AddGetECPartDuration(d time.Duration) {
	x.getECPart.Add(int64(d))
}
