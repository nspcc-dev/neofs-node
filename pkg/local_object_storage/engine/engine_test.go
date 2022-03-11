package engine

import (
	"crypto/sha256"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/address/test"
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	ownertest "github.com/nspcc-dev/neofs-sdk-go/owner/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

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
	for i := 0; i < shardNum; i++ {
		shards[i] = testNewShard(b, i)
	}

	e := testNewEngineWithShards(shards...)
	b.Cleanup(func() {
		_ = e.Close()
		_ = os.RemoveAll(b.Name())
	})

	addr := objecttest.Address()
	for i := 0; i < 100; i++ {
		obj := generateObjectWithCID(b, cidtest.ID())
		err := Put(e, obj)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
			errorCount: atomic.NewUint32(0),
			Shard:      s,
		}
		engine.shardPools[s.ID().String()] = pool
	}

	return engine
}

func testNewShard(t testing.TB, id int) *shard.Shard {
	sid, err := generateShardID()
	require.NoError(t, err)

	s := shard.New(
		shard.WithID(sid),
		shard.WithLogger(zap.L()),
		shard.WithBlobStorOptions(
			blobstor.WithRootPath(filepath.Join(t.Name(), fmt.Sprintf("%d.blobstor", id))),
			blobstor.WithBlobovniczaShallowWidth(2),
			blobstor.WithBlobovniczaShallowDepth(2),
			blobstor.WithRootPerm(0700),
		),
		shard.WithMetaBaseOptions(
			meta.WithPath(filepath.Join(t.Name(), fmt.Sprintf("%d.metabase", id))),
			meta.WithPermissions(0700),
		))

	require.NoError(t, s.Open())
	require.NoError(t, s.Init())

	return s
}

func testEngineFromShardOpts(t *testing.T, num int, extraOpts func(int) []shard.Option) *StorageEngine {
	engine := New()
	for i := 0; i < num; i++ {
		_, err := engine.AddShard(append([]shard.Option{
			shard.WithBlobStorOptions(
				blobstor.WithRootPath(filepath.Join(t.Name(), fmt.Sprintf("blobstor%d", i))),
				blobstor.WithBlobovniczaShallowWidth(1),
				blobstor.WithBlobovniczaShallowDepth(1),
				blobstor.WithRootPerm(0700),
			),
			shard.WithMetaBaseOptions(
				meta.WithPath(filepath.Join(t.Name(), fmt.Sprintf("metabase%d", i))),
				meta.WithPermissions(0700),
			)}, extraOpts(i)...)...)
		require.NoError(t, err)
	}

	require.NoError(t, engine.Open())
	require.NoError(t, engine.Init())

	return engine
}

func testOID() *oidSDK.ID {
	cs := [sha256.Size]byte{}
	_, _ = rand.Read(cs[:])

	id := oidSDK.NewID()
	id.SetSHA256(cs)

	return id
}

func generateObjectWithCID(t testing.TB, cid *cid.ID) *object.Object {
	version := version.New()
	version.SetMajor(2)
	version.SetMinor(1)

	csum := new(checksum.Checksum)
	csum.SetSHA256(sha256.Sum256(owner.PublicKeyToIDBytes(&test.DecodeKey(-1).PublicKey)))

	csumTZ := new(checksum.Checksum)
	csumTZ.SetTillichZemor(tz.Sum(csum.Sum()))

	obj := object.New()
	obj.SetID(testOID())
	obj.SetOwnerID(ownertest.ID())
	obj.SetContainerID(cid)
	obj.SetVersion(version)
	obj.SetPayloadChecksum(csum)
	obj.SetPayloadHomomorphicHash(csumTZ)
	obj.SetPayload([]byte{1, 2, 3, 4, 5})

	return obj
}

func addAttribute(obj *object.Object, key, val string) {
	attr := object.NewAttribute()
	attr.SetKey(key)
	attr.SetValue(val)

	attrs := obj.Attributes()
	attrs = append(attrs, attr)
	obj.SetAttributes(attrs...)
}

func testNewEngineWithShardNum(t *testing.T, num int) *StorageEngine {
	shards := make([]*shard.Shard, 0, num)

	for i := 0; i < num; i++ {
		shards = append(shards, testNewShard(t, i))
	}

	return testNewEngineWithShards(shards...)
}
