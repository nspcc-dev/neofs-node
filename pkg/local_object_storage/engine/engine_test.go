package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	checksumtest "github.com/nspcc-dev/neofs-sdk-go/checksum/test"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
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

	addr := oidtest.Address()
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
		shard.WithPiloramaOptions(pilorama.WithPath(filepath.Join(t.Name(), fmt.Sprintf("%d.pilorama", id)))),
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
			),
			shard.WithPiloramaOptions(
				pilorama.WithPath(filepath.Join(t.Name(), fmt.Sprintf("pilorama%d", i)))),
		}, extraOpts(i)...)...)
		require.NoError(t, err)
	}

	require.NoError(t, engine.Open())
	require.NoError(t, engine.Init())

	return engine
}

func generateObjectWithCID(t testing.TB, cnr cid.ID) *object.Object {
	var ver version.Version
	ver.SetMajor(2)
	ver.SetMinor(1)

	csum := checksumtest.Checksum()

	var csumTZ checksum.Checksum
	csumTZ.SetTillichZemor(tz.Sum(csum.Value()))

	obj := object.New()
	obj.SetID(oidtest.ID())
	obj.SetOwnerID(usertest.ID())
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

	for i := 0; i < num; i++ {
		shards = append(shards, testNewShard(t, i))
	}

	return testNewEngineWithShards(shards...)
}
