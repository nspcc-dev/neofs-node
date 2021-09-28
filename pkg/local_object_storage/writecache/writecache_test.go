package writecache

import (
	"crypto/sha256"
	"math/rand"
	"os"
	"path"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	cidtest "github.com/nspcc-dev/neofs-api-go/pkg/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	ownertest "github.com/nspcc-dev/neofs-api-go/pkg/owner/test"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	KiB = 1024
	MiB = 1024 * KiB

	putCount = 1024
)

func BenchmarkSmallObjectsPut(b *testing.B) {
	c := prepareStorage(b, 0, 512*KiB, 30*MiB)
	obj := prepareObject(b, 512*KiB)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < putCount; j++ {
			err := c.Put(obj)
			if err != nil {
				b.FailNow()
			}
		}
	}
}

func testOID() *objectSDK.ID {
	cs := [sha256.Size]byte{}
	_, _ = rand.Read(cs[:])

	id := objectSDK.NewID()
	id.SetSHA256(cs)

	return id
}

func prepareObject(t testing.TB, size uint64) *object.Object {
	cid := cidtest.Generate()
	version := pkg.NewVersion()
	version.SetMajor(2)
	version.SetMinor(1)

	w, err := owner.NEO3WalletFromPublicKey(&test.DecodeKey(-1).PublicKey)
	require.NoError(t, err)

	csum := new(pkg.Checksum)
	csum.SetSHA256(sha256.Sum256(w.Bytes()))

	csumTZ := new(pkg.Checksum)
	csumTZ.SetTillichZemor(tz.Sum(csum.Sum()))

	obj := object.NewRaw()
	obj.SetID(testOID())
	obj.SetOwnerID(ownertest.Generate())
	obj.SetContainerID(cid)
	obj.SetVersion(version)
	obj.SetPayloadChecksum(csum)
	obj.SetPayloadHomomorphicHash(csumTZ)

	payload := make([]byte, size)
	_, _ = rand.Read(payload)
	obj.SetPayload(payload)

	return object.NewFromV2(obj.ToV2())
}

func prepareStorage(t testing.TB, memSize, smallSize, bigSize uint64) Cache {
	tmp, _ := os.MkdirTemp(".", "*")
	t.Cleanup(func() { os.RemoveAll(tmp) })
	metaPath := path.Join(tmp, "neofs-meta")
	blobPath := path.Join(tmp, "neofs-blob")
	wcFSPath := path.Join(tmp, "neofs-writecache")

	log := zap.NewNop()
	md := meta.New(meta.WithLogger(log), meta.WithPath(metaPath))
	require.NoError(t, md.Open())
	require.NoError(t, md.Init())

	bs := blobstor.New(blobstor.WithLogger(log), blobstor.WithRootPath(blobPath))
	require.NoError(t, bs.Open())
	require.NoError(t, bs.Init())
	wc := New(WithLogger(log),
		WithPath(wcFSPath),
		WithMetabase(md),
		WithBlobstor(bs),
		WithMaxMemSize(memSize),
		WithSmallObjectSize(smallSize),
		WithMaxObjectSize(bigSize))
	require.NoError(t, wc.Open())
	require.NoError(t, wc.Init())
	return wc
}
