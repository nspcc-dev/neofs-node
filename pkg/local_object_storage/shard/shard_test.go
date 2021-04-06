package shard_test

import (
	"crypto/sha256"
	"math/rand"
	"os"
	"path"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newShard(t testing.TB, enableWriteCache bool) *shard.Shard {
	rootPath := t.Name()
	if enableWriteCache {
		rootPath = path.Join(rootPath, "wc")
	} else {
		rootPath = path.Join(rootPath, "nowc")
	}

	opts := []shard.Option{
		shard.WithLogger(zap.L()),
		shard.WithBlobStorOptions(
			blobstor.WithRootPath(path.Join(rootPath, "blob")),
			blobstor.WithBlobovniczaShallowWidth(2),
			blobstor.WithBlobovniczaShallowDepth(2),
		),
		shard.WithMetaBaseOptions(
			meta.WithPath(path.Join(rootPath, "meta")),
		),
		shard.WithWriteCache(enableWriteCache),
		shard.WithWriteCacheOptions(
			writecache.WithMaxMemSize(0), // disable memory batches
			writecache.WithPath(path.Join(rootPath, "wcache")),
		),
	}

	sh := shard.New(opts...)

	require.NoError(t, sh.Open())
	require.NoError(t, sh.Init())

	return sh
}

func releaseShard(s *shard.Shard, t testing.TB) {
	s.Close()
	os.RemoveAll(t.Name())
}

func generateRawObject(t *testing.T) *object.RawObject {
	return generateRawObjectWithCID(t, generateCID())
}

func generateRawObjectWithCID(t *testing.T, cid *container.ID) *object.RawObject {
	version := pkg.NewVersion()
	version.SetMajor(2)
	version.SetMinor(1)

	w, err := owner.NEO3WalletFromPublicKey(&test.DecodeKey(-1).PublicKey)
	require.NoError(t, err)

	ownerID := owner.NewID()
	ownerID.SetNeo3Wallet(w)

	csum := new(pkg.Checksum)
	csum.SetSHA256(sha256.Sum256(w.Bytes()))

	csumTZ := new(pkg.Checksum)
	csumTZ.SetTillichZemor(tz.Sum(csum.Sum()))

	obj := object.NewRaw()
	obj.SetID(generateOID())
	obj.SetOwnerID(ownerID)
	obj.SetContainerID(cid)
	obj.SetVersion(version)
	obj.SetPayloadChecksum(csum)
	obj.SetPayloadHomomorphicHash(csumTZ)

	return obj
}

func addAttribute(obj *object.RawObject, key, val string) {
	attr := objectSDK.NewAttribute()
	attr.SetKey(key)
	attr.SetValue(val)

	attrs := obj.Attributes()
	attrs = append(attrs, attr)
	obj.SetAttributes(attrs...)
}

func addPayload(obj *object.RawObject, size int) {
	buf := make([]byte, size)
	_, _ = rand.Read(buf)

	obj.SetPayload(buf)
	obj.SetPayloadSize(uint64(size))
}

func generateCID() *container.ID {
	cs := [sha256.Size]byte{}
	_, _ = rand.Read(cs[:])

	id := container.NewID()
	id.SetSHA256(cs)

	return id
}

func generateOID() *objectSDK.ID {
	cs := [sha256.Size]byte{}
	_, _ = rand.Read(cs[:])

	id := objectSDK.NewID()
	id.SetSHA256(cs)

	return id
}
