package shard_test

import (
	"crypto/sha256"
	"math/rand"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	ownertest "github.com/nspcc-dev/neofs-sdk-go/owner/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newShard(t testing.TB, enableWriteCache bool) *shard.Shard {
	return newCustomShard(t, enableWriteCache, writecache.WithMaxMemSize(0))
}

func newCustomShard(t testing.TB, enableWriteCache bool, wcOpts ...writecache.Option) *shard.Shard {
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
			append(wcOpts, writecache.WithPath(path.Join(rootPath, "wcache")))...,
		),
	}

	sh := shard.New(opts...)

	require.NoError(t, sh.Open())
	require.NoError(t, sh.Init())

	return sh
}

func releaseShard(s *shard.Shard, t testing.TB) {
	s.Close()
	os.RemoveAll(strings.Split(t.Name(), string(os.PathSeparator))[0])
}

func generateRawObject(t *testing.T) *object.RawObject {
	return generateRawObjectWithCID(t, cidtest.ID())
}

func generateRawObjectWithCID(t *testing.T, cid *cid.ID) *object.RawObject {
	data := owner.PublicKeyToIDBytes(&test.DecodeKey(-1).PublicKey)
	return generateRawObjectWithPayload(cid, data)
}

func generateRawObjectWithPayload(cid *cid.ID, data []byte) *object.RawObject {
	version := version.New()
	version.SetMajor(2)
	version.SetMinor(1)

	csum := new(checksum.Checksum)
	csum.SetSHA256(sha256.Sum256(data))

	csumTZ := new(checksum.Checksum)
	csumTZ.SetTillichZemor(tz.Sum(csum.Sum()))

	obj := object.NewRaw()
	obj.SetID(generateOID())
	obj.SetOwnerID(ownertest.ID())
	obj.SetContainerID(cid)
	obj.SetVersion(version)
	obj.SetPayload(data)
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

func generateOID() *objectSDK.ID {
	cs := [sha256.Size]byte{}
	_, _ = rand.Read(cs[:])

	id := objectSDK.NewID()
	id.SetSHA256(cs)

	return id
}
