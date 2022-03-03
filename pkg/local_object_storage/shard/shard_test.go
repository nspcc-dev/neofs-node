package shard_test

import (
	"crypto/sha256"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	ownertest "github.com/nspcc-dev/neofs-sdk-go/owner/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newShard(t testing.TB, enableWriteCache bool) *shard.Shard {
	return newCustomShard(t, t.TempDir(), enableWriteCache,
		[]writecache.Option{writecache.WithMaxMemSize(0)},
		nil)
}

func newCustomShard(t testing.TB, rootPath string, enableWriteCache bool, wcOpts []writecache.Option, bsOpts []blobstor.Option) *shard.Shard {
	if enableWriteCache {
		rootPath = filepath.Join(rootPath, "wc")
	} else {
		rootPath = filepath.Join(rootPath, "nowc")
	}

	opts := []shard.Option{
		shard.WithLogger(zap.L()),
		shard.WithBlobStorOptions(
			append([]blobstor.Option{
				blobstor.WithRootPath(filepath.Join(rootPath, "blob")),
				blobstor.WithBlobovniczaShallowWidth(2),
				blobstor.WithBlobovniczaShallowDepth(2),
			}, bsOpts...)...,
		),
		shard.WithMetaBaseOptions(
			meta.WithPath(filepath.Join(rootPath, "meta")),
		),
		shard.WithWriteCache(enableWriteCache),
		shard.WithWriteCacheOptions(
			append(
				[]writecache.Option{writecache.WithPath(filepath.Join(rootPath, "wcache"))},
				wcOpts...)...,
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

func generateObject(t *testing.T) *object.Object {
	return generateObjectWithCID(t, cidtest.ID())
}

func generateObjectWithCID(t *testing.T, cid *cid.ID) *object.Object {
	data := owner.PublicKeyToIDBytes(&test.DecodeKey(-1).PublicKey)
	return generateObjectWithPayload(cid, data)
}

func generateObjectWithPayload(cid *cid.ID, data []byte) *object.Object {
	version := version.New()
	version.SetMajor(2)
	version.SetMinor(1)

	csum := new(checksum.Checksum)
	csum.SetSHA256(sha256.Sum256(data))

	csumTZ := new(checksum.Checksum)
	csumTZ.SetTillichZemor(tz.Sum(csum.Sum()))

	obj := object.New()
	obj.SetID(generateOID())
	obj.SetOwnerID(ownertest.ID())
	obj.SetContainerID(cid)
	obj.SetVersion(version)
	obj.SetPayload(data)
	obj.SetPayloadChecksum(csum)
	obj.SetPayloadHomomorphicHash(csumTZ)

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

func addPayload(obj *object.Object, size int) {
	buf := make([]byte, size)
	_, _ = rand.Read(buf)

	obj.SetPayload(buf)
	obj.SetPayloadSize(uint64(size))
}

func generateOID() *oidSDK.ID {
	cs := [sha256.Size]byte{}
	_, _ = rand.Read(cs[:])

	id := oidSDK.NewID()
	id.SetSHA256(cs)

	return id
}
