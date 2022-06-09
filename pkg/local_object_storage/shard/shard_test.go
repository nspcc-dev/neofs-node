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
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
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
		shard.WithPiloramaOptions(pilorama.WithPath(filepath.Join(rootPath, "pilorama"))),
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

func generateObjectWithCID(t *testing.T, cnr cid.ID) *object.Object {
	data := make([]byte, 32)
	rand.Read(data)
	return generateObjectWithPayload(cnr, data)
}

func generateObjectWithPayload(cnr cid.ID, data []byte) *object.Object {
	var ver version.Version
	ver.SetMajor(2)
	ver.SetMinor(1)

	var csum checksum.Checksum
	csum.SetSHA256(sha256.Sum256(data))

	var csumTZ checksum.Checksum
	csumTZ.SetTillichZemor(tz.Sum(csum.Value()))

	obj := object.New()
	obj.SetID(oidtest.ID())
	obj.SetOwnerID(usertest.ID())
	obj.SetContainerID(cnr)
	obj.SetVersion(&ver)
	obj.SetPayload(data)
	obj.SetPayloadChecksum(csum)
	obj.SetPayloadHomomorphicHash(csumTZ)

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

func addPayload(obj *object.Object, size int) {
	buf := make([]byte, size)
	_, _ = rand.Read(buf)

	obj.SetPayload(buf)
	obj.SetPayloadSize(uint64(size))
}
