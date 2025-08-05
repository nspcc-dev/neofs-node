package shard_test

import (
	"crypto/rand"
	"crypto/sha256"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
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
	"go.uber.org/zap/zaptest"
)

type epochState struct {
	Value uint64
}

func (s epochState) CurrentEpoch() uint64 {
	return s.Value
}

func newShard(t testing.TB, enableWriteCache bool) *shard.Shard {
	return newCustomShard(t, t.TempDir(), enableWriteCache, nil)
}

func newCustomShard(t testing.TB, rootPath string, enableWriteCache bool, wcOpts []writecache.Option, options ...shard.Option) *shard.Shard {
	sh, _ := _newShardWithFSTree(t, rootPath, enableWriteCache, wcOpts, options...)
	return sh
}

func newShardWithFSTree(t testing.TB, opts ...shard.Option) (*shard.Shard, *fstree.FSTree) {
	return _newShardWithFSTree(t, t.TempDir(), false, nil, opts...)
}

func _newShardWithFSTree(t testing.TB, rootPath string, enableWriteCache bool, wcOpts []writecache.Option, options ...shard.Option) (*shard.Shard, *fstree.FSTree) {
	if enableWriteCache {
		rootPath = filepath.Join(rootPath, "wc")
	} else {
		rootPath = filepath.Join(rootPath, "nowc")
	}

	fst := fstree.New(
		fstree.WithPath(filepath.Join(rootPath, "fstree")),
	)

	opts := append([]shard.Option{
		shard.WithID(shard.NewIDFromBytes([]byte("testShard"))),
		shard.WithLogger(zap.L()),
		shard.WithMetaBaseOptions(
			meta.WithPath(filepath.Join(rootPath, "meta")),
			meta.WithEpochState(epochState{}),
			meta.WithLogger(zaptest.NewLogger(t)),
		),
		shard.WithWriteCache(enableWriteCache),
		shard.WithWriteCacheOptions(
			append(
				[]writecache.Option{
					writecache.WithLogger(zaptest.NewLogger(t)),
					writecache.WithPath(filepath.Join(rootPath, "wcache")),
				},
				wcOpts...)...,
		),
		shard.WithBlobstor(fst),
	}, options...)

	sh := shard.New(opts...)

	require.NoError(t, sh.Open())
	require.NoError(t, sh.Init())

	return sh, fst
}

func releaseShard(s *shard.Shard, t testing.TB) {
	require.NoError(t, s.Close())
}

func generateObject() *object.Object {
	return generateObjectWithCID(cidtest.ID())
}

func generateObjectWithCID(cnr cid.ID) *object.Object {
	data := make([]byte, 32)
	_, _ = rand.Read(data)
	return generateObjectWithPayload(cnr, data)
}

func generateObjectWithPayload(cnr cid.ID, data []byte) *object.Object {
	var ver version.Version
	ver.SetMajor(2)
	ver.SetMinor(1)

	csum := checksum.NewSHA256(sha256.Sum256(data))

	obj := object.New()
	obj.SetID(oidtest.ID())
	obj.SetOwner(usertest.ID())
	obj.SetContainerID(cnr)
	obj.SetVersion(&ver)
	obj.SetPayload(data)
	obj.SetPayloadChecksum(csum)
	obj.SetPayloadHomomorphicHash(checksum.NewTillichZemor(tz.Sum(csum.Value())))

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
