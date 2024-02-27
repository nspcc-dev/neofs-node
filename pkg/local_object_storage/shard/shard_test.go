package shard_test

import (
	"crypto/sha256"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/peapod"
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
	"go.uber.org/zap/zaptest"
)

type epochState struct {
	Value uint64
}

func (s epochState) CurrentEpoch() uint64 {
	return s.Value
}

func newShard(t testing.TB, enableWriteCache bool) *shard.Shard {
	return newCustomShard(t, t.TempDir(), enableWriteCache,
		nil,
		nil)
}

func newCustomShard(t testing.TB, rootPath string, enableWriteCache bool, wcOpts []writecache.Option, bsOpts []blobstor.Option, options ...shard.Option) *shard.Shard {
	if enableWriteCache {
		rootPath = filepath.Join(rootPath, "wc")
	} else {
		rootPath = filepath.Join(rootPath, "nowc")
	}

	if bsOpts == nil {
		bsOpts = []blobstor.Option{
			blobstor.WithLogger(zaptest.NewLogger(t)),
			blobstor.WithStorages([]blobstor.SubStorage{
				{
					Storage: peapod.New(filepath.Join(rootPath, "peapod.db"), 0o600, 10*time.Millisecond),
					Policy: func(_ *object.Object, data []byte) bool {
						return len(data) <= 1<<20
					},
				},
				{
					Storage: fstree.New(
						fstree.WithPath(filepath.Join(rootPath, "blob"))),
				},
			}),
		}
	}

	opts := append([]shard.Option{
		shard.WithLogger(zap.L()),
		shard.WithBlobStorOptions(bsOpts...),
		shard.WithMetaBaseOptions(
			meta.WithPath(filepath.Join(rootPath, "meta")),
			meta.WithEpochState(epochState{}),
		),
		shard.WithPiloramaOptions(pilorama.WithPath(filepath.Join(rootPath, "pilorama"))),
		shard.WithWriteCache(enableWriteCache),
		shard.WithWriteCacheOptions(
			append(
				[]writecache.Option{writecache.WithPath(filepath.Join(rootPath, "wcache"))},
				wcOpts...)...,
		),
	}, options...)

	sh := shard.New(opts...)

	require.NoError(t, sh.Open())
	require.NoError(t, sh.Init())

	return sh
}

func releaseShard(s *shard.Shard, t testing.TB) {
	require.NoError(t, s.Close())
}

func generateObject(t *testing.T) *object.Object {
	return generateObjectWithCID(t, cidtest.ID())
}

func generateObjectWithCID(t *testing.T, cnr cid.ID) *object.Object {
	data := make([]byte, 32)
	//nolint:staticcheck
	rand.Read(data)
	return generateObjectWithPayload(t, cnr, data)
}

func generateObjectWithPayload(t testing.TB, cnr cid.ID, data []byte) *object.Object {
	var ver version.Version
	ver.SetMajor(2)
	ver.SetMinor(1)

	var csum checksum.Checksum
	csum.SetSHA256(sha256.Sum256(data))

	var csumTZ checksum.Checksum
	csumTZ.SetTillichZemor(tz.Sum(csum.Value()))

	obj := object.New()
	obj.SetID(oidtest.ID())
	owner := usertest.ID(t)
	obj.SetOwnerID(&owner)
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
	//nolint:staticcheck
	_, _ = rand.Read(buf)

	obj.SetPayload(buf)
	obj.SetPayloadSize(uint64(size))
}
