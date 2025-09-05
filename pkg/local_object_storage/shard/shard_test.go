package shard_test

import (
	"crypto/rand"
	"crypto/sha256"
	"io"
	"path/filepath"
	"slices"
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
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

func assertGetStreamResult(t testing.TB, exp, hdr object.Object, rdr io.ReadCloser) {
	b, err := io.ReadAll(rdr)
	require.NoError(t, err)
	hdr.SetPayload(b)
	require.Equal(t, exp, hdr)
}

func assertSearchable(t testing.TB, sh *shard.Shard, addr oid.Address) {
	_assertObjectSearchable(t, sh, addr, require.True)
}

func assertNotSearchable(t testing.TB, sh *shard.Shard, addr oid.Address) {
	_assertObjectSearchable(t, sh, addr, require.False)
}

func _assertObjectSearchable(t testing.TB, sh *shard.Shard, addr oid.Address, requireFunc func(require.TestingT, bool, ...any)) {
	fs, c, err := objectcore.PreprocessSearchQuery(nil, nil, "")
	require.NoError(t, err)

	res, _, err := sh.Search(addr.Container(), fs, nil, c, 1000)
	require.NoError(t, err)
	requireFunc(t, slices.ContainsFunc(res, func(item client.SearchResultItem) bool {
		return item.ID == addr.Object()
	}))
}

func assertAvailableObject(t testing.TB, sh *shard.Shard, addr oid.Address, obj object.Object) {
	exists, err := sh.Exists(addr, true)
	require.NoError(t, err)
	require.True(t, exists)

	hdr, err := sh.Head(addr, false)
	require.NoError(t, err)
	require.Equal(t, obj.CutPayload(), hdr)

	hdr, rc, err := sh.GetStream(addr, false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rc.Close() })
	assertGetStreamResult(t, obj, *hdr, rc)

	got, err := sh.Get(addr, false)
	require.NoError(t, err)
	require.Equal(t, obj, *got)

	b, err := sh.GetBytes(addr)
	require.NoError(t, err)
	require.Equal(t, obj.Marshal(), b)

	b, err = sh.GetBytesWithMetadataLookup(addr)
	require.NoError(t, err)
	require.Equal(t, obj.Marshal(), b)

	st, err := sh.ObjectStatus(addr)
	require.NoError(t, err)
	require.NoError(t, st.Metabase.Error)
	require.Equal(t, []string{"AVAILABLE"}, st.Metabase.State)

	assertSearchable(t, sh, addr)

	items, err := sh.List()
	require.NoError(t, err)
	require.Contains(t, items, addr)
}

func assertAvailableObjectInFSTree(t testing.TB, fst common.Storage, addr oid.Address, obj object.Object) {
	exists, err := fst.Exists(addr)
	require.NoError(t, err)
	require.True(t, exists)

	hdr, err := fst.Head(addr)
	require.NoError(t, err)
	require.Equal(t, obj.CutPayload(), hdr)

	hdr, rc, err := fst.GetStream(addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rc.Close() })
	assertGetStreamResult(t, obj, *hdr, rc)

	got, err := fst.Get(addr)
	require.NoError(t, err)
	require.Equal(t, obj, *got)

	b, err := fst.GetBytes(addr)
	require.NoError(t, err)
	require.Equal(t, obj.Marshal(), b)

	found := false
	err = fst.IterateAddresses(func(item oid.Address) error {
		found = found || item == addr
		return nil
	}, false)
	require.NoError(t, err)
	require.True(t, found)
}

func assertNoObject(t testing.TB, sh *shard.Shard, addr oid.Address) {
	exists, err := sh.Exists(addr, true)
	require.NoError(t, err)
	require.False(t, exists)

	_, err = sh.Head(addr, false)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	_, _, err = sh.GetStream(addr, false)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	_, err = sh.Get(addr, false)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	_, err = sh.GetBytes(addr)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	_, err = sh.GetBytesWithMetadataLookup(addr)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	st, err := sh.ObjectStatus(addr)
	require.NoError(t, err)
	require.Zero(t, st.Metabase)
	require.Zero(t, st.Blob)
	require.Zero(t, st.Writecache)

	assertNotSearchable(t, sh, addr)

	items, err := sh.List()
	require.NoError(t, err)
	require.NotContains(t, items, addr)
}

func assertNoObjectInFSTree(t testing.TB, fst common.Storage, addr oid.Address) {
	exists, err := fst.Exists(addr)
	require.NoError(t, err)
	require.False(t, exists)

	_, err = fst.Head(addr)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	_, _, err = fst.GetStream(addr)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	_, err = fst.Get(addr)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	_, err = fst.GetBytes(addr)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	found := false
	err = fst.IterateAddresses(func(item oid.Address) error {
		found = found || item == addr
		return nil
	}, false)
	require.NoError(t, err)
	require.False(t, found)
}

func assertMarkedAsGarbage(t testing.TB, sh *shard.Shard, addr oid.Address, obj object.Object) {
	_, err := sh.Exists(addr, true)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	_, err = sh.Head(addr, false)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	_, _, err = sh.GetStream(addr, false)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	_, err = sh.Get(addr, false)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	b, err := sh.GetBytes(addr)
	require.NoError(t, err)
	require.Equal(t, obj.Marshal(), b)

	_, err = sh.GetBytesWithMetadataLookup(addr)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	st, err := sh.ObjectStatus(addr)
	require.NoError(t, err)
	require.NoError(t, st.Metabase.Error)
	require.ElementsMatch(t, []string{"AVAILABLE", "GC MARKED"}, st.Metabase.State)

	assertNotSearchable(t, sh, addr)

	items, err := sh.List()
	require.NoError(t, err)
	require.NotContains(t, items, addr)
}
