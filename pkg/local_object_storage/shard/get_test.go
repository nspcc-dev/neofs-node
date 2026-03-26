package shard_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"testing/iotest"

	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestShard_Get(t *testing.T) {
	t.Run("without write cache", func(t *testing.T) {
		testShardGet(t, false)
	})

	t.Run("with write cache", func(t *testing.T) {
		testShardGet(t, true)
	})
}

func testShardGet(t *testing.T, hasWriteCache bool) {
	sh := newShard(t, hasWriteCache)
	defer releaseShard(sh, t)

	t.Run("small object", func(t *testing.T) {
		obj := generateObject()
		addAttribute(obj, "foo", "bar")
		addPayload(obj, 1<<5)
		addr := obj.Address()

		err := sh.Put(obj, nil)
		require.NoError(t, err)

		res, err := sh.Get(addr, false)
		require.NoError(t, err)
		require.Equal(t, obj, res)

		testGetBytes(t, sh, addr, obj.Marshal())
		testGetStream(t, sh, addr, obj)
	})

	t.Run("big object", func(t *testing.T) {
		obj := generateObject()
		addAttribute(obj, "foo", "bar")
		obj.SetID(oidtest.ID())
		addPayload(obj, 1<<20) // big obj
		addr := obj.Address()

		err := sh.Put(obj, nil)
		require.NoError(t, err)

		res, err := sh.Get(addr, false)
		require.NoError(t, err)
		require.Equal(t, obj, res)

		testGetBytes(t, sh, addr, obj.Marshal())
		testGetStream(t, sh, addr, obj)
	})

	t.Run("parent object", func(t *testing.T) {
		cnr := cidtest.ID()
		splitID := object.NewSplitID()

		parent := generateObjectWithCID(cnr)
		addAttribute(parent, "parent", "attribute")
		parentAddr := parent.Address()

		child := generateObjectWithCID(cnr)
		child.SetParent(parent)
		idParent := parent.GetID()
		child.SetParentID(idParent)
		child.SetSplitID(splitID)
		addPayload(child, 1<<5)
		childAddr := child.Address()

		err := sh.Put(child, nil)
		require.NoError(t, err)

		res, err := sh.Get(childAddr, false)
		require.NoError(t, err)
		require.True(t, binaryEqual(child, res))

		testGetStream(t, sh, childAddr, child)

		_, _, streamErr := sh.GetStream(parentAddr, false)
		require.Error(t, streamErr)
		_, err = sh.Get(parentAddr, false)
		require.Equal(t, streamErr, err)

		var si *object.SplitInfoError
		require.True(t, errors.As(err, &si))

		link := si.SplitInfo().GetLink()
		require.True(t, link.IsZero())
		id1 := child.GetID()
		id2 := si.SplitInfo().GetLastPart()
		require.Equal(t, id1, id2)
		require.Equal(t, splitID, si.SplitInfo().SplitID())
	})
}

func testGetBytes(t testing.TB, sh *shard.Shard, addr oid.Address, objBin []byte) {
	b, err := sh.GetBytes(addr)
	require.NoError(t, err)
	require.Equal(t, objBin, b)

	b, err = sh.GetBytesWithMetadataLookup(addr)
	require.NoError(t, err)
	require.Equal(t, objBin, b)
}

func testGetStream(t testing.TB, sh *shard.Shard, addr oid.Address, obj *object.Object) {
	header, reader, err := sh.GetStream(addr, false)
	require.NoError(t, err)
	require.Equal(t, obj.CutPayload(), header)

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, obj.Payload(), data)

	require.NoError(t, reader.Close())
}

// binary equal is used when object contains empty lists in the structure and
// require.Equal fails on comparing <nil> and []{} lists.
func binaryEqual(a, b *object.Object) bool {
	return bytes.Equal(a.Marshal(), b.Marshal())
}

func TestShard_ReadObject(t *testing.T) {
	t.Run("without write cache", func(t *testing.T) {
		testShardReadObject(t, false)
	})

	t.Run("with write cache", func(t *testing.T) {
		testShardReadObject(t, true)
	})
}

func testShardReadObject(t *testing.T, hasWriteCache bool) {
	sh := newShard(t, hasWriteCache)
	defer releaseShard(sh, t)

	buf := make([]byte, 2*iobject.NonPayloadFieldsBufferLength)

	t.Run("regular object", func(t *testing.T) {
		obj := generateObject()
		addAttribute(obj, "foo", "bar")

		err := sh.Put(obj, nil)
		require.NoError(t, err)

		n, stream, err := sh.ReadObject(obj.Address(), false, buf)
		require.NoError(t, err)
		assertReadObjectOK(t, *obj, buf, n, stream)
	})

	t.Run("virtual object", func(t *testing.T) {
		cnr := cidtest.ID()

		parent := generateObjectWithCID(cnr)
		addAttribute(parent, "foo", "bar")

		child := generateObjectWithCID(cnr)
		child.SetParent(parent)

		err := sh.Put(child, nil)
		require.NoError(t, err)

		_, _, err = sh.ReadObject(parent.Address(), false, buf)
		var siErr *object.SplitInfoError
		require.ErrorAs(t, err, &siErr)
		si := siErr.SplitInfo()
		require.NotNil(t, si)
		require.Equal(t, child.GetID(), si.GetLastPart())

		_, _, err = sh.ReadObject(parent.Address(), true, buf)
		require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
	})
}

func assertReadObjectOK(t *testing.T, obj object.Object, buf []byte, n int, stream io.ReadCloser) {
	if len(obj.Payload()) > 0 {
		_, tail, ok := bytes.Cut(buf[:n], obj.CutPayload().Marshal())
		require.True(t, ok)

		prefix := make([]byte, 1+binary.MaxVarintLen64)
		prefix[0] = iprotobuf.TagBytes4 // payload field tag
		prefix = prefix[:1+binary.PutUvarint(prefix[1:], uint64(len(obj.Payload())))]

		tail, ok = bytes.CutPrefix(tail, prefix)
		require.True(t, ok)
		require.True(t, bytes.HasPrefix(obj.Payload(), tail))
	}

	require.NoError(t, iotest.TestReader(io.MultiReader(bytes.NewReader(buf[:n]), stream), obj.Marshal()))
	require.NoError(t, stream.Close())
}
