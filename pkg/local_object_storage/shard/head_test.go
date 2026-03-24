package shard_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func TestShard_Head(t *testing.T) {
	t.Run("without write cache", func(t *testing.T) {
		testShardHead(t, false)
	})

	t.Run("with write cache", func(t *testing.T) {
		testShardHead(t, true)
	})
}

func testShardHead(t *testing.T, hasWriteCache bool) {
	sh := newShard(t, hasWriteCache)
	defer releaseShard(sh, t)

	t.Run("regular object", func(t *testing.T) {
		obj := generateObject()
		addAttribute(obj, "foo", "bar")

		err := sh.Put(obj, nil)
		require.NoError(t, err)

		res, err := sh.Head(obj.Address(), false)
		require.NoError(t, err)
		require.Equal(t, obj.CutPayload(), res)
	})

	t.Run("virtual object", func(t *testing.T) {
		cnr := cidtest.ID()
		splitID := object.NewSplitID()

		parent := generateObjectWithCID(cnr)
		addAttribute(parent, "foo", "bar")

		child := generateObjectWithCID(cnr)
		child.SetParent(parent)
		idParent := parent.GetID()
		child.SetParentID(idParent)
		child.SetSplitID(splitID)

		err := sh.Put(child, nil)
		require.NoError(t, err)

		var siErr *object.SplitInfoError

		_, err = sh.Head(parent.Address(), true)
		require.True(t, errors.As(err, &siErr))

		head, err := sh.Head(parent.Address(), false)
		require.NoError(t, err)
		require.Equal(t, parent.CutPayload(), head)
	})
}

func TestShard_ReadHeader(t *testing.T) {
	t.Run("without write cache", func(t *testing.T) {
		testShardReadHeader(t, false)
	})

	t.Run("with write cache", func(t *testing.T) {
		testShardReadHeader(t, true)
	})
}

func testShardReadHeader(t *testing.T, hasWriteCache bool) {
	sh := newShard(t, hasWriteCache)
	defer releaseShard(sh, t)

	buf := make([]byte, 2*iobject.NonPayloadFieldsBufferLength)

	t.Run("regular object", func(t *testing.T) {
		obj := generateObject()
		addAttribute(obj, "foo", "bar")

		err := sh.Put(obj, nil)
		require.NoError(t, err)

		n, err := sh.ReadHeader(obj.Address(), false, buf)
		require.NoError(t, err)
		assertReadHeaderOK(t, *obj, buf, n)
	})

	t.Run("virtual object", func(t *testing.T) {
		cnr := cidtest.ID()

		parent := generateObjectWithCID(cnr)
		addAttribute(parent, "foo", "bar")

		child := generateObjectWithCID(cnr)
		child.SetParent(parent)

		err := sh.Put(child, nil)
		require.NoError(t, err)

		_, err = sh.ReadHeader(parent.Address(), true, buf)
		var siErr *object.SplitInfoError
		require.ErrorAs(t, err, &siErr)
		si := siErr.SplitInfo()
		require.NotNil(t, si)
		require.Equal(t, child.GetID(), si.GetLastPart())

		n, err := sh.ReadHeader(parent.Address(), false, buf)
		require.NoError(t, err)
		assertReadHeaderOK(t, *parent, buf, n)
	})
}

func assertReadHeaderOK(t *testing.T, obj object.Object, buf []byte, n int) {
	_, tail, ok := bytes.Cut(buf[:n], obj.CutPayload().Marshal())
	require.True(t, ok)

	prefix := make([]byte, 1+binary.MaxVarintLen64)
	prefix[0] = iprotobuf.TagBytes4 // payload field tag
	prefix = prefix[:1+binary.PutUvarint(prefix[1:], uint64(len(obj.Payload())))]

	if len(tail) < len(prefix) {
		require.True(t, bytes.HasPrefix(prefix, tail))
		return
	}

	tail, ok = bytes.CutPrefix(tail, prefix)
	require.True(t, ok)
	require.True(t, bytes.HasPrefix(obj.Payload(), tail))
}
