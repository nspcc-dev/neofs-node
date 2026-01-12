package fstree

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

func TestGetStream(t *testing.T) {
	tree := setupFSTree(t)

	payloadSizes := []int{
		1,
		1024,
		1024 * 1024,
	}

	t.Run("object not found", func(t *testing.T) {
		addr := oidtest.Address()
		obj, reader, err := tree.GetStream(addr)
		require.Error(t, err)
		require.Nil(t, obj)
		require.Nil(t, reader)
	})

	testStream := func(t *testing.T, size int) {
		payload := make([]byte, size)
		_, _ = rand.Read(payload)

		addr := oidtest.Address()
		obj := new(object.Object)
		obj.SetID(addr.Object())
		obj.SetPayload(payload)

		require.NoError(t, tree.Put(addr, obj.Marshal()))

		retrievedObj, reader, err := tree.GetStream(addr)
		require.NoError(t, err)
		require.NotNil(t, retrievedObj)
		require.Equal(t, obj.CutPayload(), retrievedObj)

		require.NotNil(t, reader)
		streamedPayload, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, payload, streamedPayload)
		require.NoError(t, reader.Close())
	}

	t.Run("different objects", func(t *testing.T) {
		for _, size := range payloadSizes {
			t.Run(fmt.Sprint(size), func(t *testing.T) {
				testStream(t, size)
			})
		}
	})

	t.Run("compressed object", func(t *testing.T) {
		compress := compression.Config{Enabled: true}
		require.NoError(t, compress.Init())
		tree.Config = &compress

		for _, size := range payloadSizes {
			t.Run(fmt.Sprint(size), func(t *testing.T) {
				testStream(t, size)
			})
		}
	})

	t.Run("specific combined object", func(t *testing.T) {
		fsTree := setupFSTree(t)

		const numObjects = 3
		const size1 = object.MaxHeaderLen - 77 - 38 - 37 // (MaxHeaderLen - headerLen - combinedDataOff) - (combinedDataOff - 1)
		const size2 = object.MaxHeaderLen - 77 - 38 - 1  // (MaxHeaderLen - headerLen - combinedDataOff) - (combinedDataOff - 37)
		// In this case, after the second read from the file,
		// the third object's calculated buffer length should be sufficient
		// to parse the combined prefix. However, if it is not expanded,
		// its actual length will be one, which causes panic.
		// Therefore, the buffer must have enough bytes for parsing the combined prefix
		// only if it has been properly expanded.

		cnr := cidtest.ID()
		payload1 := make([]byte, size1)
		_, _ = rand.Read(payload1)
		payload2 := make([]byte, size2)
		_, _ = rand.Read(payload2)

		objects := make([]*object.Object, numObjects)
		for i := range objects {
			obj := object.New(cnr, usertest.ID())
			obj.SetID(oidtest.ID())
			if i == 1 {
				obj.SetPayload(payload2)
			} else {
				obj.SetPayload(payload1)
			}

			objects[i] = obj
		}

		// Don't use map with FSTree.PutBatch because we need ordered writes
		writeDataUnits := make([]writeDataUnit, 0, len(objects))
		for _, obj := range objects {
			addr := objectcore.AddressOf(obj)
			p := fsTree.treePath(addr)
			require.NoError(t, util.MkdirAllX(filepath.Dir(p), fsTree.Permissions))
			writeDataUnits = append(writeDataUnits, writeDataUnit{
				id:   addr.Object(),
				path: p,
				data: fsTree.Compress(obj.Marshal()),
			})
		}
		require.NoError(t, fsTree.writer.writeBatch(writeDataUnits))

		for i := range objects {
			res, reader, err := fsTree.GetStream(objectcore.AddressOf(objects[i]))
			require.NoError(t, err)
			require.Equal(t, objects[i].CutPayload(), res)

			require.NotNil(t, reader)
			streamedPayload, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, objects[i].Payload(), streamedPayload)
			require.NoError(t, reader.Close())
		}
	})
}

func TestGetStreamAfterErrors(t *testing.T) {
	tree := New(WithPath(t.TempDir()))

	t.Run("corrupt header", func(t *testing.T) {
		addr := oidtest.Address()

		objPath := tree.treePath(addr)
		require.NoError(t, os.MkdirAll(filepath.Dir(objPath), 0755))

		f, err := os.Create(objPath)
		require.NoError(t, err)
		_, err = f.Write([]byte("corrupt data that isn't a valid object"))
		require.NoError(t, err)
		require.NoError(t, f.Close())

		obj, reader, err := tree.GetStream(addr)
		require.Error(t, err)
		require.Nil(t, obj)
		require.Nil(t, reader)
	})

	t.Run("corrupt compressed data", func(t *testing.T) {
		compress := compression.Config{Enabled: true}
		require.NoError(t, compress.Init())
		tree.Config = &compress

		addr := oidtest.Address()
		obj := new(object.Object)
		obj.SetID(addr.Object())
		payload := []byte("test payload")
		obj.SetPayload(payload)

		require.NoError(t, tree.Put(addr, obj.Marshal()))

		objPath := tree.treePath(addr)

		f, err := os.OpenFile(objPath, os.O_WRONLY|os.O_APPEND, 0644)
		require.NoError(t, err)
		_, err = f.Write([]byte("corruption at the end"))
		require.NoError(t, err)
		require.NoError(t, f.Close())

		_, _, err = tree.GetStream(addr)
		require.Error(t, err)
	})

	t.Run("ID not found in combined object", func(t *testing.T) {
		fsTree := setupFSTree(t)

		cnr := cidtest.ID()

		idStr := "HhW47uw6Fs48M2GtNx1Joem6qUjP1JxGPo4ffvp4QJaL"
		id, err := oid.DecodeString(idStr)
		require.NoError(t, err)
		obj := object.New(cnr, usertest.ID())
		obj.SetID(id)
		addr := objectcore.AddressOf(obj)

		require.NoError(t, fsTree.Put(addr, obj.Marshal()))

		p := fsTree.treePath(addr)

		newIDStr := "HhW47uw6Fs48M2GtNx1Joem6qUjP1JxGPo4ffvp4QJaM"
		newID, err := oid.DecodeString(newIDStr)
		require.NoError(t, err)
		newAddr := oid.NewAddress(cnr, newID)
		newPath := fsTree.treePath(newAddr)

		require.NoError(t, os.Rename(p, newPath))

		_, _, err = fsTree.GetStream(newAddr)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})
}

func setupFSTree(t *testing.T) *FSTree {
	tree := New(WithPath(t.TempDir()))
	require.NoError(t, tree.Open(false))
	require.NoError(t, tree.Init())
	t.Cleanup(func() { require.NoError(t, tree.Close()) })
	return tree
}
