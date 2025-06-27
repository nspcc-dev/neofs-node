package fstree

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestGetStream(t *testing.T) {
	tree := New(WithPath(t.TempDir()))

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
		_, err := rand.Read(payload)
		require.NoError(t, err)

		addr := oidtest.Address()
		obj := objectSDK.New()
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
		obj := objectSDK.New()
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
}
