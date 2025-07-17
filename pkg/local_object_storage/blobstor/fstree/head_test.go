package fstree_test

import (
	"fmt"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

func TestHeadStorage(t *testing.T) {
	fsTree := setupFSTree(t)

	testObjects := func(t *testing.T, fsTree *fstree.FSTree, size int) {
		obj := generateTestObject(size)

		addAttribute(obj, "test-key1", "test-value1")
		addAttribute(obj, "test-key2", "test-value2")

		err := fsTree.Put(object.AddressOf(obj), obj.Marshal())
		require.NoError(t, err)

		res, err := fsTree.Head(object.AddressOf(obj))
		require.NoError(t, err)

		require.Equal(t, obj.CutPayload(), res)
		require.Empty(t, res.Payload())

		require.Len(t, res.Attributes(), len(obj.Attributes()))

		fullObj, err := fsTree.Get(object.AddressOf(obj))
		require.NoError(t, err)
		require.Equal(t, obj, fullObj)
	}

	testCombinedObjects := func(t *testing.T, fsTree *fstree.FSTree, size int) {
		const numObjects = 100

		objMap := make(map[oid.Address][]byte, numObjects)
		objects := make([]*objectSDK.Object, numObjects)
		for i := range numObjects {
			obj := generateTestObject(size)
			obj.SetAttributes()
			addAttribute(obj, fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))

			objects[i] = obj
			objMap[object.AddressOf(obj)] = obj.Marshal()
		}

		require.NoError(t, fsTree.PutBatch(objMap))

		for i := range numObjects {
			res, err := fsTree.Head(object.AddressOf(objects[i]))
			require.NoError(t, err)
			require.Equal(t, objects[i].CutPayload(), res)

			attrs := res.Attributes()
			require.Len(t, attrs, 1)
			require.Equal(t, fmt.Sprintf("key-%d", i), attrs[0].Key())
			require.Equal(t, fmt.Sprintf("value-%d", i), attrs[0].Value())
		}
	}

	t.Run("many attributes", func(t *testing.T) {
		obj := generateTestObject(0)
		obj.SetAttributes()
		numAttrs := 100
		for i := range numAttrs {
			addAttribute(obj, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		}

		err := fsTree.Put(object.AddressOf(obj), obj.Marshal())
		require.NoError(t, err)

		res, err := fsTree.Head(object.AddressOf(obj))
		require.NoError(t, err)
		require.Equal(t, obj.CutPayload(), res)
		require.Len(t, res.Attributes(), numAttrs)
	})

	t.Run("non-existent object", func(t *testing.T) {
		obj := generateTestObject(0)
		addr := object.AddressOf(obj)

		_, err := fsTree.Head(addr)
		require.Error(t, err)
	})

	t.Run("different payload sizes", func(t *testing.T) {
		for _, size := range payloadSizes {
			t.Run(generateSizeLabel(size), func(t *testing.T) {
				testObjects(t, fsTree, size)
			})
		}
	})

	t.Run("combined objects", func(t *testing.T) {
		for _, size := range payloadSizes {
			t.Run(generateSizeLabel(size), func(t *testing.T) {
				testCombinedObjects(t, fsTree, size)
			})
		}
	})

	t.Run("with compression", func(t *testing.T) {
		fsComp := setupFSTree(t)
		setupCompressor(t, fsComp)

		for _, size := range payloadSizes {
			t.Run("compressed_"+generateSizeLabel(size), func(t *testing.T) {
				testObjects(t, fsComp, size)
			})

			t.Run("compressed_combined_"+generateSizeLabel(size), func(t *testing.T) {
				testCombinedObjects(t, fsComp, size)
			})
		}
	})
}
