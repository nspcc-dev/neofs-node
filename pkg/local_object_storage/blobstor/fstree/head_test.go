package fstree_test

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"testing"

	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func newMaxNonPayloadFieldsObject() object.Object {
	maxVerifScript := testutil.RandByteSlice(neofscrypto.MaxVerificationScriptLength)
	maxInvocScript := testutil.RandByteSlice(neofscrypto.MaxInvocationScriptLength)
	sig := neofscrypto.NewSignatureFromRawKey(math.MaxInt32, maxVerifScript, maxInvocScript)

	var obj object.Object
	obj.SetID(oidtest.ID())
	obj.SetContainerID(cidtest.ID())
	obj.SetSignature(&sig)
	obj.SetAttributes(object.NewAttribute("attr", hex.EncodeToString(testutil.RandByteSlice(8168))))
	obj.SetPayload(testutil.RandByteSlice(1024))

	return obj
}

func TestMaxNonPayloadFieldsObject(t *testing.T) {
	obj := newMaxNonPayloadFieldsObject()
	data := obj.CutPayload().Marshal()
	require.EqualValues(t, iprotobuf.MaxObjectWithoutPayloadLength, len(data))
	require.LessOrEqual(t, len(data), iobject.NonPayloadFieldsBufferLength)
}

func TestHeadStorage(t *testing.T) {
	fsTree := setupFSTree(t)

	testObjects := func(t *testing.T, fsTree *fstree.FSTree, size int) {
		obj := generateTestObject(size)

		addAttribute(obj, "test-key1", "test-value1")
		addAttribute(obj, "test-key2", "test-value2")

		testObject := func(t *testing.T, obj *object.Object) {
			err := fsTree.Put(obj.Address(), obj.Marshal())
			require.NoError(t, err)

			res, err := fsTree.Head(obj.Address())
			require.NoError(t, err)

			require.Equal(t, obj.CutPayload(), res)
			require.Empty(t, res.Payload())

			require.Len(t, res.Attributes(), len(obj.Attributes()))

			fullObj, err := fsTree.Get(obj.Address())
			require.NoError(t, err)
			require.Equal(t, obj, fullObj)

			testReadHeaderOK(t, fsTree, *obj)
		}

		t.Run("non-payload fields limit", func(t *testing.T) {
			obj := newMaxNonPayloadFieldsObject()
			if size > 0 {
				obj.SetPayload(testutil.RandByteSlice(size))
			}
			testObject(t, &obj)
		})

		testObject(t, obj)
	}

	testCombinedObjects := func(t *testing.T, fsTree *fstree.FSTree, size int) {
		const numObjects = 100

		objMap := make(map[oid.Address][]byte, numObjects)
		objects := make([]*object.Object, numObjects)

		t.Run("non-payload fields limit", func(t *testing.T) {
			for i := range numObjects {
				obj := newMaxNonPayloadFieldsObject()
				if size > 0 {
					obj.SetPayload(testutil.RandByteSlice(size))
				}
				objects[i] = &obj
				objMap[obj.Address()] = obj.Marshal()
			}

			require.NoError(t, fsTree.PutBatch(objMap))

			for i := range numObjects {
				res, err := fsTree.Head(objects[i].Address())
				require.NoError(t, err)
				require.Equal(t, objects[i].CutPayload(), res)

				testReadHeaderOK(t, fsTree, *objects[i])
			}
		})

		for i := range numObjects {
			obj := generateTestObject(size)
			obj.SetAttributes()
			addAttribute(obj, fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))

			objects[i] = obj
			objMap[obj.Address()] = obj.Marshal()
		}

		require.NoError(t, fsTree.PutBatch(objMap))

		for i := range numObjects {
			res, err := fsTree.Head(objects[i].Address())
			require.NoError(t, err)
			require.Equal(t, objects[i].CutPayload(), res)

			attrs := res.Attributes()
			require.Len(t, attrs, 1)
			require.Equal(t, fmt.Sprintf("key-%d", i), attrs[0].Key())
			require.Equal(t, fmt.Sprintf("value-%d", i), attrs[0].Value())

			testReadHeaderOK(t, fsTree, *objects[i])
		}
	}

	t.Run("many attributes", func(t *testing.T) {
		obj := generateTestObject(0)
		obj.SetAttributes()
		numAttrs := 100
		for i := range numAttrs {
			addAttribute(obj, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		}

		err := fsTree.Put(obj.Address(), obj.Marshal())
		require.NoError(t, err)

		res, err := fsTree.Head(obj.Address())
		require.NoError(t, err)
		require.Equal(t, obj.CutPayload(), res)
		require.Len(t, res.Attributes(), numAttrs)

		testReadHeaderOK(t, fsTree, *obj)
	})

	t.Run("non-existent object", func(t *testing.T) {
		obj := generateTestObject(0)
		addr := obj.Address()

		_, err := fsTree.Head(addr)
		require.Error(t, err)

		_, err = fsTree.ReadHeader(obj.Address(), make([]byte, iobject.NonPayloadFieldsBufferLength*2))
		require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
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

func testReadHeaderOK(t *testing.T, fst *fstree.FSTree, obj object.Object) {
	buf := make([]byte, iobject.NonPayloadFieldsBufferLength*2)

	n, err := fst.ReadHeader(obj.Address(), buf)
	require.NoError(t, err)

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
