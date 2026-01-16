package fstree_test

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

var payloadSizes = []int{
	0,           // Empty payload
	100,         // 100 bytes
	4 * 1024,    // 4 KB
	16 * 1024,   // 16 KB
	32 * 1024,   // 32 KB
	100 * 1024,  // 100 KB
	1024 * 1024, // 1 MB
}

func setupFSTree(tb testing.TB) *fstree.FSTree {
	fsTree := fstree.New(fstree.WithPath(tb.TempDir()))
	require.NoError(tb, fsTree.Open(false))
	require.NoError(tb, fsTree.Init())
	return fsTree
}

func setupCompressor(tb testing.TB, fsTree *fstree.FSTree) {
	compressConfig := &compression.Config{
		Enabled: true,
	}
	require.NoError(tb, compressConfig.Init())
	fsTree.SetCompressor(compressConfig)
}

func prepareSingleObject(tb testing.TB, fsTree *fstree.FSTree, payloadSize int) oid.Address {
	obj := generateTestObject(payloadSize)
	addr := obj.Address()
	require.NoError(tb, fsTree.Put(addr, obj.Marshal()))
	return addr
}

func addAttribute(obj *object.Object, key, value string) {
	var attr object.Attribute
	attr.SetKey(key)
	attr.SetValue(value)

	attrs := obj.Attributes()
	attrs = append(attrs, attr)
	obj.SetAttributes(attrs...)
}

func generateTestObject(payloadSize int) *object.Object {
	obj := objecttest.Object()
	if payloadSize > 0 {
		payload := make([]byte, payloadSize)
		_, _ = rand.Read(payload)
		obj.SetPayload(payload)
	} else {
		obj.SetPayload(nil)
	}
	obj.SetPayloadSize(uint64(payloadSize))

	return &obj
}

func generateSizeLabel(size int) string {
	switch {
	case size == 0:
		return "Empty"
	case size < 1024:
		return fmt.Sprintf("%dB", size)
	case size < 1024*1024:
		return fmt.Sprintf("%dKB", size/1024)
	default:
		return fmt.Sprintf("%dMB", size/(1024*1024))
	}
}

func prepareMultipleObjects(tb testing.TB, fsTree *fstree.FSTree, payloadSize int) []oid.Address {
	const numObjects = 10
	objMap := make(map[oid.Address][]byte, numObjects)
	addrs := make([]oid.Address, numObjects)

	for i := range numObjects {
		obj := generateTestObject(payloadSize)
		addr := obj.Address()
		objMap[addr] = obj.Marshal()
		addrs[i] = addr
	}

	require.NoError(tb, fsTree.PutBatch(objMap))
	return addrs
}
