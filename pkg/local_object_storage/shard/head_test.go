package shard_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
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

		err := sh.Put(obj, nil, 0)
		require.NoError(t, err)

		res, err := testHead(t, sh, object.AddressOf(obj), false, hasWriteCache)
		require.NoError(t, err)
		require.Equal(t, obj.CutPayload(), res)
	})

	t.Run("virtual object", func(t *testing.T) {
		cnr := cidtest.ID()
		splitID := objectSDK.NewSplitID()

		parent := generateObjectWithCID(cnr)
		addAttribute(parent, "foo", "bar")

		child := generateObjectWithCID(cnr)
		child.SetParent(parent)
		idParent := parent.GetID()
		child.SetParentID(idParent)
		child.SetSplitID(splitID)

		err := sh.Put(child, nil, 0)
		require.NoError(t, err)

		var siErr *objectSDK.SplitInfoError

		_, err = testHead(t, sh, object.AddressOf(parent), true, hasWriteCache)
		require.True(t, errors.As(err, &siErr))

		head, err := sh.Head(object.AddressOf(parent), false)
		require.NoError(t, err)
		require.Equal(t, parent.CutPayload(), head)
	})
}

func testHead(t *testing.T, sh *shard.Shard, addr oid.Address, raw bool, hasWriteCache bool) (*objectSDK.Object, error) {
	res, err := sh.Head(addr, raw)
	if hasWriteCache {
		require.Eventually(t, func() bool {
			if shard.IsErrNotFound(err) {
				res, err = sh.Head(addr, raw)
			}
			return !shard.IsErrNotFound(err)
		}, time.Second, time.Millisecond*100)
	}
	return res, err
}

func TestHeadStorage(t *testing.T) {
	sh := newCustomShard(t, t.TempDir(), false, nil, shard.WithMode(mode.Degraded))
	defer releaseShard(sh, t)

	t.Run("empty payload", func(t *testing.T) {
		obj := generateObject()
		obj.SetPayload([]byte{})

		err := sh.Put(obj, nil, 0)
		require.NoError(t, err)

		res, err := sh.Head(object.AddressOf(obj), false)
		require.NoError(t, err)
		require.Equal(t, obj.CutPayload(), res)
		require.Empty(t, res.Payload())
	})

	t.Run("large payload", func(t *testing.T) {
		obj := generateObject()
		addPayload(obj, 1024*1024)

		err := sh.Put(obj, nil, 0)
		require.NoError(t, err)

		res, err := sh.Head(object.AddressOf(obj), false)
		require.NoError(t, err)
		require.Equal(t, obj.CutPayload(), res)
		require.Empty(t, res.Payload())
	})

	t.Run("many attributes", func(t *testing.T) {
		obj := generateObject()
		numAttrs := 100
		for i := range numAttrs {
			addAttribute(obj, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		}

		err := sh.Put(obj, nil, 0)
		require.NoError(t, err)

		res, err := sh.Head(object.AddressOf(obj), false)
		require.NoError(t, err)
		require.Equal(t, obj.CutPayload(), res)
		require.Len(t, res.Attributes(), numAttrs)
	})

	t.Run("non-existent object", func(t *testing.T) {
		fakeAddr := oid.Address{}
		fakeAddr.SetContainer(cidtest.ID())
		fakeAddr.SetObject(oidtest.ID())

		_, err := sh.Head(fakeAddr, false)
		require.Error(t, err)
		require.True(t, shard.IsErrNotFound(err))
	})

	t.Run("different payload sizes", func(t *testing.T) {
		payloadSizes := []int{0, 1, 100, 1024, 1024 * 1024}

		for _, size := range payloadSizes {
			t.Run(fmt.Sprintf("%dB", size), func(t *testing.T) {
				obj := generateObject()
				if size > 0 {
					addPayload(obj, size)
				} else {
					obj.SetPayload(nil)
				}

				err := sh.Put(obj, nil, 0)
				require.NoError(t, err)

				res, err := sh.Head(object.AddressOf(obj), false)
				require.NoError(t, err)
				require.Equal(t, obj.CutPayload(), res)
			})
		}
	})

	t.Run("combined objects", func(t *testing.T) {
		const numObjects = 100

		type testObjects struct {
			addr    oid.Address
			obj     *objectSDK.Object
			payload []byte
		}

		objects := make([]testObjects, numObjects)
		for i := range numObjects {
			obj := generateObject()
			addPayload(obj, 1000+i*10)
			addAttribute(obj, fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))

			objects[i] = testObjects{
				addr:    object.AddressOf(obj),
				obj:     obj,
				payload: obj.Marshal(),
			}
		}

		const numThreads = 10
		var wg sync.WaitGroup
		wg.Add(numThreads)
		n := numObjects / numThreads
		for i := range numThreads {
			go func(i int) {
				defer wg.Done()
				for j := range n {
					err := sh.Put(objects[j+i*n].obj, objects[j+i*n].payload, 0)
					require.NoError(t, err)
				}
			}(i)
		}
		wg.Wait()

		for i := range numObjects {
			res, err := sh.Head(objects[i].addr, false)
			require.NoError(t, err)
			require.Equal(t, objects[i].obj.CutPayload(), res)

			attrs := res.Attributes()
			require.Len(t, attrs, 1)
			require.Equal(t, fmt.Sprintf("key-%d", i), attrs[0].Key())
			require.Equal(t, fmt.Sprintf("value-%d", i), attrs[0].Value())
		}
	})

	t.Run("with compression", func(t *testing.T) {
		dir := t.TempDir()

		shComp := newCustomShard(t, dir, false, nil,
			shard.WithCompressObjects(true),
			shard.WithMode(mode.Degraded),
		)
		defer releaseShard(shComp, t)

		payloadSizes := []int{
			0, 512, 1024,
			11 * 1024 * 1024,
		}

		for _, size := range payloadSizes {
			t.Run(fmt.Sprintf("compressed_%dB", size), func(t *testing.T) {
				obj := generateObject()
				if size > 0 {
					addPayload(obj, size)
				} else {
					obj.SetPayload(nil)
				}

				addAttribute(obj, "test-key1", "test-value1")
				addAttribute(obj, "test-key2", "test-value2")

				err := shComp.Put(obj, nil, 0)
				require.NoError(t, err)

				res, err := shComp.Head(object.AddressOf(obj), false)
				require.NoError(t, err)

				require.Equal(t, obj.CutPayload(), res)
				require.Empty(t, res.Payload())

				require.Len(t, res.Attributes(), len(obj.Attributes()))

				fullObj, err := shComp.Get(object.AddressOf(obj), true)
				require.NoError(t, err)
				require.Equal(t, obj, fullObj)
			})
		}
	})
}
