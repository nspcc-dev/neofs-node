package fstree

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestFSTreeReshape(t *testing.T) {
	old, tree, fallbackObj := setupReshapingTree(t)

	t.Run("secondary depth read fallback", func(t *testing.T) {
		addr := fallbackObj.Address()
		obj := fallbackObj
		require.NoError(t, old.Put(addr, obj.Marshal()))

		got, err := tree.Get(addr)
		require.NoError(t, err)
		require.Equal(t, obj.Marshal(), got.Marshal())

		exists, err := tree.Exists(addr)
		require.NoError(t, err)
		require.True(t, exists)

		head, err := tree.Head(addr)
		require.NoError(t, err)
		require.Equal(t, obj.CutPayload().Marshal(), head.Marshal())

		streamed, stream, err := tree.GetStream(addr)
		require.NoError(t, err)
		require.Equal(t, obj.CutPayload().Marshal(), streamed.Marshal())
		payload, err := io.ReadAll(stream)
		require.NoError(t, err)
		require.NoError(t, stream.Close())
		require.Equal(t, obj.Payload(), payload)

		buf := make([]byte, 40<<10)
		n, stream, err := tree.ReadObject(addr, buf)
		require.NoError(t, err)
		tail, err := io.ReadAll(stream)
		require.NoError(t, err)
		require.NoError(t, stream.Close())
		require.Equal(t, obj.Marshal(), append(buf[:n], tail...))

		payloadStream, err := tree.ReadPayloadRange(addr, 1, 3, buf, nil)
		require.NoError(t, err)
		payload, err = io.ReadAll(payloadStream)
		require.NoError(t, err)
		require.NoError(t, payloadStream.Close())
		require.Equal(t, obj.Payload()[1:4], payload)
	})

	t.Run("writes deletes and iterates", func(t *testing.T) {
		oldObj := objecttest.Object()
		oldObj.SetPayload([]byte("old object payload"))
		oldObj.SetPayloadSize(uint64(len(oldObj.Payload())))
		require.NoError(t, old.Put(oldObj.Address(), oldObj.Marshal()))

		newObj := objecttest.Object()
		newObj.SetPayload([]byte("new object payload"))
		newObj.SetPayloadSize(uint64(len(newObj.Payload())))
		require.NoError(t, tree.Put(newObj.Address(), newObj.Marshal()))

		_, err := os.Stat(tree.treePath(newObj.Address()))
		require.NoError(t, err)
		_, err = os.Stat(tree.treePathAtDepth(newObj.Address(), tree.secondaryDepth))
		require.ErrorIs(t, err, os.ErrNotExist)

		var addrs []string
		require.NoError(t, tree.IterateAddresses(func(addr oid.Address) error {
			addrs = append(addrs, addr.EncodeToString())
			return nil
		}, false))
		require.ElementsMatch(t, []string{fallbackObj.Address().EncodeToString(), oldObj.Address().EncodeToString(), newObj.Address().EncodeToString()}, addrs)

		require.NoError(t, tree.Delete(oldObj.Address()))
		_, err = os.Stat(tree.treePathAtDepth(oldObj.Address(), tree.secondaryDepth))
		require.ErrorIs(t, err, os.ErrNotExist)

		exists, err := tree.Exists(oldObj.Address())
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("iterate sizes ignores disappeared file", func(t *testing.T) {
		obj := objecttest.Object()
		path := tree.treePath(obj.Address())
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
		require.NoError(t, os.Symlink("missing", path))
		defer func() { require.NoError(t, os.Remove(path)) }()

		require.NoError(t, tree.IterateSizes(func(oid.Address, uint64) error {
			return nil
		}, true))
	})

	t.Run("iteration tolerates reshaping", func(t *testing.T) {
		obj := objecttest.Object()
		obj.SetPayload([]byte("blocking object payload"))
		obj.SetPayloadSize(uint64(len(obj.Payload())))
		addr := obj.Address()
		require.NoError(t, old.Put(addr, obj.Marshal()))

		iterating := make(chan struct{})
		releaseIteration := make(chan struct{})
		iterationDone := make(chan error, 1)
		var once sync.Once
		go func() {
			iterationDone <- tree.IterateAddresses(func(iterAddr oid.Address) error {
				if _, err := tree.GetBytes(iterAddr); err != nil {
					return err
				}
				once.Do(func() {
					close(iterating)
					<-releaseIteration
				})
				return nil
			}, false)
		}()
		<-iterating

		reshapeDone := make(chan error, 1)
		go func() {
			_, err := tree.reshapeFile(tree.treePathAtDepth(addr, tree.secondaryDepth), addr, new(bool))
			reshapeDone <- err
		}()

		require.NoError(t, <-reshapeDone)
		close(releaseIteration)
		require.NoError(t, <-iterationDone)
	})

	t.Run("retries and stops during backoff", func(t *testing.T) {
		old, tree, obj := setupReshapingTree(t)
		addr := obj.Address()
		require.NoError(t, old.Put(addr, obj.Marshal()))
		require.NoError(t, old.Close())

		primaryDir := filepath.Dir(tree.treePath(addr))
		require.NoError(t, os.WriteFile(primaryDir, nil, 0o600))

		core, logs := observer.New(zap.WarnLevel)
		tree.log = zap.New(core)
		oldInterval := reshapeRetryInterval
		reshapeRetryInterval = time.Millisecond
		t.Cleanup(func() { reshapeRetryInterval = oldInterval })

		require.NoError(t, tree.Init(old.ShardID()))
		require.Eventually(t, func() bool {
			return logs.FilterMessage("FSTree reshaping failed, will retry").Len() >= 2
		}, time.Second, time.Millisecond)

		closed := make(chan error, 1)
		go func() { closed <- tree.Close() }()
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("closing FSTree did not cancel reshape retry")
		}
	})

	t.Run("moves secondary layout", func(t *testing.T) {
		testFSTreeReshape(t, 2, 3)
	})

	t.Run("moves to shallower layout", func(t *testing.T) {
		testFSTreeReshape(t, 5, 3)
	})

	t.Run("keeps shared primary directories", func(t *testing.T) {
		dir := t.TempDir()
		id, err := common.NewID()
		require.NoError(t, err)

		old := New(WithPath(dir), WithDepth(5), WithCombinedCountLimit(1))
		require.NoError(t, old.Init(id))
		obj := objecttest.Object()
		obj.SetPayload([]byte("old layout object"))
		obj.SetPayloadSize(uint64(len(obj.Payload())))
		require.NoError(t, old.Put(obj.Address(), obj.Marshal()))

		primary := New(WithPath(dir), WithDepth(3), WithAllowDepthChange(true), WithCombinedCountLimit(1))
		primary.secondaryDepth = 5
		objectDir := filepath.Dir(primary.treePath(obj.Address()))
		sharedDir := objectDir
		for sharedDir == objectDir {
			other := objecttest.Object()
			sharedDir = filepath.Dir(primary.treePath(other.Address()))
		}
		require.NoError(t, os.MkdirAll(sharedDir, 0o700))
		require.NoError(t, old.Close())

		require.NoError(t, primary.Init(id))
		<-primary.reshapeDone
		t.Cleanup(func() { require.NoError(t, primary.Close()) })

		_, err = os.Stat(sharedDir)
		require.NoError(t, err)
	})

	t.Run("moves combined files", func(t *testing.T) {
		dir := t.TempDir()
		id, err := common.NewID()
		require.NoError(t, err)

		old := New(WithPath(dir), WithDepth(2))
		require.NoError(t, old.Init(id))
		objs := []object.Object{objecttest.Object(), objecttest.Object()}
		batch := make(map[oid.Address][]byte, len(objs))
		for i := range objs {
			objs[i].SetPayload([]byte("combined object payload"))
			objs[i].SetPayloadSize(uint64(len(objs[i].Payload())))
			batch[objs[i].Address()] = objs[i].Marshal()
		}
		require.NoError(t, old.PutBatch(batch))

		oldInfo0, err := os.Stat(old.treePath(objs[0].Address()))
		require.NoError(t, err)
		oldInfo1, err := os.Stat(old.treePath(objs[1].Address()))
		require.NoError(t, err)
		if runtime.GOOS == "linux" {
			require.True(t, os.SameFile(oldInfo0, oldInfo1))
		}
		require.NoError(t, old.Close())

		tree := New(WithPath(dir), WithDepth(3), WithAllowDepthChange(true))
		require.NoError(t, tree.Init(id))
		<-tree.reshapeDone
		t.Cleanup(func() { require.NoError(t, tree.Close()) })

		newInfo0, err := os.Stat(tree.treePath(objs[0].Address()))
		require.NoError(t, err)
		newInfo1, err := os.Stat(tree.treePath(objs[1].Address()))
		require.NoError(t, err)
		if runtime.GOOS == "linux" {
			require.True(t, os.SameFile(newInfo0, newInfo1))
		}
	})

	t.Run("drops secondary file when primary exists", func(t *testing.T) {
		old, tree, obj := setupReshapingTree(t)
		addr := obj.Address()
		require.NoError(t, old.Put(addr, obj.Marshal()))
		require.NoError(t, old.Close())

		secondaryPath := tree.treePathAtDepth(addr, tree.secondaryDepth)
		primaryPath := tree.treePath(addr)
		require.NoError(t, os.MkdirAll(filepath.Dir(primaryPath), 0o700))
		require.NoError(t, os.Link(secondaryPath, primaryPath))

		require.NoError(t, tree.Init(old.ShardID()))
		<-tree.reshapeDone
		t.Cleanup(func() { require.NoError(t, tree.Close()) })

		_, err := os.Stat(primaryPath)
		require.NoError(t, err)
		_, err = os.Stat(secondaryPath)
		require.ErrorIs(t, err, os.ErrNotExist)
	})

	t.Run("concurrent operations", func(t *testing.T) {
		dir := t.TempDir()
		id, err := common.NewID()
		require.NoError(t, err)

		old := New(WithPath(dir), WithDepth(2), WithCombinedCountLimit(1))
		require.NoError(t, old.Init(id))
		oldObjs := make([]object.Object, 64)
		for i := range oldObjs {
			oldObjs[i] = objecttest.Object()
			oldObjs[i].SetPayload(bytes.Repeat([]byte{byte(i)}, 64))
			oldObjs[i].SetPayloadSize(uint64(len(oldObjs[i].Payload())))
			require.NoError(t, old.Put(oldObjs[i].Address(), oldObjs[i].Marshal()))
		}
		require.NoError(t, old.Close())

		tree := New(WithPath(dir), WithDepth(3), WithAllowDepthChange(true), WithCombinedCountLimit(1))
		require.NoError(t, tree.Init(id))
		t.Cleanup(func() { require.NoError(t, tree.Close()) })

		var wg sync.WaitGroup
		errs := make(chan error, 32)
		for range 32 {
			wg.Go(func() {
				obj := objecttest.Object()
				obj.SetPayload([]byte("concurrent object payload"))
				obj.SetPayloadSize(uint64(len(obj.Payload())))
				if err := tree.Put(obj.Address(), obj.Marshal()); err != nil {
					errs <- err
					return
				}
				got, err := tree.Get(obj.Address())
				if err != nil {
					errs <- err
					return
				}
				if !bytes.Equal(obj.Marshal(), got.Marshal()) {
					errs <- fmt.Errorf("unexpected object data for %s", obj.Address())
					return
				}
				errs <- tree.Delete(obj.Address())
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		<-tree.reshapeDone

		for i := range oldObjs {
			got, err := tree.Get(oldObjs[i].Address())
			require.NoError(t, err)
			require.Equal(t, oldObjs[i].Marshal(), got.Marshal())
		}
	})

	t.Run("deletes both layouts", func(t *testing.T) {
		old, tree, obj := setupReshapingTree(t)
		addr := obj.Address()
		require.NoError(t, old.Put(addr, obj.Marshal()))

		secondaryPath := tree.treePathAtDepth(addr, tree.secondaryDepth)
		primaryPath := tree.treePath(addr)
		require.NoError(t, os.MkdirAll(filepath.Dir(primaryPath), 0o700))
		require.NoError(t, os.Link(secondaryPath, primaryPath))

		require.NoError(t, tree.Delete(addr))
		_, err := os.Stat(primaryPath)
		require.ErrorIs(t, err, os.ErrNotExist)
		_, err = os.Stat(secondaryPath)
		require.ErrorIs(t, err, os.ErrNotExist)
	})
}

func TestFSTreeReshapeIterateOrdered(t *testing.T) {
	dir := t.TempDir()
	id, err := common.NewID()
	require.NoError(t, err)

	old := New(WithPath(dir), WithDepth(2), WithCombinedCountLimit(1))
	require.NoError(t, old.Init(id))
	tree := New(WithPath(dir), WithDepth(3), WithCombinedCountLimit(1))
	tree.secondaryDepth = 2

	objs := make([]object.Object, 4)
	for i := range objs {
		objs[i] = objecttest.Object()
		objs[i].SetPayload([]byte{byte(i)})
		objs[i].SetPayloadSize(1)
		require.NoError(t, old.Put(objs[i].Address(), objs[i].Marshal()))
	}
	require.NoError(t, tree.Put(objs[0].Address(), objs[0].Marshal()))

	var got []string
	require.NoError(t, tree.IterateAddresses(func(addr oid.Address) error {
		got = append(got, stringifyAddress(addr))
		return nil
	}, false))

	expected := make([]string, len(objs))
	for i := range objs {
		expected[i] = stringifyAddress(objs[i].Address())
	}
	sort.Strings(expected)
	require.Equal(t, expected, got)
}

func testFSTreeReshape(t *testing.T, oldDepth, newDepth uint64) {
	dir := t.TempDir()
	id, err := common.NewID()
	require.NoError(t, err)

	old := New(WithPath(dir), WithDepth(oldDepth), WithCombinedCountLimit(1))
	require.NoError(t, old.Init(id))
	objs := []object.Object{objecttest.Object(), objecttest.Object()}
	for i := range objs {
		objs[i].SetPayload(bytes.Repeat([]byte{byte(i)}, 64))
		objs[i].SetPayloadSize(uint64(len(objs[i].Payload())))
		require.NoError(t, old.Put(objs[i].Address(), objs[i].Marshal()))
	}
	require.NoError(t, old.Close())

	tree := New(WithPath(dir), WithDepth(newDepth), WithAllowDepthChange(true), WithCombinedCountLimit(1))
	require.NoError(t, tree.Init(id))
	<-tree.reshapeDone
	t.Cleanup(func() { require.NoError(t, tree.Close()) })

	for i := range objs {
		got, err := tree.Get(objs[i].Address())
		require.NoError(t, err)
		require.Equal(t, objs[i].Marshal(), got.Marshal())
		_, err = os.Stat(tree.treePath(objs[i].Address()))
		require.NoError(t, err)
		_, err = os.Stat(tree.treePathAtDepth(objs[i].Address(), tree.secondaryDepth))
		require.ErrorIs(t, err, os.ErrNotExist)
	}
}

func setupReshapingTree(t *testing.T) (*FSTree, *FSTree, object.Object) {
	dir := t.TempDir()
	id, err := common.NewID()
	require.NoError(t, err)

	old := New(WithPath(dir), WithDepth(2), WithCombinedCountLimit(1))
	require.NoError(t, old.Init(id))

	obj := objecttest.Object()
	obj.SetPayload(bytes.Repeat([]byte("x"), 64))
	obj.SetPayloadSize(uint64(len(obj.Payload())))

	tree := New(WithPath(dir), WithDepth(3), WithAllowDepthChange(true), WithCombinedCountLimit(1))
	tree.secondaryDepth = 2 // configure fallback before Init for read-path tests
	return old, tree, obj
}
