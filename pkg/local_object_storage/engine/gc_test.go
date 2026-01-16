package engine

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/internal/testutil"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	statusSDK "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

type containerPaymantsStub struct{}

func (p containerPaymantsStub) UnpaidSince(id cid.ID) (int64, error) { return -1, nil }

func TestChildrenExpiration(t *testing.T) {
	const numOfShards = 5
	es := &asyncEpochState{e: 10}
	e := New()
	for i := range numOfShards {
		_, err := e.AddShard(
			shard.WithBlobstor(
				newStorage(filepath.Join(t.TempDir(), fmt.Sprintf("fstree%d", i))),
			),
			shard.WithMetaBaseOptions(
				meta.WithPath(filepath.Join(t.TempDir(), fmt.Sprintf("metabase%d", i))),
				meta.WithPermissions(0700),
				meta.WithEpochState(es),
			),
			shard.WithExpiredObjectsCallback(e.processExpiredObjects),
			shard.WithGCRemoverSleepInterval(100*time.Millisecond),
			shard.WithContainerPayments(containerPaymantsStub{}),
		)
		require.NoError(t, err)
	}
	require.NoError(t, e.Open())
	require.NoError(t, e.Init())
	t.Cleanup(func() {
		_ = e.Close()
	})

	t.Run("V1", func(t *testing.T) {
		cnr := cidtest.ID()
		splitID := object.NewSplitID()

		parent := generateObjectWithCID(cnr)
		parentID := parent.GetID()
		addExpirationAttribute(parent, es.CurrentEpoch())

		child1 := generateObjectWithCID(cnr)
		child1ID := child1.GetID()
		child1.SetSplitID(splitID)

		child2 := generateObjectWithCID(cnr)
		child2ID := child2.GetID()
		child2.SetSplitID(splitID)
		child2.SetPreviousID(child1ID)

		child3 := generateObjectWithCID(cnr)
		child3ID := child3.GetID()
		child3.SetSplitID(splitID)
		child3.SetPreviousID(child2ID)
		child3.SetParent(parent)
		child3.SetParentID(parentID)
		child3.SetPayloadSize(100500)

		link := generateObjectWithCID(cnr)
		link.SetParent(parent)
		link.SetParentID(parentID)
		link.SetChildren(child1ID, child2ID, child3ID)
		link.SetSplitID(splitID)

		require.NoError(t, e.Put(child1, nil))
		require.NoError(t, e.Put(child2, nil))
		require.NoError(t, e.Put(child3, nil))
		require.NoError(t, e.Put(link, nil))

		tickEpoch(es, e)

		checkObjectsAsyncRemoval(t, e, cnr, child1ID, child2ID, child3ID)
	})

	t.Run("V2", func(t *testing.T) {
		cnr := cidtest.ID()

		parent := generateObjectWithCID(cnr)
		parentID := parent.GetID()
		addExpirationAttribute(parent, es.CurrentEpoch())

		child1 := generateObjectWithCID(cnr)
		child1ID := child1.GetID()
		child1.SetParent(parent)

		child2 := generateObjectWithCID(cnr)
		child2ID := child2.GetID()
		child2.SetFirstID(child1ID)
		child2.SetPreviousID(child1ID)

		child3 := generateObjectWithCID(cnr)
		child3ID := child3.GetID()
		child3.SetFirstID(child1ID)
		child3.SetPreviousID(child2ID)
		child3.SetParent(parent)
		child3.SetParentID(parentID)

		children := make([]object.MeasuredObject, 3)
		children[0].SetObjectID(child1ID)
		children[1].SetObjectID(child2ID)
		children[2].SetObjectID(child3ID)

		var link object.Link
		link.SetObjects(children)

		var linkObj object.Object
		linkObj.WriteLink(link)
		linkObj.SetContainerID(cnr)
		linkObj.SetParent(parent)
		linkObj.SetParentID(parentID)
		linkObj.SetFirstID(child1ID)
		linkObj.SetOwner(usertest.ID())
		linkObj.CalculateAndSetPayloadChecksum()
		require.NoError(t, linkObj.CalculateAndSetID())

		require.NoError(t, e.Put(child1, nil))
		require.NoError(t, e.Put(child2, nil))
		require.NoError(t, e.Put(child3, nil))
		require.NoError(t, e.Put(&linkObj, nil))

		tickEpoch(es, e)

		checkObjectsAsyncRemoval(t, e, cnr, child1ID, child2ID, child3ID)
	})
}

func checkObjectsAsyncRemoval(t *testing.T, e *StorageEngine, cnr cid.ID, objs ...oid.ID) {
	require.Eventually(t, func() bool {
		var addr oid.Address
		addr.SetContainer(cnr)

		for _, obj := range objs {
			addr.SetObject(obj)

			_, err := e.Get(addr)
			if !errors.As(err, new(statusSDK.ObjectNotFound)) {
				return false
			}
		}

		return true
	}, 1*time.Second, 100*time.Millisecond)
}

func TestGC(t *testing.T) {
	const numOfShards = 2

	dir := t.TempDir()
	es := &asyncEpochState{e: 10}
	e := New()
	for i := range numOfShards {
		_, err := e.AddShard(
			shard.WithBlobstor(
				newStorage(filepath.Join(dir, fmt.Sprintf("fstree%d", i))),
			),
			shard.WithMetaBaseOptions(
				meta.WithPath(filepath.Join(dir, fmt.Sprintf("metabase%d", i))),
				meta.WithPermissions(0700),
				meta.WithEpochState(es),
			),
			shard.WithExpiredObjectsCallback(e.processExpiredObjects),
			shard.WithGCRemoverSleepInterval(100*time.Millisecond),
			shard.WithContainerPayments(containerPaymantsStub{}),
		)
		require.NoError(t, err)
	}
	require.NoError(t, e.Open())
	require.NoError(t, e.Init())
	t.Cleanup(func() { _ = e.Close() })

	cnr := cidtest.ID()

	t.Run("expired object", func(t *testing.T) {
		obj := generateObjectWithCID(cnr)
		addExpirationAttribute(obj, es.CurrentEpoch())
		require.NoError(t, e.Put(obj, nil))

		tickEpoch(es, e)

		checkObjectsAsyncRemoval(t, e, cnr, obj.GetID())
	})

	t.Run("object with lock - prevents deletion until lock expires", func(t *testing.T) {
		obj := generateObjectWithCID(cnr)
		addExpirationAttribute(obj, es.CurrentEpoch())
		require.NoError(t, e.Put(obj, nil))
		objAddr := obj.Address()

		lockObj := generateObjectWithCID(cnr)
		lockObj.SetType(object.TypeLock)
		addExpirationAttribute(lockObj, es.CurrentEpoch()+1) // lock expires after object
		lockObj.AssociateLocked(obj.GetID())
		require.NoError(t, e.Put(lockObj, nil))

		tickEpoch(es, e)

		// wait a bit to ensure GC cycle happened
		time.Sleep(200 * time.Millisecond)

		// object must stay because it's locked
		_, err := e.Get(objAddr)
		require.NoError(t, err)

		tickEpoch(es, e)

		// now GC must delete the object eventually
		checkObjectsAsyncRemoval(t, e, cnr, obj.GetID(), lockObj.GetID())
	})

	t.Run("expired tombstone removed", func(t *testing.T) {
		tomb := generateObjectWithCID(cnr)
		tomb.SetType(object.TypeTombstone)
		addExpirationAttribute(tomb, es.CurrentEpoch())
		require.NoError(t, e.Put(tomb, nil))

		tickEpoch(es, e)

		checkObjectsAsyncRemoval(t, e, cnr, tomb.GetID())
	})

	t.Run("object inhume by tombstone", func(t *testing.T) {
		obj := generateObjectWithCID(cnr)
		require.NoError(t, e.Put(obj, nil))
		tomb := generateObjectWithCID(cnr)

		var a object.Attribute
		a.SetKey(object.AttributeExpirationEpoch)
		a.SetValue(strconv.Itoa(int(es.e)))
		tomb.SetAttributes(a)

		objAddr := obj.Address()
		tomb.AssociateDeleted(obj.GetID())

		require.NoError(t, e.Put(tomb, nil))

		_, err := e.Get(objAddr)
		require.ErrorIs(t, err, statusSDK.ErrObjectAlreadyRemoved)

		tickEpoch(es, e)

		checkObjectsAsyncRemoval(t, e, cnr, obj.GetID(), tomb.GetID())
	})

	t.Run("object associated with expired tombstone", func(t *testing.T) {
		obj := generateObjectWithCID(cnr)
		require.NoError(t, e.Put(obj, nil))
		tomb := generateObjectWithCID(cnr)
		tomb.SetType(object.TypeTombstone)
		tomb.AssociateDeleted(obj.GetID())
		addExpirationAttribute(tomb, es.CurrentEpoch())
		require.NoError(t, e.Put(tomb, nil))

		_, err := e.Get(obj.Address())
		require.ErrorIs(t, err, statusSDK.ErrObjectAlreadyRemoved)

		tickEpoch(es, e)

		checkObjectsAsyncRemoval(t, e, cnr, obj.GetID(), tomb.GetID())
	})

	t.Run("object with tombstone expires", func(t *testing.T) {
		obj := generateObjectWithCID(cnr)
		addExpirationAttribute(obj, es.CurrentEpoch())
		require.NoError(t, e.Put(obj, nil))
		objAddr := obj.Address()

		tomb := generateObjectWithCID(cnr)
		tomb.SetType(object.TypeTombstone)
		tomb.AssociateDeleted(obj.GetID())
		addExpirationAttribute(tomb, es.CurrentEpoch()+1) // tombstone expires after object
		require.NoError(t, e.Put(tomb, nil))

		tickEpoch(es, e)

		// wait a bit to ensure GC cycle happened
		time.Sleep(200 * time.Millisecond)

		// object covered by tombstone
		_, err := e.Get(objAddr)
		require.ErrorIs(t, err, statusSDK.ErrObjectAlreadyRemoved)
		_, err = e.Get(tomb.Address())
		require.NoError(t, err)

		tickEpoch(es, e)

		// now GC must delete the object eventually
		checkObjectsAsyncRemoval(t, e, cnr, obj.GetID(), tomb.GetID())
	})
}

func TestSplitObjectExpirationWithoutLink(t *testing.T) {
	dir := t.TempDir()
	es := &asyncEpochState{e: 10}
	l := zaptest.NewLogger(t)
	e := New(WithLogger(l))
	_, err := e.AddShard(
		shard.WithLogger(l),
		shard.WithBlobstor(
			newStorage(filepath.Join(dir, "fstree")),
		),
		shard.WithMetaBaseOptions(
			meta.WithLogger(l),
			meta.WithPath(filepath.Join(dir, "metabase")),
			meta.WithPermissions(0700),
			meta.WithEpochState(es),
		),
		shard.WithExpiredObjectsCallback(e.processExpiredObjects),
		shard.WithGCRemoverSleepInterval(100*time.Millisecond),
		shard.WithContainerPayments(containerPaymantsStub{}),
	)
	require.NoError(t, err)

	require.NoError(t, e.Open())
	require.NoError(t, e.Init())
	t.Cleanup(func() { _ = e.Close() })

	cnr := cidtest.ID()
	parentID := oidtest.ID()

	parent := generateObjectWithCID(cnr)
	parent.SetID(parentID)
	parent.SetPayload(nil)
	addExpirationAttribute(parent, es.CurrentEpoch())
	parentAddr := parent.Address()

	const childCount = 3
	children := make([]*object.Object, childCount)
	childIDs := make([]oid.ID, childCount)
	childAddrs := make([]oid.Address, childCount)

	var firstID oid.ID
	for i := range children {
		children[i] = generateObjectWithCID(cnr)
		if i == 0 {
			firstID = children[i].GetID()
		}
		if i != 0 {
			children[i].SetPreviousID(childIDs[i-1])
			children[i].SetFirstID(firstID)
		}
		if i == len(children)-1 {
			children[i].SetParent(parent)
		}
		children[i].SetPayload([]byte{byte(i), byte(i + 1), byte(i + 2)})
		children[i].SetPayloadSize(3)
		childIDs[i] = children[i].GetID()
		childAddrs[i] = children[i].Address()
	}

	for i := range children {
		require.NoError(t, e.Put(children[i], nil))
	}

	_, err = e.Get(parentAddr)
	var splitErr *object.SplitInfoError
	require.ErrorAs(t, err, &splitErr)
	// Do not put link, so parent will have SplitInfo without link
	require.True(t, splitErr.SplitInfo().GetLink().IsZero())
	require.Equal(t, childIDs[len(childIDs)-1], splitErr.SplitInfo().GetLastPart())
	require.Equal(t, childIDs[0], splitErr.SplitInfo().GetFirstPart())

	tickEpoch(es, e)

	require.Eventually(t, func() bool {
		// Check this way because Get returns ErrObjectNotFound for expired error
		for _, sh := range e.sortedShards(parentAddr) {
			_, err = sh.Get(parentAddr, false)
			if errors.Is(err, statusSDK.ErrObjectNotFound) {
				return true
			}
		}
		return false
	}, 1*time.Second, 100*time.Millisecond)
}

func TestSplitObjectExpirationWithLinkNotFound(t *testing.T) {
	dir := t.TempDir()
	es := &asyncEpochState{e: 10}
	l, logBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)
	e := New(WithLogger(l))
	fstr := newStorage(filepath.Join(dir, "fstree"))
	_, err := e.AddShard(
		shard.WithLogger(l),
		shard.WithBlobstor(fstr),
		shard.WithMetaBaseOptions(
			meta.WithLogger(l),
			meta.WithPath(filepath.Join(dir, "metabase")),
			meta.WithPermissions(0700),
			meta.WithEpochState(es),
		),
		shard.WithExpiredObjectsCallback(e.processExpiredObjects),
		shard.WithGCRemoverSleepInterval(100*time.Millisecond),
		shard.WithContainerPayments(containerPaymantsStub{}),
	)
	require.NoError(t, err)

	require.NoError(t, e.Open())
	require.NoError(t, e.Init())
	t.Cleanup(func() { _ = e.Close() })

	cnr := cidtest.ID()
	parentID := oidtest.ID()

	parent := generateObjectWithCID(cnr)
	parent.SetID(parentID)
	parent.SetPayload(nil)
	addExpirationAttribute(parent, es.CurrentEpoch())
	parentAddr := parent.Address()

	const childCount = 3
	children := make([]*object.Object, childCount)
	childIDs := make([]oid.ID, childCount)

	var firstID oid.ID
	for i := range children {
		children[i] = generateObjectWithCID(cnr)
		if i == 0 {
			firstID = children[i].GetID()
		}
		if i != 0 {
			children[i].SetPreviousID(childIDs[i-1])
			children[i].SetFirstID(firstID)
		}
		if i == len(children)-1 {
			children[i].SetParent(parent)
		}
		children[i].SetPayload([]byte{byte(i), byte(i + 1), byte(i + 2)})
		children[i].SetPayloadSize(3)
		childIDs[i] = children[i].GetID()
	}

	var linkObj object.Object
	linkObj.WriteLink(object.Link{})
	linkObj.SetContainerID(cnr)
	linkObj.SetParent(parent)
	linkObj.SetParentID(parentID)
	linkObj.SetFirstID(firstID)
	linkObj.SetOwner(usertest.ID())
	linkObj.CalculateAndSetPayloadChecksum()
	require.NoError(t, linkObj.CalculateAndSetID())

	linkAddr := linkObj.Address()

	// Put link object and all children
	require.NoError(t, e.Put(&linkObj, nil))
	for i := range children {
		require.NoError(t, e.Put(children[i], nil))
	}

	_, err = e.Get(linkAddr)
	require.NoError(t, err)

	_, err = e.Get(parentAddr)
	var splitErr *object.SplitInfoError
	require.ErrorAs(t, err, &splitErr)
	require.Equal(t, linkObj.GetID(), splitErr.SplitInfo().GetLink())
	require.Equal(t, childIDs[len(childIDs)-1], splitErr.SplitInfo().GetLastPart())
	require.Equal(t, childIDs[0], splitErr.SplitInfo().GetFirstPart())

	// Now delete the link object to simulate missing link scenario
	require.NoError(t, fstr.Delete(linkAddr))

	_, err = e.Get(linkAddr)
	require.ErrorIs(t, err, statusSDK.ErrObjectNotFound)

	_, err = e.Get(parentAddr)
	require.ErrorAs(t, err, &splitErr)
	require.Equal(t, linkObj.GetID(), splitErr.SplitInfo().GetLink()) // Pretends to be here, but not in fact.
	require.Equal(t, childIDs[len(childIDs)-1], splitErr.SplitInfo().GetLastPart())
	require.Equal(t, childIDs[0], splitErr.SplitInfo().GetFirstPart())

	tickEpoch(es, e)

	require.Eventually(t, func() bool {
		// Check this way because Get returns ErrObjectNotFound for expired error
		for _, sh := range e.sortedShards(parentAddr) {
			_, err = sh.Get(parentAddr, false)
			if errors.Is(err, statusSDK.ErrObjectNotFound) {
				return true
			}
		}
		return false
	}, 1*time.Second, 100*time.Millisecond)

	logBuf.AssertContainsMsg(zap.DebugLevel, "inhuming root object but no link object is found")
	logBuf.AssertContainsMsg(zap.InfoLevel, "root object has no link object in split upload")
}

func addExpirationAttribute(obj *object.Object, epoch uint64) {
	addAttribute(obj, object.AttributeExpirationEpoch, fmt.Sprint(epoch))
}

func tickEpoch(es *asyncEpochState, e *StorageEngine) {
	e.HandleNewEpoch(es.Inc())
}

type asyncEpochState struct {
	mu sync.Mutex
	e  uint64
}

func (s *asyncEpochState) CurrentEpoch() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.e
}

func (s *asyncEpochState) Inc() uint64 {
	s.mu.Lock()
	s.e++
	v := s.e
	s.mu.Unlock()
	return v
}
