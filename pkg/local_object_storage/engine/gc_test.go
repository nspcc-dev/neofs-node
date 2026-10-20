package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nspcc-dev/neofs-node/internal/chaintime"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
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

func (p containerPaymantsStub) PaymentsDisabled() bool { return false }

func (p containerPaymantsStub) UnpaidSince(id cid.ID) (int64, error) { return -1, nil }

func TestChildrenExpiration(t *testing.T) {
	const numOfShards = 5
	es := &asyncEpochState{e: 10}
	synctest.Test(t, func(t *testing.T) {
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
		require.NoError(t, e.Init())
		t.Cleanup(func() {
			_ = e.Close()
		})

		// V1
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

		require.NoError(t, e.Put(context.Background(), child1, nil))
		require.NoError(t, e.Put(context.Background(), child2, nil))
		require.NoError(t, e.Put(context.Background(), child3, nil))
		require.NoError(t, e.Put(context.Background(), link, nil))

		tickEpoch(es, e)

		checkObjectsAsyncRemoval(t, e, cnr, child1ID, child2ID, child3ID)

		// V2
		parent = generateObjectWithCID(cnr)
		parentID = parent.GetID()
		addExpirationAttribute(parent, es.CurrentEpoch())

		child1 = generateObjectWithCID(cnr)
		child1ID = child1.GetID()
		child1.SetParent(parent)

		child2 = generateObjectWithCID(cnr)
		child2ID = child2.GetID()
		child2.SetFirstID(child1ID)
		child2.SetPreviousID(child1ID)

		child3 = generateObjectWithCID(cnr)
		child3ID = child3.GetID()
		child3.SetFirstID(child1ID)
		child3.SetPreviousID(child2ID)
		child3.SetParent(parent)
		child3.SetParentID(parentID)

		children := make([]object.MeasuredObject, 3)
		children[0].SetObjectID(child1ID)
		children[1].SetObjectID(child2ID)
		children[2].SetObjectID(child3ID)

		var linkData object.Link
		linkData.SetObjects(children)

		var linkObj object.Object
		linkObj.WriteLink(linkData)
		linkObj.SetContainerID(cnr)
		linkObj.SetParent(parent)
		linkObj.SetParentID(parentID)
		linkObj.SetFirstID(child1ID)
		linkObj.SetOwner(usertest.ID())
		linkObj.CalculateAndSetPayloadChecksum()
		require.NoError(t, linkObj.CalculateAndSetID())

		require.NoError(t, e.Put(context.Background(), child1, nil))
		require.NoError(t, e.Put(context.Background(), child2, nil))
		require.NoError(t, e.Put(context.Background(), child3, nil))
		require.NoError(t, e.Put(context.Background(), &linkObj, nil))

		tickEpoch(es, e)

		checkObjectsAsyncRemoval(t, e, cnr, child1ID, child2ID, child3ID)
	})
}

func checkObjectsAsyncRemoval(t *testing.T, e *StorageEngine, cnr cid.ID, objs ...oid.ID) {
	time.Sleep(1 * time.Second)

	var addr oid.Address
	addr.SetContainer(cnr)

	for _, obj := range objs {
		addr.SetObject(obj)

		_, err := e.Get(context.Background(), addr)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	}
}

func TestGC_CopyLocks(t *testing.T) {
	newEngine := func(t *testing.T, numShards int, metaOpts []meta.Option, extraOpts ...shard.Option) (*StorageEngine, []blobstor.Storage) {
		e := New()
		stores := make([]blobstor.Storage, numShards)
		for i := range stores {
			root := t.TempDir()
			stores[i] = newStorage(filepath.Join(root, "fstree"))
			_, err := e.AddShard(append([]shard.Option{
				shard.WithBlobstor(stores[i]),
				shard.WithMetaBaseOptions(append([]meta.Option{
					meta.WithPath(filepath.Join(root, "meta")),
					meta.WithMaxBatchDelay(time.Microsecond),
				}, metaOpts...)...),
				shard.WithGCRemoverSleepInterval(time.Hour),
				shard.WithContainerPayments(containerPaymantsStub{}),
			}, extraOpts...)...)
			require.NoError(t, err)
		}
		require.NoError(t, e.Init())
		t.Cleanup(func() { require.NoError(t, e.Close()) })
		return e, stores
	}

	t.Run("progress", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			timeLock bool
		}{
			{name: "epoch lock"},
			{name: "time lock", timeLock: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					const (
						batchSize  = 2
						gcInterval = 100 * time.Millisecond
					)
					var clock chaintime.AtomicChainTimeProvider
					clock.Set(99000)
					es := &asyncEpochState{e: 1}
					e, _ := newEngine(t, 2, []meta.Option{
						meta.WithEpochState(es),
						meta.WithTimeSource(clock.Now),
					},
						shard.WithRemoverBatchSize(batchSize),
						shard.WithGCRemoverSleepInterval(gcInterval),
					)
					shs := e.unsortedShards()
					cnr := cidtest.ID()
					objects := make([]*object.Object, batchSize)
					locks := make([]*object.Object, batchSize)
					for i := range batchSize {
						obj := generateObjectWithCID(cnr)
						addExpirationAttribute(obj, 1)
						require.NoError(t, shs[0].Put(obj, nil))
						lock := generateObjectWithCID(cnr)
						lock.AssociateLocked(obj.GetID())
						if tc.timeLock {
							require.NoError(t, lock.SetExpirationTime(time.Unix(105, 0)))
						} else {
							addExpirationAttribute(lock, 2)
						}
						// Simulate a missed LOCK broadcast to the object's shard.
						require.NoError(t, shs[1].Put(lock, nil))
						objects[i], locks[i] = obj, lock
					}
					timeObj := generateObjectWithCID(cnr)
					require.NoError(t, timeObj.SetExpirationTime(time.Unix(100, 0)))
					require.NoError(t, shs[0].Put(timeObj, nil))

					clock.Set(101000)
					tickEpoch(es, e)
					time.Sleep(10 * gcInterval)

					// A normal Get hides expired objects even if GC never deletes them.
					_, err := shs[0].Get(timeObj.Address(), true)
					require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
					for i, obj := range objects {
						_, err := shs[0].Get(obj.Address(), false)
						require.NoError(t, err)
						for _, sh := range shs {
							got, err := sh.Get(locks[i].Address(), false)
							require.NoError(t, err)
							require.Equal(t, locks[i].Marshal(), got.Marshal())
						}
					}
					// Repeating the callback must preserve all existing LOCK copies.
					for _, obj := range objects {
						e.processExpiredObjects([]oid.Address{obj.Address()})
					}

					clock.Set(106000)
					tickEpoch(es, e)
					time.Sleep(10 * gcInterval)
					for i, obj := range objects {
						_, err := shs[0].Get(obj.Address(), true)
						require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
						for _, sh := range shs {
							_, err := sh.Get(locks[i].Address(), true)
							require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
						}
					}
				})
			})
		}
	})

	t.Run("failure", func(t *testing.T) {
		for _, scenario := range []string{"read-only destination", "conflicting tombstone", "missing source blob"} {
			t.Run(scenario, func(t *testing.T) {
				e, stores := newEngine(t, 3, []meta.Option{meta.WithEpochState(epochState{e: 1})})
				shs := e.unsortedShards()
				obj := generateObjectWithCID(cidtest.ID())
				addExpirationAttribute(obj, 0)
				require.NoError(t, shs[0].Put(obj, nil))
				lock := generateObjectWithCID(obj.GetContainerID())
				lock.AssociateLocked(obj.GetID())
				addExpirationAttribute(lock, 2)
				require.NoError(t, shs[1].Put(lock, nil))

				switch scenario {
				case "read-only destination":
					require.NoError(t, shs[2].SetMode(mode.ReadOnly))
				case "conflicting tombstone":
					tomb := generateObjectWithCID(obj.GetContainerID())
					tomb.AssociateDeleted(obj.GetID())
					require.NoError(t, shs[2].Put(tomb, nil))
				case "missing source blob":
					for _, store := range stores {
						if store.ShardID() == shs[1].ID() {
							require.NoError(t, store.Delete(lock.Address()))
						}
					}
				}

				for range 2 {
					e.processExpiredObjects([]oid.Address{obj.Address()})
					// Even if copying fails, the protected object's bytes must remain.
					_, err := shs[0].Get(obj.Address(), true)
					require.NoError(t, err)
					if scenario == "missing source blob" {
						lockID, err := shs[1].GetLockID(obj.Address())
						require.NoError(t, err)
						require.Equal(t, lock.GetID(), lockID)
						_, err = shs[0].Get(lock.Address(), false)
						require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
					} else {
						for _, sh := range shs[:2] {
							got, err := sh.Get(lock.Address(), false)
							require.NoError(t, err)
							require.Equal(t, lock.Marshal(), got.Marshal())
						}
					}
				}
				// A temporarily read-only shard can be repaired by a later callback.
				if scenario == "read-only destination" {
					require.NoError(t, shs[2].SetMode(mode.ReadWrite))
					e.processExpiredObjects([]oid.Address{obj.Address()})
					got, err := shs[2].Get(lock.Address(), false)
					require.NoError(t, err)
					require.Equal(t, lock.Marshal(), got.Marshal())
				}
			})
		}
	})
}

func TestGC(t *testing.T) {
	const numOfShards = 2

	dir := t.TempDir()
	es := &asyncEpochState{e: 10}
	synctest.Test(t, func(t *testing.T) {
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
		require.NoError(t, e.Init())
		t.Cleanup(func() { _ = e.Close() })

		cnr := cidtest.ID()

		// t.Run("expired object", func(t *testing.T) {
		obj := generateObjectWithCID(cnr)
		addExpirationAttribute(obj, es.CurrentEpoch())
		require.NoError(t, e.Put(context.Background(), obj, nil))

		tickEpoch(es, e)

		checkObjectsAsyncRemoval(t, e, cnr, obj.GetID())

		// t.Run("object with lock - prevents deletion until lock expires", func(t *testing.T) {
		obj = generateObjectWithCID(cnr)
		addExpirationAttribute(obj, es.CurrentEpoch())
		require.NoError(t, e.Put(context.Background(), obj, nil))
		objAddr := obj.Address()

		lockObj := generateObjectWithCID(cnr)
		lockObj.AssociateLocked(obj.GetID())
		addExpirationAttribute(lockObj, es.CurrentEpoch()+1) // lock expires after object
		require.NoError(t, e.Put(context.Background(), lockObj, nil))

		tickEpoch(es, e)

		// wait a bit to ensure GC cycle happened
		time.Sleep(200 * time.Millisecond)

		// object must stay because it's locked
		_, err := e.Get(context.Background(), objAddr)
		require.NoError(t, err)

		tickEpoch(es, e)

		// now GC must delete the object eventually
		checkObjectsAsyncRemoval(t, e, cnr, obj.GetID(), lockObj.GetID())

		// t.Run("expired tombstone removed", func(t *testing.T) {
		tomb := generateObjectWithCID(cnr)
		tomb.AssociateDeleted(oidtest.ID())
		addExpirationAttribute(tomb, es.CurrentEpoch())
		require.NoError(t, e.Put(context.Background(), tomb, nil))

		tickEpoch(es, e)

		checkObjectsAsyncRemoval(t, e, cnr, tomb.GetID())

		// t.Run("object inhume by tombstone", func(t *testing.T) {
		obj = generateObjectWithCID(cnr)
		require.NoError(t, e.Put(context.Background(), obj, nil))
		tomb = generateObjectWithCID(cnr)

		var a object.Attribute
		a.SetKey(object.AttributeExpirationEpoch) //nolint:staticcheck
		a.SetValue(strconv.Itoa(int(es.e)))
		tomb.SetAttributes(a)

		objAddr = obj.Address()
		tomb.AssociateDeleted(obj.GetID())

		require.NoError(t, e.Put(context.Background(), tomb, nil))

		_, err = e.Get(context.Background(), objAddr)
		require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)

		tickEpoch(es, e)

		checkObjectsAsyncRemoval(t, e, cnr, obj.GetID(), tomb.GetID())

		// t.Run("object associated with expired tombstone", func(t *testing.T) {
		obj = generateObjectWithCID(cnr)
		require.NoError(t, e.Put(context.Background(), obj, nil))
		tomb = generateObjectWithCID(cnr)
		tomb.AssociateDeleted(obj.GetID())
		addExpirationAttribute(tomb, es.CurrentEpoch())
		require.NoError(t, e.Put(context.Background(), tomb, nil))

		_, err = e.Get(context.Background(), obj.Address())
		require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)

		tickEpoch(es, e)

		checkObjectsAsyncRemoval(t, e, cnr, obj.GetID(), tomb.GetID())

		// t.Run("object with tombstone expires", func(t *testing.T) {
		obj = generateObjectWithCID(cnr)
		addExpirationAttribute(obj, es.CurrentEpoch())
		require.NoError(t, e.Put(context.Background(), obj, nil))
		objAddr = obj.Address()

		tomb = generateObjectWithCID(cnr)
		tomb.AssociateDeleted(obj.GetID())
		addExpirationAttribute(tomb, es.CurrentEpoch()+1) // tombstone expires after object
		require.NoError(t, e.Put(context.Background(), tomb, nil))

		tickEpoch(es, e)

		// wait a bit to ensure GC cycle happened
		time.Sleep(200 * time.Millisecond)

		// object covered by tombstone
		_, err = e.Get(context.Background(), objAddr)
		require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		_, err = e.Get(context.Background(), tomb.Address())
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
	synctest.Test(t, func(t *testing.T) {
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
			require.NoError(t, e.Put(context.Background(), children[i], nil))
		}

		_, err = e.Get(context.Background(), parentAddr)
		var splitErr *object.SplitInfoError
		require.ErrorAs(t, err, &splitErr)
		// Do not put link, so parent will have SplitInfo without link
		require.True(t, splitErr.SplitInfo().GetLink().IsZero())
		require.Equal(t, childIDs[len(childIDs)-1], splitErr.SplitInfo().GetLastPart())
		require.Equal(t, childIDs[0], splitErr.SplitInfo().GetFirstPart())

		tickEpoch(es, e)

		time.Sleep(1 * time.Second)
		// Check this way because Get returns ErrObjectNotFound for expired error
		for _, sh := range e.sortedShards(parentID) {
			_, err = sh.Get(parentAddr, false)
			require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
		}
	})
}

func TestSplitObjectExpirationWithLinkNotFound(t *testing.T) {
	dir := t.TempDir()
	es := &asyncEpochState{e: 10}
	l, logBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

	synctest.Test(t, func(t *testing.T) {
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
		require.NoError(t, e.Put(context.Background(), &linkObj, nil))
		for i := range children {
			require.NoError(t, e.Put(context.Background(), children[i], nil))
		}

		_, err = e.Get(context.Background(), linkAddr)
		require.NoError(t, err)

		_, err = e.Get(context.Background(), parentAddr)
		var splitErr *object.SplitInfoError
		require.ErrorAs(t, err, &splitErr)
		require.Equal(t, linkObj.GetID(), splitErr.SplitInfo().GetLink())
		require.Equal(t, childIDs[len(childIDs)-1], splitErr.SplitInfo().GetLastPart())
		require.Equal(t, childIDs[0], splitErr.SplitInfo().GetFirstPart())

		// Now delete the link object to simulate missing link scenario
		require.NoError(t, fstr.Delete(linkAddr))

		_, err = e.Get(context.Background(), linkAddr)
		require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

		_, err = e.Get(context.Background(), parentAddr)
		require.ErrorAs(t, err, &splitErr)
		require.Equal(t, linkObj.GetID(), splitErr.SplitInfo().GetLink()) // Pretends to be here, but not in fact.
		require.Equal(t, childIDs[len(childIDs)-1], splitErr.SplitInfo().GetLastPart())
		require.Equal(t, childIDs[0], splitErr.SplitInfo().GetFirstPart())

		tickEpoch(es, e)

		time.Sleep(1 * time.Second)
		// Check this way because Get returns ErrObjectNotFound for expired error
		for _, sh := range e.sortedShards(parentID) {
			_, err = sh.Get(parentAddr, false)
			require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
		}

		logBuf.AssertContainsMsg(zap.DebugLevel, "inhuming root object but no link object is found")
		logBuf.AssertContainsMsg(zap.InfoLevel, "root object has no link object in split upload")
	})
}

func addExpirationAttribute(obj *object.Object, epoch uint64) {
	addAttribute(obj, object.AttributeExpirationEpoch, fmt.Sprint(epoch)) //nolint:staticcheck
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
