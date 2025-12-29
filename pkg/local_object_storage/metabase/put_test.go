package meta_test

import (
	"crypto/sha256"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/internal/testutil"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/util/rand"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
)

func prepareObjects(t testing.TB, n int) []*object.Object {
	cnr := cidtest.ID()
	parentID := oidtest.ID()
	objs := make([]*object.Object, n)
	for i := range objs {
		objs[i] = generateObjectWithCID(t, cnr)

		// FKBT indices.
		attrs := make([]object.Attribute, 20)
		for j := range attrs {
			attrs[j].SetKey("abc" + strconv.FormatUint(rand.Uint64()%4, 16))
			attrs[j].SetValue("xyz" + strconv.FormatUint(rand.Uint64()%4, 16))
		}
		objs[i].SetAttributes(attrs...)

		// List indices.
		if i%2 == 0 {
			objs[i].SetParentID(parentID)
		}
	}
	return objs
}

func BenchmarkPut(b *testing.B) {
	b.Run("parallel", func(b *testing.B) {
		db := newDB(b,
			meta.WithMaxBatchDelay(time.Millisecond*10),
			meta.WithMaxBatchSize(runtime.NumCPU()))
		// Ensure the benchmark is bound by CPU and not waiting batch-delay time.
		b.SetParallelism(1)

		index := new(atomic.Int64)
		index.Store(-1)
		objs := prepareObjects(b, b.N)
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := metaPut(db, objs[index.Add(1)]); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
	b.Run("sequential", func(b *testing.B) {
		db := newDB(b,
			meta.WithMaxBatchDelay(time.Millisecond*10),
			meta.WithMaxBatchSize(1))
		index := new(atomic.Int64)
		index.Store(-1)
		objs := prepareObjects(b, b.N)
		b.ReportAllocs()
		for b.Loop() {
			if err := metaPut(db, objs[index.Add(1)]); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func metaPut(db *meta.DB, obj *object.Object) error {
	return db.Put(obj)
}

func TestDB_Put_ObjectWithTombstone(t *testing.T) {
	db := newDB(t)

	var obj object.Object
	ver := version.Current()
	obj.SetVersion(&ver)
	var cnr = cidtest.ID()
	obj.SetContainerID(cnr)
	obj.SetID(oidtest.ID())
	obj.SetOwner(usertest.ID())
	obj.SetPayloadChecksum(checksum.NewSHA256([32]byte(testutil.RandByteSlice(32))))

	ts := obj
	ts.SetID(oidtest.OtherID(obj.GetID()))
	ts.AssociateDeleted(obj.GetID())

	addr := oid.NewAddress(obj.GetContainerID(), obj.GetID())
	tsAddr := oid.NewAddress(ts.GetContainerID(), ts.GetID())

	require.NoError(t, db.Put(&obj))

	t.Run("before tombstone", func(t *testing.T) {
		assertObjectAvailability(t, db, addr, obj)
	})

	require.NoError(t, db.Put(&ts))

	t.Run("with tombstone", func(t *testing.T) {
		t.Run("get", func(t *testing.T) {
			_, err := db.Get(addr, false)
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		})
		t.Run("exists", func(t *testing.T) {
			_, err := db.Exists(addr, true)
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		})
		t.Run("status", func(t *testing.T) {
			st, err := db.ObjectStatus(addr)
			require.NoError(t, err)
			require.NoError(t, st.Error)
			require.ElementsMatch(t, st.State, []string{"AVAILABLE", "IN GRAVEYARD"})
		})
		t.Run("list with cursor", func(t *testing.T) {
			res, c, err := db.ListWithCursor(2, nil)
			require.NoError(t, err)
			require.Equal(t, []objectcore.AddressWithAttributes{{Address: tsAddr, Type: object.TypeTombstone}}, res)

			require.NotZero(t, c)
			_, _, err = db.ListWithCursor(1, c)
			require.ErrorIs(t, err, meta.ErrEndOfListing)
		})
		t.Run("get garbage", func(t *testing.T) {
			gObjs, gCnrs, err := db.GetGarbage(100)
			require.NoError(t, err)
			require.Empty(t, gCnrs)
			require.Equal(t, []oid.Address{addr}, gObjs)
		})
		t.Run("iterate garbage", func(t *testing.T) {
			var collected []oid.Address
			err := db.IterateOverGarbage(func(id oid.ID) error {
				collected = append(collected, oid.NewAddress(cnr, id))
				return nil
			}, cnr, oid.ID{})
			require.NoError(t, err)
			require.Equal(t, []oid.Address{addr}, collected)
		})
		t.Run("mark garbage", func(t *testing.T) {
			// already garbage, should not do anything
			n, locks, err := db.MarkGarbage(addr)
			require.NoError(t, err)
			require.Empty(t, locks)
			require.Zero(t, n)
		})
	})

	rs, err := db.ReviveObject(addr)
	require.NoError(t, err)
	require.Equal(t, meta.ReviveStatusGraveyard, rs.StatusType())
	require.Contains(t, rs.Message(), "successful revival from graveyard")
	require.Equal(t, tsAddr, rs.TombstoneAddress())

	t.Run("after revival", func(t *testing.T) {
		// tombstone is deleted and the garbage was cleared
		t.Run("get garbage", func(t *testing.T) {
			gObjs, _, err := db.GetGarbage(100)
			require.NoError(t, err)
			require.NotContains(t, gObjs, addr)
		})
		t.Run("iterate garbage", func(t *testing.T) {
			var collected []oid.Address
			err := db.IterateOverGarbage(func(id oid.ID) error {
				collected = append(collected, oid.NewAddress(cnr, id))
				return nil
			}, cnr, oid.ID{})
			require.NoError(t, err)
			require.NotContains(t, collected, addr)
		})
		_, err = db.Delete([]oid.Address{tsAddr})
		require.NoError(t, err)

		assertObjectAvailability(t, db, addr, obj)
	})
}

func assertObjectAvailability(t *testing.T, db *meta.DB, addr oid.Address, obj object.Object) {
	t.Run("get", func(t *testing.T) {
		res, err := db.Get(addr, false)
		require.NoError(t, err)
		require.Equal(t, obj, *res)
	})
	t.Run("exists", func(t *testing.T) {
		exists, err := db.Exists(addr, true)
		require.NoError(t, err)
		require.True(t, exists)
	})
	t.Run("status", func(t *testing.T) {
		st, err := db.ObjectStatus(addr)
		require.NoError(t, err)
		require.NoError(t, st.Error)
		require.ElementsMatch(t, st.State, []string{"AVAILABLE"})
	})
	t.Run("list with cursor", func(t *testing.T) {
		res, c, err := db.ListWithCursor(2, nil)
		require.NoError(t, err)
		require.Equal(t, []objectcore.AddressWithAttributes{{Address: addr, Type: obj.Type()}}, res)

		require.NotZero(t, c)
		_, _, err = db.ListWithCursor(1, c)
		require.ErrorIs(t, err, meta.ErrEndOfListing)
	})
	t.Run("get garbage", func(t *testing.T) {
		gObjs, _, err := db.GetGarbage(100)
		require.NoError(t, err)
		require.NotContains(t, gObjs, addr)
	})
	t.Run("iterate garbage", func(t *testing.T) {
		var collected []oid.Address
		err := db.IterateOverGarbage(func(id oid.ID) error {
			collected = append(collected, oid.NewAddress(obj.GetContainerID(), id))
			return nil
		}, obj.GetContainerID(), oid.ID{})
		require.NoError(t, err)
		require.NotContains(t, collected, addr)
	})
}

func TestDB_Put_Lock(t *testing.T) {
	var obj object.Object
	ver := version.Current()
	obj.SetVersion(&ver)
	obj.SetContainerID(cidtest.ID())
	obj.SetID(oidtest.ID())
	obj.SetOwner(usertest.ID())
	obj.SetPayloadChecksum(checksum.NewSHA256([32]byte(testutil.RandByteSlice(32))))

	objID := obj.GetID()
	objAddr := oid.NewAddress(obj.GetContainerID(), objID)

	lock := obj
	lock.SetID(oidtest.OtherID(objID))
	lock.AssociateLocked(objID)

	lockAddr := oid.NewAddress(lock.GetContainerID(), lock.GetID())

	tomb := obj
	tomb.SetID(oidtest.OtherID(objID, lock.GetID()))
	tomb.AssociateDeleted(objID)

	t.Run("non-regular target", func(t *testing.T) {
		for _, typ := range []object.Type{
			object.TypeTombstone,
			object.TypeLock,
			object.TypeLink,
		} {
			db := newDB(t)

			obj := obj
			obj.SetType(typ)

			require.NoError(t, db.Put(&obj))

			require.ErrorIs(t, db.Put(&lock), apistatus.ErrLockNonRegularObject)

			locked, err := db.IsLocked(objAddr)
			require.NoError(t, err)
			require.False(t, locked)

			exists, err := db.Exists(lockAddr, false)
			require.NoError(t, err)
			require.False(t, exists)

			_, err = db.Get(lockAddr, false)
			require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
		}
	})

	for _, tc := range []struct {
		name         string
		preset       func(*testing.T, *meta.DB)
		assertPutErr func(t *testing.T, err error)
	}{
		{name: "no target", preset: func(t *testing.T, db *meta.DB) {}},
		{name: "with target", preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&obj))
		}},
		{name: "with target and tombstone", preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&obj))
			require.NoError(t, db.Put(&tomb))
		}, assertPutErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		}},
		{name: "tombstone without target", preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&tomb))
		}, assertPutErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		}},
		{name: "with target and GC mark", preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&obj))

			n, _, err := db.MarkGarbage(objAddr)
			require.NoError(t, err)
			require.EqualValues(t, 1, n)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newDB(t)

			tc.preset(t, db)

			putErr := db.Put(&lock)
			locked, lockedErr := db.IsLocked(objAddr)
			exists, existsErr := db.Exists(lockAddr, false)
			got, getErr := db.Get(lockAddr, false)

			if tc.assertPutErr != nil {
				tc.assertPutErr(t, putErr)

				require.NoError(t, lockedErr)
				require.False(t, locked)

				require.NoError(t, existsErr)
				require.False(t, exists)

				require.ErrorIs(t, getErr, apistatus.ErrObjectNotFound)
			} else {
				require.NoError(t, putErr)

				require.NoError(t, lockedErr)
				require.True(t, locked)

				require.NoError(t, existsErr)
				require.True(t, exists)

				require.NoError(t, getErr)
				require.Equal(t, lock, *got)
			}
		})
	}
}

func TestDB_Put_Tombstone(t *testing.T) {
	var obj object.Object
	ver := version.Current()
	obj.SetVersion(&ver)
	obj.SetContainerID(cidtest.ID())
	obj.SetID(oidtest.ID())
	obj.SetOwner(usertest.ID())
	obj.SetPayloadChecksum(checksum.NewSHA256([32]byte(testutil.RandByteSlice(32))))

	objID := obj.GetID()
	objAddr := oid.NewAddress(obj.GetContainerID(), objID)

	lock := obj
	lock.SetID(oidtest.OtherID(objID))
	lock.AssociateLocked(objID)

	tomb := obj
	tomb.SetID(oidtest.OtherID(objID, lock.GetID()))
	tomb.AssociateDeleted(objID)

	tombAddr := oid.NewAddress(tomb.GetContainerID(), tomb.GetID())

	for _, tc := range []struct {
		name         string
		preset       func(*testing.T, *meta.DB)
		assertPutErr func(t *testing.T, err error)
	}{
		{name: "no target", preset: func(t *testing.T, db *meta.DB) {}},
		{name: "with target", preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&obj))
		}},
		{name: "with target and lock", preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&obj))
			require.NoError(t, db.Put(&lock))
		}, assertPutErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectLocked)
		}},
		{name: "lock without target", preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&lock))
		}, assertPutErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectLocked)
		}},
		{name: "target is lock", preset: func(t *testing.T, db *meta.DB) {
			obj := obj
			obj.SetType(object.TypeLock)
			require.NoError(t, db.Put(&obj))
		}, assertPutErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, meta.ErrLockObjectRemoval)
		}},
		{name: "target is tombstone", preset: func(t *testing.T, db *meta.DB) {
			obj := obj
			obj.SetAttributes(
				object.NewAttribute("__NEOFS__EXPIRATION_EPOCH", strconv.Itoa(100)),
			)
			obj.AssociateDeleted(oidtest.ID())
			require.NoError(t, db.Put(&obj))
		}, assertPutErr: func(t *testing.T, err error) {
			require.ErrorContains(t, err, "TS's target is another TS")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newDB(t)

			tc.preset(t, db)

			putTombErr := db.Put(&tomb)
			tombExists, tombExistsErr := db.Exists(tombAddr, false)
			gotTomb, getTombErr := db.Get(tombAddr, false)
			_, objExistsErr := db.Exists(objAddr, false)
			_, getObjErr := db.Get(objAddr, false)

			if tc.assertPutErr != nil {
				tc.assertPutErr(t, putTombErr)

				require.NoError(t, tombExistsErr)
				require.False(t, tombExists)

				require.ErrorIs(t, getTombErr, apistatus.ErrObjectNotFound)

				require.NotErrorIs(t, objExistsErr, apistatus.ErrObjectAlreadyRemoved)
				require.NotErrorIs(t, getObjErr, apistatus.ErrObjectAlreadyRemoved)
			} else {
				require.NoError(t, putTombErr)

				require.NoError(t, tombExistsErr)
				require.True(t, tombExists)

				require.NoError(t, getTombErr)
				require.Equal(t, tomb, *gotTomb)

				require.ErrorIs(t, objExistsErr, apistatus.ErrObjectAlreadyRemoved)
				require.ErrorIs(t, getObjErr, apistatus.ErrObjectAlreadyRemoved)
			}
		})
	}
}

func createTSForObject(cnr cid.ID, id oid.ID) *object.Object {
	var ts = &object.Object{}
	ts.SetContainerID(cnr)
	ts.SetOwner(usertest.ID())
	ts.SetID(oidtest.ID())
	ts.SetPayloadChecksum(checksum.NewSHA256(sha256.Sum256(ts.Payload())))
	ts.AssociateDeleted(id)
	return ts
}
