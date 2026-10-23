package meta_test

import (
	"errors"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/internal/chaintime"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestDB_IterateExpired(t *testing.T) {
	db := newDB(t)

	const epoch uint64 = 13

	mAlive := map[object.Type]oid.Address{}
	mExpired := map[object.Type]oid.Address{}

	for _, typ := range []object.Type{
		object.TypeRegular,
		object.TypeTombstone,
		object.TypeLock,
	} {
		mAlive[typ] = putWithExpiration(t, db, typ, epoch)
		expires := epoch - 1
		if typ == object.TypeTombstone {
			expires-- // Tombstones are retained for one additional epoch.
		}
		mExpired[typ] = putWithExpiration(t, db, typ, expires)
	}

	expiredLocked := putWithExpiration(t, db, object.TypeRegular, epoch-1)

	locker := generateObjectWithCID(t, expiredLocked.Container())
	locker.AssociateLocked(expiredLocked.Object())
	require.NoError(t, db.Put(locker))

	gcExpired := generateObjectWithCID(t, cidtest.ID())
	gcExpired.SetType(object.TypeRegular)
	addAttribute(gcExpired, object.AttributeExpirationEpoch, strconv.FormatUint(epoch-1, 10)) //nolint:staticcheck
	require.NoError(t, putBig(db, gcExpired))
	_, err := db.InhumeContainer(gcExpired.GetContainerID())
	require.NoError(t, err)

	handler := func(addr oid.Address, typ object.Type) error {
		if alive, ok := mAlive[typ]; ok {
			require.NotEqual(t, alive, addr)
		}

		require.NotEqual(t, expiredLocked, addr)
		require.NotEqual(t, gcExpired.Address(), addr)

		exp, ok := mExpired[typ]
		require.True(t, ok)
		require.Equal(t, exp, addr)

		delete(mExpired, typ)

		return nil
	}
	err = db.IterateExpired(epoch, handler)
	require.NoError(t, err)

	require.Empty(t, mExpired)
}

func TestDB_IterateExpired_EpochGrace(t *testing.T) {
	for _, tc := range []struct {
		name      string
		epoch     uint64
		expires   uint64
		tombstone bool
		collect   bool
	}{
		{name: "epoch zero", epoch: 0, expires: 0},
		{name: "first expired epoch", epoch: 1, expires: 0},
		{name: "after grace epoch", epoch: 2, expires: 0, collect: true},
		{name: "not expired", epoch: 10, expires: 10},
		{name: "grace epoch", epoch: 10, expires: 9},
		{name: "grace elapsed", epoch: 10, expires: 8, collect: true},
		{name: "maximum expiration", epoch: math.MaxUint64, expires: math.MaxUint64},
		{name: "maximum epoch grace", epoch: math.MaxUint64, expires: math.MaxUint64 - 1},
		{name: "maximum epoch collected", epoch: math.MaxUint64, expires: math.MaxUint64 - 2, collect: true},
		{name: "tombstone first expired epoch", epoch: 11, expires: 10, tombstone: true},
		{name: "tombstone grace elapsed", epoch: 12, expires: 10, tombstone: true, collect: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			epoch := &epochState{}
			db := newDB(t, meta.WithEpochState(epoch))
			var addr oid.Address
			if tc.tombstone {
				addr = putWithExpiration(t, db, object.TypeTombstone, tc.expires)
			} else {
				addr = putSplitWithExpiration(t, db, tc.expires)
			}
			physical := putWithExpiration(t, db, object.TypeRegular, tc.expires)
			epoch.e = tc.epoch
			var collected []oid.Address
			require.NoError(t, db.IterateExpired(tc.epoch, func(a oid.Address, _ object.Type) error {
				collected = append(collected, a)
				return nil
			}))
			var expected []oid.Address
			if tc.collect {
				expected = append(expected, addr)
			}
			if tc.epoch > tc.expires {
				expected = append(expected, physical)
			}
			require.ElementsMatch(t, expected, collected, "virtual parents and tombstones receive a grace epoch")
			if tc.epoch > tc.expires && !tc.tombstone {
				_, err := db.Get(addr, false)
				require.ErrorIs(t, err, meta.ErrObjectIsExpired, "grace must not extend logical availability")
			}
		})
	}
}

func TestDB_IterateExpired_PhysicalPart(t *testing.T) {
	for _, tc := range []struct {
		name         string
		parentExp    bool
		locked       bool
		expectChild  bool
		expectParent bool
	}{
		{name: "unexpired parent", parentExp: true},
		{name: "expired locked parent", parentExp: true, locked: true},
		{name: "expired unlocked parent", parentExp: true, expectParent: true},
		{name: "parent without expiration", expectChild: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			epoch := &epochState{}
			db := newDB(t, meta.WithEpochState(epoch))
			parent := generateObject(t)
			if tc.parentExp {
				parent.SetExpirationEpoch(10) //nolint:staticcheck // Cover legacy parent expiration.
			}
			child := generateObjectWithCID(t, parent.GetContainerID())
			child.SetParent(parent)
			child.SetParentID(parent.GetID())
			child.SetExpirationEpoch(1) //nolint:staticcheck // Cover early physical-part expiration.
			require.NoError(t, db.Put(child))
			if tc.locked {
				lock := generateObjectWithCID(t, parent.GetContainerID())
				lock.AssociateLocked(parent.GetID())
				lock.SetExpirationEpoch(30) //nolint:staticcheck // Active LOCK outlives the parent's grace period.
				require.NoError(t, db.Put(lock))
			}
			if tc.locked || tc.expectParent {
				epoch.e = 20
			} else {
				epoch.e = 5
			}
			var collected []oid.Address
			require.NoError(t, db.IterateExpired(epoch.e, func(addr oid.Address, _ object.Type) error {
				collected = append(collected, addr)
				return nil
			}))
			var expected []oid.Address
			if tc.expectChild {
				expected = append(expected, child.Address())
			}
			if tc.expectParent {
				expected = append(expected, parent.Address())
			}
			require.ElementsMatch(t, expected, collected)
		})
	}
}

func TestDB_IterateExpired_TimeGrace(t *testing.T) {
	for _, tc := range []struct {
		name    string
		now     uint64 // Chain time in milliseconds.
		collect bool
		restart bool
	}{
		{name: "before expiration", now: 99000},
		{name: "at expiration", now: 100000},
		{name: "last valid millisecond", now: 100999},
		{name: "first expired second", now: 101000},
		{name: "grace boundary", now: 700000},
		{name: "last grace millisecond", now: 700999},
		{name: "grace elapsed", now: 701000, collect: true},
		{name: "restart during grace", now: 105000, restart: true},
		{name: "restart after grace", now: 701000, collect: true, restart: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var clock chaintime.AtomicChainTimeProvider
			clock.Set(99000)
			db := newDB(t, meta.WithTimeSource(clock.Now))
			parent := putTimeSplitWithExpiration(t, db, 100)
			var physical []oid.Address
			for _, typ := range []object.Type{object.TypeRegular, object.TypeLock, object.TypeTombstone, object.TypeLink} {
				obj := generateTypedObject(t, typ, 0)
				require.NoError(t, obj.SetExpirationTime(time.Unix(100, 0)))
				require.NoError(t, db.Put(obj))
				physical = append(physical, obj.Address())
			}
			clock.Set(tc.now)
			if tc.restart {
				require.NoError(t, db.Close())
				require.NoError(t, db.Open(false))
				require.NoError(t, db.Init(blobstor.ID{}))
			}
			var collected []oid.Address
			require.NoError(t, db.IterateExpired(0, func(addr oid.Address, _ object.Type) error {
				collected = append(collected, addr)
				return nil
			}))
			var expected []oid.Address
			if tc.collect {
				expected = append(expected, parent)
			}
			if tc.now/1000 > 100 {
				for i, addr := range physical {
					if tc.now > 700999 ||
						[]object.Type{object.TypeRegular, object.TypeLock, object.TypeTombstone, object.TypeLink}[i] != object.TypeTombstone {
						expected = append(expected, addr)
					}
				}
				_, err := db.Get(parent, false)
				require.ErrorIs(t, err, meta.ErrObjectIsExpired, "GC grace must not extend logical availability")
			}
			require.ElementsMatch(t, expected, collected)
		})
	}
}

func TestDB_IterateExpired_TimeGrace_LockExpiration(t *testing.T) {
	var clock chaintime.AtomicChainTimeProvider
	clock.Set(99000)
	db := newDB(t, meta.WithTimeSource(clock.Now))
	parent := putTimeSplitWithExpiration(t, db, 100)
	lock := generateObjectWithCID(t, parent.Container())
	lock.AssociateLocked(parent.Object())
	require.NoError(t, lock.SetExpirationTime(time.Unix(105, 0)))
	require.NoError(t, db.Put(lock))
	for _, now := range []uint64{104999, 106000, 701000} {
		clock.Set(now)
		lockID, err := db.GetLockID(parent)
		require.NoError(t, err)
		_, err = db.Get(parent, false)
		if now < 106000 {
			require.Equal(t, lock.GetID(), lockID)
			require.NoError(t, err)
		} else {
			require.True(t, lockID.IsZero())
			require.ErrorIs(t, err, meta.ErrObjectIsExpired)
		}
		var collected []oid.Address
		require.NoError(t, db.IterateExpired(0, func(addr oid.Address, _ object.Type) error {
			collected = append(collected, addr)
			return nil
		}))
		var expected []oid.Address
		if now >= 106000 {
			expected = append(expected, lock.Address())
		}
		if now >= 701000 {
			expected = append(expected, parent)
		}
		require.ElementsMatch(t, expected, collected)
	}
}

func TestDB_IterateExpired_EpochGrace_Locks(t *testing.T) {
	for _, tc := range []struct {
		name       string
		gcEpoch    uint64
		stateEpoch uint64
		lockExpiry uint64
		collect    bool
	}{
		{name: "live lock", gcEpoch: 13, stateEpoch: 13, lockExpiry: 13},
		{name: "expired lock", gcEpoch: 13, stateEpoch: 13, lockExpiry: 12, collect: true},
		{name: "epoch state lags GC", gcEpoch: 13, stateEpoch: 0, lockExpiry: 12, collect: true},
		{name: "GC lags epoch state", gcEpoch: 13, stateEpoch: 14, lockExpiry: 13, collect: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			epoch := &epochState{}
			db := newDB(t, meta.WithEpochState(epoch))
			addr := putSplitWithExpiration(t, db, 11)
			lock := generateObjectWithCID(t, addr.Container())
			lock.AssociateLocked(addr.Object())
			lock.SetExpirationEpoch(tc.lockExpiry) //nolint:staticcheck // Cover legacy LOCKs during the grace epoch.
			require.NoError(t, db.Put(lock))
			epoch.e = tc.stateEpoch
			var collected []oid.Address
			require.NoError(t, db.IterateExpired(tc.gcEpoch, func(a oid.Address, _ object.Type) error {
				collected = append(collected, a)
				return nil
			}))
			var expected []oid.Address
			if tc.collect {
				expected = append(expected, addr)
			}
			if tc.gcEpoch > tc.lockExpiry {
				expected = append(expected, lock.Address())
			}
			require.ElementsMatch(t, expected, collected)
		})
	}
}

func TestDB_IterateExpired_StaleEpoch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		epoch  uint64
		locked bool
	}{
		{name: "live epoch lock", epoch: 2, locked: true},
		{name: "expired epoch lock", epoch: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var clock chaintime.AtomicChainTimeProvider
			clock.Set(99000)
			db := newDB(t, meta.WithEpochState(epochState{e: tc.epoch}), meta.WithTimeSource(clock.Now))
			testObj := generateObject(t)
			require.NoError(t, testObj.SetExpirationTime(time.Unix(100, 0)))
			require.NoError(t, db.Put(testObj))
			lock := generateObjectWithCID(t, testObj.GetContainerID())
			lock.AssociateLocked(testObj.GetID())
			lock.SetExpirationEpoch(2) //nolint:staticcheck // Test an epoch-based LOCK with time-based expiration.
			require.NoError(t, db.Put(lock))
			clock.Set(101000)

			lockID, err := db.GetLockID(testObj.Address())
			require.NoError(t, err)
			if tc.locked {
				require.Equal(t, lock.GetID(), lockID)
			} else {
				require.True(t, lockID.IsZero())
			}
			var expired []oid.Address
			handler := func(addr oid.Address, _ object.Type) error {
				expired = append(expired, addr)
				return nil
			}
			// The GC epoch is still zero before its first epoch notification.
			require.NoError(t, db.IterateExpired(0, handler))
			if tc.locked {
				require.Empty(t, expired)
			} else {
				require.Equal(t, []oid.Address{testObj.Address()}, expired)
			}
		})
	}
}

func TestDB_IterateExpired_SharedBatchLimit(t *testing.T) {
	for _, tc := range []struct {
		name      string
		batchSize int
		epochObjs int
		timeObjs  int
	}{
		{name: "both full", batchSize: 2, epochObjs: 3, timeObjs: 3},
		{name: "epoch empty", batchSize: 2, timeObjs: 3},
		{name: "time empty", batchSize: 2, epochObjs: 3},
		{name: "epoch partial", batchSize: 2, epochObjs: 1, timeObjs: 3},
		{name: "time partial", batchSize: 2, epochObjs: 3, timeObjs: 1},
		{name: "unlimited", epochObjs: 3, timeObjs: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var clock chaintime.AtomicChainTimeProvider
			clock.Set(99000)
			db := newDB(t, meta.WithEpochState(epochState{e: 3}), meta.WithTimeSource(clock.Now))
			isTimeExpired := make(map[oid.Address]bool)
			for i := range tc.epochObjs + tc.timeObjs {
				// Different containers ensure the limit applies across both indexes
				// and all container buckets.
				testObj := generateObject(t)
				byTime := i >= tc.epochObjs
				if byTime {
					require.NoError(t, testObj.SetExpirationTime(time.Unix(100, 0)))
				} else {
					testObj.SetExpirationEpoch(1) //nolint:staticcheck // Test the shared expiration batch limit.
				}
				require.NoError(t, db.Put(testObj))
				isTimeExpired[testObj.Address()] = byTime
			}
			clock.Set(101000)
			var epochCount, timeCount int
			seen := make(map[oid.Address]bool)
			handler := func(addr oid.Address, typ object.Type) error {
				byTime, ok := isTimeExpired[addr]
				require.True(t, ok)
				require.Equal(t, object.TypeRegular, typ)
				require.False(t, seen[addr])
				seen[addr] = true
				if byTime {
					timeCount++
				} else {
					epochCount++
				}
				if tc.batchSize > 0 && len(seen) >= tc.batchSize {
					return meta.ErrInterruptIterator
				}
				return nil
			}
			require.NoError(t, db.IterateExpired(3, handler))
			wantEpoch, wantTime := tc.epochObjs, tc.timeObjs
			if tc.batchSize > 0 {
				wantEpoch = min(wantEpoch, tc.batchSize)
				wantTime = min(wantTime, tc.batchSize-wantEpoch)
			}
			require.Equal(t, wantEpoch, epochCount)
			require.Equal(t, wantTime, timeCount)
		})
	}
}

func TestDB_IterateExpired_CallbackError(t *testing.T) {
	var clock chaintime.AtomicChainTimeProvider
	clock.Set(99000)
	db := newDB(t, meta.WithEpochState(epochState{e: 3}), meta.WithTimeSource(clock.Now))
	epochAddr := putWithExpiration(t, db, object.TypeRegular, 1)
	for range 2 {
		testObj := generateObject(t)
		require.NoError(t, testObj.SetExpirationTime(time.Unix(100, 0)))
		require.NoError(t, db.Put(testObj))
	}
	clock.Set(101000)
	handlerErr := errors.New("handler failure")
	for _, tc := range []struct {
		name          string
		epochErr      error
		timeErr       error
		wantErr       error
		wantTimeCalls int
	}{
		{name: "continue both", wantTimeCalls: 2},
		{name: "interrupt epoch", epochErr: meta.ErrInterruptIterator},
		{name: "wrapped interrupt epoch", epochErr: errors.Join(meta.ErrInterruptIterator, handlerErr)},
		{name: "error epoch", epochErr: handlerErr, wantErr: handlerErr},
		{name: "interrupt time", timeErr: meta.ErrInterruptIterator, wantTimeCalls: 1},
		{name: "wrapped interrupt time", timeErr: errors.Join(meta.ErrInterruptIterator, handlerErr), wantTimeCalls: 1},
		{name: "error time", timeErr: handlerErr, wantErr: handlerErr, wantTimeCalls: 1},
		{name: "interrupt both", epochErr: meta.ErrInterruptIterator, timeErr: meta.ErrInterruptIterator},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var epochCalls, timeCalls int
			err := db.IterateExpired(3, func(addr oid.Address, _ object.Type) error {
				if addr == epochAddr {
					epochCalls++
					return tc.epochErr
				}
				timeCalls++
				return tc.timeErr
			})
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, 1, epochCalls)
			require.Equal(t, tc.wantTimeCalls, timeCalls)
		})
	}
}

func putSplitWithExpiration(t *testing.T, db *meta.DB, expiresAt uint64) oid.Address {
	t.Helper()
	parent := generateObject(t)
	parent.SetExpirationEpoch(expiresAt) //nolint:staticcheck // Exercise epoch-based virtual-parent GC.
	return putSplitParent(t, db, parent)
}

func putTimeSplitWithExpiration(t *testing.T, db *meta.DB, expiresAt int64) oid.Address {
	t.Helper()
	parent := generateObject(t)
	require.NoError(t, parent.SetExpirationTime(time.Unix(expiresAt, 0)))
	return putSplitParent(t, db, parent)
}

func putSplitParent(t *testing.T, db *meta.DB, parent *object.Object) oid.Address {
	t.Helper()
	first := generateObjectWithCID(t, parent.GetContainerID())
	first.SetParent(parent)
	last := generateObjectWithCID(t, parent.GetContainerID())
	last.SetFirstID(first.GetID())
	last.SetPreviousID(first.GetID())
	last.SetParent(parent)
	last.SetParentID(parent.GetID())
	require.NoError(t, db.Put(first))
	require.NoError(t, db.Put(last))
	return parent.Address()
}

func putWithExpiration(t *testing.T, db *meta.DB, typ object.Type, expiresAt uint64) oid.Address {
	obj := generateObject(t)
	obj.SetType(typ)
	switch typ {
	case object.TypeTombstone:
		obj.AssociateDeleted(oidtest.ID())
	case object.TypeLock:
		obj.AssociateLocked(oidtest.ID())
	default:
	}
	addAttribute(obj, object.AttributeExpirationEpoch, strconv.FormatUint(expiresAt, 10)) //nolint:staticcheck

	require.NoError(t, putBig(db, obj))

	return obj.Address()
}
