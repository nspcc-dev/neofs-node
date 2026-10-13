package meta_test

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/internal/chaintime"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestDB_IterateExpired(t *testing.T) {
	db := newDB(t)

	const epoch = 13

	mAlive := map[object.Type]oid.Address{}
	mExpired := map[object.Type]oid.Address{}

	for _, typ := range []object.Type{
		object.TypeRegular,
		object.TypeTombstone,
		object.TypeLock,
	} {
		mAlive[typ] = putWithExpiration(t, db, typ, epoch)
		mExpired[typ] = putWithExpiration(t, db, typ, epoch-1)
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

			locked, err := db.IsLocked(testObj.Address())
			require.NoError(t, err)
			require.Equal(t, tc.locked, locked)
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
