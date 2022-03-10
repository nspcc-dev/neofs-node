package engine

import (
	"os"
	"strconv"
	"testing"
	"time"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/object/address"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/address/test"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

func TestLockUserScenario(t *testing.T) {
	t.Skip("posted bug neofs-node#1227")
	// Tested user actions:
	//   1. stores some object
	//   2. locks the object
	//   3. tries to inhume the object with tombstone and expects failure
	//   4. saves tombstone for LOCK-object and inhumes the LOCK-object using it
	//   5. waits for an epoch after the tombstone expiration one
	//   6. tries to inhume the object and expects success
	chEvents := make([]chan shard.Event, 2)

	for i := range chEvents {
		chEvents[i] = make(chan shard.Event, 1)
	}

	e := testEngineFromShardOpts(t, 2, func(i int) []shard.Option {
		return []shard.Option{
			shard.WithGCEventChannelInitializer(func() <-chan shard.Event {
				return chEvents[i]
			}),
			shard.WithGCWorkerPoolInitializer(func(sz int) util.WorkerPool {
				pool, err := ants.NewPool(sz)
				require.NoError(t, err)

				return pool
			}),
		}
	})

	t.Cleanup(func() {
		_ = e.Close()
		_ = os.RemoveAll(t.Name())
	})

	const lockerTombExpiresAfter = 13

	lockerID := oidtest.ID()
	tombForLockID := oidtest.ID()
	tombID := oidtest.ID()
	cnr := cidtest.ID()
	var err error

	var objAddr address.Address
	objAddr.SetContainerID(cnr)

	var tombAddr address.Address
	tombAddr.SetContainerID(cnr)
	tombAddr.SetObjectID(tombID)

	var lockerAddr address.Address
	lockerAddr.SetContainerID(cnr)
	lockerAddr.SetObjectID(lockerID)

	var tombForLockAddr address.Address
	tombForLockAddr.SetContainerID(cnr)
	tombForLockAddr.SetObjectID(tombForLockID)

	// 1.
	obj := generateObjectWithCID(t, cnr)

	objAddr.SetObjectID(obj.ID())

	err = Put(e, obj)
	require.NoError(t, err)

	// 2.
	err = e.Lock(*cnr, *lockerID, []oid.ID{*obj.ID()})
	require.NoError(t, err)

	// 3.
	_, err = e.Inhume(new(InhumePrm).WithTarget(&tombAddr, &objAddr))
	require.ErrorAs(t, err, new(apistatus.ObjectLocked))

	// 4.
	var a object.Attribute
	a.SetKey(objectV2.SysAttributeExpEpoch)
	a.SetValue(strconv.Itoa(lockerTombExpiresAfter))

	tombObj := generateObjectWithCID(t, cnr)
	tombObj.SetType(object.TypeTombstone)
	tombObj.SetID(tombForLockID)
	tombObj.SetAttributes(&a)

	err = Put(e, tombObj)
	require.NoError(t, err)

	_, err = e.Inhume(new(InhumePrm).WithTarget(&tombForLockAddr, &lockerAddr))
	require.NoError(t, err, new(apistatus.ObjectLocked))

	// 5.
	for i := range chEvents {
		chEvents[i] <- shard.EventNewEpoch(lockerTombExpiresAfter + 1)
	}

	// delay for GC
	time.Sleep(time.Second)

	_, err = e.Inhume(new(InhumePrm).WithTarget(&tombAddr, &objAddr))
	require.NoError(t, err)
}

func TestLockExpiration(t *testing.T) {
	// Tested scenario:
	//   1. some object is stored
	//   2. lock object for it is stored, and the object is locked
	//   3. lock expiration epoch is coming
	//   4. after some delay the object is not locked anymore
	chEvents := make([]chan shard.Event, 2)

	for i := range chEvents {
		chEvents[i] = make(chan shard.Event, 1)
	}

	e := testEngineFromShardOpts(t, 2, func(i int) []shard.Option {
		return []shard.Option{
			shard.WithGCEventChannelInitializer(func() <-chan shard.Event {
				return chEvents[i]
			}),
			shard.WithGCWorkerPoolInitializer(func(sz int) util.WorkerPool {
				pool, err := ants.NewPool(sz)
				require.NoError(t, err)

				return pool
			}),
		}
	})

	t.Cleanup(func() {
		_ = e.Close()
		_ = os.RemoveAll(t.Name())
	})

	const lockerExpiresAfter = 13

	cnr := cidtest.ID()
	var err error

	// 1.
	obj := generateObjectWithCID(t, cnr)

	err = Put(e, obj)
	require.NoError(t, err)

	// 2.
	var a object.Attribute
	a.SetKey(objectV2.SysAttributeExpEpoch)
	a.SetValue(strconv.Itoa(lockerExpiresAfter))

	lock := generateObjectWithCID(t, cnr)
	lock.SetType(object.TypeLock)
	lock.SetAttributes(&a)

	err = Put(e, lock)
	require.NoError(t, err)

	err = e.Lock(*cnr, *lock.ID(), []oid.ID{*obj.ID()})
	require.NoError(t, err)

	_, err = e.Inhume(new(InhumePrm).WithTarget(objecttest.Address(), objectcore.AddressOf(obj)))
	require.ErrorAs(t, err, new(apistatus.ObjectLocked))

	// 3.
	for i := range chEvents {
		chEvents[i] <- shard.EventNewEpoch(lockerExpiresAfter + 1)
	}

	// delay for GC processing. It can't be estimated, but making it bigger
	// will slow down test
	time.Sleep(time.Second)

	// 4.
	_, err = e.Inhume(new(InhumePrm).WithTarget(objecttest.Address(), objectcore.AddressOf(obj)))
	require.NoError(t, err)
}
