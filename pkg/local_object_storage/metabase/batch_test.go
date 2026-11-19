package meta

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

func TestTxDelay(t *testing.T) {
	for _, tc := range []struct {
		ema float64
		exp time.Duration
	}{
		{ema: -1, exp: minTxDelay},
		{ema: 0, exp: minTxDelay},
		{ema: 1000, exp: minTxDelay + 1900*time.Nanosecond},
		{ema: 5000, exp: minTxDelay + 1187500*time.Nanosecond},
		{ema: 9000, exp: minTxDelay + 12465900*time.Nanosecond},
		{ema: 10000, exp: maxTxDelay},
		{ema: 20000, exp: maxTxDelay},
	} {
		require.Equal(t, tc.exp, txDelay(tc.ema), tc.ema)
	}
}

func TestNextWritesEMA(t *testing.T) {
	for _, tc := range []struct {
		prev, writes, exp float64
	}{
		{prev: 0, writes: 0, exp: 0},
		{prev: 0, writes: 1000, exp: 300},
		{prev: 300, writes: 1000, exp: 510},
		{prev: 1000, writes: 0, exp: 700},
	} {
		require.InDelta(t, tc.exp, nextWritesEMA(tc.prev, tc.writes), 1e-9)
	}
}

func TestDB_Put_WriteController(t *testing.T) {
	t.Run("delay follows write rate", func(t *testing.T) {
		db := newDB(t)
		setPutTxDelay(db, minTxDelay)

		for range 20 {
			db.putCtrl.writes.Store(20000)
			db.tickWriteController()
		}
		require.EqualValues(t, maxTxDelay, db.putCtrl.txDelay.Load())

		require.NoError(t, db.Put(newBatchTestObject(0)))
		require.EqualValues(t, 1, db.putCtrl.writes.Load())

		for range 20 {
			db.tickWriteController()
		}
		require.EqualValues(t, minTxDelay, db.putCtrl.txDelay.Load())
	})

	t.Run("lone put does not wait for batch delay", func(t *testing.T) {
		db := newDB(t, WithMaxBatchDelay(time.Second))

		start := time.Now()
		require.NoError(t, db.Put(newBatchTestObject(0)))
		require.Less(t, time.Since(start), 500*time.Millisecond)
	})

	t.Run("concurrent puts share transactions", func(t *testing.T) {
		const batchSize, n = 10, 50
		db := newDB(t, WithMaxBatchSize(batchSize))
		setPutTxDelay(db, time.Hour)

		objs := make([]*object.Object, n)
		for i := range objs {
			objs[i] = newBatchTestObject(0)
		}
		errs := putConcurrently(db, objs)
		for i := range objs {
			require.NoError(t, errs[i])
			assertBatchTestObjectExists(t, db, objs[i], true)
		}

		s := putStats(db)
		require.EqualValues(t, n/batchSize, s.batches)
		require.EqualValues(t, n, s.objects)
	})

	t.Run("puts join pending batch during commit", func(t *testing.T) {
		const n = 5
		db := newDB(t)
		setPutTxDelay(db, minTxDelay)

		tx, err := db.boltDB.Begin(true)
		require.NoError(t, err)

		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := range n {
			wg.Go(func() { errs[i] = db.Put(newBatchTestObject(0)) })
			time.Sleep(5 * minTxDelay)
		}
		require.NoError(t, tx.Rollback())
		wg.Wait()
		for i := range errs {
			require.NoError(t, errs[i])
		}

		s := putStats(db)
		require.LessOrEqual(t, s.batches, uint64(2))
		require.EqualValues(t, n, s.objects)
	})

	t.Run("batch size limit", func(t *testing.T) {
		db := newDB(t)
		setPutTxDelay(db, 100*time.Millisecond)

		const attrLen = object.MaxHeaderLen - 1024
		objs := []*object.Object{newBatchTestObject(attrLen)}
		for range 2 * maxPutBatchBytes / objs[0].HeaderLen() {
			objs = append(objs, newBatchTestObject(attrLen))
		}

		errs := putConcurrently(db, objs)
		for i := range objs {
			require.NoError(t, errs[i])
		}

		s := putStats(db)
		require.GreaterOrEqual(t, s.batches, uint64(3))
		require.EqualValues(t, len(objs), s.objects)
	})

	t.Run("failed put does not affect others", func(t *testing.T) {
		const n = 10
		db := newDB(t, WithMaxBatchSize(n))
		setPutTxDelay(db, time.Hour)

		removed := newBatchTestObject(0)
		_, err := db.InhumeContainer(removed.GetContainerID())
		require.NoError(t, err)

		objs := make([]*object.Object, n)
		objs[0] = removed
		for i := 1; i < n; i++ {
			objs[i] = newBatchTestObject(0)
		}
		errs := putConcurrently(db, objs)

		require.ErrorIs(t, errs[0], apistatus.ErrObjectAlreadyRemoved)
		assertBatchTestObjectExists(t, db, removed, false)
		for i := 1; i < n; i++ {
			require.NoError(t, errs[i])
			assertBatchTestObjectExists(t, db, objs[i], true)
		}

		s := putStats(db)
		require.EqualValues(t, 1, s.batches)
		require.EqualValues(t, n-1, s.objects)
	})
}

// setPutTxDelay stops the write controller so that it does not change the
// given PUT transaction delay.
func setPutTxDelay(db *DB, d time.Duration) {
	db.stopWriteController()
	db.putCtrl.txDelay.Store(int64(d))
}

func putStats(db *DB) putBatchStats {
	db.putCtrl.mtx.Lock()
	defer db.putCtrl.mtx.Unlock()
	return db.putCtrl.stats
}

func putConcurrently(db *DB, objs []*object.Object) []error {
	errs := make([]error, len(objs))
	var wg sync.WaitGroup
	for i := range objs {
		wg.Go(func() { errs[i] = db.Put(objs[i]) })
	}
	wg.Wait()
	return errs
}

// newBatchTestObject returns a valid object with an attribute of attrLen
// bytes.
func newBatchTestObject(attrLen int) *object.Object {
	obj := new(object.Object)
	obj.SetContainerID(cidtest.ID())
	obj.SetID(oidtest.ID())
	obj.SetOwner(usertest.ID())
	obj.SetPayloadChecksum(checksum.NewSHA256([32]byte(testutil.RandByteSlice(32))))
	if attrLen > 0 {
		obj.SetAttributes(object.NewAttribute("attr", strings.Repeat("a", attrLen)))
	}
	return obj
}

func assertBatchTestObjectExists(t *testing.T, db *DB, obj *object.Object, exp bool) {
	exists, err := db.Exists(obj.Address(), false)
	if !exp && err != nil {
		return
	}
	require.NoError(t, err)
	require.Equal(t, exp, exists)
}
