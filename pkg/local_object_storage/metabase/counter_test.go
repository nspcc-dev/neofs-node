package meta_test

import (
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

const objCount = 10

func TestCounters(t *testing.T) {
	db := newDB(t)

	var c meta.ObjectCounters
	var err error

	t.Run("defaults", func(t *testing.T) {
		c, err = db.ObjectCounters()
		require.NoError(t, err)
		require.Zero(t, c.Phy())
		require.Zero(t, c.Logic())
	})

	t.Run("put", func(t *testing.T) {
		oo := make([]*object.Object, 0, objCount)
		for range objCount {
			oo = append(oo, generateObject(t))
		}

		for i := range objCount {
			err = db.Put(oo[i])
			require.NoError(t, err)

			c, err = db.ObjectCounters()
			require.NoError(t, err)

			require.Equal(t, uint64(i+1), c.Phy())
			require.Equal(t, uint64(i+1), c.Logic())
		}
	})

	require.NoError(t, db.Reset())

	t.Run("delete", func(t *testing.T) {
		oo := putObjs(t, db, objCount, false)

		for i := objCount - 1; i >= 0; i-- {
			res, err := db.Delete([]oid.Address{objectcore.AddressOf(oo[i])})
			require.NoError(t, err)
			require.Equal(t, uint64(1), res.AvailableRemoved)

			c, err = db.ObjectCounters()
			require.NoError(t, err)

			require.Equal(t, uint64(i), c.Phy())
			require.Equal(t, uint64(i), c.Logic())
		}
	})

	require.NoError(t, db.Reset())

	t.Run("inhume", func(t *testing.T) {
		oo := putObjs(t, db, objCount, false)

		inhumedObjs := make([]oid.Address, objCount/2)

		for i, o := range oo {
			if i == len(inhumedObjs) {
				break
			}

			inhumedObjs[i] = objectcore.AddressOf(o)
		}

		for _, addr := range inhumedObjs {
			err := db.Put(createTSForObject(addr.Container(), addr.Object()))
			require.NoError(t, err)
		}

		c, err = db.ObjectCounters()
		require.NoError(t, err)

		require.Equal(t, uint64(objCount+len(inhumedObjs)), c.Phy())
		require.Equal(t, uint64(objCount-len(inhumedObjs)), c.Logic())
	})

	require.NoError(t, db.Reset())

	t.Run("put_split", func(t *testing.T) {
		parObj := generateObject(t)

		// put objects and check that parent info
		// does not affect the counter
		for i := range objCount {
			o := generateObject(t)
			if i < objCount/2 { // half of the objs will have the parent
				o.SetParent(parObj)
			}

			require.NoError(t, putBig(db, o))

			c, err = db.ObjectCounters()
			require.NoError(t, err)
			require.Equal(t, uint64(i+1), c.Phy())
			require.Equal(t, uint64(i+1), c.Logic())
		}
	})

	require.NoError(t, db.Reset())

	t.Run("delete_split", func(t *testing.T) {
		oo := putObjs(t, db, objCount, true)

		// delete objects that have parent info
		// and check that it does not affect
		// the counter
		for i, o := range oo {
			require.NoError(t, metaDelete(db, objectcore.AddressOf(o)))

			c, err := db.ObjectCounters()
			require.NoError(t, err)
			require.Equal(t, uint64(objCount-i-1), c.Phy())
			require.Equal(t, uint64(objCount-i-1), c.Logic())
		}
	})

	require.NoError(t, db.Reset())

	t.Run("inhume_split", func(t *testing.T) {
		oo := putObjs(t, db, objCount, true)

		inhumedObjs := make([]oid.Address, objCount/2)

		for i, o := range oo {
			if i == len(inhumedObjs) {
				break
			}

			inhumedObjs[i] = objectcore.AddressOf(o)
		}

		for _, addr := range inhumedObjs {
			err := db.Put(createTSForObject(addr.Container(), addr.Object()))
			require.NoError(t, err)
		}

		c, err = db.ObjectCounters()
		require.NoError(t, err)

		require.Equal(t, uint64(objCount+len(inhumedObjs)), c.Phy())
		require.Equal(t, uint64(objCount-len(inhumedObjs)), c.Logic())
	})
}

func TestCounters_Expired(t *testing.T) {
	// That test is about expired objects without
	// GCMark yet. Such objects should be treated as
	// logically available: decrementing logic counter
	// should be done explicitly and only in `Delete`
	// and `Inhume` operations, otherwise, it would be
	// impossible to maintain logic counter.

	const epoch = 123

	es := &epochState{epoch}
	db := newDB(t, meta.WithEpochState(es))

	oo := make([]oid.Address, objCount)
	for i := range oo {
		oo[i] = putWithExpiration(t, db, object.TypeRegular, epoch+1)
	}

	// 1. objects are available and counters are correct

	c, err := db.ObjectCounters()
	require.NoError(t, err)
	require.Equal(t, uint64(objCount), c.Phy())
	require.Equal(t, uint64(objCount), c.Logic())

	for _, o := range oo {
		_, err := metaGet(db, o, true)
		require.NoError(t, err)
	}

	// 2. objects are expired, not available but logic counter
	// is the same

	es.e = epoch + 2

	c, err = db.ObjectCounters()
	require.NoError(t, err)
	require.Equal(t, uint64(objCount), c.Phy())
	require.Equal(t, uint64(objCount), c.Logic())

	for _, o := range oo {
		_, err := metaGet(db, o, true)
		require.ErrorIs(t, err, meta.ErrObjectIsExpired)
	}

	// 3. inhuming an expired object with GCMark (like it would
	// the GC do) should decrease the logic counter despite the
	// expiration fact

	inhumed, deleted, err := db.MarkGarbage(oo[0])
	require.NoError(t, err)
	require.Equal(t, uint64(1), inhumed)
	require.Nil(t, deleted)

	c, err = db.ObjectCounters()
	require.NoError(t, err)

	require.Equal(t, uint64(len(oo)), c.Phy())
	require.Equal(t, uint64(len(oo)-1), c.Logic())

	// 4. `Delete` an object with GCMark should decrease the
	// phy counter but does not affect the logic counter (after
	// that step they should be equal)

	deleteRes, err := db.Delete(oo[:1])
	require.NoError(t, err)
	require.Zero(t, deleteRes.AvailableRemoved)

	oo = oo[1:]

	c, err = db.ObjectCounters()
	require.NoError(t, err)
	require.Equal(t, uint64(len(oo)), c.Phy())
	require.Equal(t, uint64(len(oo)), c.Logic())

	// 5 `Delete` an expired object (like it would the control
	// service do) should decrease both counters despite the
	// expiration fact

	deleteRes, err = db.Delete(oo[:1])
	require.NoError(t, err)
	require.Equal(t, uint64(1), deleteRes.AvailableRemoved)

	oo = oo[1:]

	c, err = db.ObjectCounters()
	require.NoError(t, err)
	require.Equal(t, uint64(len(oo)), c.Phy())
	require.Equal(t, uint64(len(oo)), c.Logic())
}

func putObjs(t *testing.T, db *meta.DB, count int, withParent bool) []*object.Object {
	var err error
	parent := generateObject(t)

	oo := make([]*object.Object, 0, count)
	for i := range count {
		o := generateObject(t)
		if withParent {
			o.SetParent(parent)
		}

		oo = append(oo, o)

		err = db.Put(o)
		require.NoError(t, err)

		c, err := db.ObjectCounters()
		require.NoError(t, err)

		require.Equal(t, uint64(i+1), c.Phy())
		require.Equal(t, uint64(i+1), c.Logic())
	}

	return oo
}
