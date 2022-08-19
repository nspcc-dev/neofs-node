package meta_test

import (
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

const objCount = 10

func TestCounter_Default(t *testing.T) {
	db := newDB(t)

	c, err := db.ObjectCounter()
	require.NoError(t, err)
	require.Zero(t, c)
}

func TestCounter(t *testing.T) {
	db := newDB(t)

	var c uint64
	var err error

	oo := make([]*object.Object, 0, objCount)
	for i := 0; i < objCount; i++ {
		oo = append(oo, generateObject(t))
	}

	var prm meta.PutPrm

	for i := 0; i < objCount; i++ {
		prm.SetObject(oo[i])

		_, err = db.Put(prm)
		require.NoError(t, err)

		c, err = db.ObjectCounter()
		require.NoError(t, err)

		require.Equal(t, uint64(i+1), c)
	}
}

func TestCounter_Dec(t *testing.T) {
	db := newDB(t)
	oo := putObjs(t, db, objCount, false)

	var err error
	var c uint64

	var prm meta.DeletePrm
	for i := objCount - 1; i >= 0; i-- {
		prm.SetAddresses(objectcore.AddressOf(oo[i]))

		_, err = db.Delete(prm)
		require.NoError(t, err)

		c, err = db.ObjectCounter()
		require.NoError(t, err)

		require.Equal(t, uint64(i), c)
	}
}

func TestCounter_PutSplit(t *testing.T) {
	db := newDB(t)

	parObj := generateObject(t)
	var err error
	var c uint64

	// put objects and check that parent info
	// does not affect the counter
	for i := 0; i < objCount; i++ {
		o := generateObject(t)
		if i < objCount/2 { // half of the objs will have the parent
			o.SetParent(parObj)
		}

		require.NoError(t, putBig(db, o))

		c, err = db.ObjectCounter()
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), c)
	}
}

func TestCounter_DeleteSplit(t *testing.T) {
	db := newDB(t)
	oo := putObjs(t, db, objCount, true)

	// delete objects that have parent info
	// and check that it does not affect
	// the counter
	for i, o := range oo {
		require.NoError(t, metaDelete(db, objectcore.AddressOf(o)))

		c, err := db.ObjectCounter()
		require.NoError(t, err)
		require.Equal(t, uint64(objCount-i-1), c)
	}
}

func putObjs(t *testing.T, db *meta.DB, count int, withParent bool) []*object.Object {
	var prm meta.PutPrm
	var err error
	parent := generateObject(t)

	oo := make([]*object.Object, 0, count)
	for i := 0; i < count; i++ {
		o := generateObject(t)
		if withParent {
			o.SetParent(parent)
		}

		oo = append(oo, o)

		prm.SetObject(o)
		_, err = db.Put(prm)
		require.NoError(t, err)

		c, err := db.ObjectCounter()
		require.NoError(t, err)

		require.Equal(t, uint64(i+1), c)
	}

	return oo
}
