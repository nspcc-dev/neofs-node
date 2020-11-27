package meta_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDB_Delete(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	cid := testCID()
	parent := generateRawObjectWithCID(t, cid)
	addAttribute(parent, "foo", "bar")

	child := generateRawObjectWithCID(t, cid)
	child.SetParent(parent.Object().SDK())
	child.SetParentID(parent.ID())

	// put object with parent
	err := db.Put(child.Object(), nil)
	require.NoError(t, err)

	// fill ToMoveIt index
	err = db.ToMoveIt(child.Object().Address())
	require.NoError(t, err)

	// check if Movable list is not empty
	l, err := db.Movable()
	require.NoError(t, err)
	require.Len(t, l, 1)

	// inhume parent and child so they will be on graveyard
	ts := generateRawObjectWithCID(t, cid)

	err = db.Inhume(child.Object().Address(), ts.Object().Address())
	require.NoError(t, err)

	err = db.Inhume(child.Object().Address(), ts.Object().Address())
	require.NoError(t, err)

	// delete object
	err = db.Delete(child.Object().Address())
	require.NoError(t, err)

	// check if there is no data in Movable index
	l, err = db.Movable()
	require.NoError(t, err)
	require.Len(t, l, 0)

	// check if they removed from graveyard
	ok, err := db.Exists(child.Object().Address())
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = db.Exists(parent.Object().Address())
	require.NoError(t, err)
	require.False(t, ok)
}
