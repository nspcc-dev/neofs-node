package meta_test

import (
	"testing"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
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
	err := putBig(db, child.Object())
	require.NoError(t, err)

	// fill ToMoveIt index
	err = meta.ToMoveIt(db, child.Object().Address())
	require.NoError(t, err)

	// check if Movable list is not empty
	l, err := meta.Movable(db)
	require.NoError(t, err)
	require.Len(t, l, 1)

	// inhume parent and child so they will be on graveyard
	ts := generateRawObjectWithCID(t, cid)

	err = meta.Inhume(db, child.Object().Address(), ts.Object().Address())
	require.NoError(t, err)

	err = meta.Inhume(db, child.Object().Address(), ts.Object().Address())
	require.NoError(t, err)

	// delete object
	err = meta.Delete(db, child.Object().Address())
	require.NoError(t, err)

	// check if there is no data in Movable index
	l, err = meta.Movable(db)
	require.NoError(t, err)
	require.Len(t, l, 0)

	// check if they removed from graveyard
	ok, err := meta.Exists(db, child.Object().Address())
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = meta.Exists(db, parent.Object().Address())
	require.NoError(t, err)
	require.False(t, ok)
}
