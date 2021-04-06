package meta_test

import (
	"testing"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/pkg/errors"
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

	// try to remove parent unsuccessfully
	err = meta.Delete(db, parent.Object().Address())
	require.Error(t, err)

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

func TestDeleteAllChildren(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	cid := testCID()

	// generate parent object
	parent := generateRawObjectWithCID(t, cid)

	// generate 2 children
	child1 := generateRawObjectWithCID(t, cid)
	child1.SetParent(parent.Object().SDK())
	child1.SetParentID(parent.ID())

	child2 := generateRawObjectWithCID(t, cid)
	child2.SetParent(parent.Object().SDK())
	child2.SetParentID(parent.ID())

	// put children
	require.NoError(t, putBig(db, child1.Object()))
	require.NoError(t, putBig(db, child2.Object()))

	// Exists should return split info for parent
	_, err := meta.Exists(db, parent.Object().Address())
	siErr := objectSDK.NewSplitInfoError(nil)
	require.True(t, errors.As(err, &siErr))

	// remove all children in single call
	err = meta.Delete(db, child1.Object().Address(), child2.Object().Address())
	require.NoError(t, err)

	// parent should not be found now
	ex, err := meta.Exists(db, parent.Object().Address())
	require.NoError(t, err)
	require.False(t, ex)
}

func TestGraveOnlyDelete(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	addr := generateAddress()

	// inhume non-existent object by address
	require.NoError(t, meta.Inhume(db, addr, nil))

	// delete the object data
	require.NoError(t, meta.Delete(db, addr))
}
