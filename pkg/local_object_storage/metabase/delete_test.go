package meta_test

import (
	"errors"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func TestDB_Delete(t *testing.T) {
	db := newDB(t)

	cid := cidtest.ID()
	parent := generateObjectWithCID(t, cid)
	addAttribute(parent, "foo", "bar")

	child := generateObjectWithCID(t, cid)
	child.SetParent(parent)
	child.SetParentID(parent.ID())

	// put object with parent
	err := putBig(db, child)
	require.NoError(t, err)

	// fill ToMoveIt index
	err = meta.ToMoveIt(db, object.AddressOf(child))
	require.NoError(t, err)

	// check if Movable list is not empty
	l, err := meta.Movable(db)
	require.NoError(t, err)
	require.Len(t, l, 1)

	// try to remove parent unsuccessfully
	err = meta.Delete(db, object.AddressOf(parent))
	require.Error(t, err)

	// inhume parent and child so they will be on graveyard
	ts := generateObjectWithCID(t, cid)

	err = meta.Inhume(db, object.AddressOf(child), object.AddressOf(ts))
	require.NoError(t, err)

	// delete object
	err = meta.Delete(db, object.AddressOf(child))
	require.NoError(t, err)

	// check if there is no data in Movable index
	l, err = meta.Movable(db)
	require.NoError(t, err)
	require.Len(t, l, 0)

	// check if they marked as already removed

	ok, err := meta.Exists(db, object.AddressOf(child))
	require.Error(t, apistatus.ObjectAlreadyRemoved{})
	require.False(t, ok)

	ok, err = meta.Exists(db, object.AddressOf(parent))
	require.Error(t, apistatus.ObjectAlreadyRemoved{})
	require.False(t, ok)
}

func TestDeleteAllChildren(t *testing.T) {
	db := newDB(t)

	cid := cidtest.ID()

	// generate parent object
	parent := generateObjectWithCID(t, cid)

	// generate 2 children
	child1 := generateObjectWithCID(t, cid)
	child1.SetParent(parent)
	child1.SetParentID(parent.ID())

	child2 := generateObjectWithCID(t, cid)
	child2.SetParent(parent)
	child2.SetParentID(parent.ID())

	// put children
	require.NoError(t, putBig(db, child1))
	require.NoError(t, putBig(db, child2))

	// Exists should return split info for parent
	_, err := meta.Exists(db, object.AddressOf(parent))
	siErr := objectSDK.NewSplitInfoError(nil)
	require.True(t, errors.As(err, &siErr))

	// remove all children in single call
	err = meta.Delete(db, object.AddressOf(child1), object.AddressOf(child2))
	require.NoError(t, err)

	// parent should not be found now
	ex, err := meta.Exists(db, object.AddressOf(parent))
	require.NoError(t, err)
	require.False(t, ex)
}

func TestGraveOnlyDelete(t *testing.T) {
	db := newDB(t)

	addr := generateAddress()

	// inhume non-existent object by address
	require.NoError(t, meta.Inhume(db, addr, nil))

	// delete the object data
	require.NoError(t, meta.Delete(db, addr))
}
