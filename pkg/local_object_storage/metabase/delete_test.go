package meta_test

import (
	"errors"
	"testing"

	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestDB_Delete(t *testing.T) {
	db := newDB(t)

	cnr := cidtest.ID()
	parent := generateObjectWithCID(t, cnr)
	addAttribute(parent, "foo", "bar")

	child := generateObjectWithCID(t, cnr)
	child.SetParent(parent)
	idParent := parent.GetID()
	child.SetParentID(idParent)

	// put object with parent
	err := putBig(db, child)
	require.NoError(t, err)

	// fill ToMoveIt index
	err = metaToMoveIt(db, object.AddressOf(child))
	require.NoError(t, err)

	// check if Movable list is not empty
	l, err := metaMovable(db)
	require.NoError(t, err)
	require.Len(t, l, 1)

	// try to remove parent, should be no-op, error-free
	err = metaDelete(db, object.AddressOf(parent))
	require.NoError(t, err)

	// inhume child so it will be on graveyard
	ts := generateObjectWithCID(t, cnr)

	err = metaInhume(db, object.AddressOf(child), object.AddressOf(ts))
	require.NoError(t, err)

	// delete object
	err = metaDelete(db, object.AddressOf(child))
	require.NoError(t, err)

	// check if there is no data in Movable index
	l, err = metaMovable(db)
	require.NoError(t, err)
	require.Len(t, l, 0)

	// check if the child is still inhumed (deletion should not affect
	// TS status that should be kept for some epochs and be handled
	// separately) and parent is not found

	ok, err := metaExists(db, object.AddressOf(child))
	require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
	require.False(t, ok)

	ok, err = metaExists(db, object.AddressOf(parent))
	require.NoError(t, err)
	require.False(t, ok)
}

func TestDeleteAllChildren(t *testing.T) {
	db := newDB(t)

	cnr := cidtest.ID()

	// generate parent object
	parent := generateObjectWithCID(t, cnr)

	// generate 2 children
	child1 := generateObjectWithCID(t, cnr)
	child1.SetParent(parent)
	idParent := parent.GetID()
	child1.SetParentID(idParent)

	child2 := generateObjectWithCID(t, cnr)
	child2.SetParent(parent)
	child2.SetParentID(idParent)

	// put children
	require.NoError(t, putBig(db, child1))
	require.NoError(t, putBig(db, child2))

	// Exists should return split info for parent
	_, err := metaExists(db, object.AddressOf(parent))
	siErr := objectSDK.NewSplitInfoError(nil)
	require.ErrorIs(t, err, ierrors.ErrParentObject)
	require.True(t, errors.As(err, &siErr))

	// remove all children in single call
	err = metaDelete(db, object.AddressOf(child1), object.AddressOf(child2))
	require.NoError(t, err)

	// parent should not be found now
	ex, err := metaExists(db, object.AddressOf(parent))
	require.NoError(t, err)
	require.False(t, ex)
}

func TestGraveOnlyDelete(t *testing.T) {
	db := newDB(t)

	addr := oidtest.Address()

	// inhume non-existent object by address
	require.NoError(t, metaInhume(db, addr, oidtest.Address()))

	// delete the object data
	require.NoError(t, metaDelete(db, addr))
}

func TestExpiredObject(t *testing.T) {
	db := newDB(t, meta.WithEpochState(epochState{currEpoch}))

	checkExpiredObjects(t, db, func(exp, nonExp *objectSDK.Object) {
		// removing expired object should be error-free
		require.NoError(t, metaDelete(db, object.AddressOf(exp)))

		require.NoError(t, metaDelete(db, object.AddressOf(nonExp)))
	})
}

func metaDelete(db *meta.DB, addrs ...oid.Address) error {
	_, err := db.Delete(addrs)
	return err
}
