package meta_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/stretchr/testify/require"
)

func TestDB_Movable(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	raw1 := generateRawObject(t)
	raw2 := generateRawObject(t)

	obj1 := object.NewFromV2(raw1.ToV2())
	obj2 := object.NewFromV2(raw2.ToV2())

	// put two objects in metabase
	err := db.Put(obj1, nil)
	require.NoError(t, err)

	err = db.Put(obj2, nil)
	require.NoError(t, err)

	// check if toMoveIt index empty
	toMoveList, err := db.Movable()
	require.NoError(t, err)
	require.Len(t, toMoveList, 0)

	// mark to move object2
	err = db.ToMoveIt(obj2.Address())
	require.NoError(t, err)

	// check if toMoveIt index contains address of object 2
	toMoveList, err = db.Movable()
	require.NoError(t, err)
	require.Len(t, toMoveList, 1)
	require.Contains(t, toMoveList, obj2.Address())

	// remove from toMoveIt index non existing address
	err = db.DoNotMove(obj1.Address())
	require.NoError(t, err)

	// check if toMoveIt index hasn't changed
	toMoveList, err = db.Movable()
	require.NoError(t, err)
	require.Len(t, toMoveList, 1)

	// remove from toMoveIt index existing address
	err = db.DoNotMove(obj2.Address())
	require.NoError(t, err)

	// check if toMoveIt index is empty now
	toMoveList, err = db.Movable()
	require.NoError(t, err)
	require.Len(t, toMoveList, 0)
}
