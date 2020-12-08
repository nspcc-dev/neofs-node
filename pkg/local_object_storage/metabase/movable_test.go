package meta_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDB_Movable(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	raw1 := generateRawObject(t)
	raw2 := generateRawObject(t)

	// put two objects in metabase
	err := db.Put(raw1.Object(), nil)
	require.NoError(t, err)

	err = db.Put(raw2.Object(), nil)
	require.NoError(t, err)

	// check if toMoveIt index empty
	toMoveList, err := db.Movable()
	require.NoError(t, err)
	require.Len(t, toMoveList, 0)

	// mark to move object2
	err = db.ToMoveIt(raw2.Object().Address())
	require.NoError(t, err)

	// check if toMoveIt index contains address of object 2
	toMoveList, err = db.Movable()
	require.NoError(t, err)
	require.Len(t, toMoveList, 1)
	require.Contains(t, toMoveList, raw2.Object().Address())

	// remove from toMoveIt index non existing address
	err = db.DoNotMove(raw1.Object().Address())
	require.NoError(t, err)

	// check if toMoveIt index hasn't changed
	toMoveList, err = db.Movable()
	require.NoError(t, err)
	require.Len(t, toMoveList, 1)

	// remove from toMoveIt index existing address
	err = db.DoNotMove(raw2.Object().Address())
	require.NoError(t, err)

	// check if toMoveIt index is empty now
	toMoveList, err = db.Movable()
	require.NoError(t, err)
	require.Len(t, toMoveList, 0)
}
