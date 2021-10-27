package meta_test

import (
	"testing"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/stretchr/testify/require"
)

func TestDB_Movable(t *testing.T) {
	db := newDB(t)

	raw1 := generateRawObject(t)
	raw2 := generateRawObject(t)

	// put two objects in metabase
	err := putBig(db, raw1.Object())
	require.NoError(t, err)

	err = putBig(db, raw2.Object())
	require.NoError(t, err)

	// check if toMoveIt index empty
	toMoveList, err := meta.Movable(db)
	require.NoError(t, err)
	require.Len(t, toMoveList, 0)

	// mark to move object2
	err = meta.ToMoveIt(db, raw2.Object().Address())
	require.NoError(t, err)

	// check if toMoveIt index contains address of object 2
	toMoveList, err = meta.Movable(db)
	require.NoError(t, err)
	require.Len(t, toMoveList, 1)
	require.Contains(t, toMoveList, raw2.Object().Address())

	// remove from toMoveIt index non existing address
	err = meta.DoNotMove(db, raw1.Object().Address())
	require.NoError(t, err)

	// check if toMoveIt index hasn't changed
	toMoveList, err = meta.Movable(db)
	require.NoError(t, err)
	require.Len(t, toMoveList, 1)

	// remove from toMoveIt index existing address
	err = meta.DoNotMove(db, raw2.Object().Address())
	require.NoError(t, err)

	// check if toMoveIt index is empty now
	toMoveList, err = meta.Movable(db)
	require.NoError(t, err)
	require.Len(t, toMoveList, 0)
}
