package meta_test

import (
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

func TestDB_Movable(t *testing.T) {
	db := newDB(t)

	raw1 := generateObject(t)
	raw2 := generateObject(t)

	// put two objects in metabase
	err := putBig(db, raw1)
	require.NoError(t, err)

	err = putBig(db, raw2)
	require.NoError(t, err)

	// check if toMoveIt index empty
	toMoveList, err := metaMovable(db)
	require.NoError(t, err)
	require.Len(t, toMoveList, 0)

	// mark to move object2
	err = metaToMoveIt(db, objectcore.AddressOf(raw2))
	require.NoError(t, err)

	// check if toMoveIt index contains address of object 2
	toMoveList, err = metaMovable(db)
	require.NoError(t, err)
	require.Len(t, toMoveList, 1)
	require.Contains(t, toMoveList, objectcore.AddressOf(raw2))

	// remove from toMoveIt index non existing address
	err = metaDoNotMove(db, objectcore.AddressOf(raw1))
	require.NoError(t, err)

	// check if toMoveIt index hasn't changed
	toMoveList, err = metaMovable(db)
	require.NoError(t, err)
	require.Len(t, toMoveList, 1)

	// remove from toMoveIt index existing address
	err = metaDoNotMove(db, objectcore.AddressOf(raw2))
	require.NoError(t, err)

	// check if toMoveIt index is empty now
	toMoveList, err = metaMovable(db)
	require.NoError(t, err)
	require.Len(t, toMoveList, 0)
}

func metaToMoveIt(db *meta.DB, addr oid.Address) error {
	return db.ToMoveIt(addr)
}

func metaMovable(db *meta.DB) ([]oid.Address, error) {
	return db.Movable()
}

func metaDoNotMove(db *meta.DB, addr oid.Address) error {
	return db.DoNotMove(addr)
}
