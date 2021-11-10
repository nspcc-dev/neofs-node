package meta_test

import (
	"testing"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func TestDB_IterateOverGraveyard(t *testing.T) {
	db := newDB(t)

	// generate and put 2 objects
	obj1 := generateRawObject(t)
	obj2 := generateRawObject(t)

	var err error

	err = putBig(db, obj1.Object())
	require.NoError(t, err)

	err = putBig(db, obj2.Object())
	require.NoError(t, err)

	inhumePrm := new(meta.InhumePrm)

	// inhume with tombstone
	addrTombstone := generateAddress()

	_, err = db.Inhume(inhumePrm.
		WithAddresses(obj1.Object().Address()).
		WithTombstoneAddress(addrTombstone),
	)
	require.NoError(t, err)

	// inhume with GC mark
	_, err = db.Inhume(inhumePrm.
		WithAddresses(obj2.Object().Address()).
		WithGCMark(),
	)

	var (
		counterAll         int
		buriedTS, buriedGC []*object.Address
	)

	err = db.IterateOverGraveyard(func(g *meta.Grave) error {
		if g.WithGCMark() {
			buriedGC = append(buriedGC, g.Address())
		} else {
			buriedTS = append(buriedTS, g.Address())
		}

		counterAll++

		return nil
	})

	require.NoError(t, err)

	require.Equal(t, 2, counterAll)
	require.Equal(t, []*object.Address{obj1.Object().Address()}, buriedTS)
	require.Equal(t, []*object.Address{obj2.Object().Address()}, buriedGC)
}
