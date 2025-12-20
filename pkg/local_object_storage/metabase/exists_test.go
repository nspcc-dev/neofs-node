package meta_test

import (
	"errors"
	"os"
	"testing"

	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

const currEpoch = 1000

func TestDB_Exists(t *testing.T) {
	db := newDB(t, meta.WithEpochState(epochState{currEpoch}))

	t.Run("no object", func(t *testing.T) {
		nonExist := generateObject(t)
		exists, err := metaExists(db, objectcore.AddressOf(nonExist))
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("regular object", func(t *testing.T) {
		regular := generateObject(t)
		err := putBig(db, regular)
		require.NoError(t, err)

		exists, err := metaExists(db, objectcore.AddressOf(regular))
		require.NoError(t, err)
		require.True(t, exists)

		t.Run("removed object", func(t *testing.T) {
			err := metaInhume(db, objectcore.AddressOf(regular), oidtest.Address())
			require.NoError(t, err)

			exists, err := metaExists(db, objectcore.AddressOf(regular))
			require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))
			require.False(t, exists)
		})
	})

	t.Run("tombstone object", func(t *testing.T) {
		ts := generateObject(t)
		ts.SetType(object.TypeTombstone)

		err := putBig(db, ts)
		require.NoError(t, err)

		exists, err := metaExists(db, objectcore.AddressOf(ts))
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("lock object", func(t *testing.T) {
		lock := generateObject(t)
		lock.SetType(object.TypeLock)

		err := putBig(db, lock)
		require.NoError(t, err)

		exists, err := metaExists(db, objectcore.AddressOf(lock))
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("virtual object", func(t *testing.T) {
		cnr := cidtest.ID()
		parent := generateObjectWithCID(t, cnr)

		child := generateObjectWithCID(t, cnr)
		child.SetParent(parent)
		idParent := parent.GetID()
		child.SetParentID(idParent)

		err := putBig(db, child)
		require.NoError(t, err)

		_, err = metaExists(db, objectcore.AddressOf(parent))

		var expectedErr *object.SplitInfoError
		require.ErrorIs(t, err, ierrors.ErrParentObject)
		require.True(t, errors.As(err, &expectedErr))
	})

	t.Run("merge split info", func(t *testing.T) {
		cnr := cidtest.ID()
		splitID := object.NewSplitID()

		parent := generateObjectWithCID(t, cnr)
		addAttribute(parent, "foo", "bar")

		child := generateObjectWithCID(t, cnr)
		child.SetParent(parent)
		idParent := parent.GetID()
		child.SetParentID(idParent)
		child.SetSplitID(splitID)

		link := generateObjectWithCID(t, cnr)
		link.SetParent(parent)
		link.SetParentID(idParent)
		idChild := child.GetID()
		link.SetChildren(idChild)
		link.SetSplitID(splitID)
		link.SetPayloadSize(0)

		t.Run("direct order", func(t *testing.T) {
			err := putBig(db, child)
			require.NoError(t, err)

			err = putBig(db, link)
			require.NoError(t, err)

			_, err = metaExists(db, objectcore.AddressOf(parent))
			require.Error(t, err)

			var si *object.SplitInfoError
			require.ErrorIs(t, err, ierrors.ErrParentObject)
			require.ErrorAs(t, err, &si)
			require.Equal(t, splitID, si.SplitInfo().SplitID())

			id1 := child.GetID()
			id2 := si.SplitInfo().GetLastPart()
			require.Equal(t, id1.String(), id2.String())

			id1 = link.GetID()
			id2 = si.SplitInfo().GetLink()
			require.Equal(t, id1.String(), id2.String())
		})

		t.Run("reverse order", func(t *testing.T) {
			err := metaPut(db, link)
			require.NoError(t, err)

			err = putBig(db, child)
			require.NoError(t, err)

			_, err = metaExists(db, objectcore.AddressOf(parent))
			require.Error(t, err)

			var si *object.SplitInfoError
			require.ErrorIs(t, err, ierrors.ErrParentObject)
			require.ErrorAs(t, err, &si)
			require.Equal(t, splitID, si.SplitInfo().SplitID())

			id1 := child.GetID()
			id2 := si.SplitInfo().GetLastPart()
			require.Equal(t, id1.String(), id2.String())

			id1 = link.GetID()
			id2 = si.SplitInfo().GetLink()
			require.Equal(t, id1.String(), id2.String())
		})
	})

	t.Run("random object", func(t *testing.T) {
		addr := oidtest.Address()

		exists, err := metaExists(db, addr)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("expired object", func(t *testing.T) {
		checkExpiredObjects(t, db, func(exp, nonExp *object.Object) {
			gotObj, err := metaExists(db, objectcore.AddressOf(exp))
			require.False(t, gotObj)
			require.ErrorIs(t, err, meta.ErrObjectIsExpired)

			gotObj, err = metaExists(db, objectcore.AddressOf(nonExp))
			require.NoError(t, err)
			require.True(t, gotObj)
		})
	})

	t.Run("EC", testExistsEC)
}

func BenchmarkExists(b *testing.B) {
	const numOfObjects = 100
	var (
		addrs = make([]oid.Address, 0, numOfObjects)
		db    = newDB(b)
	)

	b.Cleanup(func() { _ = os.RemoveAll(b.Name()) })

	for range numOfObjects {
		raw := generateObject(b)
		addrs = append(addrs, objectcore.AddressOf(raw))

		require.NoError(b, putBig(db, raw))
	}

	b.Run("existing", func(b *testing.B) {
		var i int

		for b.Loop() {
			ex, err := db.Exists(addrs[i%len(addrs)], false)
			require.NoError(b, err)
			require.True(b, ex)
			i++
		}
	})
	b.Run("inexisting", func(b *testing.B) {
		var addr oid.Address
		for b.Loop() {
			ex, err := db.Exists(addr, false)
			require.NoError(b, err)
			require.False(b, ex)
		}
	})
}
