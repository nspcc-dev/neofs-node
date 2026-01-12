package meta_test

import (
	"errors"
	"math/rand/v2"
	"slices"
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
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

	// try to remove parent, should be no-op, error-free
	err = metaDelete(db, objectcore.AddressOf(parent))
	require.NoError(t, err)

	// inhume child so it will be on graveyard
	ts := generateObjectWithCID(t, cnr)

	err = metaInhume(db, objectcore.AddressOf(child), objectcore.AddressOf(ts))
	require.NoError(t, err)

	// delete object
	err = metaDelete(db, objectcore.AddressOf(child))
	require.NoError(t, err)

	// check if the child is still inhumed (deletion should not affect
	// TS status that should be kept for some epochs and be handled
	// separately) and parent is not found

	ok, err := metaExists(db, objectcore.AddressOf(child))
	require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
	require.False(t, ok)

	ok, err = metaExists(db, objectcore.AddressOf(parent))
	require.NoError(t, err)
	require.False(t, ok)

	t.Run("EC", func(t *testing.T) {
		cnr1 := cidtest.ID()
		cnr2 := cidtest.ID()
		const anyRuleIdx = 1
		anySigner := neofscryptotest.Signer()

		newGroup := func(cnr cid.ID, partNum int) (object.Object, []object.Object) {
			parent := *generateObjectWithCID(t, cnr)
			parent.SetPayloadSize(rand.Uint64())

			var ecParts []object.Object
			for i := range partNum {
				partObj, err := iec.FormObjectForECPart(anySigner, parent, nil, iec.PartInfo{
					RuleIndex: anyRuleIdx,
					Index:     i,
				})
				require.NoError(t, err)
				partObj.SetPayloadSize(rand.Uint64())
				require.NoError(t, db.Put(&partObj))

				ecParts = append(ecParts, partObj)
			}

			return parent, ecParts
		}

		parent1, ecParts1 := newGroup(cnr1, 3)
		parent2, ecParts2 := newGroup(cnr2, 5)

		parent1Addr := objectcore.AddressOf(&parent1)
		parent2Addr := objectcore.AddressOf(&parent2)

		res, err := db.Delete([]oid.Address{parent1Addr, parent2Addr})
		require.NoError(t, err)

		all := 2 + len(ecParts1) + len(ecParts2)
		require.EqualValues(t, all, res.AvailableRemoved)
		require.EqualValues(t, all, res.RawRemoved)
		require.Len(t, res.RemovedObjects, all)

		require.ElementsMatch(t, res.RemovedObjects[:2], []meta.RemovedObject{
			{Address: parent1Addr, PayloadLen: 0},
			{Address: parent2Addr, PayloadLen: 0},
		})
		for _, partObj := range slices.Concat(ecParts1, ecParts2) {
			require.Contains(t, res.RemovedObjects, meta.RemovedObject{
				Address:    objectcore.AddressOf(&partObj),
				PayloadLen: partObj.PayloadSize(),
			})
		}
	})
}

func TestContainerInfo(t *testing.T) {
	db := newDB(t)

	cID := cidtest.ID()
	obj := generateObjectWithCID(t, cID)
	obj.ResetRelations()

	payloadSize := obj.PayloadSize()

	err := putBig(db, obj)
	require.NoError(t, err)

	info, err := db.GetContainerInfo(cID)
	require.NoError(t, err)

	require.Equal(t, uint64(1), info.ObjectsNumber)
	require.Equal(t, payloadSize, info.StorageSize)

	addr := objectcore.AddressOf(obj)

	res, err := db.Delete([]oid.Address{addr})
	require.NoError(t, err)

	require.Equal(t, uint64(1), res.AvailableRemoved)
	require.Equal(t, uint64(1), res.RawRemoved)
	require.Len(t, res.RemovedObjects, 1)
	require.Equal(t, payloadSize, res.RemovedObjects[0].PayloadLen)
	require.Equal(t, addr, res.RemovedObjects[0].Address)

	info, err = db.GetContainerInfo(cID)
	require.NoError(t, err)

	require.Zero(t, info.ObjectsNumber)
	require.Zero(t, info.StorageSize)
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
	_, err := metaExists(db, objectcore.AddressOf(parent))
	siErr := object.NewSplitInfoError(nil)
	require.ErrorIs(t, err, ierrors.ErrParentObject)
	require.True(t, errors.As(err, &siErr))

	// remove all children in single call
	err = metaDelete(db, objectcore.AddressOf(child1), objectcore.AddressOf(child2))
	require.NoError(t, err)

	// parent should not be found now
	ex, err := metaExists(db, objectcore.AddressOf(parent))
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

	checkExpiredObjects(t, db, func(exp, nonExp *object.Object) {
		// removing expired object should be error-free
		require.NoError(t, metaDelete(db, objectcore.AddressOf(exp)))

		require.NoError(t, metaDelete(db, objectcore.AddressOf(nonExp)))
	})
}

func metaDelete(db *meta.DB, addrs ...oid.Address) error {
	_, err := db.Delete(addrs)
	return err
}

func BenchmarkDB_Delete(b *testing.B) {
	db := newDB(b)
	const containerNum = 100
	const objectsPerContainer = 100
	const totalObjects = containerNum * objectsPerContainer

	bench := func(b *testing.B, addrs []oid.Address) {
		for b.Loop() {
			_, err := db.Delete(addrs)
			require.NoError(b, err)
		}
	}

	b.Run("grouped", func(b *testing.B) {
		bench := func(b *testing.B, sortFn func(a, b cid.ID) int) {
			cnrs := cidtest.IDs(containerNum)
			slices.SortFunc(cnrs, sortFn)

			addrs := make([]oid.Address, 0, totalObjects)
			for i := range cnrs {
				for range objectsPerContainer {
					addrs = append(addrs, oid.NewAddress(cnrs[i], oidtest.ID()))
				}
			}

			bench(b, addrs)
		}
		b.Run("sorted", func(b *testing.B) {
			bench(b, cid.ID.Compare)
		})
		b.Run("reverse", func(b *testing.B) {
			bench(b, func(a, b cid.ID) int { return b.Compare(a) })
		})
	})

	b.Run("mixed", func(b *testing.B) {
		bench := func(b *testing.B, sortFn func(a, b cid.ID) int) {
			cnrs := cidtest.IDs(containerNum)
			slices.SortFunc(cnrs, sortFn)

			addrs := make([]oid.Address, 0, totalObjects)
			for range objectsPerContainer {
				for i := range cnrs {
					addrs = append(addrs, oid.NewAddress(cnrs[i], oidtest.ID()))
				}
			}

			bench(b, addrs)
		}
		b.Run("sorted", func(b *testing.B) {
			bench(b, cid.ID.Compare)
		})
		b.Run("reverse", func(b *testing.B) {
			bench(b, func(a, b cid.ID) int { return b.Compare(a) })
		})
	})
}
