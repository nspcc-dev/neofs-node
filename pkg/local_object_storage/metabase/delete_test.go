package meta_test

import (
	"errors"
	"math/rand/v2"
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
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
	res, err := db.Delete(cnr, []oid.ID{idParent})
	require.NoError(t, err)
	require.Zero(t, res.Counters.Phy)

	// inhume child so it will be on graveyard
	ts := generateObjectWithCID(t, cnr)

	err = metaInhume(db, child.Address(), ts.Address())
	require.NoError(t, err)

	// delete object
	_, err = db.Delete(cnr, []oid.ID{child.GetID()})
	require.NoError(t, err)

	// check if the child is still inhumed (deletion should not affect
	// TS status that should be kept for some epochs and be handled
	// separately) and parent is not found

	ok, err := metaExists(db, child.Address())
	require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
	require.False(t, ok)

	ok, err = metaExists(db, parent.Address())
	require.NoError(t, err)
	require.False(t, ok)

	t.Run("EC", func(t *testing.T) {
		cnr1 := cidtest.ID()
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

		parent1, ecParts1 := newGroup(cnr1, 5)

		parent1ID := parent1.GetID()

		res, err := db.Delete(cnr1, []oid.ID{parent1ID})
		require.NoError(t, err)

		all := 1 + len(ecParts1)
		require.EqualValues(t, -(all - 1), res.Counters.Phy)
		require.Len(t, res.RemovedObjects, all)

		require.ElementsMatch(t, res.RemovedObjects[:1], []meta.RemovedObject{
			{ID: parent1ID, PayloadLen: 0},
		})
		for _, partObj := range ecParts1 {
			require.Contains(t, res.RemovedObjects, meta.RemovedObject{
				ID:         partObj.GetID(),
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

	objID := obj.GetID()

	res, err := db.Delete(cID, []oid.ID{objID})
	require.NoError(t, err)

	require.Equal(t, -1, res.Counters.Phy)
	require.Len(t, res.RemovedObjects, 1)
	require.Equal(t, payloadSize, res.RemovedObjects[0].PayloadLen)
	require.Equal(t, objID, res.RemovedObjects[0].ID)

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
	_, err := metaExists(db, parent.Address())
	siErr := object.NewSplitInfoError(nil)
	require.ErrorIs(t, err, ierrors.ErrParentObject)
	require.True(t, errors.As(err, &siErr))

	// remove all children in single call
	_, err = db.Delete(cnr, []oid.ID{child1.GetID(), child2.GetID()})
	require.NoError(t, err)

	// parent should not be found now
	ex, err := metaExists(db, parent.Address())
	require.NoError(t, err)
	require.False(t, ex)
}

func TestGraveOnlyDelete(t *testing.T) {
	db := newDB(t)

	addr := oidtest.Address()

	// inhume non-existent object by address
	require.NoError(t, metaInhume(db, addr, oidtest.Address()))

	// delete the object data
	_, err := db.Delete(addr.Container(), []oid.ID{addr.Object()})
	require.NoError(t, err)
}

func TestExpiredObject(t *testing.T) {
	db := newDB(t, meta.WithEpochState(epochState{currEpoch}))

	checkExpiredObjects(t, db, func(exp, nonExp *object.Object) {
		// removing expired object should be error-free
		_, err := db.Delete(exp.GetContainerID(), []oid.ID{exp.GetID()})
		require.NoError(t, err)

		_, err = db.Delete(nonExp.GetContainerID(), []oid.ID{nonExp.GetID()})
		require.NoError(t, err)
	})
}
