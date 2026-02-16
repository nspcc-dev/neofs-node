package meta

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func objsToAddrMap(oo []object.Object) map[oid.Address]object.Object {
	res := make(map[oid.Address]object.Object)
	for _, o := range oo {
		res[oid.NewAddress(o.GetContainerID(), o.GetID())] = o
	}
	return res
}

func setExpiration(o *object.Object, epoch uint64) {
	var attr object.Attribute

	attr.SetKey(object.AttributeExpirationEpoch)
	attr.SetValue(strconv.FormatUint(epoch, 10))

	o.SetAttributes(append(o.Attributes(), attr)...)
}

func TestObjectExpiration(t *testing.T) {
	t.Run("expired objects", func(t *testing.T) {
		const objNum = 10
		cID := cidtest.ID()
		var oo []object.Object
		for i := range objNum {
			o := objecttest.Object()
			o.ResetRelations()
			o.SetContainerID(cID)
			setExpiration(&o, uint64(i))

			oo = append(oo, o)
		}

		st, err := storageForContainer(zaptest.NewLogger(t), t.TempDir(), cID)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = st.drop()
		})

		net := testNetwork{}
		net.setContainers([]cid.ID{cID})
		net.setObjects(objsToAddrMap(oo))
		ee := make([]objEvent, 0, objNum)
		for _, o := range oo {
			ee = append(ee, objEvent{
				cID:     cID,
				oID:     o.GetID(),
				size:    big.NewInt(testObjectSize),
				network: big.NewInt(testNetworkMagic),
			})
		}

		st.putObjects(context.Background(), zaptest.NewLogger(t), 0, ee, &net)

		for i, o := range oo {
			oIDToDelete := o.GetID()
			kExpect := append([]byte{oidIndex}, oIDToDelete[:]...)

			_, err = st.db.Get(kExpect)
			require.NoError(t, err)

			err = st.handleNewEpoch(uint64(i + 1))
			require.NoError(t, err)

			_, err = st.db.Get(kExpect)
			require.ErrorIs(t, err, storage.ErrKeyNotFound, fmt.Sprintf("%d object was not expired", i))
		}

		// all objects now are expired, empty db is expected
		st.db.Seek(storage.SeekRange{}, func(k, v []byte) bool {
			require.Fail(t, "no KV after expirations are expected")
			return true
		})
	})

	t.Run("lock expiration", func(t *testing.T) {
		cID := cidtest.ID()

		const (
			objExp = iota
			lockExp
		)

		o := objecttest.Object()
		oID := o.GetID()
		o.ResetRelations()
		o.SetContainerID(cID)
		setExpiration(&o, objExp)
		lock := objecttest.Object()
		lock.ResetRelations()
		lock.SetContainerID(cID)
		setExpiration(&lock, lockExp)

		st, err := storageForContainer(zaptest.NewLogger(t), t.TempDir(), cID)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = st.drop()
		})

		net := testNetwork{}
		net.setContainers([]cid.ID{cID})
		net.setObjects(objsToAddrMap([]object.Object{o, lock}))

		eObj := objEvent{
			cID:     cID,
			oID:     oID,
			size:    big.NewInt(testObjectSize),
			network: big.NewInt(testNetworkMagic),
		}
		eLock := objEvent{
			cID:           cID,
			oID:           lock.GetID(),
			size:          big.NewInt(testObjectSize),
			network:       big.NewInt(testNetworkMagic),
			lockedObjects: oID[:],
		}

		st.putObjects(context.Background(), zaptest.NewLogger(t), 0, []objEvent{eObj}, &net)
		st.putObjects(context.Background(), zaptest.NewLogger(t), 0, []objEvent{eLock}, &net)

		kExpect := append([]byte{oidIndex}, oID[:]...)

		err = st.handleNewEpoch(uint64(objExp + 1))
		require.NoError(t, err)

		_, err = st.db.Get(kExpect)
		require.NoError(t, err, "locked object expired")

		err = st.handleNewEpoch(uint64(lockExp + 1))
		require.NoError(t, err)

		_, err = st.db.Get(kExpect)
		require.ErrorIs(t, err, storage.ErrKeyNotFound, "unlocked object has not expired")

		// all objects now are expired, empty db is expected
		st.db.Seek(storage.SeekRange{}, func(k, v []byte) bool {
			require.Fail(t, "no KV after expirations are expected")
			return true
		})
	})
}

func objectChain(cID cid.ID, length int) (object.Object, []object.Object) {
	reset := func(o *object.Object) {
		o.SetContainerID(cID)
		o.ResetRelations()
		o.SetType(object.TypeRegular)
	}

	root := objecttest.Object()
	reset(&root)
	first := objecttest.Object()
	reset(&first)

	children := make([]object.Object, length-1)
	children[0] = first
	for i := range children {
		if i != 0 {
			children[i] = objecttest.Object()
			reset(&children[i])
			children[i].SetFirstID(first.GetID())
			children[i].SetPreviousID(children[i-1].GetID())
		}
		if i == len(children)-1 {
			children[i].SetParentID(root.GetID())
			children[i].SetParent(&root)
		}
	}

	link := objecttest.Object()
	reset(&link)
	link.SetFirstID(first.GetID())
	link.SetParentID(root.GetID())
	link.SetParent(&root)
	link.SetType(object.TypeLink)

	return root, append(children, link)
}

type bigObj struct {
	root     object.Object
	children []object.Object
}

func TestBigObjects(t *testing.T) {
	cID := cidtest.ID()
	l := zaptest.NewLogger(t)
	ctx := context.Background()

	var bigObjs []bigObj
	for range 10 {
		root, children := objectChain(cID, 10)
		bigObjs = append(bigObjs, bigObj{root, children})
	}

	var rawObjSlice []object.Object
	for _, obj := range bigObjs {
		rawObjSlice = append(rawObjSlice, obj.children...)
	}

	net := testNetwork{}
	net.setContainers([]cid.ID{cID})
	net.setObjects(objsToAddrMap(rawObjSlice))
	ee := make([]objEvent, 0, len(rawObjSlice))
	for _, o := range rawObjSlice {
		ev := objEvent{
			cID:     cID,
			oID:     o.GetID(),
			size:    big.NewInt(testObjectSize),
			network: big.NewInt(testNetworkMagic),
		}
		ev.typ = o.Type()
		if id := o.GetPreviousID(); !id.IsZero() {
			ev.prevObject = id[:]
		}
		if id := o.GetFirstID(); !id.IsZero() {
			ev.firstObject = id[:]
		}

		ee = append(ee, ev)
	}

	st, err := storageForContainer(zaptest.NewLogger(t), t.TempDir(), cID)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = st.drop()
	})

	st.putObjects(ctx, l, 0, ee, &net)

	tsObj := objecttest.Object()
	tsObj.ResetRelations()
	tsObj.SetContainerID(cID)
	tsObj.SetType(object.TypeTombstone)
	tsEv := objEvent{
		cID:     cID,
		oID:     tsObj.GetID(),
		size:    big.NewInt(testObjectSize),
		network: big.NewInt(testNetworkMagic),
	}
	net.setObjects(objsToAddrMap([]object.Object{tsObj}))

	for _, bigO := range bigObjs {
		rID := bigO.root.GetID()
		tsEv.deletedObjects = rID[:]

		st.putObjects(ctx, l, 0, []objEvent{tsEv}, &net)

		k := make([]byte, 1+oid.Size)
		k[0] = oidIndex

		for _, child := range bigO.children {
			chID := child.GetID()
			copy(k[1:], chID[:])

			_, err = st.db.Get(k)
			require.ErrorIs(t, err, storage.ErrKeyNotFound)
		}
	}

	// only last operation from TS should be kept
	tsID := tsObj.GetID()
	st.db.Seek(storage.SeekRange{}, func(k, _ []byte) bool {
		switch k[0] {
		case oidIndex, oidToAttrIndex, deletedIndex:
			if bytes.Equal(k[1:1+oid.Size], tsID[:]) {
				return true
			}
		case attrIntToOIDIndex, attrPlainToOIDIndex:
			if bytes.Equal(k[len(k)-oid.Size:], tsID[:]) {
				return true
			}
		default:
		}

		t.Fatalf("empty db is expected after full clean, found key: %d", k[0])
		return true
	})
}
