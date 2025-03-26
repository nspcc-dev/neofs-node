package meta

import (
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
			o.SetContainerID(cID)
			setExpiration(&o, uint64(i))

			oo = append(oo, o)
		}

		st, err := storageForContainer(t.TempDir(), cID)
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
		o.SetContainerID(cID)
		setExpiration(&o, objExp)
		lock := objecttest.Object()
		lock.SetContainerID(cID)
		setExpiration(&lock, lockExp)

		st, err := storageForContainer(t.TempDir(), cID)
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
