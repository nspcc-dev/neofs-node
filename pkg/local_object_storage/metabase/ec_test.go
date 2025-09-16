package meta_test

import (
	"slices"
	"strconv"
	"testing"

	"github.com/nspcc-dev/bbolt"
	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func BenchmarkDB_ResolveECPart(b *testing.B) {
	signer := neofscryptotest.Signer()
	cnr := cidtest.ID()
	parentID := oidtest.ID()
	pi := iec.PartInfo{
		RuleIndex: 123,
		Index:     456,
	}

	parentObj := newBlankObject(cnr, parentID)

	partObj, err := iec.FormObjectForECPart(signer, parentObj, testutil.RandByteSlice(32), pi)
	require.NoError(b, err)

	db := newDB(b)
	require.NoError(b, db.Put(&partObj))

	b.ResetTimer()

	for range b.N {
		_, err = db.ResolveECPart(cnr, parentID, pi)
		require.NoError(b, err)
	}
}

func TestDB_ResolveECPart(t *testing.T) {
	const currentEpoch = 123
	cnr := cidtest.ID()
	parentID := oidtest.ID()
	partID := oidtest.OtherID(parentID)
	signer := neofscryptotest.Signer()
	pi := iec.PartInfo{
		RuleIndex: 12,
		Index:     34,
	}

	parentObj := newBlankObject(cnr, parentID)
	parentAddr := objectcore.AddressOf(&parentObj)

	newPart := func(t *testing.T, pi iec.PartInfo) object.Object {
		obj, err := iec.FormObjectForECPart(signer, parentObj, testutil.RandByteSlice(32), pi)
		require.NoError(t, err)
		return obj
	}

	addPart := func(t *testing.T, db *meta.DB, pi iec.PartInfo) oid.ID {
		obj := newPart(t, pi)
		require.NoError(t, db.Put(&obj))
		return obj.GetID()
	}

	partObj := newPart(t, pi)
	partObj.SetID(partID)

	expiredObj := parentObj // same address as parentObj
	addAttribute(&expiredObj, "__NEOFS__EXPIRATION_EPOCH", strconv.Itoa(currentEpoch-1))

	locker := newBlankObject(cnr, oidtest.OtherID(partID))
	locker.AssociateLocked(partID)

	tomb := newBlankObject(cnr, oidtest.OtherID(partID, locker.GetID()))
	tomb.AssociateDeleted(parentID)

	partAddr := oid.NewAddress(cnr, partID)
	tombAddr := oid.NewAddress(cnr, tomb.GetID())

	type testcase struct {
		name      string
		assertErr func(*testing.T, error)
		preset    func(t *testing.T) *meta.DB
	}

	// failure cases
	var tcs []testcase

	// logic failures
	for _, tc := range []struct {
		name      string
		assertErr func(*testing.T, error)
		preset    func(t *testing.T, db *meta.DB)
	}{
		{name: "empty DB", assertErr: assertObjectNotFoundError, preset: func(t *testing.T, db *meta.DB) {}},
		{name: "without parent object", assertErr: assertObjectNotFoundError, preset: func(t *testing.T, db *meta.DB) {
			noParentObj := newBlankObject(cnr, oidtest.ID())
			require.NoError(t, db.Put(&noParentObj))
		}},
		{name: "another rule", assertErr: assertObjectNotFoundError, preset: func(t *testing.T, db *meta.DB) {
			addPart(t, db, iec.PartInfo{RuleIndex: pi.RuleIndex + 1, Index: pi.Index})
		}},
		{name: "another part", assertErr: assertObjectNotFoundError, preset: func(t *testing.T, db *meta.DB) {
			addPart(t, db, iec.PartInfo{RuleIndex: pi.RuleIndex, Index: pi.Index + 1})
		}},
		{name: "another rule and part", assertErr: assertObjectNotFoundError, preset: func(t *testing.T, db *meta.DB) {
			addPart(t, db, iec.PartInfo{RuleIndex: pi.RuleIndex + 1, Index: pi.Index + 1})
		}},
		{name: "tombstone mark only", assertErr: assertObjectNotFoundError, preset: func(t *testing.T, db *meta.DB) {
			_, _, err := db.Inhume(tombAddr, 0, false, partAddr)
			require.NoError(t, err)
		}},
		{name: "stored with tombstone mark", assertErr: assertObjectAlreadyRemovedError, preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&partObj))
			_, _, err := db.Inhume(tombAddr, 0, false, parentAddr)
			require.NoError(t, err)
		}},
		{name: "tombstone only", assertErr: assertObjectAlreadyRemovedError, preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&tomb))
		}},
		{name: "stored with tombstone", assertErr: assertObjectAlreadyRemovedError, preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&partObj))
			require.NoError(t, db.Put(&tomb))
		}},
		{name: "garbage mark only", assertErr: assertObjectNotFoundError, preset: func(t *testing.T, db *meta.DB) {
			_, _, err := db.MarkGarbage(false, false, partAddr)
			require.NoError(t, err)
		}},
		{name: "stored with garbage mark", assertErr: assertObjectNotFoundError, preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&partObj))
			_, _, err := db.MarkGarbage(false, false, parentAddr)
			require.NoError(t, err)
		}},
		{name: "container garbage mark only", assertErr: assertObjectNotFoundError, preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.DeleteContainer(cnr))
		}},
		{name: "lock mark only", assertErr: assertObjectNotFoundError, preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Lock(cnr, locker.GetID(), []oid.ID{partID}))
		}},
		{name: "locker only", assertErr: assertObjectNotFoundError, preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&locker))
		}},
		{name: "expired", assertErr: assertObjectExpiredError, preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&expiredObj))
		}},
		{name: "expired with tombstone mark", assertErr: assertObjectExpiredError, preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&expiredObj))
			_, _, err := db.Inhume(tombAddr, 0, false, partAddr)
			require.NoError(t, err)
		}},
		{name: "expired with tombstone", assertErr: assertObjectExpiredError, preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&expiredObj))
			require.NoError(t, db.Put(&tomb))
		}},
		{name: "expired with garbage mark", assertErr: assertObjectExpiredError, preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&expiredObj))
			_, _, err := db.MarkGarbage(false, false, partAddr)
			require.NoError(t, err)
		}},
		{name: "expired with container garbage mark", assertErr: assertObjectNotFoundError, preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&expiredObj))
			require.NoError(t, db.DeleteContainer(cnr))
		}},
	} {
		tcs = append(tcs, testcase{name: tc.name, assertErr: tc.assertErr, preset: func(t *testing.T) *meta.DB {
			db := newDB(t, meta.WithEpochState(epochState{e: currentEpoch}))
			tc.preset(t, db)
			return db
		}})
	}

	for _, m := range []mode.Mode{mode.Degraded, mode.DegradedReadOnly, mode.Disabled} {
		tcs = append(tcs, testcase{name: "mode=" + m.String(), assertErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, meta.ErrDegradedMode)
		}, preset: func(t *testing.T) *meta.DB {
			db := newDB(t)
			require.NoError(t, db.SetMode(m))
			return db
		}})
	}

	// broken data
	tcs = append(tcs, testcase{
		name: "wrong OID len in parent index", assertErr: func(t *testing.T, err error) {
			require.EqualError(t, err, "invalid meta bucket key (prefix 0x2): wrong OID len 31")
		}, preset: func(t *testing.T) *meta.DB {
			return presetBoltDB(t, func(tx *bbolt.Tx) error {
				b, err := tx.CreateBucketIfNotExists(slices.Concat([]byte{0xFF}, cnr[:]))
				if err != nil {
					return err
				}
				k := slices.Concat([]byte{0x02}, []byte(object.FilterParentID), objectcore.MetaAttributeDelimiter,
					parentID[:], objectcore.MetaAttributeDelimiter, testutil.RandByteSlice(31),
				)
				return b.Put(k, nil)
			})
		}}, testcase{
		name: "zero OID in parent index", assertErr: func(t *testing.T, err error) {
			require.EqualError(t, err, "invalid meta bucket key (prefix 0x2): zero object ID")
		}, preset: func(t *testing.T) *meta.DB {
			return presetBoltDB(t, func(tx *bbolt.Tx) error {
				b, err := tx.CreateBucketIfNotExists(slices.Concat([]byte{0xFF}, cnr[:]))
				if err != nil {
					return err
				}
				k := slices.Concat([]byte{0x02}, []byte(object.FilterParentID), objectcore.MetaAttributeDelimiter,
					parentID[:], objectcore.MetaAttributeDelimiter, make([]byte, 32),
				)
				return b.Put(k, nil)
			})
		}},
	)

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			db := tc.preset(t)

			_, err := db.ResolveECPart(cnr, parentID, pi)
			tc.assertErr(t, err)
		})
	}

	// OK cases
	checkOK := func(t *testing.T, db *meta.DB, pi iec.PartInfo, exp oid.ID) {
		res, err := db.ResolveECPart(cnr, parentID, pi)
		require.NoError(t, err)
		require.Equal(t, exp, res)
	}

	for _, tc := range []testcase{
		{name: "stored with garbage mark and lock mark", preset: func(t *testing.T) *meta.DB {
			db := newDB(t)
			require.NoError(t, db.Put(&partObj))
			_, _, err := db.MarkGarbage(false, false, partAddr)
			require.NoError(t, err)
			require.NoError(t, db.Lock(cnr, locker.GetID(), []oid.ID{partID}))
			return db
		}},
		{name: "stored with garbage mark and locker", preset: func(t *testing.T) *meta.DB {
			db := newDB(t)
			require.NoError(t, db.Put(&partObj))
			_, _, err := db.MarkGarbage(false, false, partAddr)
			require.NoError(t, err)
			require.NoError(t, db.Put(&locker))
			return db
		}},
		{name: "expired with lock mark", preset: func(t *testing.T) *meta.DB {
			partObj := partObj
			addAttribute(&partObj, "__NEOFS__EXPIRATION_EPOCH", "123")

			db := newDB(t, meta.WithEpochState(epochState{e: 124}))
			require.NoError(t, db.Put(&partObj))
			require.NoError(t, db.Lock(cnr, locker.GetID(), []oid.ID{partID}))

			return db
		}},
		{name: "expired with locker", preset: func(t *testing.T) *meta.DB {
			partObj := partObj
			addAttribute(&partObj, "__NEOFS__EXPIRATION_EPOCH", "123")

			db := newDB(t, meta.WithEpochState(epochState{e: 124}))
			require.NoError(t, db.Put(&partObj))
			require.NoError(t, db.Put(&locker))

			return db
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := tc.preset(t)
			checkOK(t, db, pi, partID)
		})
	}

	t.Run("multiple", func(t *testing.T) {
		db := newDB(t)

		pi := pi
		const partNum = 10

		var partIDs []oid.ID
		for i := range partNum {
			pi.Index = i
			id := addPart(t, db, pi)
			partIDs = append(partIDs, id)
		}

		for i := range partIDs {
			pi.Index = i
			checkOK(t, db, pi, partIDs[i])
		}
	})

	db := newDB(t)
	require.NoError(t, db.Put(&partObj))
	checkOK(t, db, pi, partID)
}

func testExistsEC(t *testing.T) {
	state := epochState{e: 123}
	db := newDB(t,
		meta.WithEpochState(&state),
	)

	const partNum = 10
	cnr := cidtest.ID()
	signer := neofscryptotest.Signer()

	parent := *generateObjectWithCID(t, cnr)
	addAttribute(&parent, "__NEOFS__EXPIRATION_EPOCH", strconv.FormatUint(state.e, 10))

	parentAddr := objectcore.AddressOf(&parent)

	var partIDs []oid.ID
	for i := range partNum {
		part, err := iec.FormObjectForECPart(signer, parent, nil, iec.PartInfo{
			RuleIndex: 123, // any
			Index:     i,
		}) // payload does not matter
		require.NoError(t, err)

		require.NoError(t, db.Put(&part))

		partIDs = append(partIDs, part.GetID())
	}

	assertChildrenAvailable := func(t *testing.T) {
		for i := range partIDs {
			exists, err := db.Exists(oid.NewAddress(cnr, partIDs[i]), false)
			require.NoError(t, err)
			require.True(t, exists)
			exists, err = db.Exists(oid.NewAddress(cnr, partIDs[i]), true)
			require.NoError(t, err)
			require.True(t, exists)
		}
	}

	t.Run("parent expired", func(t *testing.T) {
		state.e++

		_, err := db.Exists(parentAddr, false)
		require.ErrorIs(t, err, meta.ErrObjectIsExpired)

		_, err = db.Exists(parentAddr, true)
		assertECPartsError(t, err, partIDs)

		assertChildrenAvailable(t)

		state.e--
	})

	_, err := db.Exists(parentAddr, false)
	assertECPartsError(t, err, partIDs)
	_, err = db.Exists(parentAddr, true)
	assertECPartsError(t, err, partIDs)

	assertChildrenAvailable(t)
}

func testGetEC(t *testing.T) {
	state := epochState{e: 123}
	db := newDB(t,
		meta.WithEpochState(&state),
	)

	const partNum = 10
	cnr := cidtest.ID()
	signer := neofscryptotest.Signer()

	parent := *generateObjectWithCID(t, cnr)
	addAttribute(&parent, "__NEOFS__EXPIRATION_EPOCH", strconv.FormatUint(state.e, 10))

	parentAddr := objectcore.AddressOf(&parent)

	var parts []object.Object
	var partIDs []oid.ID
	for i := range partNum {
		part, err := iec.FormObjectForECPart(signer, parent, nil, iec.PartInfo{
			RuleIndex: 123, // any
			Index:     i,
		}) // payload does not matter
		require.NoError(t, err)

		require.NoError(t, db.Put(&part))

		parts = append(parts, part)
		partIDs = append(partIDs, part.GetID())
	}

	assertChildrenAvailable := func(t *testing.T) {
		for i := range parts {
			_, err := db.Get(oid.NewAddress(cnr, parts[i].GetID()), false)
			require.NoError(t, err)
			_, err = db.Get(oid.NewAddress(cnr, parts[i].GetID()), true)
			require.NoError(t, err)
		}
	}

	t.Run("parent expired", func(t *testing.T) {
		state.e++

		_, err := db.Get(parentAddr, false)
		require.ErrorIs(t, err, meta.ErrObjectIsExpired)
		_, err = db.Get(parentAddr, true)
		require.ErrorIs(t, err, meta.ErrObjectIsExpired)

		assertChildrenAvailable(t)

		state.e--
	})

	got, err := db.Get(parentAddr, false)
	require.NoError(t, err)
	require.Equal(t, parent.CutPayload(), got)

	_, err = db.Get(parentAddr, true)
	require.ErrorIs(t, err, ierrors.ErrParentObject)
	var ecParts iec.ErrParts
	require.ErrorAs(t, err, &ecParts)
	require.ElementsMatch(t, partIDs, ecParts)

	assertChildrenAvailable(t)
}

func testInhumeEC(t *testing.T) {
	db := newDB(t)

	const partNum = 10
	cnr := cidtest.ID()
	signer := neofscryptotest.Signer()

	parent := *generateObjectWithCID(t, cnr)
	parentAddr := objectcore.AddressOf(&parent)

	var parts []object.Object
	var partAddrs []oid.Address
	for i := range partNum {
		part, err := iec.FormObjectForECPart(signer, parent, nil, iec.PartInfo{
			RuleIndex: 123, // any
			Index:     i,
		}) // payload does not matter
		require.NoError(t, err)

		require.NoError(t, db.Put(&part))

		parts = append(parts, part)
		partAddrs = append(partAddrs, objectcore.AddressOf(&parts[i]))
	}

	assertECGroupAvailable(t, db, parent, parts)

	tombAddr := oid.NewAddress(cnr, oidtest.ID())

	inhumed, _, err := db.Inhume(tombAddr, 0, false, parentAddr)
	require.NoError(t, err)

	allAddrs := append(partAddrs, parentAddr)

	for _, addr := range allAddrs {
		_, err = db.Exists(addr, true)
		assertObjectAlreadyRemovedError(t, err)
		_, err = db.Get(addr, false)
		assertObjectAlreadyRemovedError(t, err)
		_, err = db.Get(addr, true)
		assertObjectAlreadyRemovedError(t, err)
	}

	g, _, err := db.GetGarbage(100)
	require.NoError(t, err)
	require.ElementsMatch(t, g, append(partAddrs, parentAddr))

	g = g[:0]
	err = db.IterateOverGarbage(func(item meta.GarbageObject) error {
		g = append(g, item.Address())
		return nil
	}, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, g, append(partAddrs, parentAddr))

	g = g[:0]
	err = db.IterateOverGraveyard(func(item meta.TombstonedObject) error {
		g = append(g, item.Address())
		return nil
	}, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, g, append(partAddrs, parentAddr))

	require.EqualValues(t, len(parts), inhumed) // parent is virtual so it doesn't count
}

func testMarkGarbageEC(t *testing.T) {
	db := newDB(t)

	const partNum = 10
	cnr := cidtest.ID()
	signer := neofscryptotest.Signer()

	parent := *generateObjectWithCID(t, cnr)
	parentAddr := objectcore.AddressOf(&parent)

	var parts []object.Object
	var partAddrs []oid.Address
	for i := range partNum {
		part, err := iec.FormObjectForECPart(signer, parent, nil, iec.PartInfo{
			RuleIndex: 123, // any
			Index:     i,
		}) // payload does not matter
		require.NoError(t, err)

		require.NoError(t, db.Put(&part))

		parts = append(parts, part)
		partAddrs = append(partAddrs, objectcore.AddressOf(&parts[i]))
	}

	assertECGroupAvailable(t, db, parent, parts)

	inhumed, _, err := db.MarkGarbage(false, false, parentAddr)
	require.NoError(t, err)

	allAddrs := append(partAddrs, parentAddr)

	for _, addr := range allAddrs {
		_, err = db.Exists(addr, true)
		assertObjectNotFoundError(t, err)
		_, err = db.Get(addr, false)
		assertObjectNotFoundError(t, err)
		_, err = db.Get(addr, true)
		assertObjectNotFoundError(t, err)
	}

	g, _, err := db.GetGarbage(100)
	require.NoError(t, err)
	require.ElementsMatch(t, g, append(partAddrs, parentAddr))

	g = g[:0]
	err = db.IterateOverGarbage(func(item meta.GarbageObject) error {
		g = append(g, item.Address())
		return nil
	}, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, g, append(partAddrs, parentAddr))

	require.EqualValues(t, len(parts), inhumed) // parent is virtual so it doesn't count
}

func assertECGroupAvailable(t *testing.T, db *meta.DB, parent object.Object, parts []object.Object) {
	parentAddr := objectcore.AddressOf(&parent)

	partIDs := make([]oid.ID, len(parts))
	for i := range parts {
		partIDs[i] = parts[i].GetID()
	}

	_, err := db.Exists(parentAddr, true)
	assertECPartsError(t, err, partIDs)

	gotHdr, err := db.Get(parentAddr, false)
	require.NoError(t, err)
	require.Equal(t, parent.CutPayload(), gotHdr)

	_, err = db.Get(parentAddr, true)
	require.ErrorIs(t, err, ierrors.ErrParentObject)
	var ecParts iec.ErrParts
	require.ErrorAs(t, err, &ecParts)
	require.ElementsMatch(t, partIDs, ecParts)

	for i := range parts {
		partAddr := oid.NewAddress(parts[i].GetContainerID(), parts[i].GetID())

		exists, err2 := db.Exists(partAddr, true)
		require.NoError(t, err2)
		require.True(t, exists)

		_, err2 = db.Get(partAddr, false)
		require.NoError(t, err2)

		_, err2 = db.Get(partAddr, true)
		require.NoError(t, err2)
	}
}

func assertECPartsError(t *testing.T, err error, partIDs []oid.ID) {
	require.ErrorIs(t, err, ierrors.ErrParentObject)
	var ecParts iec.ErrParts
	require.ErrorAs(t, err, &ecParts)
	require.ElementsMatch(t, ecParts, partIDs)
}
