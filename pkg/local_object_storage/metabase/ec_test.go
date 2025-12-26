package meta_test

import (
	"errors"
	"fmt"
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
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
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

	for b.Loop() {
		_, err = db.ResolveECPart(cnr, parentID, pi)
		require.NoError(b, err)
	}
}

func BenchmarkDB_ResolveECPartWithPayloadLen(b *testing.B) {
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

	for b.Loop() {
		_, _, err = db.ResolveECPartWithPayloadLen(cnr, parentID, pi)
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
		// artificially make parts of diff len, in reality they are the same
		payloadLen := 1000 + pi.Index
		obj, err := iec.FormObjectForECPart(signer, parentObj, testutil.RandByteSlice(payloadLen), pi)
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
		{name: "tombstone only", assertErr: assertObjectAlreadyRemovedError, preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&tomb))
		}},
		{name: "stored with tombstone", assertErr: assertObjectAlreadyRemovedError, preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&partObj))
			require.NoError(t, db.Put(&tomb))
		}},
		{name: "garbage mark only", assertErr: assertObjectNotFoundError, preset: func(t *testing.T, db *meta.DB) {
			_, _, err := db.MarkGarbage(partAddr)
			require.NoError(t, err)
		}},
		{name: "stored with garbage mark", assertErr: assertObjectNotFoundError, preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&partObj))
			_, _, err := db.MarkGarbage(parentAddr)
			require.NoError(t, err)
		}},
		{name: "container garbage mark only", assertErr: assertObjectNotFoundError, preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.DeleteContainer(cnr))
		}},
		{name: "locker only", assertErr: assertObjectNotFoundError, preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&locker))
		}},
		{name: "expired", assertErr: assertObjectExpiredError, preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&expiredObj))
		}},
		{name: "expired with tombstone", assertErr: assertObjectExpiredError, preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&expiredObj))
			require.NoError(t, db.Put(&tomb))
		}},
		{name: "expired with garbage mark", assertErr: assertObjectExpiredError, preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&expiredObj))
			_, _, err := db.MarkGarbage(partAddr)
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
			_, _, err = db.ResolveECPartWithPayloadLen(cnr, parentID, pi)
			tc.assertErr(t, err)
		})
	}

	// OK cases
	checkOKWithLenAndParent := func(t *testing.T, db *meta.DB, pi iec.PartInfo, parentID oid.ID, expID oid.ID, expLen int) {
		res, err := db.ResolveECPart(cnr, parentID, pi)
		require.NoError(t, err)
		require.Equal(t, expID, res)

		id, ln, err := db.ResolveECPartWithPayloadLen(cnr, parentID, pi)
		require.NoError(t, err)
		require.Equal(t, expID, id)
		require.EqualValues(t, expLen, ln)
	}

	checkOKWithLen := func(t *testing.T, db *meta.DB, pi iec.PartInfo, expID oid.ID, expLen int) {
		checkOKWithLenAndParent(t, db, pi, parentID, expID, expLen)
	}

	checkOK := func(t *testing.T, db *meta.DB, pi iec.PartInfo, exp oid.ID) {
		checkOKWithLen(t, db, pi, exp, 1000+pi.Index)
	}

	for _, tc := range []testcase{
		{name: "stored with garbage mark and locker", preset: func(t *testing.T) *meta.DB {
			db := newDB(t)
			require.NoError(t, db.Put(&partObj))
			_, _, err := db.MarkGarbage(partAddr)
			require.NoError(t, err)
			require.NoError(t, db.Put(&locker))
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

	for _, tc := range []struct {
		typ       object.Type
		associate func(*object.Object, oid.ID)
	}{
		{typ: object.TypeTombstone, associate: (*object.Object).AssociateDeleted},
		{typ: object.TypeLock, associate: (*object.Object).AssociateLocked},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			db := newDB(t)

			sysObj := *generateObjectWithCID(t, cnr)
			tc.associate(&sysObj, oidtest.ID())

			require.NoError(t, db.Put(&partObj))
			require.NoError(t, db.Put(&sysObj))

			id, err := db.ResolveECPart(cnr, sysObj.GetID(), pi)
			require.NoError(t, err)
			require.Equal(t, sysObj.GetID(), id)

			id, ln, err := db.ResolveECPartWithPayloadLen(cnr, sysObj.GetID(), pi)
			require.NoError(t, err)
			require.Equal(t, sysObj.GetID(), id)
			require.EqualValues(t, sysObj.PayloadSize(), ln)
		})
	}

	t.Run("size-split", func(t *testing.T) {
		linkerID := oidtest.OtherID(parentID)
		const linkerPayloadLen = 1234 // any
		lastID := oidtest.OtherID(parentID)

		linker := newBlankObject(cnr, linkerID)
		linker.SetParent(&parentObj)
		linker.SetType(object.TypeLink)
		linker.SetPayloadSize(linkerPayloadLen)

		last := newBlankObject(cnr, lastID)
		last.SetParent(&parentObj)
		last.SetFirstID(oidtest.ID()) // any

		t.Run("LINK only", func(t *testing.T) {
			db := newDB(t)

			require.NoError(t, db.Put(&linker))

			res, err := db.ResolveECPart(cnr, parentID, pi)
			require.NoError(t, err)
			require.Equal(t, linkerID, res)

			_, _, err = db.ResolveECPartWithPayloadLen(cnr, parentID, pi)
			require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

			checkOKWithLenAndParent(t, db, pi, linkerID, linkerID, linkerPayloadLen)
		})

		t.Run("last child only", func(t *testing.T) {
			db := newDB(t)

			require.NoError(t, db.Put(&last))

			_, err := db.ResolveECPart(cnr, parentID, pi)
			var se *object.SplitInfoError
			require.ErrorAs(t, err, &se)
			require.NotNil(t, se)
			si := se.SplitInfo()
			require.NotNil(t, si)
			require.Equal(t, lastID, si.GetLastPart())
			require.Zero(t, si.GetLink())
			require.Zero(t, si.GetFirstPart())
			require.Zero(t, si.SplitID())

			_, _, err = db.ResolveECPartWithPayloadLen(cnr, parentID, pi)
			require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

			_, err = db.ResolveECPart(cnr, lastID, pi)
			require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

			_, _, err = db.ResolveECPartWithPayloadLen(cnr, lastID, pi)
			require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
		})

		t.Run("LINK and last child", func(t *testing.T) {
			db := newDB(t)

			require.NoError(t, db.Put(&linker))
			require.NoError(t, db.Put(&last))

			res, err := db.ResolveECPart(cnr, parentID, pi)
			require.NoError(t, err)
			require.Equal(t, linkerID, res)

			_, _, err = db.ResolveECPartWithPayloadLen(cnr, parentID, pi)
			require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

			checkOKWithLenAndParent(t, db, pi, linkerID, linkerID, linkerPayloadLen)

			_, err = db.ResolveECPart(cnr, lastID, pi)
			require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

			_, _, err = db.ResolveECPartWithPayloadLen(cnr, lastID, pi)
			require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
		})
	})

	db := newDB(t)
	require.NoError(t, db.Put(&partObj))
	checkOK(t, db, pi, partID)
}

// mostly tested by TestDB_ResolveECPart.
func TestDB_ResolveECPartWithPayloadLen(t *testing.T) {
	cnr := cidtest.ID()
	parentID := oidtest.ID()
	signer := neofscryptotest.Signer()
	pi := iec.PartInfo{
		RuleIndex: 12,
		Index:     34,
	}

	parentObj := newBlankObject(cnr, parentID)

	const partPayloadLen = 32
	partObj, err := iec.FormObjectForECPart(signer, parentObj, testutil.RandByteSlice(partPayloadLen), pi)
	require.NoError(t, err)
	partID := partObj.GetID()

	db := newDB(t)
	require.NoError(t, db.Put(&partObj))

	t.Run("broken payload len index", func(t *testing.T) {
		t.Run("missing", func(t *testing.T) {
			db := newDB(t)
			require.NoError(t, db.Put(&partObj))

			corruptDB(t, db, func(tx *bbolt.Tx) error {
				b := tx.Bucket(slices.Concat([]byte{0xFF}, cnr[:]))
				if b == nil {
					return errors.New("[test] missing meta bucket")
				}
				key := slices.Concat([]byte{0x03}, partID[:], []byte("$Object:payloadLength"), []byte{0x00}, []byte(strconv.Itoa(partPayloadLen)))
				return b.Delete(key)
			})

			_, _, err := db.ResolveECPartWithPayloadLen(cnr, parentID, pi)
			var id ierrors.ObjectID
			require.ErrorAs(t, err, &id)
			require.EqualValues(t, partID, id)
			require.EqualError(t, err, fmt.Sprintf("missing index for payload len attribute of object %s", partID))
		})

		t.Run("wrong format", func(t *testing.T) {
			db := newDB(t)
			require.NoError(t, db.Put(&partObj))

			corruptDB(t, db, func(tx *bbolt.Tx) error {
				b := tx.Bucket(slices.Concat([]byte{0xFF}, cnr[:]))
				if b == nil {
					return errors.New("[test] missing meta bucket")
				}
				key := slices.Concat([]byte{0x03}, partID[:], []byte("$Object:payloadLength"), []byte{0x00}, []byte("-1"))
				return b.Put(key, nil)
			})

			_, _, err := db.ResolveECPartWithPayloadLen(cnr, parentID, pi)
			var id ierrors.ObjectID
			require.ErrorAs(t, err, &id)
			require.EqualValues(t, partID, id)
			require.EqualError(t, err, fmt.Sprintf("invalid payload len attribute of object %s: parse to uint64: invalid syntax", partID))
		})
	})
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

	assertChildrenAvailable := func(t *testing.T, expired bool) {
		for i := range partIDs {
			exists, err := db.Exists(oid.NewAddress(cnr, partIDs[i]), false)
			if expired {
				require.ErrorIs(t, err, meta.ErrObjectIsExpired)
				require.False(t, exists)
			} else {
				require.NoError(t, err)
				require.True(t, exists)
			}
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

		assertChildrenAvailable(t, true)

		state.e--
	})

	_, err := db.Exists(parentAddr, false)
	assertECPartsError(t, err, partIDs)
	_, err = db.Exists(parentAddr, true)
	assertECPartsError(t, err, partIDs)

	assertChildrenAvailable(t, false)
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

	assertChildrenAvailable := func(t *testing.T, expired bool) {
		for i := range parts {
			_, err1 := db.Get(oid.NewAddress(cnr, parts[i].GetID()), false)
			_, err2 := db.Get(oid.NewAddress(cnr, parts[i].GetID()), true)
			if expired {
				require.ErrorIs(t, err1, meta.ErrObjectIsExpired)
				require.ErrorIs(t, err2, meta.ErrObjectIsExpired)
			} else {
				require.NoError(t, err1)
				require.NoError(t, err2)
			}
		}
	}

	t.Run("parent expired", func(t *testing.T) {
		state.e++

		_, err := db.Get(parentAddr, false)
		require.ErrorIs(t, err, meta.ErrObjectIsExpired)
		_, err = db.Get(parentAddr, true)
		require.ErrorIs(t, err, meta.ErrObjectIsExpired)

		assertChildrenAvailable(t, true)

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

	assertChildrenAvailable(t, false)
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

	err := db.Put(createTSForObject(cnr, parent.GetID()))
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
	err = db.IterateOverGarbage(func(id oid.ID) error {
		g = append(g, oid.NewAddress(cnr, id))
		return nil
	}, cnr, oid.ID{})
	require.NoError(t, err)
	require.ElementsMatch(t, g, append(partAddrs, parentAddr))
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

	inhumed, _, err := db.MarkGarbage(parentAddr)
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
	err = db.IterateOverGarbage(func(id oid.ID) error {
		g = append(g, oid.NewAddress(cnr, id))
		return nil
	}, cnr, oid.ID{})
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
