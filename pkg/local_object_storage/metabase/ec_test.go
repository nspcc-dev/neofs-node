package meta_test

import (
	"slices"
	"strconv"
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
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
	"go.etcd.io/bbolt"
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

	expiredObj := partObj // same address as partObj
	addAttribute(&expiredObj, "__NEOFS__EXPIRATION_EPOCH", strconv.Itoa(currentEpoch-1))

	locker := newBlankObject(cnr, oidtest.OtherID(partID))
	locker.AssociateLocked(partID)

	tomb := newBlankObject(cnr, oidtest.OtherID(partID, locker.GetID()))
	tomb.AssociateDeleted(partID)

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
			_, _, err := db.Inhume(tombAddr, 0, false, partAddr)
			require.NoError(t, err)
		}},
		{name: "tombstone only", assertErr: assertObjectNotFoundError, preset: func(t *testing.T, db *meta.DB) {
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
			_, _, err := db.MarkGarbage(false, false, partAddr)
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
	checkOK := func(t *testing.T, db *meta.DB) {
		res, err := db.ResolveECPart(cnr, parentID, pi)
		require.NoError(t, err)
		require.Equal(t, partObj.GetID(), res)
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
			checkOK(t, db)
		})
	}

	db := newDB(t)
	require.NoError(t, db.Put(&partObj))
	checkOK(t, db)
}
