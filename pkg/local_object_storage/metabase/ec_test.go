package meta_test

import (
	"path"
	"slices"
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
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
	"go.etcd.io/bbolt"
)

func TestDB_ResolveECPart(t *testing.T) {
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

	locker := newBlankObject(cnr, oidtest.OtherID(partID))
	locker.AssociateLocked(partID)

	tomb := newBlankObject(cnr, oidtest.OtherID(partID, locker.GetID()))
	tomb.AssociateDeleted(partID)

	partAddr := oid.NewAddress(cnr, partID)
	tombAddr := oid.NewAddress(cnr, tomb.GetID())

	assertNotFound := func(t *testing.T, err error) { require.ErrorIs(t, err, apistatus.ErrObjectNotFound) }
	assertAlreadyRemoved := func(t *testing.T, err error) { require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved) }
	assertExpired := func(t *testing.T, err error) { require.ErrorIs(t, err, meta.ErrObjectIsExpired) }

	type testcase struct {
		name      string
		assertErr func(*testing.T, error)
		preset    func(t *testing.T) *meta.DB
	}

	// failure cases

	tcs := []testcase{
		{name: "empty DB", assertErr: assertNotFound, preset: func(t *testing.T) *meta.DB {
			return newDB(t)
		}},
		{name: "without parent object", assertErr: assertNotFound, preset: func(t *testing.T) *meta.DB {
			db := newDB(t)
			noParentObj := newBlankObject(cnr, oidtest.ID())
			require.NoError(t, db.Put(&noParentObj))
			return db
		}},
		{name: "broken parent index", assertErr: func(t *testing.T, err error) {
			require.EqualError(t, err, "invalid meta bucket key (prefix 0x2): wrong OID len 31")
		}, preset: func(t *testing.T) *meta.DB {
			p := path.Join(t.TempDir(), "meta.db")
			const perm = 0o600

			db := meta.New(
				meta.WithPath(p),
				meta.WithPermissions(perm),
				meta.WithEpochState(epochState{}),
			)

			require.NoError(t, db.Open(false))
			t.Cleanup(func() { _ = db.Close() })
			require.NoError(t, db.Init())

			require.NoError(t, db.Close())

			boltDB, err := bbolt.Open(p, perm, nil)
			require.NoError(t, err)
			require.NoError(t, boltDB.Update(func(tx *bbolt.Tx) error {
				b, err := tx.CreateBucketIfNotExists(slices.Concat([]byte{0xFF}, cnr[:]))
				if err != nil {
					return err
				}
				k := slices.Concat([]byte{0x02}, []byte(object.FilterParentID), objectcore.MetaAttributeDelimiter,
					parentID[:], objectcore.MetaAttributeDelimiter, testutil.RandByteSlice(31),
				)
				return b.Put(k, nil)
			}))
			require.NoError(t, boltDB.Close())

			require.NoError(t, db.Open(true))
			return db
		}},
		{name: "another rule", assertErr: assertNotFound, preset: func(t *testing.T) *meta.DB {
			db := newDB(t)
			addPart(t, db, iec.PartInfo{RuleIndex: pi.RuleIndex + 1, Index: pi.Index})
			return db
		}},
		{name: "another part", assertErr: assertNotFound, preset: func(t *testing.T) *meta.DB {
			db := newDB(t)
			addPart(t, db, iec.PartInfo{RuleIndex: pi.RuleIndex, Index: pi.Index + 1})
			return db
		}},
		{name: "another rule and part", assertErr: assertNotFound, preset: func(t *testing.T) *meta.DB {
			db := newDB(t)
			addPart(t, db, iec.PartInfo{RuleIndex: pi.RuleIndex + 1, Index: pi.Index + 1})
			return db
		}},
		{name: "tombstone mark only", assertErr: assertNotFound, preset: func(t *testing.T) *meta.DB {
			db := newDB(t)
			_, _, err := db.Inhume(tombAddr, 0, false, partAddr)
			require.NoError(t, err)
			return db
		}},
		{name: "stored with tombstone mark", assertErr: assertAlreadyRemoved, preset: func(t *testing.T) *meta.DB {
			db := newDB(t)
			require.NoError(t, db.Put(&partObj))
			_, _, err := db.Inhume(tombAddr, 0, false, partAddr)
			require.NoError(t, err)
			return db
		}},
		{name: "tombstone only", assertErr: assertNotFound, preset: func(t *testing.T) *meta.DB {
			db := newDB(t)
			require.NoError(t, db.Put(&tomb))
			return db
		}},
		{name: "stored with tombstone", assertErr: assertAlreadyRemoved, preset: func(t *testing.T) *meta.DB {
			db := newDB(t)
			require.NoError(t, db.Put(&partObj))
			require.NoError(t, db.Put(&tomb))
			return db
		}},
		{name: "garbage mark only", assertErr: assertNotFound, preset: func(t *testing.T) *meta.DB {
			db := newDB(t)
			_, _, err := db.MarkGarbage(false, false, partAddr)
			require.NoError(t, err)
			return db
		}},
		{name: "stored with garbage mark", assertErr: assertNotFound, preset: func(t *testing.T) *meta.DB {
			db := newDB(t)
			require.NoError(t, db.Put(&partObj))
			_, _, err := db.MarkGarbage(false, false, partAddr)
			require.NoError(t, err)
			return db
		}},
		{name: "container garbage mark only", assertErr: assertNotFound, preset: func(t *testing.T) *meta.DB {
			db := newDB(t)
			require.NoError(t, db.DeleteContainer(cnr))
			return db
		}},
		{name: "lock mark only", assertErr: assertNotFound, preset: func(t *testing.T) *meta.DB {
			db := newDB(t)
			require.NoError(t, db.Lock(cnr, locker.GetID(), []oid.ID{partID}))
			return db
		}},
		{name: "locker only", assertErr: assertNotFound, preset: func(t *testing.T) *meta.DB {
			db := newDB(t)
			require.NoError(t, db.Put(&locker))
			return db
		}},
		{name: "expired", assertErr: assertExpired, preset: func(t *testing.T) *meta.DB {
			partObj := partObj
			addAttribute(&partObj, "__NEOFS__EXPIRATION_EPOCH", "123")

			db := newDB(t, meta.WithEpochState(epochState{e: 124}))
			require.NoError(t, db.Put(&partObj))
			return db
		}},
		{name: "expired with tombstone mark", assertErr: assertExpired, preset: func(t *testing.T) *meta.DB {
			partObj := partObj
			addAttribute(&partObj, "__NEOFS__EXPIRATION_EPOCH", "123")

			db := newDB(t, meta.WithEpochState(epochState{e: 124}))
			require.NoError(t, db.Put(&partObj))
			_, _, err := db.Inhume(tombAddr, 0, false, partAddr)
			require.NoError(t, err)

			return db
		}},
		{name: "expired with tombstone", assertErr: assertExpired, preset: func(t *testing.T) *meta.DB {
			partObj := partObj
			addAttribute(&partObj, "__NEOFS__EXPIRATION_EPOCH", "123")

			db := newDB(t, meta.WithEpochState(epochState{e: 124}))
			require.NoError(t, db.Put(&partObj))
			require.NoError(t, db.Put(&tomb))

			return db
		}},
		{name: "expired with garbage mark", assertErr: assertExpired, preset: func(t *testing.T) *meta.DB {
			partObj := partObj
			addAttribute(&partObj, "__NEOFS__EXPIRATION_EPOCH", "123")

			db := newDB(t, meta.WithEpochState(epochState{e: 124}))
			require.NoError(t, db.Put(&partObj))
			_, _, err := db.MarkGarbage(false, false, partAddr)
			require.NoError(t, err)

			return db
		}},
		{name: "expired with container garbage mark", assertErr: assertNotFound, preset: func(t *testing.T) *meta.DB {
			partObj := partObj
			addAttribute(&partObj, "__NEOFS__EXPIRATION_EPOCH", "123")

			db := newDB(t, meta.WithEpochState(epochState{e: 124}))
			require.NoError(t, db.Put(&partObj))
			require.NoError(t, db.DeleteContainer(cnr))

			return db
		}},
	}

	for _, m := range []mode.Mode{mode.Degraded, mode.DegradedReadOnly, mode.Disabled} {
		tcs = append(tcs, testcase{
			name:      "mode=" + m.String(),
			assertErr: func(t *testing.T, err error) { require.ErrorIs(t, err, meta.ErrDegradedMode) },
			preset: func(t *testing.T) *meta.DB {
				db := newDB(t)
				require.NoError(t, db.SetMode(m))
				return db
			},
		})
	}

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
		{name: "stored with tombstone mark and lock mark", preset: func(t *testing.T) *meta.DB {
			db := newDB(t)
			require.NoError(t, db.Put(&partObj))
			_, _, err := db.Inhume(tombAddr, 0, false, partAddr)
			require.NoError(t, err)
			require.NoError(t, db.Lock(cnr, locker.GetID(), []oid.ID{partID}))
			return db
		}},
		{name: "stored with tombstone mark and locker", preset: func(t *testing.T) *meta.DB {
			db := newDB(t)
			require.NoError(t, db.Put(&partObj))
			_, _, err := db.Inhume(tombAddr, 0, false, partAddr)
			require.NoError(t, err)
			require.NoError(t, db.Put(&locker))
			return db
		}},
		{name: "stored with tombstone and lock mark", preset: func(t *testing.T) *meta.DB {
			db := newDB(t)
			require.NoError(t, db.Put(&partObj))
			require.NoError(t, db.Put(&tomb))
			require.NoError(t, db.Lock(cnr, locker.GetID(), []oid.ID{partID}))
			return db
		}},
		{name: "stored with tombstone and locker", preset: func(t *testing.T) *meta.DB {
			db := newDB(t)
			require.NoError(t, db.Put(&partObj))
			require.NoError(t, db.Put(&tomb))
			require.NoError(t, db.Put(&locker))
			return db
		}},
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
