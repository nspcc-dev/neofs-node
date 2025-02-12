package meta

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	objectconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

type epochStateImpl struct{}

func (s epochStateImpl) CurrentEpoch() uint64 {
	return 0
}

func TestVersion(t *testing.T) {
	dir := t.TempDir()

	newDB := func(t *testing.T) *DB {
		return New(WithPath(filepath.Join(dir, t.Name())),
			WithPermissions(0o600), WithEpochState(epochStateImpl{}))
	}
	check := func(t *testing.T, db *DB) {
		require.NoError(t, db.boltDB.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket(shardInfoBucket)
			if b == nil {
				return errors.New("shard info bucket not found")
			}
			data := b.Get(versionKey)
			if len(data) != 8 {
				return errors.New("invalid version data")
			}
			if stored := binary.LittleEndian.Uint64(data); stored != currentMetaVersion {
				return fmt.Errorf("invalid version: %d != %d", stored, currentMetaVersion)
			}
			return nil
		}))
	}
	t.Run("simple", func(t *testing.T) {
		db := newDB(t)
		require.NoError(t, db.Open(false))
		require.NoError(t, db.Init())
		check(t, db)
		require.NoError(t, db.Close())

		t.Run("reopen", func(t *testing.T) {
			require.NoError(t, db.Open(false))
			require.NoError(t, db.Init())
			check(t, db)
			require.NoError(t, db.Close())
		})
	})
	t.Run("old data", func(t *testing.T) {
		db := newDB(t)
		require.NoError(t, db.Open(false))
		require.NoError(t, db.WriteShardID([]byte{1, 2, 3, 4}))
		require.NoError(t, db.Close())

		require.NoError(t, db.Open(false))
		require.NoError(t, db.Init())
		check(t, db)
		require.NoError(t, db.Close())
	})
	t.Run("invalid version", func(t *testing.T) {
		db := newDB(t)
		require.NoError(t, db.Open(false))
		require.NoError(t, db.boltDB.Update(func(tx *bbolt.Tx) error {
			return updateVersion(tx, currentMetaVersion+1)
		}))
		require.NoError(t, db.Close())

		require.NoError(t, db.Open(false))
		require.Error(t, db.Init())
		require.NoError(t, db.Close())

		t.Run("reset", func(t *testing.T) {
			require.NoError(t, db.Open(false))
			require.NoError(t, db.Reset())
			check(t, db)
			require.NoError(t, db.Close())
		})
	})
}

type inhumeV2Prm struct {
	tomb   *oid.Address
	target []oid.Address

	lockObjectHandling bool

	forceRemoval bool
}

func (db *DB) inhumeV2(prm inhumeV2Prm) (uint64, []oid.Address, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return 0, nil, ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return 0, nil, ErrReadOnlyMode
	}

	var (
		currEpoch       = db.epochState.CurrentEpoch()
		deletedLockObjs []oid.Address
		err             error
		inhumed         uint64
	)

	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		garbageObjectsBKT := tx.Bucket(garbageObjectsBucketName)
		garbageContainersBKT := tx.Bucket(garbageContainersBucketName)
		graveyardBKT := tx.Bucket(graveyardBucketName)

		var (
			// target bucket of the operation, one of the:
			//	1. Graveyard if Inhume was called with a Tombstone
			//	2. Garbage if Inhume was called with a GC mark
			bkt *bbolt.Bucket
			// value that will be put in the bucket, one of the:
			// 1. tombstone address if Inhume was called with
			//    a Tombstone
			// 2. zeroValue if Inhume was called with a GC mark
			value []byte
		)

		if prm.tomb != nil {
			bkt = graveyardBKT
			tombKey := addressKey(*prm.tomb, make([]byte, addressKeySize))

			// it is forbidden to have a tomb-on-tomb in NeoFS,
			// so graveyard keys must not be addresses of tombstones
			data := bkt.Get(tombKey)
			if data != nil {
				err := bkt.Delete(tombKey)
				if err != nil {
					return fmt.Errorf("could not remove grave with tombstone key: %w", err)
				}
			}

			value = tombKey
		} else {
			bkt = garbageObjectsBKT
			value = zeroValue
		}

		buf := make([]byte, addressKeySize)
		for i := range prm.target {
			id := prm.target[i].Object()
			cnr := prm.target[i].Container()

			// prevent locked objects to be inhumed
			if !prm.forceRemoval && objectLocked(tx, cnr, id) {
				return apistatus.ObjectLocked{}
			}

			var lockWasChecked bool

			// prevent lock objects to be inhumed
			// if `Inhume` was called not with the
			// `WithForceGCMark` option
			if !prm.forceRemoval {
				if isLockObject(tx, cnr, id) {
					return ErrLockObjectRemoval
				}

				lockWasChecked = true
			}

			obj, err := db.get(tx, prm.target[i], buf, false, true, currEpoch)
			targetKey := addressKey(prm.target[i], buf)
			if err == nil {
				if inGraveyardWithKey(targetKey, graveyardBKT, garbageObjectsBKT, garbageContainersBKT) == 0 {
					// object is available, decrement the
					// logical counter
					inhumed++
				}

				// if object is stored, and it is regular object then update bucket
				// with container size estimations
				if obj.Type() == object.TypeRegular {
					err := changeContainerSize(tx, cnr, obj.PayloadSize(), false)
					if err != nil {
						return err
					}
				}
			}

			if prm.tomb != nil {
				targetIsTomb := false

				// iterate over graveyard and check if target address
				// is the address of tombstone in graveyard.
				err = bkt.ForEach(func(k, v []byte) error {
					// check if graveyard has record with key corresponding
					// to tombstone address (at least one)
					targetIsTomb = bytes.Equal(v, targetKey)

					if targetIsTomb {
						// break bucket iterator
						return errBreakBucketForEach
					}

					return nil
				})
				if err != nil && !errors.Is(err, errBreakBucketForEach) {
					return err
				}

				// do not add grave if target is a tombstone
				if targetIsTomb {
					continue
				}

				// if tombstone appears object must be
				// additionally marked with GC
				err = garbageObjectsBKT.Put(targetKey, zeroValue)
				if err != nil {
					return err
				}
			}

			// consider checking if target is already in graveyard?
			err = bkt.Put(targetKey, value)
			if err != nil {
				return err
			}

			if prm.lockObjectHandling {
				// do not perform lock check if
				// it was already called
				if lockWasChecked {
					// inhumed object is not of
					// the LOCK type
					continue
				}

				if isLockObject(tx, cnr, id) {
					deletedLockObjs = append(deletedLockObjs, prm.target[i])
				}
			}
		}

		return db.updateCounter(tx, logical, inhumed, false)
	})

	return inhumed, deletedLockObjs, err
}

type epochState uint64

func (s epochState) CurrentEpoch() uint64 { return uint64(s) }

func testEpochState(e uint64) *epochState {
	s := epochState(e)
	return &s
}

func newDB(t testing.TB, opts ...Option) *DB {
	p := path.Join(t.TempDir(), "meta.db")

	bdb := New(
		append([]Option{
			WithPath(p),
			WithPermissions(0o600),
			WithEpochState(testEpochState(123)),
		}, opts...)...,
	)

	require.NoError(t, bdb.Open(false))
	require.NoError(t, bdb.Init())

	t.Cleanup(func() {
		bdb.Close()
		os.Remove(bdb.DumpInfo().Path)
	})

	return bdb
}

func TestMigrate2to3(t *testing.T) {
	const testEpoch = 123
	expectedEpoch := uint64(testEpoch + objectconfig.DefaultTombstoneLifetime)
	expectedEpochRaw := make([]byte, 8)
	binary.LittleEndian.PutUint64(expectedEpochRaw, expectedEpoch)

	db := newDB(t, WithEpochState(testEpochState(testEpoch)))

	testObjs := oidtest.Addresses(1024)
	tomb := oidtest.Address()
	tombRaw := addressKey(tomb, make([]byte, addressKeySize))

	_, _, err := db.inhumeV2(inhumeV2Prm{
		target: testObjs,
		tomb:   &tomb,
	})
	require.NoError(t, err)

	// inhumeV2 stores data in the old format, but new DB has current version by default, force old version.
	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		return updateVersion(tx, 2)
	})
	require.NoError(t, err)

	db.mode = mode.DegradedReadOnly // Force reload.
	ok, err := db.Reload(WithPath(db.info.Path), WithEpochState(testEpochState(123)))
	require.NoError(t, err)
	require.True(t, ok)

	err = db.Init() // Migration happens here.
	require.NoError(t, err)

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(graveyardBucketName).ForEach(func(k, v []byte) error {
			require.Len(t, v, addressKeySize+8)
			require.Equal(t, v[:addressKeySize], tombRaw)
			require.Equal(t, v[addressKeySize:], expectedEpochRaw)

			return nil
		})
	})
	require.NoError(t, err)
	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		gotV, ok := getVersion(tx)
		if !ok {
			return errors.New("missing version")
		}
		if gotV != currentMetaVersion {
			return errors.New("version was not updated")
		}
		return nil
	})
	require.NoError(t, err)
}

func TestMigrate3to4(t *testing.T) {
	db := newDB(t)

	typs := []object.Type{object.TypeRegular, object.TypeTombstone, object.TypeStorageGroup, object.TypeLock, object.TypeLink}
	objs := make([]object.Object, len(typs))
	var css, hcss [][]byte
	for i := range objs {
		objs[i].SetContainerID(cidtest.ID())
		id := oidtest.ID()
		objs[i].SetID(id)
		ver := version.New(uint32(100*i), uint32(100*i+1))
		objs[i].SetVersion(&ver)
		objs[i].SetOwner(usertest.ID())
		objs[i].SetType(typs[i])
		objs[i].SetCreationEpoch(rand.Uint64())
		objs[i].SetPayloadSize(rand.Uint64())
		objs[i].SetPayloadChecksum(checksum.NewSHA256(id))
		css = append(css, id[:])
		var tzh [tz.Size]byte
		rand.Read(tzh[:]) //nolint:staticcheck
		objs[i].SetPayloadHomomorphicHash(checksum.NewTillichZemor(tzh))
		hcss = append(hcss, tzh[:])
		sid := objecttest.SplitID()
		objs[i].SetSplitID(&sid)
		objs[i].SetParentID(oidtest.ID())
		objs[i].SetFirstID(oidtest.ID())
		objs[i].SetAttributes(*object.NewAttribute("Index", strconv.Itoa(i)))
	}

	var par object.Object
	par.SetContainerID(objs[0].GetContainerID())
	par.SetID(oidtest.ID())
	ver := version.New(1000, 1001)
	par.SetVersion(&ver)
	par.SetOwner(usertest.ID())
	par.SetType(typs[0])
	par.SetCreationEpoch(rand.Uint64())
	par.SetPayloadSize(rand.Uint64())
	pcs := oidtest.ID()
	par.SetPayloadChecksum(checksum.NewSHA256(pcs))
	var phcs [tz.Size]byte
	rand.Read(phcs[:]) //nolint:staticcheck
	par.SetPayloadHomomorphicHash(checksum.NewTillichZemor(phcs))
	sid := objecttest.SplitID()
	par.SetSplitID(&sid)
	par.SetParentID(oidtest.ID())
	par.SetFirstID(oidtest.ID())
	par.SetAttributes(*object.NewAttribute("Index", "9999"))

	objs[0].SetParent(&par)

	for _, item := range []struct {
		pref byte
		hdr  *object.Object
	}{
		{pref: 0x06, hdr: &objs[0]},
		{pref: 0x06, hdr: &par},
		{pref: 0x09, hdr: &objs[1]},
		{pref: 0x08, hdr: &objs[2]},
		{pref: 0x07, hdr: &objs[3]},
		{pref: 0x12, hdr: &objs[4]},
	} {
		err := db.boltDB.Update(func(tx *bbolt.Tx) error {
			cnr := item.hdr.GetContainerID()
			bkt, err := tx.CreateBucketIfNotExists(slices.Concat([]byte{item.pref}, cnr[:]))
			require.NoError(t, err)
			id := item.hdr.GetID()
			return bkt.Put(id[:], item.hdr.Marshal())
		})
		require.NoError(t, err)
	}

	// force old version
	err := db.boltDB.Update(func(tx *bbolt.Tx) error {
		if err := tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			if name[0] == 0xFF {
				return tx.DeleteBucket(name)
			}
			return nil
		}); err != nil {
			return err
		}

		bkt := tx.Bucket([]byte{0x05})
		require.NotNil(t, bkt)
		return bkt.Put([]byte("version"), []byte{0x03, 0, 0, 0, 0, 0, 0, 0})
	})
	require.NoError(t, err)
	// migrate
	require.NoError(t, db.Init())
	// check
	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket([]byte{0x05})
		require.NotNil(t, bkt)
		require.Equal(t, []byte{0x04, 0, 0, 0, 0, 0, 0, 0}, bkt.Get([]byte("version")))
		return nil
	})
	require.NoError(t, err)

	res, _, err := db.Search(objs[0].GetContainerID(), nil, nil, nil, nil, 1000)
	require.NoError(t, err)
	require.Len(t, res, 2)
	require.True(t, slices.ContainsFunc(res, func(r client.SearchResultItem) bool { return r.ID == objs[0].GetID() }))
	require.True(t, slices.ContainsFunc(res, func(r client.SearchResultItem) bool { return r.ID == par.GetID() }))

	for i := range objs[1:] {
		res, _, err := db.Search(objs[1+i].GetContainerID(), nil, nil, nil, nil, 1000)
		require.NoError(t, err, i)
		require.Len(t, res, 1, i)
		require.Equal(t, objs[1+i].GetID(), res[0].ID, i)
	}

	for _, tc := range []struct {
		attr string
		val  string
		cnr  cid.ID
		exp  oid.ID
		par  bool
	}{
		{attr: "$Object:version", val: "v0.1", cnr: objs[0].GetContainerID(), exp: objs[0].GetID()},
		{attr: "$Object:version", val: "v100.101", cnr: objs[1].GetContainerID(), exp: objs[1].GetID()},
		{attr: "$Object:version", val: "v200.201", cnr: objs[2].GetContainerID(), exp: objs[2].GetID()},
		{attr: "$Object:version", val: "v300.301", cnr: objs[3].GetContainerID(), exp: objs[3].GetID()},
		{attr: "$Object:version", val: "v400.401", cnr: objs[4].GetContainerID(), exp: objs[4].GetID()},
		{attr: "$Object:version", val: "v1000.1001", cnr: par.GetContainerID(), exp: par.GetID()},
		{attr: "$Object:ownerID", val: objs[0].Owner().String(), cnr: objs[0].GetContainerID(), exp: objs[0].GetID()},
		{attr: "$Object:ownerID", val: objs[1].Owner().String(), cnr: objs[1].GetContainerID(), exp: objs[1].GetID()},
		{attr: "$Object:ownerID", val: objs[2].Owner().String(), cnr: objs[2].GetContainerID(), exp: objs[2].GetID()},
		{attr: "$Object:ownerID", val: objs[3].Owner().String(), cnr: objs[3].GetContainerID(), exp: objs[3].GetID()},
		{attr: "$Object:ownerID", val: objs[4].Owner().String(), cnr: objs[4].GetContainerID(), exp: objs[4].GetID()},
		{attr: "$Object:ownerID", val: par.Owner().String(), cnr: par.GetContainerID(), exp: par.GetID()},
		{attr: "$Object:objectType", val: "REGULAR", cnr: objs[0].GetContainerID(), par: true},
		{attr: "$Object:objectType", val: "TOMBSTONE", cnr: objs[1].GetContainerID(), exp: objs[1].GetID()},
		{attr: "$Object:objectType", val: "STORAGE_GROUP", cnr: objs[2].GetContainerID(), exp: objs[2].GetID()},
		{attr: "$Object:objectType", val: "LOCK", cnr: objs[3].GetContainerID(), exp: objs[3].GetID()},
		{attr: "$Object:objectType", val: "LINK", cnr: objs[4].GetContainerID(), exp: objs[4].GetID()},
		{attr: "$Object:creationEpoch", val: strconv.FormatUint(objs[0].CreationEpoch(), 10), cnr: objs[0].GetContainerID(), exp: objs[0].GetID()},
		{attr: "$Object:creationEpoch", val: strconv.FormatUint(objs[1].CreationEpoch(), 10), cnr: objs[1].GetContainerID(), exp: objs[1].GetID()},
		{attr: "$Object:creationEpoch", val: strconv.FormatUint(objs[2].CreationEpoch(), 10), cnr: objs[2].GetContainerID(), exp: objs[2].GetID()},
		{attr: "$Object:creationEpoch", val: strconv.FormatUint(objs[3].CreationEpoch(), 10), cnr: objs[3].GetContainerID(), exp: objs[3].GetID()},
		{attr: "$Object:creationEpoch", val: strconv.FormatUint(objs[4].CreationEpoch(), 10), cnr: objs[4].GetContainerID(), exp: objs[4].GetID()},
		{attr: "$Object:creationEpoch", val: strconv.FormatUint(par.CreationEpoch(), 10), cnr: par.GetContainerID(), exp: par.GetID()},
		{attr: "$Object:payloadLength", val: strconv.FormatUint(objs[0].PayloadSize(), 10), cnr: objs[0].GetContainerID(), exp: objs[0].GetID()},
		{attr: "$Object:payloadLength", val: strconv.FormatUint(objs[1].PayloadSize(), 10), cnr: objs[1].GetContainerID(), exp: objs[1].GetID()},
		{attr: "$Object:payloadLength", val: strconv.FormatUint(objs[2].PayloadSize(), 10), cnr: objs[2].GetContainerID(), exp: objs[2].GetID()},
		{attr: "$Object:payloadLength", val: strconv.FormatUint(objs[3].PayloadSize(), 10), cnr: objs[3].GetContainerID(), exp: objs[3].GetID()},
		{attr: "$Object:payloadLength", val: strconv.FormatUint(objs[4].PayloadSize(), 10), cnr: objs[4].GetContainerID(), exp: objs[4].GetID()},
		{attr: "$Object:payloadLength", val: strconv.FormatUint(par.PayloadSize(), 10), cnr: par.GetContainerID(), exp: par.GetID()},
		{attr: "$Object:payloadHash", val: hex.EncodeToString(css[0]), cnr: objs[0].GetContainerID(), exp: objs[0].GetID()},
		{attr: "$Object:payloadHash", val: hex.EncodeToString(css[1]), cnr: objs[1].GetContainerID(), exp: objs[1].GetID()},
		{attr: "$Object:payloadHash", val: hex.EncodeToString(css[2]), cnr: objs[2].GetContainerID(), exp: objs[2].GetID()},
		{attr: "$Object:payloadHash", val: hex.EncodeToString(css[3]), cnr: objs[3].GetContainerID(), exp: objs[3].GetID()},
		{attr: "$Object:payloadHash", val: hex.EncodeToString(css[4]), cnr: objs[4].GetContainerID(), exp: objs[4].GetID()},
		{attr: "$Object:payloadHash", val: hex.EncodeToString(pcs[:]), cnr: par.GetContainerID(), exp: par.GetID()},
		{attr: "$Object:homomorphicHash", val: hex.EncodeToString(hcss[0]), cnr: objs[0].GetContainerID(), exp: objs[0].GetID()},
		{attr: "$Object:homomorphicHash", val: hex.EncodeToString(hcss[1]), cnr: objs[1].GetContainerID(), exp: objs[1].GetID()},
		{attr: "$Object:homomorphicHash", val: hex.EncodeToString(hcss[2]), cnr: objs[2].GetContainerID(), exp: objs[2].GetID()},
		{attr: "$Object:homomorphicHash", val: hex.EncodeToString(hcss[3]), cnr: objs[3].GetContainerID(), exp: objs[3].GetID()},
		{attr: "$Object:homomorphicHash", val: hex.EncodeToString(hcss[4]), cnr: objs[4].GetContainerID(), exp: objs[4].GetID()},
		{attr: "$Object:homomorphicHash", val: hex.EncodeToString(phcs[:]), cnr: par.GetContainerID(), exp: par.GetID()},
		{attr: "$Object:split.splitID", val: objs[0].SplitID().String(), cnr: objs[0].GetContainerID(), exp: objs[0].GetID()},
		{attr: "$Object:split.splitID", val: objs[1].SplitID().String(), cnr: objs[1].GetContainerID(), exp: objs[1].GetID()},
		{attr: "$Object:split.splitID", val: objs[2].SplitID().String(), cnr: objs[2].GetContainerID(), exp: objs[2].GetID()},
		{attr: "$Object:split.splitID", val: objs[3].SplitID().String(), cnr: objs[3].GetContainerID(), exp: objs[3].GetID()},
		{attr: "$Object:split.splitID", val: objs[4].SplitID().String(), cnr: objs[4].GetContainerID(), exp: objs[4].GetID()},
		{attr: "$Object:split.splitID", val: par.SplitID().String(), cnr: par.GetContainerID(), exp: par.GetID()},
		{attr: "$Object:split.parent", val: objs[0].GetParentID().String(), cnr: objs[0].GetContainerID(), exp: objs[0].GetID()},
		{attr: "$Object:split.parent", val: objs[1].GetParentID().String(), cnr: objs[1].GetContainerID(), exp: objs[1].GetID()},
		{attr: "$Object:split.parent", val: objs[2].GetParentID().String(), cnr: objs[2].GetContainerID(), exp: objs[2].GetID()},
		{attr: "$Object:split.parent", val: objs[3].GetParentID().String(), cnr: objs[3].GetContainerID(), exp: objs[3].GetID()},
		{attr: "$Object:split.parent", val: objs[4].GetParentID().String(), cnr: objs[4].GetContainerID(), exp: objs[4].GetID()},
		{attr: "$Object:split.parent", val: par.GetParentID().String(), cnr: par.GetContainerID(), exp: par.GetID()},
		{attr: "$Object:split.first", val: objs[0].GetFirstID().String(), cnr: objs[0].GetContainerID(), exp: objs[0].GetID()},
		{attr: "$Object:split.first", val: objs[1].GetFirstID().String(), cnr: objs[1].GetContainerID(), exp: objs[1].GetID()},
		{attr: "$Object:split.first", val: objs[2].GetFirstID().String(), cnr: objs[2].GetContainerID(), exp: objs[2].GetID()},
		{attr: "$Object:split.first", val: objs[3].GetFirstID().String(), cnr: objs[3].GetContainerID(), exp: objs[3].GetID()},
		{attr: "$Object:split.first", val: objs[4].GetFirstID().String(), cnr: objs[4].GetContainerID(), exp: objs[4].GetID()},
		{attr: "$Object:split.first", val: par.GetFirstID().String(), cnr: par.GetContainerID(), exp: par.GetID()},
		{attr: "Index", val: "0", cnr: objs[0].GetContainerID(), exp: objs[0].GetID()},
		{attr: "Index", val: "1", cnr: objs[1].GetContainerID(), exp: objs[1].GetID()},
		{attr: "Index", val: "2", cnr: objs[2].GetContainerID(), exp: objs[2].GetID()},
		{attr: "Index", val: "3", cnr: objs[3].GetContainerID(), exp: objs[3].GetID()},
		{attr: "Index", val: "4", cnr: objs[4].GetContainerID(), exp: objs[4].GetID()},
		{attr: "Index", val: "9999", cnr: par.GetContainerID(), exp: par.GetID()},
	} {
		var fs object.SearchFilters
		fs.AddFilter(tc.attr, tc.val, object.MatchStringEqual)
		res, _, err := db.Search(tc.cnr, fs, nil, nil, nil, 1000)
		require.NoError(t, err, tc)
		if !tc.par {
			require.Len(t, res, 1, tc)
			require.Equal(t, tc.exp, res[0].ID, tc)
		} else {
			require.Len(t, res, 2, tc)
			require.True(t, slices.ContainsFunc(res, func(r client.SearchResultItem) bool { return r.ID == objs[0].GetID() }))
			require.True(t, slices.ContainsFunc(res, func(r client.SearchResultItem) bool { return r.ID == par.GetID() }))
		}
	}

	for i := range objs {
		var fs object.SearchFilters
		fs.AddRootFilter()
		res, _, err = db.Search(objs[i].GetContainerID(), fs, nil, nil, nil, 1000)
		require.NoError(t, err, i)
		require.Len(t, res, 1, i)
		if i == 0 {
			require.Equal(t, par.GetID(), res[0].ID)
		} else {
			require.Equal(t, objs[i].GetID(), res[0].ID, i)
		}
		fs = fs[:0]
		fs.AddPhyFilter()
		res, _, err = db.Search(objs[i].GetContainerID(), fs, nil, nil, nil, 1000)
		require.NoError(t, err, i)
		if i == 0 {
			require.Len(t, res, 2)
			require.True(t, slices.ContainsFunc(res, func(r client.SearchResultItem) bool { return r.ID == objs[0].GetID() }))
			require.True(t, slices.ContainsFunc(res, func(r client.SearchResultItem) bool { return r.ID == par.GetID() }))
		} else {
			require.Len(t, res, 1)
			require.Equal(t, objs[i].GetID(), res[0].ID, i)
		}
	}
}
