package meta

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
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
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	checksumtest "github.com/nspcc-dev/neofs-sdk-go/checksum/test"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"
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
		metaBkt := tx.Bucket(metaBucketKey(prm.tomb.Container()))
		var metaCursor *bbolt.Cursor
		if metaBkt != nil {
			metaCursor = metaBkt.Cursor()
		}

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
			if !prm.forceRemoval && objectLocked(tx, currEpoch, metaCursor, cnr, id) {
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

			obj, err := getCompat(tx, prm.target[i], buf, true, currEpoch)
			targetKey := addressKey(prm.target[i], buf)
			if err == nil {
				if inGraveyardWithKey(metaCursor, targetKey, graveyardBKT, garbageObjectsBKT, garbageContainersBKT) == statusAvailable {
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
			WithContainers(mockContainers{}),
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

	typs := []object.Type{object.TypeRegular, object.TypeTombstone, object.TypeStorageGroup, object.TypeLock, object.TypeLink} //nolint:staticcheck // storage groups are deprecated, but this is a migration test.
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
		tzh := [tz.Size]byte(testutil.RandByteSlice(tz.Size))
		objs[i].SetPayloadHomomorphicHash(checksum.NewTillichZemor(tzh))
		hcss = append(hcss, tzh[:])
		sid := objecttest.SplitID()
		objs[i].SetSplitID(&sid)
		objs[i].SetParentID(oidtest.ID())
		objs[i].SetFirstID(oidtest.ID())
		objs[i].SetAttributes(object.NewAttribute("Index", strconv.Itoa(i)))
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
	phcs := [tz.Size]byte(testutil.RandByteSlice(tz.Size))
	par.SetPayloadHomomorphicHash(checksum.NewTillichZemor(phcs))
	sid := objecttest.SplitID()
	par.SetSplitID(&sid)
	par.SetParentID(oidtest.ID())
	par.SetFirstID(oidtest.ID())
	par.SetAttributes(object.NewAttribute("Index", "9999"))

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
		require.Equal(t, []byte{0x07, 0, 0, 0, 0, 0, 0, 0}, bkt.Get([]byte("version")))
		return nil
	})
	require.NoError(t, err)

	exp := searchResultForIDs(sortObjectIDs([]oid.ID{objs[0].GetID(), par.GetID()}))
	assertSearchResult(t, db, objs[0].GetContainerID(), nil, nil, exp)

	for i := range objs[1:] {
		exp := searchResultForIDs([]oid.ID{objs[1+i].GetID()})
		assertSearchResult(t, db, objs[1+i].GetContainerID(), nil, nil, exp)
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
		if !tc.par {
			exp = searchResultForIDs([]oid.ID{tc.exp})
		} else {
			exp = searchResultForIDs(sortObjectIDs([]oid.ID{objs[0].GetID(), par.GetID()}))
		}
		assertSearchResult(t, db, tc.cnr, fs, nil, exp)
	}

	for i := range objs {
		var fs object.SearchFilters
		fs.AddRootFilter() // There are no root objects in the set above.
		assertSearchResult(t, db, objs[i].GetContainerID(), fs, nil, nil)

		fs = fs[:0]
		fs.AddPhyFilter()
		if i == 0 {
			exp = searchResultForIDs(sortObjectIDs([]oid.ID{objs[0].GetID(), par.GetID()}))
		} else {
			exp = searchResultForIDs([]oid.ID{objs[i].GetID()})
		}
		assertSearchResult(t, db, objs[i].GetContainerID(), fs, nil, exp)
	}
	t.Run("failure", func(t *testing.T) {
		t.Run("zero by in attribute", func(t *testing.T) {
			testWithAttr := func(t *testing.T, k, v, msg string) {
				var logBuf zaptest.Buffer
				db := newDB(t, WithLogger(zap.New(zapcore.NewCore(
					zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
					zap.CombineWriteSyncers(&logBuf),
					zapcore.InfoLevel,
				))))
				cnr := cid.ID{74, 207, 174, 156, 40, 231, 114, 55, 114, 92, 232, 152, 106, 247, 193, 112, 158, 52, 3, 52, 184, 14, 75, 215, 86, 203, 76, 88, 158, 253, 241, 195}
				id := oid.ID{254, 229, 187, 147, 179, 23, 187, 50, 37, 212, 113, 82, 18, 24, 192, 81, 251, 204, 82, 56, 211, 244, 161, 185, 71, 248, 118, 213, 134, 26, 49, 79}
				var obj object.Object
				obj.SetContainerID(cnr)
				obj.SetOwner(usertest.ID())
				obj.SetPayloadChecksum(checksumtest.Checksum())
				obj.SetAttributes(
					object.NewAttribute("valid key", "valid value"),
					object.NewAttribute(k, v),
				)
				// put object and force old version
				require.NoError(t, db.boltDB.Update(func(tx *bbolt.Tx) error {
					b, err := tx.CreateBucket(slices.Concat([]byte{unusedPrimaryPrefix}, cnr[:]))
					require.NoError(t, err)
					require.NoError(t, b.Put(id[:], obj.Marshal()))
					bkt := tx.Bucket([]byte{0x05})
					require.NotNil(t, bkt)
					return bkt.Put([]byte("version"), []byte{0x03, 0, 0, 0, 0, 0, 0, 0})
				}))
				require.NoError(t, err)
				// migrate
				require.NoError(t, db.Init())
				// assert ignored
				assertSearchResult(t, db, cnr, nil, nil, nil)
				// assert log message
				msgs := logBuf.Lines()
				require.Len(t, msgs, 1)
				var m map[string]any
				require.NoError(t, json.Unmarshal([]byte(msgs[0]), &m))
				require.Subset(t, m, map[string]any{
					"level":     "info",
					"msg":       "invalid header in the container bucket, ignoring",
					"error":     msg,
					"container": "632qzc5qrxpvB1PZam23Xq5AXQ5Kbt2h6G1gtWDb8AzW",
					"object":    "JA1jTW3qwWK9hWs95tesMVbrSLpjCjW6URv8xM7woPnv",
					"data":      base64.StdEncoding.EncodeToString(obj.Marshal()),
				})
			}
			t.Run("in key", func(t *testing.T) {
				testWithAttr(t, "k\x00y", "value", "attribute #1 key contains 0x00 byte used in sep")
			})
			t.Run("in value", func(t *testing.T) {
				testWithAttr(t, "key", "va\x00ue", "attribute #1 value contains 0x00 byte used in sep")
			})
		})
	})
	t.Run("invalid protobuf", func(t *testing.T) {
		var logBuf zaptest.Buffer
		db := newDB(t, WithLogger(zap.New(zapcore.NewCore(
			zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
			zap.CombineWriteSyncers(&logBuf),
			zapcore.InfoLevel,
		))))
		invalidProtobuf := []byte("Hello, protobuf!")
		errProto := proto.Unmarshal(invalidProtobuf, new(protoobject.Object))
		require.Error(t, errProto)
		cnr := cidtest.ID()
		ids := sortObjectIDs(oidtest.IDs(5))
		objs := make([][]byte, len(ids))
		for i := range ids {
			var obj object.Object
			obj.SetContainerID(cnr)
			obj.SetID(ids[i])
			obj.SetOwner(usertest.ID())
			obj.SetPayloadChecksum(checksumtest.Checksum())
			objs[i] = obj.Marshal()
		}
		// store objects and force version#3
		require.NoError(t, db.boltDB.Update(func(tx *bbolt.Tx) error {
			b, err := tx.CreateBucket(slices.Concat([]byte{unusedPrimaryPrefix}, cnr[:]))
			require.NoError(t, err)
			for i := range objs {
				require.NoError(t, b.Put(ids[i][:], objs[i]))
			}
			bkt := tx.Bucket([]byte{0x05})
			require.NotNil(t, bkt)
			require.NoError(t, bkt.Put([]byte("version"), []byte{0x03, 0, 0, 0, 0, 0, 0, 0}))
			return nil
		}))
		// corrupt one object
		require.NoError(t, db.boltDB.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket(slices.Concat([]byte{unusedPrimaryPrefix}, cnr[:]))
			require.NotNil(t, b)
			require.NoError(t, b.Put(ids[1][:], invalidProtobuf))
			return nil
		}))
		// migrate
		require.NoError(t, db.Init())
		// assert all others are available
		assertSearchResult(t, db, cnr, nil, nil, searchResultForIDs(slices.Concat(ids[:1], ids[2:])))
		// assert log message
		msgs := logBuf.Lines()
		require.Len(t, msgs, 1)
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(msgs[0]), &m))
		require.Subset(t, m, map[string]any{
			"level":     "info",
			"msg":       "invalid object binary in the container bucket's value, ignoring",
			"error":     errProto.Error(),
			"container": cnr.String(),
			"object":    ids[1].String(),
			"data":      base64.StdEncoding.EncodeToString(invalidProtobuf),
		})
	})
	t.Run("header limit overflow", func(t *testing.T) {
		var logBuf zaptest.Buffer
		db := newDB(t, WithLogger(zap.New(zapcore.NewCore(
			zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
			zap.CombineWriteSyncers(&logBuf),
			zapcore.InfoLevel,
		))))
		cnr := cidtest.ID()
		ids := sortObjectIDs(oidtest.IDs(5))
		objs := make([]object.Object, len(ids))
		objBins := make([][]byte, len(ids))
		for i := range ids {
			objs[i].SetContainerID(cnr)
			objs[i].SetID(ids[i])
			objs[i].SetOwner(usertest.ID())
			objs[i].SetPayloadChecksum(checksumtest.Checksum())
			objBins[i] = objs[i].Marshal()
		}
		// store objects and force version#3
		require.NoError(t, db.boltDB.Update(func(tx *bbolt.Tx) error {
			b, err := tx.CreateBucket(slices.Concat([]byte{unusedPrimaryPrefix}, cnr[:]))
			require.NoError(t, err)
			for i := range objBins {
				require.NoError(t, b.Put(ids[i][:], objBins[i]))
			}
			bkt := tx.Bucket([]byte{0x05})
			require.NotNil(t, bkt)
			require.NoError(t, bkt.Put([]byte("version"), []byte{0x03, 0, 0, 0, 0, 0, 0, 0}))
			return nil
		}))
		// corrupt one object
		bigAttrVal := testutil.RandByteSlice(16 << 10)
		objs[1].SetAttributes(object.NewAttribute("attr", base64.StdEncoding.EncodeToString(bigAttrVal))) // preserve valid chars
		objBins[1] = objs[1].Marshal()
		require.NoError(t, db.boltDB.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket(slices.Concat([]byte{unusedPrimaryPrefix}, cnr[:]))
			require.NotNil(t, b)
			require.NoError(t, b.Put(ids[1][:], objBins[1]))
			return nil
		}))
		// migrate
		require.NoError(t, db.Init())
		// assert all others are available
		assertSearchResult(t, db, cnr, nil, nil, searchResultForIDs(slices.Concat(ids[:1], ids[2:])))
		// assert log message
		msgs := logBuf.Lines()
		require.Len(t, msgs, 1)
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(msgs[0]), &m))
		require.Subset(t, m, map[string]any{
			"level":     "info",
			"msg":       "invalid header in the container bucket, ignoring",
			"error":     fmt.Sprintf("header len %d exceeds the limit", objs[1].HeaderLen()),
			"container": cnr.String(),
			"object":    ids[1].String(),
			"data":      base64.StdEncoding.EncodeToString(objBins[1]),
		})
	})
	t.Run("container presence", func(t *testing.T) {
		var cnrs mockContainers
		var logBuf zaptest.Buffer
		db := newDB(t, WithContainers(&cnrs), WithLogger(zap.New(zapcore.NewCore(
			zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
			zap.CombineWriteSyncers(&logBuf),
			zapcore.InfoLevel,
		))))
		cnr := cidtest.ID()
		ids := sortObjectIDs(oidtest.IDs(5))
		objBins := make([][]byte, len(ids))
		for i := range ids {
			objs[i].SetContainerID(cnr)
			objs[i].SetID(ids[i])
			objs[i].SetOwner(usertest.ID())
			objs[i].SetPayloadChecksum(checksumtest.Checksum())
			objBins[i] = objs[i].Marshal()
		}
		// store objects and force version#3
		var pushObjects = func(tx *bbolt.Tx) error {
			b, err := tx.CreateBucket(slices.Concat([]byte{unusedPrimaryPrefix}, cnr[:]))
			require.NoError(t, err)
			for i := range objBins {
				require.NoError(t, b.Put(ids[i][:], objBins[i]))
			}
			bkt := tx.Bucket([]byte{0x05})
			require.NotNil(t, bkt)
			require.NoError(t, bkt.Put([]byte("version"), []byte{0x03, 0, 0, 0, 0, 0, 0, 0}))
			return nil
		}
		require.NoError(t, db.boltDB.Update(pushObjects))
		t.Run("failed to check", func(t *testing.T) {
			anyErr := errors.New("any error")
			cnrs.err = anyErr
			err = db.Init()
			cnrs.err = nil
			require.ErrorIs(t, err, anyErr)
			require.EqualError(t, err, "migrating from meta version 3 failed, consider database resync: "+
				"check container presence: "+anyErr.Error())
		})
		t.Run("missing", func(t *testing.T) {
			cnrs.absent = true
			require.NoError(t, db.Init())
			cnrs.absent = false
			// return version#3 back
			require.NoError(t, db.boltDB.Update(func(tx *bbolt.Tx) error {
				bkt := tx.Bucket([]byte{0x05})
				require.NotNil(t, bkt)
				require.Equal(t, []byte{0x07, 0, 0, 0, 0, 0, 0, 0}, bkt.Get([]byte("version")))
				require.NoError(t, bkt.Put([]byte("version"), []byte{0x03, 0, 0, 0, 0, 0, 0, 0}))
				return nil
			}))
			// assert none were migrated
			assertSearchResult(t, db, cnr, nil, nil, nil)
			// assert log message
			msgs := logBuf.Lines()
			require.Len(t, msgs, 1)
			var m map[string]any
			require.NoError(t, json.Unmarshal([]byte(msgs[0]), &m))
			require.Subset(t, m, map[string]any{"level": "info", "msg": "container no longer exists, ignoring", "container": cnr.String()})
		})
		// Previous test updated meta to 6 and wiped objects, get them back.
		require.NoError(t, db.boltDB.Update(pushObjects))
		// migrate
		require.NoError(t, db.Init())
		// assert all others are available
		assertSearchResult(t, db, cnr, nil, nil, searchResultForIDs(ids))
	})
	t.Run("various object sets", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			m    map[object.Type][]uint
		}{
			{name: "no objects", m: nil},
			{name: "empty containers only", m: map[object.Type][]uint{
				object.TypeRegular:      make([]uint, 3),
				object.TypeTombstone:    make([]uint, 5),
				object.TypeStorageGroup: make([]uint, 10), //nolint:staticcheck // storage groups are deprecated, but this is a migration test.
				object.TypeLock:         make([]uint, 1),
				object.TypeLink:         make([]uint, 100),
			}},
			{name: "some containers are empty", m: map[object.Type][]uint{
				object.TypeRegular:      {1, 7, 0, 20},
				object.TypeTombstone:    {0, 15, 0},
				object.TypeStorageGroup: make([]uint, 10), //nolint:staticcheck // storage groups are deprecated, but this is a migration test.
			}},
			{name: "some containers are empty", m: map[object.Type][]uint{
				object.TypeRegular:      {1, 7, 0, 20},
				object.TypeTombstone:    {0, 15, 0},
				object.TypeStorageGroup: make([]uint, 10), //nolint:staticcheck // storage groups are deprecated, but this is a migration test.
			}},
			{name: "one big container", m: map[object.Type][]uint{
				object.TypeRegular: {3999},
			}},
			{name: "big counts", m: map[object.Type][]uint{
				object.TypeRegular:      {200, 700, 600},
				object.TypeTombstone:    {20, 30},
				object.TypeStorageGroup: {10, 0, 20, 0, 30, 0, 40, 0}, //nolint:staticcheck // storage groups are deprecated, but this is a migration test.
				object.TypeLock:         {1, 2, 3, 4, 5, 6, 7, 8, 9},
				object.TypeLink:         {99},
			}},
			{name: "big counts aligned", m: map[object.Type][]uint{
				object.TypeRegular:      {1000},
				object.TypeTombstone:    {500, 500, 500},
				object.TypeStorageGroup: {200, 200, 200, 200, 200}, //nolint:staticcheck // storage groups are deprecated, but this is a migration test.
			}},
			{name: "big counts not aligned", m: map[object.Type][]uint{
				object.TypeRegular:      {999, 999},
				object.TypeTombstone:    {999},
				object.TypeStorageGroup: {999, 999, 999}, //nolint:staticcheck // storage groups are deprecated, but this is a migration test.
			}},
		} {
			t.Run(tc.name, func(t *testing.T) { testMigrationV3To4(t, tc.m) })
		}
	})
}

func TestSlicesCloneNil(t *testing.T) {
	// not stated in docs, but migrateContainersToMetaBucket relies on this
	require.Nil(t, slices.Clone([]byte(nil)))
}

func testMigrationV3To4(t *testing.T, mAll map[object.Type][]uint) {
	db := newDB(t)
	// force version#3
	require.NoError(t, db.boltDB.Update(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket([]byte{0x05})
		require.NotNil(t, bkt)
		require.NoError(t, bkt.Put([]byte("version"), []byte{0x03, 0, 0, 0, 0, 0, 0, 0}))
		return nil
	}))
	// store configured objects
	mCnrs := make(map[cid.ID][]oid.ID)
	for typ, counts := range mAll {
		for _, count := range counts {
			cnr := cidtest.ID()
			var ids []oid.ID
			for range count {
				id := oidtest.ID()
				require.NoError(t, db.boltDB.Update(func(tx *bbolt.Tx) error {
					var obj object.Object
					obj.SetID(id)
					obj.SetContainerID(cnr)
					obj.SetOwner(usertest.ID())
					obj.SetPayloadChecksum(checksumtest.Checksum())

					var prefix byte
					switch typ {
					default:
						t.Fatalf("unexpected object type %v", typ)
					case object.TypeRegular:
						prefix = 0x06
					case object.TypeTombstone:
						prefix = 0x09
					case object.TypeStorageGroup: //nolint:staticcheck // storage groups are deprecated, but this is a migration test.
						prefix = 0x08
					case object.TypeLock:
						prefix = 0x07
					case object.TypeLink:
						prefix = 0x12
					}
					b, err := tx.CreateBucketIfNotExists(slices.Concat([]byte{prefix}, cnr[:]))
					require.NoError(t, err)
					require.NoError(t, b.Put(id[:], obj.Marshal()))
					return nil
				}))
				ids = append(ids, id)
			}
			mCnrs[cnr] = ids
		}
	}
	// migrate
	require.NoError(t, db.Init())
	// TODO: would also be nice to check tx num which is known
	// check all objects are available
	for cnr, ids := range mCnrs {
		assertSearchResult(t, db, cnr, nil, nil, searchResultForIDs(sortObjectIDs(ids)))
	}
}

func TestMigrate4to5(t *testing.T) {
	var (
		db    = newDB(t)
		cnr   = cidtest.ID()
		owner = usertest.ID()
		ver   version.Version
	)

	ver.SetMajor(2)
	ver.SetMinor(1)

	payload := make([]byte, 10)

	csum, err := checksum.NewFromData(checksum.SHA256, payload)
	require.NoError(t, err)
	csumTZ, err := checksum.NewFromData(checksum.TillichZemor, payload)
	require.NoError(t, err)

	// Emulate split.

	var leftParent = object.New()
	leftParent.SetOwner(owner)
	leftParent.SetType(object.TypeRegular)
	leftParent.SetContainerID(cnr)
	leftParent.SetVersion(&ver)

	var parent = object.New()
	parent.SetID(oidtest.ID())
	parent.SetOwner(owner)
	parent.SetType(object.TypeRegular)
	parent.SetContainerID(cnr)
	parent.SetVersion(&ver)
	parent.SetPayloadChecksum(csum)

	var (
		leftObj   = object.New()
		middleObj = object.New()
		rightObj  = object.New()
	)
	for _, obj := range []*object.Object{leftObj, middleObj, rightObj} {
		obj.SetID(oidtest.ID())
		obj.SetOwner(owner)
		obj.SetType(object.TypeRegular)
		obj.SetContainerID(cnr)
		obj.SetVersion(&ver)
		obj.SetPayloadChecksum(csum)
		obj.SetPayloadHomomorphicHash(csumTZ)
		obj.SetPayload(payload)
		obj.SetPayloadSize(uint64(len(payload)))
	}

	leftObj.SetParent(leftParent)

	middleObj.SetFirstID(leftObj.GetID())
	middleObj.SetPreviousID(leftObj.GetID())

	rightObj.SetFirstID(leftObj.GetID())
	rightObj.SetPreviousID(middleObj.GetID())
	rightObj.SetParent(parent)

	require.NoError(t, db.Put(leftObj))
	require.NoError(t, db.Put(middleObj))
	require.NoError(t, db.Put(rightObj))

	// primary bucket was deleted in version 6 and Put() no longer adds it,
	// so put additional data manually here that version 4 had.
	require.NoError(t, db.boltDB.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucket(slices.Concat([]byte{unusedPrimaryPrefix}, cnr[:]))
		require.NoError(t, err)
		objId := leftObj.GetID()
		require.NoError(t, b.Put(objId[:], leftObj.CutPayload().Marshal()))
		objId = middleObj.GetID()
		require.NoError(t, b.Put(objId[:], middleObj.CutPayload().Marshal()))
		objId = rightObj.GetID()
		require.NoError(t, b.Put(objId[:], rightObj.CutPayload().Marshal()))
		return nil
	}))

	var fs object.SearchFilters
	fs.AddRootFilter()

	assertSearchResult(t, db, cnr, fs, nil, searchResultForIDs([]oid.ID{parent.GetID()})) // v5 behavior.

	require.NoError(t, db.boltDB.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(slices.Concat([]byte{metadataPrefix}, cnr[:]))
		// v4 behavior.
		err := putPlainAttribute(b, &keyBuffer{}, middleObj.GetID(), object.FilterRoot, binPropMarker)
		require.NoError(t, err)
		return updateVersion(tx, 4)
	}))

	assertSearchResult(t, db, cnr, fs, nil, searchResultForIDs([]oid.ID{middleObj.GetID(), parent.GetID()}))

	require.NoError(t, db.Init())
	assertSearchResult(t, db, cnr, fs, nil, searchResultForIDs([]oid.ID{parent.GetID()}))
}

func TestMigrate6to7(t *testing.T) {
	db := newDB(t)

	const cnrNum = 5
	const objsPerCnr = 20
	mObjs := make(map[cid.ID][]oid.ID)
	trashKey := []byte("trash")
	leakingObj := oidtest.ID()

	err := db.boltDB.Update(func(tx *bbolt.Tx) error {
		garbageBkt := tx.Bucket([]byte{0x01})
		require.NotNil(t, garbageBkt)

		cnrs := cidtest.IDs(cnrNum)

		for _, cnr := range cnrs {
			metaBkt, err := tx.CreateBucketIfNotExists(slices.Concat([]byte{0xFF}, cnr[:]))
			require.NoError(t, err)

			ids := oidtest.IDs(objsPerCnr)

			for j, id := range ids {
				metaKey := slices.Concat([]byte{0x00}, id[:])
				require.NoError(t, metaBkt.Put(metaKey, nil))

				var garbageKey []byte
				if j%2 == 0 { // correct
					garbageKey = id[:]
				} else { // broken
					garbageKey = slices.Concat(cnr[:], id[:])
				}

				require.NoError(t, garbageBkt.Put(garbageKey, nil))
			}

			mObjs[cnr] = ids
		}

		// add trash key which is neither OID nor CID+OID
		require.NoError(t, garbageBkt.Put(trashKey, nil))
		// add random OID which should be cleaned because there is no container for it
		require.NoError(t, garbageBkt.Put(leakingObj[:], nil))

		// force old version
		bkt := tx.Bucket([]byte{0x05})
		require.NotNil(t, bkt)
		require.NoError(t, bkt.Put([]byte("version"), []byte{0x06, 0, 0, 0, 0, 0, 0, 0}))

		return nil
	})
	require.NoError(t, err)

	// migrate
	require.NoError(t, db.Init())

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		garbageBkt := tx.Bucket([]byte{0x01})
		require.NotNil(t, garbageBkt)

		require.EqualValues(t, cnrNum*objsPerCnr+1, garbageBkt.Stats().KeyN) // + 1 for leaking OID

		for cnr, ids := range mObjs {
			for _, id := range ids {
				garbageKey := slices.Concat(cnr[:], id[:])
				require.NotNil(t, garbageBkt.Get(garbageKey))
			}
		}

		require.NotNil(t, garbageBkt.Get(trashKey))
		require.Nil(t, garbageBkt.Get(leakingObj[:]))

		// check new version
		bkt := tx.Bucket([]byte{0x05})
		require.NotNil(t, bkt)
		require.Equal(t, []byte{0x07, 0, 0, 0, 0, 0, 0, 0}, bkt.Get([]byte("version")))

		return nil
	})
	require.NoError(t, err)
}
