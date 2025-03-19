package meta

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/mpt"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectsdk "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"go.uber.org/zap/zaptest"
)

const (
	testNetworkMagic = 123
	testVUB          = 12345
	testObjectSize   = 1234567
)

type testNetwork struct {
	m sync.RWMutex

	resCIDs    map[cid.ID]struct{}
	resObjects map[oid.Address]objectsdk.Object
	resErr     error
}

func cidMap(v []cid.ID) map[cid.ID]struct{} {
	var cids = make(map[cid.ID]struct{}, len(v))
	for _, c := range v {
		cids[c] = struct{}{}
	}
	return cids
}

func (t *testNetwork) setContainers(v []cid.ID) {
	var cids = cidMap(v)
	t.m.Lock()
	t.resCIDs = cids
	t.m.Unlock()
}

func (t *testNetwork) setObjects(v map[oid.Address]objectsdk.Object) {
	t.m.Lock()
	t.resObjects = v
	t.m.Unlock()
}

func (t *testNetwork) Head(_ context.Context, cID cid.ID, oID oid.ID) (objectsdk.Object, error) {
	t.m.RLock()
	defer t.m.RUnlock()

	return t.resObjects[oid.NewAddress(cID, oID)], t.resErr
}

func (t *testNetwork) IsMineWithMeta(id cid.ID) (bool, error) {
	return true, nil
}

func (t *testNetwork) List() (map[cid.ID]struct{}, error) {
	t.m.RLock()
	defer t.m.RUnlock()

	return t.resCIDs, t.resErr
}

func testContainers(t *testing.T, num int) []cid.ID {
	res := make([]cid.ID, num)
	for i := range num {
		_, err := rand.Read(res[i][:])
		require.NoError(t, err)
	}

	return res
}

func newEpoch(m *Meta, epoch int) {
	m.epochEv <- &state.ContainedNotificationEvent{
		NotificationEvent: state.NotificationEvent{
			Name: newEpochName,
			Item: stackitem.NewArray([]stackitem.Item{stackitem.Make(epoch)}),
		},
	}
}

func checkDBFiles(t *testing.T, path string, cnrs []cid.ID) {
	require.Eventually(t, func() bool {
		entries, err := os.ReadDir(path)
		if err != nil {
			return false
		}

		cnrsMap := cidMap(cnrs)
		if len(entries) != len(cnrsMap) {
			return false
		}
		for _, e := range entries {
			var cID cid.ID
			err = cID.DecodeString(e.Name())
			if err != nil {
				t.Fatal("unexpected db file name", e.Name())
			}

			if _, ok := cnrsMap[cID]; !ok {
				return false
			}

			delete(cnrsMap, cID)
		}

		return true
	}, 5*time.Second, time.Millisecond*100, "expected to find db files")
}

type testWS struct {
	m             sync.RWMutex
	bCh           chan<- *block.Header
	notifications []state.ContainedNotificationEvent
	err           error
}

func (t *testWS) blockCh() chan<- *block.Header {
	t.m.RLock()
	defer t.m.RUnlock()

	return t.bCh
}

func (t *testWS) Unsubscribe(id string) error {
	// TODO implement me
	panic("not expected for now")
}

func (t *testWS) swapResults(notifications []state.ContainedNotificationEvent, err error) {
	t.m.Lock()
	defer t.m.Unlock()

	t.notifications = notifications
	t.err = err
}

func (t *testWS) GetBlockNotifications(blockHash util.Uint256, filters ...*neorpc.NotificationFilter) (*result.BlockNotifications, error) {
	t.m.RLock()
	defer t.m.RUnlock()

	return &result.BlockNotifications{
		Application: t.notifications,
	}, t.err
}

func (t *testWS) GetVersion() (*result.Version, error) {
	panic("not expected for now")
}

func (t *testWS) ReceiveHeadersOfAddedBlocks(flt *neorpc.BlockFilter, rcvr chan<- *block.Header) (string, error) {
	t.m.Lock()
	t.bCh = rcvr
	t.m.Unlock()

	return "", nil
}

func (t *testWS) ReceiveExecutionNotifications(flt *neorpc.NotificationFilter, rcvr chan<- *state.ContainedNotificationEvent) (string, error) {
	panic("not expected for now")
}

func (t *testWS) Close() {
	panic("not expected for now")
}

func createAndRunTestMeta(t *testing.T, ws wsClient, network NeoFSNetwork) (*Meta, func(), chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	m := &Meta{
		l:           zaptest.NewLogger(t),
		rootPath:    t.TempDir(),
		magicNumber: 102938475,
		bCh:         make(chan *block.Header),
		cnrDelEv:    make(chan *state.ContainedNotificationEvent),
		cnrPutEv:    make(chan *state.ContainedNotificationEvent),
		epochEv:     make(chan *state.ContainedNotificationEvent),
		blockBuff:   make(chan *block.Header, blockBuffSize),
		ws:          ws,

		// no-op, to be filled by test cases if needed
		storages:  make(map[cid.ID]*containerStorage),
		netmapH:   util.Uint160{},
		cnrH:      util.Uint160{},
		net:       network,
		endpoints: []string{},
		timeout:   time.Second,
	}

	exitCh := make(chan struct{})

	go m.flusher(ctx)
	go m.blockFetcher(ctx, m.blockBuff)
	go func() {
		_ = m.listenNotifications(ctx)
		exitCh <- struct{}{}
	}()

	return m, cancel, exitCh
}

// args list consists of [testing.T], [Meta] and the list from
// [object.EncodeReplicationMetaInfo] can be improved at the time [object]
// package will do it.
func checkObject(t *testing.T, m *Meta, cID cid.ID, oID, firstPart, previousPart oid.ID, pSize uint64, typ objectsdk.Type, deleted, locked []oid.ID, _ uint64, _ uint32) bool {
	getBoth := func(trie *mpt.Trie, st storage.Store, key, expV []byte) bool {
		mptV, err := trie.Get(key)
		if err != nil {
			if errors.Is(err, mpt.ErrNotFound) {
				return false
			}
			t.Fatalf("failed to get oid value from mpt: %v", err)
		}

		dbV, err := st.Get(key)
		if err != nil {
			if errors.Is(err, storage.ErrKeyNotFound) {
				return false
			}
			t.Fatalf("failed to get oid value from db: %v", err)
		}

		require.Equal(t, dbV, mptV)
		require.Equal(t, dbV, expV)

		return true
	}

	getMPT := func(trie *mpt.Trie, st storage.Store, key, expV []byte) bool {
		v, err := trie.Get(key)
		if err != nil {
			if errors.Is(err, mpt.ErrNotFound) {
				return false
			}
			t.Fatalf("failed to get oid value from mpt: %v", err)
		}

		require.Equal(t, expV, v)

		return true
	}

	m.stM.RLock()
	st := m.storages[cID]
	m.stM.RUnlock()

	st.m.RLock()
	defer st.m.RUnlock()

	commSuffix := oID[:]

	ok := getBoth(st.mpt, st.db, append([]byte{oidIndex}, commSuffix...), []byte{})
	if !ok {
		return false
	}

	if len(deleted) != 0 {
		expVal := make([]byte, 0, oid.Size*(len(deleted)))
		for _, d := range deleted {
			expVal = append(expVal, d[:]...)
		}

		ok = getBoth(st.mpt, st.db, append([]byte{deletedIndex}, commSuffix...), expVal)
		if !ok {
			return false
		}
	}

	if len(locked) != 0 {
		expVal := make([]byte, 0, oid.Size*(len(locked)))
		for _, l := range locked {
			expVal = append(expVal, l[:]...)
		}

		ok = getBoth(st.mpt, st.db, append([]byte{lockedIndex}, commSuffix...), expVal)
		if !ok {
			return false
		}
	}

	var sizeB big.Int
	sizeB.SetUint64(pSize)
	ok = getMPT(st.mpt, st.db, append([]byte{sizeIndex}, commSuffix...), sizeB.Bytes())
	if !ok {
		return false
	}

	if firstPart != (oid.ID{}) {
		ok = getMPT(st.mpt, st.db, append([]byte{firstPartIndex}, commSuffix...), firstPart[:])
		if !ok {
			return false
		}
	}

	if previousPart != (oid.ID{}) {
		ok = getMPT(st.mpt, st.db, append([]byte{previousPartIndex}, commSuffix...), previousPart[:])
		if !ok {
			return false
		}
	}

	if typ != objectsdk.TypeRegular {
		ok = getMPT(st.mpt, st.db, append([]byte{typeIndex}, commSuffix...), []byte{byte(typ)})
		if !ok {
			return false
		}
	}

	return true
}

func TestObjectPut(t *testing.T) {
	ws := testWS{}
	net := testNetwork{}
	m, stop, exitCh := createAndRunTestMeta(t, &ws, &net)
	t.Cleanup(func() {
		stop()
		<-exitCh
	})

	testCnrs := testContainers(t, 10)

	var epoch int
	net.setContainers(testCnrs)
	newEpoch(m, epoch)

	time.Sleep(time.Second)

	t.Run("storages for containers", func(t *testing.T) {
		checkDBFiles(t, m.rootPath, testCnrs)
	})

	t.Run("drop storage", func(t *testing.T) {
		var newContainers = testCnrs[1:]
		net.setContainers(newContainers)

		epoch++
		newEpoch(m, epoch)
		checkDBFiles(t, m.rootPath, newContainers)
	})

	t.Run("add storage", func(t *testing.T) {
		// add just dropped storage back
		epoch++
		net.setContainers(testCnrs)
		newEpoch(m, epoch)
		checkDBFiles(t, m.rootPath, testCnrs)
	})

	t.Run("put object", func(t *testing.T) {
		cID := testCnrs[0]
		oID := oidtest.ID()
		fPart := oidtest.ID()
		pPart := oidtest.ID()
		size := uint64(testObjectSize)
		typ := objectsdk.TypeRegular

		o := objecttest.Object()
		o.SetContainerID(cID)
		o.SetID(oID)
		o.SetFirstID(fPart)
		o.SetPreviousID(pPart)
		o.SetPayloadSize(size)
		o.SetType(typ)

		metaRaw := object.EncodeReplicationMetaInfo(cID, oID, fPart, pPart, size, typ, nil, nil, testVUB, m.magicNumber)
		metaStack, err := stackitem.Deserialize(metaRaw)
		require.NoError(t, err)

		bCH := ws.blockCh()

		net.setObjects(map[oid.Address]objectsdk.Object{oid.NewAddress(cID, oID): o})
		ws.swapResults([]state.ContainedNotificationEvent{{
			NotificationEvent: state.NotificationEvent{
				Name: objPutEvName,
				Item: stackitem.NewArray([]stackitem.Item{stackitem.Make(cID[:]), stackitem.Make(oID[:]), metaStack}),
			},
		}}, nil)
		bCH <- &block.Header{Index: 0}

		require.Eventually(t, func() bool {
			return checkObject(t, m, cID, oID, fPart, pPart, size, typ, nil, nil, testVUB, m.magicNumber)
		}, 3*time.Second, time.Millisecond*100, "object was not handled properly")
	})

	t.Run("delete object", func(t *testing.T) {
		cID := testCnrs[0]
		objToDeleteOID := oidtest.ID()
		size := uint64(testObjectSize)

		o := objecttest.Object()
		o.SetContainerID(cID)
		o.SetID(objToDeleteOID)
		o.SetPayloadSize(size)

		metaRaw := object.EncodeReplicationMetaInfo(cID, objToDeleteOID, oid.ID{}, oid.ID{}, size, objectsdk.TypeRegular, nil, nil, testVUB, m.magicNumber)
		metaStack, err := stackitem.Deserialize(metaRaw)
		require.NoError(t, err)

		bCH := ws.blockCh()
		net.setObjects(map[oid.Address]objectsdk.Object{oid.NewAddress(cID, objToDeleteOID): o})
		ws.swapResults([]state.ContainedNotificationEvent{{
			NotificationEvent: state.NotificationEvent{
				Name: objPutEvName,
				Item: stackitem.NewArray([]stackitem.Item{stackitem.Make(cID[:]), stackitem.Make(objToDeleteOID[:]), metaStack}),
			},
		}}, nil)
		bCH <- &block.Header{Index: 1}

		require.Eventually(t, func() bool {
			return checkObject(t, m, cID, objToDeleteOID, oid.ID{}, oid.ID{}, size, objectsdk.TypeRegular, nil, nil, testVUB, m.magicNumber)
		}, 3*time.Second, time.Millisecond*100, "object was not handled properly before deletion")

		tsCID := cID
		tsOID := oidtest.ID()
		tsSize := uint64(testObjectSize)
		deleted := []oid.ID{objToDeleteOID}

		ts := objecttest.Object()
		ts.SetContainerID(tsCID)
		ts.SetID(tsOID)
		ts.SetPayloadSize(tsSize)

		metaRaw = object.EncodeReplicationMetaInfo(tsCID, tsOID, oid.ID{}, oid.ID{}, tsSize, objectsdk.TypeTombstone, deleted, nil, testVUB, m.magicNumber)
		metaStack, err = stackitem.Deserialize(metaRaw)
		require.NoError(t, err)

		net.setObjects(map[oid.Address]objectsdk.Object{oid.NewAddress(tsCID, tsOID): ts})
		ws.swapResults([]state.ContainedNotificationEvent{{
			NotificationEvent: state.NotificationEvent{
				Name: objPutEvName,
				Item: stackitem.NewArray([]stackitem.Item{stackitem.Make(tsCID[:]), stackitem.Make(tsOID[:]), metaStack}),
			},
		}}, nil)
		bCH <- &block.Header{Index: 2}

		require.Eventually(t, func() bool {
			m.stM.RLock()
			st := m.storages[tsCID]
			m.stM.RUnlock()

			st.m.RLock()
			defer st.m.RUnlock()

			commSuffix := objToDeleteOID[:]
			mptKeys := [][]byte{
				append([]byte{0, oidIndex}, commSuffix...),
				append([]byte{0, sizeIndex}, commSuffix...),
				append([]byte{0, firstPartIndex}, commSuffix...),
				append([]byte{0, previousPartIndex}, commSuffix...),
				append([]byte{0, deletedIndex}, commSuffix...),
				append([]byte{0, lockedIndex}, commSuffix...),
				append([]byte{0, typeIndex}, commSuffix...),
			}

			tempM := make(map[string][]byte)
			fillObjectIndex(tempM, o)
			// dbKeys := maps.Keys(tempM) // go 1.23+
			dbKeys := make([][]byte, 0, len(tempM))
			for k := range tempM {
				dbKeys = append(dbKeys, []byte(k))
			}

			for _, key := range mptKeys {
				_, err = st.mpt.Get(key)
				if !errors.Is(err, mpt.ErrNotFound) {
					return false
				}
			}

			for _, key := range dbKeys {
				_, err = st.db.Get(key)
				if !errors.Is(err, storage.ErrKeyNotFound) {
					return false
				}
			}

			return true
		}, 3*time.Second, time.Millisecond*100, "object was not deleted")
	})
}

func TestValidation(t *testing.T) {
	cID := cidtest.ID()
	ctx := context.Background()
	l := zaptest.NewLogger(t)

	path.Join(t.TempDir(), "db.db")
	s, err := storageForContainer(path.Join(t.TempDir(), "db.db"), cID)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = s.drop()
	})

	t.Run("delete non-existent", func(t *testing.T) {
		objToDelete := oidtest.ID()
		ev := objEvent{
			cID:            cID,
			oID:            oidtest.ID(),
			size:           big.NewInt(testObjectSize),
			network:        big.NewInt(testNetworkMagic),
			deletedObjects: objToDelete[:],
			typ:            objectsdk.TypeTombstone,
		}

		require.ErrorContains(t, isOpAllowed(s.db, ev), "object-to-delete is missing")
	})

	t.Run("lock non-existent", func(t *testing.T) {
		objToLock := oidtest.ID()
		ev := objEvent{
			cID:           cID,
			oID:           oidtest.ID(),
			size:          big.NewInt(testObjectSize),
			network:       big.NewInt(testNetworkMagic),
			lockedObjects: objToLock[:],
			typ:           objectsdk.TypeLock,
		}

		require.ErrorContains(t, isOpAllowed(s.db, ev), "presence check")
	})

	t.Run("delete locked", func(t *testing.T) {
		obj1 := objecttest.Object()
		oID1 := obj1.GetID()
		obj1.SetContainerID(cID)
		obj2 := objecttest.Object()
		oID2 := obj2.GetID()
		obj2.SetContainerID(cID)

		net := &testNetwork{}
		ee := []objEvent{
			{
				cID:     cID,
				oID:     oID1,
				size:    big.NewInt(testObjectSize),
				network: big.NewInt(testNetworkMagic),
			},
			{
				cID:     cID,
				oID:     oID2,
				size:    big.NewInt(testObjectSize),
				network: big.NewInt(testNetworkMagic),
			},
		}
		net.setObjects(map[oid.Address]objectsdk.Object{
			oid.NewAddress(cID, oID1): obj1,
			oid.NewAddress(cID, oID2): obj2,
		})

		s.putObjects(ctx, l, 0, ee, net)

		lock := objecttest.Object()
		lock.SetContainerID(cID)
		lock.SetType(objectsdk.TypeLock)

		ee = []objEvent{
			{
				cID:           cID,
				oID:           lock.GetID(),
				size:          big.NewInt(testObjectSize),
				network:       big.NewInt(testNetworkMagic),
				lockedObjects: slices.Concat(oID1[:], oID2[:]),
				typ:           objectsdk.TypeLock,
			},
		}
		net.setObjects(map[oid.Address]objectsdk.Object{
			oid.NewAddress(cID, lock.GetID()): lock,
		})

		s.putObjects(ctx, l, 0, ee, net)

		ts := objecttest.Object()
		ts.SetContainerID(cID)
		ts.SetType(objectsdk.TypeTombstone)

		e := objEvent{
			cID:            cID,
			oID:            ts.GetID(),
			size:           big.NewInt(testObjectSize),
			network:        big.NewInt(testNetworkMagic),
			deletedObjects: slices.Concat(oID1[:], oID2[:]),
			typ:            objectsdk.TypeTombstone,
		}
		net.setObjects(map[oid.Address]objectsdk.Object{
			oid.NewAddress(cID, ts.GetID()): ts,
		})

		require.ErrorContains(t, isOpAllowed(s.db, e), fmt.Sprintf("is locked by %s", lock.GetID()))
	})
}

func TestCompatibility(t *testing.T) {
	o := objecttest.Object()
	o.SetSplitID(nil) // no split info is expected for split V2 era

	// database from engine's metabases

	db, err := bbolt.Open(path.Join(t.TempDir(), "db.db"), 0600, bbolt.DefaultOptions)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	metabaseMap := make(map[string][]byte)

	err = db.Update(func(tx *bbolt.Tx) error {
		err = meta.PutMetadataForObject(tx, o, true, true)
		require.NoError(t, err)

		cID := o.GetContainerID()
		metaBucketKey := []byte{255}
		metaBucketKey = append(metaBucketKey, cID[:]...)

		b := tx.Bucket(metaBucketKey)
		return b.ForEach(func(k, v []byte) error {
			metabaseMap[string(k)] = v
			return nil
		})
	})
	require.NoError(t, err)

	// batch for meta-data service

	serviceMap := make(map[string][]byte)
	fillObjectIndex(serviceMap, o)

	require.Equal(t, len(metabaseMap), len(serviceMap))
	for k := range metabaseMap {
		_, found := serviceMap[k]
		require.Truef(t, found, "%s key not found: %v", k, []byte(k))
	}
}
