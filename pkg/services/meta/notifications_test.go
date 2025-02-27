package meta

import (
	"context"
	"crypto/rand"
	"errors"
	"maps"
	"math/big"
	"os"
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
	utilcore "github.com/nspcc-dev/neofs-node/pkg/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectsdk "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

const (
	testVUB        = 12345
	testObjectSize = 1234567
)

type testContainerLister struct {
	res    map[cid.ID]struct{}
	resErr error
}

func (t *testContainerLister) IsMineWithMeta(id cid.ID) (bool, error) {
	return true, nil
}

func (t *testContainerLister) List() (map[cid.ID]struct{}, error) {
	return t.res, t.resErr
}

func testContainers(t *testing.T, num int) []cid.ID {
	res := make([]cid.ID, num)
	for i := range num {
		_, err := rand.Read(res[i][:])
		require.NoError(t, err)
	}

	return res
}

func newEpoch(m *Meta, cnrs map[cid.ID]struct{}, epoch int) {
	m.cLister = &testContainerLister{res: cnrs, resErr: nil}

	m.epochEv <- &state.ContainedNotificationEvent{
		NotificationEvent: state.NotificationEvent{
			Name: newEpochName,
			Item: stackitem.NewArray([]stackitem.Item{stackitem.Make(epoch)}),
		},
	}
}

func checkDBFiles(t *testing.T, path string, cnrs map[cid.ID]struct{}) {
	require.Eventually(t, func() bool {
		entries, err := os.ReadDir(path)
		if err != nil {
			return false
		}

		cnrsCopy := maps.Clone(cnrs)
		if len(entries) != len(cnrsCopy) {
			return false
		}
		for _, e := range entries {
			var cID cid.ID
			err = cID.DecodeString(e.Name())
			if err != nil {
				t.Fatal("unexpected db file name", e.Name())
			}

			if _, ok := cnrsCopy[cID]; !ok {
				return false
			}

			delete(cnrsCopy, cID)
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

func createAndRunTestMeta(t *testing.T, ws wsClient) (*Meta, func(), chan struct{}) {
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
		cLister:   &testContainerLister{},
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
	get := func(trie *mpt.Trie, st storage.Store, key, expV []byte) bool {
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

	m.m.RLock()
	st := m.storages[cID]
	m.m.RUnlock()

	st.m.RLock()
	defer st.m.RUnlock()

	commSuffix := oID[:]

	ok := get(st.mpt, st.db, append([]byte{oidIndex}, commSuffix...), []byte{})
	if !ok {
		return false
	}

	var sizeB big.Int
	sizeB.SetUint64(pSize)
	ok = get(st.mpt, st.db, append([]byte{sizeIndex}, commSuffix...), sizeB.Bytes())
	if !ok {
		return false
	}

	if firstPart != (oid.ID{}) {
		ok = get(st.mpt, st.db, append([]byte{firstPartIndex}, commSuffix...), firstPart[:])
		if !ok {
			return false
		}
	}

	if previousPart != (oid.ID{}) {
		ok = get(st.mpt, st.db, append([]byte{previousPartIndex}, commSuffix...), previousPart[:])
		if !ok {
			return false
		}
	}

	if len(deleted) != 0 {
		expVal := make([]byte, 0, oid.Size*(len(deleted)))
		for _, d := range deleted {
			expVal = append(expVal, d[:]...)
		}

		ok = get(st.mpt, st.db, append([]byte{deletedIndex}, commSuffix...), expVal)
		if !ok {
			return false
		}
	}

	if len(locked) != 0 {
		expVal := make([]byte, 0, oid.Size*(len(locked)))
		for _, l := range locked {
			expVal = append(expVal, l[:]...)
		}

		ok = get(st.mpt, st.db, append([]byte{lockedIndex}, commSuffix...), expVal)
		if !ok {
			return false
		}
	}

	if typ != objectsdk.TypeRegular {
		ok = get(st.mpt, st.db, append([]byte{typeIndex}, commSuffix...), []byte{byte(typ)})
		if !ok {
			return false
		}
	}

	return true
}

func TestObjectPut(t *testing.T) {
	ws := testWS{}
	m, stop, exitCh := createAndRunTestMeta(t, &ws)
	t.Cleanup(func() {
		stop()
		<-exitCh
	})

	testCnrs := testContainers(t, 10)
	mTestCnrs := utilcore.SliceToMap(testCnrs)

	var epoch int
	newEpoch(m, mTestCnrs, epoch)

	t.Run("storages for containers", func(t *testing.T) {
		checkDBFiles(t, m.rootPath, mTestCnrs)
	})

	t.Run("drop storage", func(t *testing.T) {
		newContainers := utilcore.SliceToMap(testCnrs[1:])

		epoch++
		newEpoch(m, newContainers, epoch)
		checkDBFiles(t, m.rootPath, newContainers)
	})

	t.Run("add storage", func(t *testing.T) {
		// add just dropped storage back
		epoch++
		newEpoch(m, mTestCnrs, epoch)
		checkDBFiles(t, m.rootPath, mTestCnrs)
	})

	t.Run("put object", func(t *testing.T) {
		cID := testCnrs[0]
		oID := oidtest.ID()
		fPart := oidtest.ID()
		pPart := oidtest.ID()
		size := uint64(testObjectSize)
		typ := objectsdk.TypeTombstone
		deleted := []oid.ID{oidtest.ID(), oidtest.ID(), oidtest.ID()}

		metaRaw := object.EncodeReplicationMetaInfo(cID, oID, fPart, pPart, size, typ, deleted, nil, testVUB, m.magicNumber)
		metaStack, err := stackitem.Deserialize(metaRaw)
		require.NoError(t, err)

		bCH := ws.blockCh()

		ws.swapResults(append(ws.notifications, state.ContainedNotificationEvent{
			NotificationEvent: state.NotificationEvent{
				Name: objPutEvName,
				Item: stackitem.NewArray([]stackitem.Item{stackitem.Make(cID[:]), stackitem.Make(oID[:]), metaStack}),
			},
		}), nil)
		bCH <- &block.Header{Index: 0}

		require.Eventually(t, func() bool {
			return checkObject(t, m, cID, oID, fPart, pPart, size, typ, deleted, nil, testVUB, m.magicNumber)
		}, 3*time.Second, time.Millisecond*100, "object was not handled properly")
	})

	t.Run("delete object", func(t *testing.T) {
		cID := testCnrs[0]
		objToDeleteOID := oidtest.ID()
		size := uint64(testObjectSize)

		metaRaw := object.EncodeReplicationMetaInfo(cID, objToDeleteOID, oid.ID{}, oid.ID{}, size, objectsdk.TypeRegular, nil, nil, testVUB, m.magicNumber)
		metaStack, err := stackitem.Deserialize(metaRaw)
		require.NoError(t, err)

		bCH := ws.blockCh()
		ws.swapResults(append(ws.notifications, state.ContainedNotificationEvent{
			NotificationEvent: state.NotificationEvent{
				Name: objPutEvName,
				Item: stackitem.NewArray([]stackitem.Item{stackitem.Make(cID[:]), stackitem.Make(objToDeleteOID[:]), metaStack}),
			},
		}), nil)
		bCH <- &block.Header{Index: 0}

		require.Eventually(t, func() bool {
			return checkObject(t, m, cID, objToDeleteOID, oid.ID{}, oid.ID{}, size, objectsdk.TypeRegular, nil, nil, testVUB, m.magicNumber)
		}, 3*time.Second, time.Millisecond*100, "object was not handled properly before deletion")

		tsCID := cID
		tsOID := oidtest.ID()
		tsSize := uint64(testObjectSize)
		deleted := []oid.ID{objToDeleteOID}

		metaRaw = object.EncodeReplicationMetaInfo(tsCID, tsOID, oid.ID{}, oid.ID{}, tsSize, objectsdk.TypeTombstone, deleted, nil, testVUB, m.magicNumber)
		metaStack, err = stackitem.Deserialize(metaRaw)
		require.NoError(t, err)

		ws.swapResults(append(ws.notifications, state.ContainedNotificationEvent{
			NotificationEvent: state.NotificationEvent{
				Name: objPutEvName,
				Item: stackitem.NewArray([]stackitem.Item{stackitem.Make(tsCID[:]), stackitem.Make(tsOID[:]), metaStack}),
			},
		}), nil)
		bCH <- &block.Header{Index: 0}

		require.Eventually(t, func() bool {
			m.m.RLock()
			st := m.storages[tsCID]
			m.m.RUnlock()

			st.m.RLock()
			defer st.m.RUnlock()

			commSuffix := objToDeleteOID[:]
			keysToCheck := [][]byte{
				append([]byte{oidIndex}, commSuffix...),
				append([]byte{sizeIndex}, commSuffix...),
				append([]byte{firstPartIndex}, commSuffix...),
				append([]byte{previousPartIndex}, commSuffix...),
				append([]byte{deletedIndex}, commSuffix...),
				append([]byte{lockedIndex}, commSuffix...),
				append([]byte{typeIndex}, commSuffix...),
			}

			for _, key := range keysToCheck {
				_, err = st.mpt.Get(key)
				if !errors.Is(err, mpt.ErrNotFound) {
					return false
				}

				_, err = st.db.Get(key)
				if !errors.Is(err, storage.ErrKeyNotFound) {
					return false
				}
			}

			return true
		}, 3*time.Second, time.Millisecond*100, "object was not deleted")
	})
}
