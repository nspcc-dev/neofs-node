package loadcontroller_test

import (
	"context"
	"math/rand"
	"sync"
	"testing"

	loadcontroller "github.com/nspcc-dev/neofs-node/pkg/services/container/announcement/load/controller"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/stretchr/testify/require"
)

type testAnnouncementStorage struct {
	w loadcontroller.Writer

	i loadcontroller.Iterator

	mtx sync.RWMutex

	m map[uint64][]container.SizeEstimation
}

func newTestStorage() *testAnnouncementStorage {
	return &testAnnouncementStorage{
		m: make(map[uint64][]container.SizeEstimation),
	}
}

func (s *testAnnouncementStorage) InitIterator(context.Context) (loadcontroller.Iterator, error) {
	if s.i != nil {
		return s.i, nil
	}

	return s, nil
}

func (s *testAnnouncementStorage) Iterate(f loadcontroller.UsedSpaceFilter, h loadcontroller.UsedSpaceHandler) error {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	for _, v := range s.m {
		for _, a := range v {
			if f(a) {
				if err := h(a); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (s *testAnnouncementStorage) InitWriter(context.Context) (loadcontroller.Writer, error) {
	if s.w != nil {
		return s.w, nil
	}

	return s, nil
}

func (s *testAnnouncementStorage) Put(v container.SizeEstimation) error {
	s.mtx.Lock()
	s.m[v.Epoch()] = append(s.m[v.Epoch()], v)
	s.mtx.Unlock()

	return nil
}

func (s *testAnnouncementStorage) Close() error {
	return nil
}

func randAnnouncement() (a container.SizeEstimation) {
	a.SetContainer(cidtest.ID())
	a.SetValue(rand.Uint64())

	return
}

func TestSimpleScenario(t *testing.T) {
	// create storage to write final estimations
	resultStorage := newTestStorage()

	// create storages to accumulate announcements
	accumulatingStorageN2 := newTestStorage()

	// create storage of local metrics
	localStorageN1 := newTestStorage()
	localStorageN2 := newTestStorage()

	// create 2 controllers:  1st writes announcements to 2nd, 2nd directly to final destination
	ctrlN1 := loadcontroller.New(loadcontroller.Prm{
		LocalMetrics:            localStorageN1,
		AnnouncementAccumulator: newTestStorage(),
		LocalAnnouncementTarget: &testAnnouncementStorage{
			w: accumulatingStorageN2,
		},
		ResultReceiver: resultStorage,
	})

	ctrlN2 := loadcontroller.New(loadcontroller.Prm{
		LocalMetrics:            localStorageN2,
		AnnouncementAccumulator: accumulatingStorageN2,
		LocalAnnouncementTarget: &testAnnouncementStorage{
			w: resultStorage,
		},
		ResultReceiver: resultStorage,
	})

	const processEpoch uint64 = 10

	const goodNum = 4

	// create 2 random values for processing epoch and 1 for some different
	announces := make([]container.SizeEstimation, 0, goodNum)

	for range goodNum {
		a := randAnnouncement()
		a.SetEpoch(processEpoch)

		announces = append(announces, a)
	}

	// store one half of "good" announcements to 1st metrics storage, another - to 2nd
	// and "bad" to both
	for i := range goodNum / 2 {
		require.NoError(t, localStorageN1.Put(announces[i]))
	}

	for i := goodNum / 2; i < goodNum; i++ {
		require.NoError(t, localStorageN2.Put(announces[i]))
	}

	wg := new(sync.WaitGroup)
	wg.Add(2)

	startPrm := loadcontroller.StartPrm{
		Epoch: processEpoch,
	}

	// start both controllers
	go func() {
		ctrlN1.Start(startPrm)
		wg.Done()
	}()

	go func() {
		ctrlN2.Start(startPrm)
		wg.Done()
	}()

	wg.Wait()
	wg.Add(2)

	stopPrm := loadcontroller.StopPrm{
		Epoch: processEpoch,
	}

	// stop both controllers
	go func() {
		ctrlN1.Stop(stopPrm)
		wg.Done()
	}()

	go func() {
		ctrlN2.Stop(stopPrm)
		wg.Done()
	}()

	wg.Wait()

	// result target should contain all "good" announcements and should not container the "bad" one
	var res []container.SizeEstimation

	err := resultStorage.Iterate(
		func(a container.SizeEstimation) bool {
			return true
		},
		func(a container.SizeEstimation) error {
			res = append(res, a)
			return nil
		},
	)
	require.NoError(t, err)

	for i := range announces {
		require.Contains(t, res, announces[i])
	}
}
