package shard

import (
	"sync"
	"sync/atomic"
	"time"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Event represents class of external events.
type Event interface {
	typ() eventType
}

type eventType int

const (
	_ eventType = iota
	eventNewEpoch
)

type newEpoch struct {
	epoch uint64
}

func (e newEpoch) typ() eventType {
	return eventNewEpoch
}

// EventNewEpoch returns new epoch event.
func EventNewEpoch(e uint64) Event {
	return newEpoch{
		epoch: e,
	}
}

type eventHandler func(Event)

type eventHandlers struct {
	prevGroup sync.WaitGroup

	handlers []eventHandler
}

type gc struct {
	*gcCfg

	onceStop    sync.Once
	stopChannel chan struct{}
	wg          sync.WaitGroup

	workerPool util.WorkerPool

	remover func()

	eventChan     chan Event
	mEventHandler map[eventType]*eventHandlers

	// currentEpoch stores the latest epoch announced via EventNewEpoch.
	currentEpoch atomic.Uint64
	// processedEpoch indicates highest epoch for which expired processing found nothing.
	processedEpoch atomic.Uint64
}

type gcCfg struct {
	removerInterval time.Duration

	log *zap.Logger

	workerPoolInit func(int) util.WorkerPool
}

func defaultGCCfg() gcCfg {
	return gcCfg{
		removerInterval: 10 * time.Second,
		log:             zap.L(),
		workerPoolInit: func(int) util.WorkerPool {
			return nil
		},
	}
}

func (gc *gc) init() {
	sz := 0

	for _, v := range gc.mEventHandler {
		sz += len(v.handlers)
	}

	if sz > 0 {
		gc.workerPool = gc.workerPoolInit(sz)
	}

	gc.wg.Add(2)
	go gc.tickRemover()
	go gc.listenEvents()
}

func (gc *gc) listenEvents() {
	defer gc.wg.Done()

	for {
		select {
		case <-gc.stopChannel:
			gc.log.Warn("stop event listener")
			return
		case event := <-gc.eventChan:
			v, ok := gc.mEventHandler[event.typ()]
			if !ok {
				continue
			}

			v.prevGroup.Wait()

			v.prevGroup.Add(len(v.handlers))

			for i := range v.handlers {
				h := v.handlers[i]

				err := gc.workerPool.Submit(func() {
					h(event)
					v.prevGroup.Done()
				})
				if err != nil {
					gc.log.Warn("could not submit GC job to worker pool",
						zap.Error(err),
					)

					v.prevGroup.Done()
				}
			}
		}
	}
}

func (gc *gc) tickRemover() {
	defer gc.wg.Done()

	timer := time.NewTimer(gc.removerInterval)

	for {
		select {
		case <-gc.stopChannel:
			if gc.workerPool != nil {
				gc.workerPool.Release()
			}

			gc.log.Debug("GC is stopped")
			return
		case <-timer.C:
			gc.remover()
			timer.Reset(gc.removerInterval)
		}
	}
}

func (gc *gc) stop() {
	gc.onceStop.Do(func() {
		close(gc.stopChannel)
	})

	gc.log.Info("waiting for GC workers to stop...")
	gc.wg.Wait()
}

// iterates over metabase and deletes objects
// with GC-marked graves.
// Does nothing if shard is in "read-only" mode.
func (s *Shard) removeGarbage() {
	s.m.RLock()
	defer s.m.RUnlock()

	if s.info.Mode != mode.ReadWrite {
		return
	}

	s.collectExpiredObjects()

	gObjs, gContainers, err := s.metaBase.GetGarbage(s.rmBatchSize)
	if err != nil {
		s.log.Warn("fetching garbage objects",
			zap.Error(err),
		)

		return
	}

	// delete accumulated objects
	err = s.deleteObjs(gObjs)
	if err != nil {
		s.log.Warn("could not delete the objects",
			zap.Error(err),
		)

		return
	}

	// objects are removed, clean up empty container (all the object
	// were deleted from the disk) information from the metabase
	for _, cID := range gContainers {
		err = s.metaBase.DeleteContainer(cID)
		if err != nil {
			s.log.Warn("clean up container in metabase",
				zap.Stringer("cID", cID),
				zap.Error(err),
			)
		}
	}
}

func (s *Shard) collectExpiredObjects() {
	epoch := s.gc.currentEpoch.Load()
	doneUpTo := s.gc.processedEpoch.Load()
	if s.info.Mode.NoMetabase() || doneUpTo == epoch {
		return
	}
	if doneUpTo > epoch {
		s.log.Warn("current epoch is less than the last processed epoch in GC",
			zap.Uint64("current", epoch),
			zap.Uint64("processed", doneUpTo),
		)
		s.gc.processedEpoch.Store(epoch)
		return
	}

	var (
		toDeleteTombstones []oid.Address
		expiredLocks       []oid.Address
		expiredObjects     []oid.Address
	)
	log := s.log.With(zap.Uint64("epoch", epoch))
	log.Debug("started expired objects handling")

	if dropped, err := s.metaBase.DropExpiredTSMarks(epoch); err != nil {
		log.Warn("drop expired tombstone marks", zap.Error(err))
	} else if dropped > 0 {
		log.Debug("dropped expired tombstone marks", zap.Int("count", dropped))
	}

	collected := 0
	err := s.metaBase.IterateExpired(epoch, func(expiredObject *meta.ExpiredObject) error {
		switch expiredObject.Type() {
		case object.TypeTombstone:
			toDeleteTombstones = append(toDeleteTombstones, expiredObject.Address())
		case object.TypeLock:
			expiredLocks = append(expiredLocks, expiredObject.Address())
		default:
			expiredObjects = append(expiredObjects, expiredObject.Address())
		}
		collected++
		if collected >= s.rmBatchSize {
			return meta.ErrInterruptIterator
		}
		return nil
	})
	if err != nil {
		log.Warn("iterate expired objects", zap.Error(err))
	}
	if collected == 0 {
		s.gc.processedEpoch.Store(epoch)
	}

	log.Debug("collected expired locks", zap.Int("num", len(expiredLocks)))
	if len(expiredLocks) > 0 && s.expiredLocksCallback != nil {
		s.expiredLocksCallback(expiredLocks)
	}

	log.Debug("collected expired tombstones", zap.Int("num", len(toDeleteTombstones)))
	if len(toDeleteTombstones) > 0 {
		err = s.deleteObjs(toDeleteTombstones)
		if err != nil {
			log.Warn("could not delete tombstones",
				zap.Error(err),
			)
		}
	}

	log.Debug("collected expired objects", zap.Int("num", len(expiredObjects)))
	if len(expiredObjects) > 0 && s.expiredObjectsCallback != nil {
		s.expiredObjectsCallback(expiredObjects)
	}
	log.Debug("finished expired objects handling")
}

// FreeLockedBy unlocks all objects which were locked by lockers.
// Returns every object that is unlocked.
func (s *Shard) FreeLockedBy(lockers []oid.Address) []oid.Address {
	if s.GetMode().NoMetabase() {
		return nil
	}

	unlocked, err := s.metaBase.FreeLockedBy(lockers)
	if err != nil {
		s.log.Warn("failure to unlock objects",
			zap.Error(err),
		)

		return nil
	}

	return unlocked
}

// FilterExpired filters expired objects by address through the metabase and returns them.
func (s *Shard) FilterExpired(addrs []oid.Address) []oid.Address {
	expired, err := s.metaBase.FilterExpired(addrs)
	if err != nil {
		s.log.Warn("expired object filtering",
			zap.Error(err),
		)

		return nil
	}

	return expired
}

// NotificationChannel returns channel for shard events.
func (s *Shard) NotificationChannel() chan<- Event {
	return s.gc.eventChan
}

// setEpochEventHandler handles EventNewEpoch by setting current epoch
// and marking that expired objects up to this epoch are not yet processed.
func (s *Shard) setEpochEventHandler(e Event) {
	ne := e.(newEpoch)
	s.gc.currentEpoch.Store(ne.epoch)
}
