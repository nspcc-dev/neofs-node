package shard

import (
	"sync"
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

func (s *Shard) collectExpiredObjects(e Event) {
	epoch := e.(newEpoch).epoch
	log := s.log.With(zap.Uint64("epoch", epoch))

	log.Debug("started expired objects handling")

	expired, err := s.getExpiredObjects(e.(newEpoch).epoch, func(typ object.Type) bool {
		return typ != object.TypeLock && typ != object.TypeTombstone
	})
	if err != nil {
		log.Warn("iterator over expired objects failed", zap.Error(err))
		return
	}
	if len(expired) == 0 {
		log.Debug("no expired objects")
		return
	}

	log.Debug("collected expired objects", zap.Int("num", len(expired)))

	s.expiredObjectsCallback(expired)

	log.Debug("finished expired objects handling")
}

func (s *Shard) collectExpiredTombstones(e Event) {
	epoch := e.(newEpoch).epoch
	log := s.log.With(zap.Uint64("epoch", epoch))

	log.Debug("started expired graveyard mark handling")

	dropped, err := s.metaBase.DropExpiredTSMarks(epoch)
	if err != nil {
		log.Error("cleaning graveyard up failed", zap.Error(err))
		return
	}

	log.Debug("finished expired graveyard mark handling", zap.Int("dropped marks", dropped))

	log.Debug("started expired tombstones handling")

	expired, err := s.getExpiredObjects(e.(newEpoch).epoch, func(typ object.Type) bool {
		return typ == object.TypeTombstone
	})
	if err != nil {
		log.Warn("iterator over expired tombstones failed", zap.Error(err))
		return
	}
	if len(expired) == 0 {
		log.Debug("no expired tombstones")
		return
	}

	err = s.MarkGarbage(false, expired...)
	if err != nil {
		log.Warn("failed to mark expired tombstones as garbage", zap.Error(err))
		return
	}

	log.Debug("finished expired tombstones handling", zap.Int("count", len(expired)))
}

func (s *Shard) collectExpiredLocks(e Event) {
	expired, err := s.getExpiredObjects(e.(newEpoch).epoch, func(typ object.Type) bool {
		return typ == object.TypeLock
	})
	if err != nil || len(expired) == 0 {
		if err != nil {
			s.log.Warn("iterator over expired locks failed", zap.Error(err))
		}
		return
	}

	s.expiredLocksCallback(expired)
}

func (s *Shard) getExpiredObjects(epoch uint64, typeCond func(object.Type) bool) ([]oid.Address, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	if s.info.Mode.NoMetabase() {
		return nil, ErrDegradedMode
	}

	var expired []oid.Address

	err := s.metaBase.IterateExpired(epoch, func(expiredObject *meta.ExpiredObject) error {
		if typeCond(expiredObject.Type()) {
			expired = append(expired, expiredObject.Address())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return expired, nil
}

// HandleExpiredLocks unlocks all objects which were locked by lockers.
// If successful, marks lockers themselves as garbage.
// Returns every object that is unlocked.
func (s *Shard) HandleExpiredLocks(lockers []oid.Address) []oid.Address {
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

	inhumed, _, err := s.metaBase.MarkGarbage(true, false, lockers...)
	if err != nil {
		s.log.Warn("failure to mark lockers as garbage",
			zap.Error(err),
		)

		return nil
	}

	s.decObjectCounterBy(logical, inhumed)

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

// HandleDeletedLocks unlocks all objects which were locked by lockers.
// Returns every object that is unlocked.
func (s *Shard) HandleDeletedLocks(lockers []oid.Address) []oid.Address {
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

// NotificationChannel returns channel for shard events.
func (s *Shard) NotificationChannel() chan<- Event {
	return s.gc.eventChan
}
