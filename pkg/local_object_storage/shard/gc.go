package shard

import (
	"sync"
	"sync/atomic"
	"time"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
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

type eventHandler struct {
	prevGroup sync.WaitGroup
	handler   func(Event)
}

type gc struct {
	*gcCfg

	onceStop    sync.Once
	stopChannel chan struct{}
	wg          sync.WaitGroup

	remover func()

	eventChan     chan Event
	mEventHandler map[eventType]*eventHandler

	// currentEpoch stores the latest epoch announced via EventNewEpoch.
	currentEpoch atomic.Uint64
	// processedEpoch indicates highest epoch for which expired processing found nothing.
	processedEpoch atomic.Uint64
}

type gcCfg struct {
	removerInterval   time.Duration
	containerPayments ContainerPayments

	log *zap.Logger
}

func defaultGCCfg() gcCfg {
	return gcCfg{
		removerInterval: 10 * time.Second,
		log:             zap.L(),
	}
}

func (gc *gc) init() {
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
			v.prevGroup.Add(1)
			go func() {
				v.handler(event)
				v.prevGroup.Done()
			}()
		}
	}
}

func (gc *gc) tickRemover() {
	defer gc.wg.Done()

	timer := time.NewTimer(gc.removerInterval)

	for {
		select {
		case <-gc.stopChannel:
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
	for _, h := range gc.mEventHandler {
		h.prevGroup.Wait()
	}
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

	l := s.log.With(zap.Uint64("epoch", ne.epoch))
	l.Debug("handling new epoch event...")

	cnrs, err := s.ListContainers()
	if err != nil {
		l.Warn("reading containers list failed", zap.Error(err))
		return
	}

	// maxUnpaidEpochDelay is a maximum number of epoch GC does not delete unpaid
	// containers.
	const maxUnpaidEpochDelay = 3

	for _, cID := range cnrs {
		unpaidSince, err := s.gcCfg.containerPayments.UnpaidSince(cID)
		if err != nil {
			l.Warn("cannot check payment status for container", zap.Stringer("cID", cID), zap.Error(err))
			continue
		}

		if unpaidSince < 0 {
			continue
		}

		l.Warn("found unpaid container", zap.Stringer("cID", cID), zap.Int64("unpaidSince", unpaidSince))

		if ne.epoch-uint64(unpaidSince) >= maxUnpaidEpochDelay {
			// TODO: delete container after v0.50.0 release.

			//l.Info("deleting unpaid container", zap.Stringer("cID", cID), zap.Int64("unpaidSince", unpaidSince))
			//
			//err := s.DeleteContainer(context.Background(), cID)
			//if err != nil {
			//	l.Warn("cannot delete unpaid container", zap.Stringer("cID", cID), zap.Error(err))
			//	continue
			//}

			l.Info("WARNING: unpaid container will be deleted in the next release",
				zap.Stringer("cID", cID), zap.Int64("unpaidSince", unpaidSince))
		}
	}

	s.log.Debug("finished handling new epoch event")
}
