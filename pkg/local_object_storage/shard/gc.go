package shard

import (
	"context"
	"sync"
	"time"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
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

type eventHandler func(context.Context, Event)

type eventHandlers struct {
	prevGroup sync.WaitGroup

	cancelFunc context.CancelFunc

	handlers []eventHandler
}

type gc struct {
	*gcCfg

	onceStop    sync.Once
	stopChannel chan struct{}

	workerPool util.WorkerPool

	remover func()

	mEventHandler map[eventType]*eventHandlers
}

type gcCfg struct {
	eventChan <-chan Event

	removerInterval time.Duration

	log *logger.Logger

	workerPoolInit func(int) util.WorkerPool
}

func defaultGCCfg() *gcCfg {
	ch := make(chan Event)
	close(ch)

	return &gcCfg{
		eventChan:       ch,
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

	go gc.tickRemover()
	go gc.listenEvents()
}

func (gc *gc) listenEvents() {
	for {
		event, ok := <-gc.eventChan
		if !ok {
			gc.log.Warn("stop event listener by closed channel")
			return
		}

		v, ok := gc.mEventHandler[event.typ()]
		if !ok {
			continue
		}

		v.cancelFunc()
		v.prevGroup.Wait()

		var ctx context.Context
		ctx, v.cancelFunc = context.WithCancel(context.Background())

		v.prevGroup.Add(len(v.handlers))

		for i := range v.handlers {
			h := v.handlers[i]

			err := gc.workerPool.Submit(func() {
				h(ctx, event)
				v.prevGroup.Done()
			})
			if err != nil {
				gc.log.Warn("could not submit GC job to worker pool",
					zap.String("error", err.Error()),
				)

				v.prevGroup.Done()
			}
		}
	}
}

func (gc *gc) tickRemover() {
	timer := time.NewTimer(gc.removerInterval)
	defer timer.Stop()

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
		gc.stopChannel <- struct{}{}
	})
}

// iterates over metabase and deletes objects
// with GC-marked graves.
// Does nothing if shard is in "read-only" mode.
func (s *Shard) removeGarbage() {
	if s.GetMode() != ModeReadWrite {
		return
	}

	buf := make([]*addressSDK.Address, 0, s.rmBatchSize)

	// iterate over metabase's objects with GC mark
	// (no more than s.rmBatchSize objects)
	err := s.metaBase.IterateOverGarbage(func(g *meta.DeletedObject) error {
		buf = append(buf, g.Address())

		if len(buf) == s.rmBatchSize {
			return meta.ErrInterruptIterator
		}

		return nil
	})
	if err != nil {
		s.log.Warn("iterator over metabase graveyard failed",
			zap.String("error", err.Error()),
		)

		return
	} else if len(buf) == 0 {
		return
	}

	// delete accumulated objects
	_, err = s.Delete(new(DeletePrm).
		WithAddresses(buf...),
	)
	if err != nil {
		s.log.Warn("could not delete the objects",
			zap.String("error", err.Error()),
		)

		return
	}
}

func (s *Shard) collectExpiredObjects(ctx context.Context, e Event) {
	expired, err := s.getExpiredObjects(ctx, e.(newEpoch).epoch, func(typ object.Type) bool {
		return typ != object.TypeTombstone && typ != object.TypeLock
	})
	if err != nil || len(expired) == 0 {
		if err != nil {
			s.log.Warn("iterator over expired objects failed", zap.String("error", err.Error()))
		}
		return
	}

	// inhume the collected objects
	_, err = s.metaBase.Inhume(new(meta.InhumePrm).
		WithAddresses(expired...).
		WithGCMark(),
	)
	if err != nil {
		s.log.Warn("could not inhume the objects",
			zap.String("error", err.Error()),
		)

		return
	}
}

func (s *Shard) collectExpiredTombstones(ctx context.Context, e Event) {
	expired, err := s.getExpiredObjects(ctx, e.(newEpoch).epoch, func(typ object.Type) bool {
		return typ == object.TypeTombstone
	})
	if err != nil || len(expired) == 0 {
		if err != nil {
			s.log.Warn("iterator over expired tombstones failed", zap.String("error", err.Error()))
		}
		return
	}

	s.expiredTombstonesCallback(ctx, expired)
}

func (s *Shard) collectExpiredLocks(ctx context.Context, e Event) {
	expired, err := s.getExpiredObjects(ctx, e.(newEpoch).epoch, func(typ object.Type) bool {
		return typ == object.TypeLock
	})
	if err != nil || len(expired) == 0 {
		if err != nil {
			s.log.Warn("iterator over expired locks failed", zap.String("error", err.Error()))
		}
		return
	}

	s.expiredLocksCallback(ctx, expired)
}

func (s *Shard) getExpiredObjects(ctx context.Context, epoch uint64, typeCond func(object.Type) bool) ([]*addressSDK.Address, error) {
	var expired []*addressSDK.Address

	err := s.metaBase.IterateExpired(epoch, func(expiredObject *meta.ExpiredObject) error {
		select {
		case <-ctx.Done():
			return meta.ErrInterruptIterator
		default:
			if typeCond(expiredObject.Type()) {
				expired = append(expired, expiredObject.Address())
			}
			return nil
		}
	})
	if err != nil {
		return nil, err
	}
	return expired, ctx.Err()
}

// HandleExpiredTombstones marks to be removed all objects that are
// protected by tombstones with string addresses from tss.
// If successful, marks tombstones themselves as garbage.
//
// Does not modify tss.
func (s *Shard) HandleExpiredTombstones(tss map[string]*addressSDK.Address) {
	inhume := make([]*addressSDK.Address, 0, len(tss))

	// Collect all objects covered by the tombstones.

	err := s.metaBase.IterateCoveredByTombstones(tss, func(addr *addressSDK.Address) error {
		inhume = append(inhume, addr)
		return nil
	})
	if err != nil {
		s.log.Warn("iterator over expired objects failed",
			zap.String("error", err.Error()),
		)

		return
	}

	// Mark collected objects as garbage.

	var pInhume meta.InhumePrm

	pInhume.WithGCMark()

	if len(inhume) > 0 {
		// inhume objects
		pInhume.WithAddresses(inhume...)

		_, err = s.metaBase.Inhume(&pInhume)
		if err != nil {
			s.log.Warn("could not inhume objects under the expired tombstone",
				zap.String("error", err.Error()),
			)

			return
		}
	}

	// Mark the tombstones as garbage.

	inhume = inhume[:0]

	for _, addr := range tss {
		inhume = append(inhume, addr)
	}

	pInhume.WithAddresses(inhume...) // GC mark is already set above

	// inhume tombstones
	_, err = s.metaBase.Inhume(&pInhume)
	if err != nil {
		s.log.Warn("could not mark tombstones as garbage",
			zap.String("error", err.Error()),
		)

		return
	}
}

// HandleExpiredLocks unlocks all objects which were locked by lockers.
// If successful, marks lockers themselves as garbage.
func (s *Shard) HandleExpiredLocks(lockers []*addressSDK.Address) {
	err := s.metaBase.FreeLockedBy(lockers)
	if err != nil {
		s.log.Warn("failure to unlock objects",
			zap.String("error", err.Error()),
		)

		return
	}

	var pInhume meta.InhumePrm
	pInhume.WithAddresses(lockers...)
	pInhume.WithGCMark()

	_, err = s.metaBase.Inhume(&pInhume)
	if err != nil {
		s.log.Warn("failure to mark lockers as garbage",
			zap.String("error", err.Error()),
		)

		return
	}
}
