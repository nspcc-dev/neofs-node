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

// TombstoneSource is an interface that checks
// tombstone status in the NeoFS network.
type TombstoneSource interface {
	// IsTombstoneAvailable must return boolean value that means
	// provided tombstone's presence in the NeoFS network at the
	// time of the passed epoch.
	IsTombstoneAvailable(ctx context.Context, addr *addressSDK.Address, epoch uint64) bool
}

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

	iterPrm := new(meta.GarbageIterationPrm)

	// iterate over metabase's objects with GC mark
	// (no more than s.rmBatchSize objects)
	err := s.metaBase.IterateOverGarbage(iterPrm.SetHandler(func(g meta.GarbageObject) error {
		buf = append(buf, g.Address())

		if len(buf) == s.rmBatchSize {
			return meta.ErrInterruptIterator
		}

		return nil
	}))
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
	epoch := e.(newEpoch).epoch
	log := s.log.With(zap.Uint64("epoch", epoch))

	log.Debug("started expired tombstones handling")

	const tssDeleteBatch = 50
	tss := make([]meta.TombstonedObject, 0, tssDeleteBatch)
	tssExp := make([]meta.TombstonedObject, 0, tssDeleteBatch)

	iterPrm := new(meta.GraveyardIterationPrm).SetHandler(func(deletedObject meta.TombstonedObject) error {
		tss = append(tss, deletedObject)

		if len(tss) == tssDeleteBatch {
			return meta.ErrInterruptIterator
		}

		return nil
	})

	for {
		log.Debug("iterating tombstones")

		err := s.metaBase.IterateOverGraveyard(iterPrm)
		if err != nil {
			log.Error("iterator over graveyard failed", zap.Error(err))
			return
		}

		tssLen := len(tss)
		if tssLen == 0 {
			break
		}

		for _, ts := range tss {
			if !s.tsSource.IsTombstoneAvailable(ctx, ts.Tombstone(), epoch) {
				tssExp = append(tssExp, ts)
			}
		}

		log.Debug("handling expired tombstones batch", zap.Int("number", tssLen))
		s.expiredTombstonesCallback(ctx, tss)

		iterPrm.SetOffset(tss[tssLen-1].Address())
		tss = tss[:0]
		tssExp = tssExp[:0]
	}

	log.Debug("finished expired tombstones handling")
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

// HandleExpiredTombstones marks tombstones themselves as garbage
// and clears up corresponding graveyard records.
//
// Does not modify tss.
func (s *Shard) HandleExpiredTombstones(tss []meta.TombstonedObject) {
	// Mark tombstones as garbage.
	var pInhume meta.InhumePrm

	tsAddrs := make([]*addressSDK.Address, 0, len(tss))
	for _, ts := range tss {
		tsAddrs = append(tsAddrs, ts.Tombstone())
	}

	pInhume.WithGCMark()
	pInhume.WithAddresses(tsAddrs...)

	// inhume tombstones
	_, err := s.metaBase.Inhume(&pInhume)
	if err != nil {
		s.log.Warn("could not mark tombstones as garbage",
			zap.String("error", err.Error()),
		)

		return
	}

	// drop just processed expired tombstones
	// from graveyard
	err = s.metaBase.DropGraves(tss)
	if err != nil {
		s.log.Warn("could not drop expired grave records", zap.Error(err))
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
