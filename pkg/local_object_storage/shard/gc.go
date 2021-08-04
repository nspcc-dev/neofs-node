package shard

import (
	"context"
	"sync"
	"time"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
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

	stopChannel chan struct{}

	workerPool util.WorkerPool

	remover func()

	mEventHandler map[eventType]*eventHandlers
}

type gcCfg struct {
	eventChanInit func() <-chan Event

	removerInterval time.Duration

	log *logger.Logger

	workerPoolInit func(int) util.WorkerPool
}

func defaultGCCfg() *gcCfg {
	ch := make(chan Event)
	close(ch)

	return &gcCfg{
		eventChanInit: func() <-chan Event {
			return ch
		},
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
	eventChan := gc.eventChanInit()

	for {
		event, ok := <-eventChan
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
			gc.log.Debug("GC is stopped")
			return
		case <-timer.C:
			gc.remover()
			timer.Reset(gc.removerInterval)
		}
	}
}

func (gc *gc) stop() {
	gc.stopChannel <- struct{}{}
}

// iterates over metabase graveyard and deletes objects
// with GC-marked graves.
func (s *Shard) removeGarbage() {
	buf := make([]*object.Address, 0, s.rmBatchSize)

	// iterate over metabase graveyard and accumulate
	// objects with GC mark (no more the s.rmBatchSize objects)
	err := s.metaBase.IterateOverGraveyard(func(g *meta.Grave) error {
		if g.WithGCMark() {
			buf = append(buf, g.Address())
		}

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
	epoch := e.(newEpoch).epoch

	var expired []*object.Address

	// collect expired non-tombstone object
	err := s.metaBase.IterateExpired(epoch, func(expiredObject *meta.ExpiredObject) error {
		select {
		case <-ctx.Done():
			return meta.ErrInterruptIterator
		default:
		}

		if expiredObject.Type() != object.TypeTombstone {
			expired = append(expired, expiredObject.Address())
		}

		return nil
	})
	if err != nil {
		s.log.Warn("iterator over expired objects failed",
			zap.String("error", err.Error()),
		)

		return
	} else if len(expired) == 0 {
		return
	}

	// check if context canceled
	select {
	case <-ctx.Done():
		return
	default:
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

// TODO: can be unified with Shard.collectExpiredObjects.
func (s *Shard) collectExpiredTombstones(ctx context.Context, e Event) {
	epoch := e.(newEpoch).epoch

	var expired []*object.Address

	// collect expired tombstone objects
	err := s.metaBase.IterateExpired(epoch, func(expiredObject *meta.ExpiredObject) error {
		select {
		case <-ctx.Done():
			return meta.ErrInterruptIterator
		default:
		}

		if expiredObject.Type() == object.TypeTombstone {
			expired = append(expired, expiredObject.Address())
		}

		return nil
	})
	if err != nil {
		s.log.Warn("iterator over expired tombstones failed",
			zap.String("error", err.Error()),
		)

		return
	} else if len(expired) == 0 {
		return
	}

	// check if context canceled
	select {
	case <-ctx.Done():
		return
	default:
	}

	s.expiredTombstonesCallback(ctx, expired)
}

// HandleExpiredTombstones mark to be removed all objects that are expired in epoch
// and protected by tombstone with string address from tss.
//
// Does not modify tss.
func (s *Shard) HandleExpiredTombstones(tss map[string]struct{}) {
	inhume := make([]*object.Address, 0, len(tss))

	err := s.metaBase.IterateCoveredByTombstones(tss, func(addr *object.Address) error {
		inhume = append(inhume, addr)
		return nil
	})
	if err != nil {
		s.log.Warn("iterator over expired objects failed",
			zap.String("error", err.Error()),
		)

		return
	} else if len(inhume) == 0 {
		return
	}

	_, err = s.metaBase.Inhume(new(meta.InhumePrm).
		WithAddresses(inhume...).
		WithGCMark(),
	)
	if err != nil {
		s.log.Warn("could not inhume objects under the expired tombstone",
			zap.String("error", err.Error()),
		)

		return
	}
}
