package innerring

import (
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"go.uber.org/zap"
)

type (
	epochState interface {
		EpochCounter() uint64
	}

	subEpochEventHandler struct {
		handler     event.Handler // handle to execute
		durationMul uint32        // X: X/Y of epoch in seconds
		durationDiv uint32        // Y: X/Y of epoch in seconds
	}

	newEpochHandler func()

	epochTimerArgs struct {
		l *zap.Logger

		newEpochHandlers []newEpochHandler

		cnrWrapper *container.Client // to invoke stop container estimation
		epoch      epochState        // to specify which epoch to stop, and epoch duration

		stopEstimationDMul uint32 // X: X/Y of epoch in seconds
		stopEstimationDDiv uint32 // Y: X/Y of epoch in seconds

		basicIncome subEpochEventHandler
	}
)

type deltaHandler struct {
	nextTickAt uint64 // recalculated every [epochTimer.reset] call
	done       bool

	mul, div uint64
	h        func()
}

type epochTimer struct {
	m          sync.Mutex
	done       bool
	nextTickAt uint64 // timestamp with milliseconds precision as in NEO's blocks

	eHandlers     []newEpochHandler
	deltaHandlers []*deltaHandler
}

func (et *epochTimer) updateTime(curr uint64) {
	et.m.Lock()
	defer et.m.Unlock()

	if et.done {
		return
	}

	if et.nextTickAt <= curr {
		for _, h := range et.eHandlers {
			h()
		}
		et.done = true
	}
	for _, dh := range et.deltaHandlers {
		if !dh.done && dh.nextTickAt <= curr {
			dh.h()
			dh.done = true
		}
	}
}

func (et *epochTimer) reset(lastTick, curr, dur uint64) {
	et.m.Lock()
	defer et.m.Unlock()

	et.nextTickAt = lastTick + dur
	et.done = false

	for _, dh := range et.deltaHandlers {
		dh.nextTickAt = lastTick + dur*dh.mul/dh.div
		dh.done = dh.nextTickAt < curr
	}
}

func newEpochTimer(args *epochTimerArgs) *epochTimer {
	et := epochTimer{
		eHandlers:     args.newEpochHandlers,
		deltaHandlers: make([]*deltaHandler, 0, 2),
	}

	// sub-timer for epoch timer to tick stop container estimation events at
	// some block in epoch
	et.deltaHandlers = append(et.deltaHandlers, &deltaHandler{
		mul: uint64(args.stopEstimationDMul),
		div: uint64(args.stopEstimationDDiv),
		h: func() {
			epochN := args.epoch.EpochCounter()
			if epochN == 0 { // estimates are invalid in genesis epoch
				return
			}

			estimationEpoch := epochN - 1
			args.l.Info("stop estimation collections", zap.Uint64("epoch", estimationEpoch))

			prm := container.StopEstimationPrm{}
			prm.SetEpoch(estimationEpoch)

			err := args.cnrWrapper.StopEstimation(prm)
			if err != nil {
				args.l.Warn("can't stop epoch estimation",
					zap.Uint64("epoch", epochN),
					zap.Error(err))
			}
		}})

	et.deltaHandlers = append(et.deltaHandlers, &deltaHandler{
		mul: uint64(args.basicIncome.durationMul),
		div: uint64(args.basicIncome.durationDiv),
		h: func() {
			epochN := args.epoch.EpochCounter()
			if epochN == 0 { // estimates are invalid in genesis epoch
				return
			}

			args.basicIncome.handler(
				settlement.NewBasicIncomeEvent(epochN - 1),
			)
		}})

	return &et
}
