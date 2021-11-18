package innerring

import (
	"context"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/alphabet"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement"
	timerEvent "github.com/nspcc-dev/neofs-node/pkg/innerring/timers"
	container "github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/timer"
	"go.uber.org/zap"
)

type (
	epochState interface {
		EpochCounter() uint64
		EpochDuration() uint64
	}

	subEpochEventHandler struct {
		handler     event.Handler // handle to execute
		durationMul uint32        // X: X/Y of epoch in blocks
		durationDiv uint32        // Y: X/Y of epoch in blocks
	}

	newEpochHandler func()

	epochTimerArgs struct {
		l *zap.Logger

		newEpochHandlers []newEpochHandler

		cnrWrapper *container.Wrapper // to invoke stop container estimation
		epoch      epochState         // to specify which epoch to stop, and epoch duration

		stopEstimationDMul uint32 // X: X/Y of epoch in blocks
		stopEstimationDDiv uint32 // Y: X/Y of epoch in blocks

		collectBasicIncome    subEpochEventHandler
		distributeBasicIncome subEpochEventHandler
	}

	emitTimerArgs struct {
		ap *alphabet.Processor // to handle new emission tick

		emitDuration uint32 // in blocks
	}

	depositor func() (util.Uint256, error)
	awaiter   func(context.Context, util.Uint256) error
)

func (s *Server) addBlockTimer(t *timer.BlockTimer) {
	s.blockTimers = append(s.blockTimers, t)
}

func (s *Server) startBlockTimers() error {
	for i := range s.blockTimers {
		if err := s.blockTimers[i].Reset(); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) tickTimers() {
	for i := range s.blockTimers {
		s.blockTimers[i].Tick()
	}
}

func newEpochTimer(args *epochTimerArgs) *timer.BlockTimer {
	epochTimer := timer.NewBlockTimer(
		func() (uint32, error) {
			return uint32(args.epoch.EpochDuration()), nil
		},
		func() {
			for _, handler := range args.newEpochHandlers {
				handler()
			}
		},
	)

	// sub-timer for epoch timer to tick stop container estimation events at
	// some block in epoch
	epochTimer.OnDelta(
		args.stopEstimationDMul,
		args.stopEstimationDDiv,
		func() {
			epochN := args.epoch.EpochCounter()
			if epochN == 0 { // estimates are invalid in genesis epoch
				return
			}

			prm := container.StopEstimationPrm{}
			prm.SetEpoch(epochN - 1)

			err := args.cnrWrapper.StopEstimation(prm)
			if err != nil {
				args.l.Warn("can't stop epoch estimation",
					zap.Uint64("epoch", epochN),
					zap.String("error", err.Error()))
			}
		})

	epochTimer.OnDelta(
		args.collectBasicIncome.durationMul,
		args.collectBasicIncome.durationDiv,
		func() {
			epochN := args.epoch.EpochCounter()
			if epochN == 0 { // estimates are invalid in genesis epoch
				return
			}

			args.collectBasicIncome.handler(
				settlement.NewBasicIncomeCollectEvent(epochN - 1),
			)
		})

	epochTimer.OnDelta(
		args.distributeBasicIncome.durationMul,
		args.distributeBasicIncome.durationDiv,
		func() {
			epochN := args.epoch.EpochCounter()
			if epochN == 0 { // estimates are invalid in genesis epoch
				return
			}

			args.distributeBasicIncome.handler(
				settlement.NewBasicIncomeDistributeEvent(epochN - 1),
			)
		})

	return epochTimer
}

func newEmissionTimer(args *emitTimerArgs) *timer.BlockTimer {
	return timer.NewBlockTimer(
		timer.StaticBlockMeter(args.emitDuration),
		func() {
			args.ap.HandleGasEmission(timerEvent.NewAlphabetEmitTick{})
		},
	)
}
