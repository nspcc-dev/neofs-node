package innerring

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/alphabet"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/timers"
	container "github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	"go.uber.org/zap"
)

type (
	epochState interface {
		EpochCounter() uint64
	}

	epochTimerArgs struct {
		l *zap.Logger

		nm *netmap.Processor // to handle new epoch tick

		cnrWrapper *container.Wrapper // to invoke stop container estimation
		epoch      epochState         // to specify which epoch to stop

		epochDuration      uint32 // in blocks
		stopEstimationDMul uint32 // X: X/Y of epoch in blocks
		stopEstimationDDiv uint32 // Y: X/Y of epoch in blocks
	}

	emitTimerArgs struct {
		ap *alphabet.Processor // to handle new emission tick

		emitDuration uint32 // in blocks
	}
)

func (s *Server) addBlockTimer(t *timers.BlockTimer) {
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

func newEpochTimer(args *epochTimerArgs) *timers.BlockTimer {
	epochTimer := timers.NewBlockTimer(
		timers.StaticBlockMeter(args.epochDuration),
		func() {
			args.nm.HandleNewEpochTick(timers.NewEpochTick{})
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

			err := args.cnrWrapper.StopEstimation(epochN - 1)
			if err != nil {
				args.l.Warn("can't stop epoch estimation",
					zap.Uint64("epoch", epochN),
					zap.String("error", err.Error()))
			}
		})

	return epochTimer
}

func newEmissionTimer(args *emitTimerArgs) *timers.BlockTimer {
	return timers.NewBlockTimer(
		timers.StaticBlockMeter(args.emitDuration),
		func() {
			args.ap.HandleGasEmission(timers.NewAlphabetEmitTick{})
		},
	)
}
