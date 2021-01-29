package innerring

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/alphabet"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/timers"
)

type (
	epochTimerArgs struct {
		nm *netmap.Processor // to handle new epoch tick

		epochDuration uint32 // in blocks
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
