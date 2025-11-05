package timers

import (
	"fmt"
	"sync"
)

type (
	// Tick is a handler that must be executed every time any of [EpochTimers]
	// expires.
	Tick func()

	// SubEpochTick is like a [Tick] but will be executed every epoch at:
	// epochDuration * [SubEpochTick.EpochMul] / [SubEpochTick.EpochDiv].
	// [SubEpochTick.EpochDiv] must not be zero.
	SubEpochTick struct {
		Tick     Tick   // handle to execute
		EpochMul uint32 // X: X/Y of epoch in seconds
		EpochDiv uint32 // Y: X/Y of epoch in seconds
	}

	// EpochTicks defines start epoch timers and epoch subtimers for [NewTimers].
	// Must be initialized with [NewTimers].
	EpochTicks struct {
		NewEpochTicks []Tick
		DeltaTicks    []SubEpochTick
	}

	deltaHandler struct {
		nextTickAt uint64 // recalculated every [epochTimer.reset] call
		done       bool

		mul, div uint64
		tick     Tick
	}
)

// EpochTimers is a timer that executes any predefined [Tick] or [SubEpochTick]
// based on chain information provided via [EpochTimers.UpdateTime]. EpochTicks are
// both block- and time-based, see [EpochTimers.Reset] and
// [EpochTimers.UpdateTime].
type EpochTimers struct {
	m          sync.Mutex
	done       bool
	nextTickAt uint64 // timestamp with milliseconds precision as in NEO's blocks

	eHandlers     []Tick
	deltaHandlers []*deltaHandler
}

// UpdateTime must be called every time [EpochTimers] user receives information
// about chain's new block acceptance. curr must be just-arrived block's
// timestamp.
func (et *EpochTimers) UpdateTime(curr uint64) {
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
			dh.tick()
			dh.done = true
		}
	}
}

// Reset must be called every time [EpochTimers] user receives information
// about new epoch event. lastTick must be block's timestamp that contains new
// epoch event; dur is an actual epoch duration in milliseconds.
func (et *EpochTimers) Reset(lastTick, dur uint64) {
	et.m.Lock()
	defer et.m.Unlock()

	et.nextTickAt = lastTick + dur
	et.done = false

	for _, dh := range et.deltaHandlers {
		dh.nextTickAt = lastTick + dur*dh.mul/dh.div
		dh.done = false
	}
}

// NewTimers inits [EpochTimers]. If [EpochTicks.DeltaTicks] is not empty, none of
// them must have zero [SubEpochTick.EpochDiv].
func NewTimers(tt EpochTicks) *EpochTimers {
	et := EpochTimers{
		eHandlers:     tt.NewEpochTicks,
		deltaHandlers: make([]*deltaHandler, 0, len(tt.DeltaTicks)),
	}

	for i, dt := range tt.DeltaTicks {
		if dt.EpochDiv == 0 {
			panic(fmt.Sprintf("%d delta tick has zero divisor", i))
		}

		et.deltaHandlers = append(et.deltaHandlers, &deltaHandler{
			mul:  uint64(dt.EpochMul),
			div:  uint64(dt.EpochDiv),
			tick: dt.Tick,
		})
	}

	return &et
}
