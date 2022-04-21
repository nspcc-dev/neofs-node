package ticker

import (
	"fmt"
	"sync"
)

// IterationHandler is a callback of a certain block advance.
type IterationHandler func()

// IterationsTicker represents a fixed tick number block timer.
//
// It can tick the blocks and perform certain actions
// on block time intervals.
type IterationsTicker struct {
	m sync.Mutex

	curr   uint64
	period uint64

	times uint64

	h IterationHandler
}

// NewIterationsTicker creates a new IterationsTicker.
//
// It guaranties that a handler would be called the
// specified amount of times in the specified amount
// of blocks. After the last meaningful Tick, IterationsTicker
// becomes no-op timer.
//
// Returns an error only if times is greater than totalBlocks.
func NewIterationsTicker(totalBlocks uint64, times uint64, h IterationHandler) (*IterationsTicker, error) {
	period := totalBlocks / times

	if period == 0 {
		return nil, fmt.Errorf("impossible to tick %d times in %d blocks",
			times, totalBlocks,
		)
	}

	var curr uint64

	// try to make handler calls as rare as possible
	if totalBlocks%times != 0 {
		extraBlocks := (period+1)*times - totalBlocks

		if period >= extraBlocks {
			curr = extraBlocks + (period-extraBlocks)/2
			period++
		}
	}

	return &IterationsTicker{
		curr:   curr,
		period: period,
		times:  times,
		h:      h,
	}, nil
}

// Tick ticks one block in the IterationsTicker.
//
// Returns `false` if the timer has finished its operations
// and there will be no more handler calls.
// Calling Tick after the returned `false` is safe, no-op
// and also returns `false`.
func (ft *IterationsTicker) Tick() bool {
	ft.m.Lock()
	defer ft.m.Unlock()

	if ft.times == 0 {
		return false
	}

	ft.curr++

	if ft.curr%ft.period == 0 {
		ft.h()

		ft.times--

		if ft.times == 0 {
			return false
		}
	}

	return true
}
