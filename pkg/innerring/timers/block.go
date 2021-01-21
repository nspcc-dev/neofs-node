package timers

import (
	"sync"
)

// BlockMeter calculates block time interval dynamically.
type BlockMeter func() (uint32, error)

// BlockTickHandler is a callback of a certain block advance.
type BlockTickHandler func()

// BlockTimer represents block timer.
//
// It can tick the blocks and perform certain actions
// on block time intervals.
type BlockTimer struct {
	mtx *sync.Mutex

	dur BlockMeter

	mul, div uint32

	cur, tgt uint32

	h BlockTickHandler

	ps []BlockTimer
}

// StaticBlockMeter returns BlockMeters that always returns (d, nil).
func StaticBlockMeter(d uint32) BlockMeter {
	return func() (uint32, error) {
		return d, nil
	}
}

// NewBlockTimer creates a new BlockTimer.
//
// Reset should be called before timer ticking.
func NewBlockTimer(dur BlockMeter, h BlockTickHandler) *BlockTimer {
	return &BlockTimer{
		mtx: new(sync.Mutex),
		dur: dur,
		mul: 1,
		div: 1,
		h:   h,
	}
}

// OnDelta registers handler which is executed every (mul / div * BlockMeter()) block.
func (t *BlockTimer) OnDelta(mul, div uint32, h BlockTickHandler) {
	t.ps = append(t.ps, BlockTimer{
		mul: mul,
		div: div,
		h:   h,
	})
}

// Reset resets previous ticks of the BlockTimer.
//
// Returns BlockMeter's error upon occurrence.
func (t *BlockTimer) Reset() error {
	d, err := t.dur()
	if err != nil {
		return err
	}

	t.mtx.Lock()
	t.reset(d)
	t.mtx.Unlock()

	return nil
}

func (t *BlockTimer) reset(dur uint32) {
	delta := t.mul * dur / t.div
	if delta == 0 {
		delta = 1
	}

	t.tgt = delta
	t.cur = 0

	for i := range t.ps {
		t.ps[i].reset(dur)
	}
}

// Tick ticks one block in the BlockTimer.
//
// Executes all callbacks which are awaiting execution at the new block.
func (t *BlockTimer) Tick() {
	t.mtx.Lock()
	t.tick()
	t.mtx.Unlock()
}

func (t *BlockTimer) tick() {
	t.cur++

	if t.cur == t.tgt {
		t.cur = 0

		// it would be advisable to optimize such execution, for example:
		//   1. push handler to worker pool t.wp.Submit(h);
		//   2. call t.tickH(h)
		t.h()
	}

	for i := range t.ps {
		t.ps[i].tick()
	}
}
