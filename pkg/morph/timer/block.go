package timer

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
	rolledBack bool

	mtx *sync.Mutex

	dur BlockMeter

	baseDur uint32

	mul, div uint32

	cur, tgt uint32

	last uint32

	h BlockTickHandler

	ps []BlockTimer

	once bool

	pulse   bool
	stopped bool
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
		mtx:   new(sync.Mutex),
		dur:   dur,
		mul:   1,
		div:   1,
		h:     h,
		pulse: true,
	}
}

// NewOneTickTimer creates a new BlockTimer that ticks only once.
//
// Do not use delta handlers with pulse in this timer.
func NewOneTickTimer(dur BlockMeter, h BlockTickHandler) *BlockTimer {
	return &BlockTimer{
		mtx:  new(sync.Mutex),
		dur:  dur,
		mul:  1,
		div:  1,
		h:    h,
		once: true,
	}
}

// OnDelta registers handler which is executed on (mul / div * BlockMeter()) block
// after basic interval reset.
//
// If WithPulse option is provided, handler is executed (mul / div * BlockMeter()) block
// during base interval.
func (t *BlockTimer) OnDelta(mul, div uint32, h BlockTickHandler) {
	t.ps = append(t.ps, BlockTimer{
		mul:  mul,
		div:  div,
		h:    h,
		once: t.once,

		pulse: false,
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

	t.resetWithBaseInterval(d)

	for i := range t.ps {
		t.ps[i].resetWithBaseInterval(d)
	}

	t.mtx.Unlock()

	return nil
}

func (t *BlockTimer) resetWithBaseInterval(d uint32) {
	t.rolledBack = false
	t.baseDur = d
	t.reset()
}

func (t *BlockTimer) reset() {
	mul, div := t.mul, t.div

	if !t.pulse && t.rolledBack && mul < div {
		mul, div = 1, 1
	}

	delta := mul * t.baseDur / div
	if delta == 0 {
		delta = 1
	}

	t.tgt = delta
	t.cur = 0
}

// Tick ticks one block in the BlockTimer.
//
// Executes all callbacks which are awaiting execution at the new block.
func (t *BlockTimer) Tick(h uint32) {
	t.mtx.Lock()
	if !t.stopped {
		t.tick(h)
	}
	t.mtx.Unlock()
}

func (t *BlockTimer) tick(h uint32) {
	if h != 0 && t.last == h {
		return
	}

	t.last = h
	t.cur++

	if t.cur == t.tgt {
		// it would be advisable to optimize such execution, for example:
		//   1. push handler to worker pool t.wp.Submit(h);
		//   2. call t.tickH(h)
		t.h()

		if !t.once {
			t.cur = 0
			t.rolledBack = true
			t.reset()
		}
	}

	for i := range t.ps {
		t.ps[i].tick(h)
	}
}

func (t *BlockTimer) Stop() {
	t.mtx.Lock()
	t.stopped = true
	t.mtx.Unlock()
}
