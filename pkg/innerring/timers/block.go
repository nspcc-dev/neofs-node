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
	rolledBack bool

	mtx *sync.Mutex

	dur BlockMeter

	baseDur uint32

	mul, div uint32

	cur, tgt uint32

	h BlockTickHandler

	ps []BlockTimer

	deltaCfg
}

// DeltaOption is an option of delta-interval handler.
type DeltaOption func(*deltaCfg)

type deltaCfg struct {
	pulse bool
}

// WithPulse returns option to call delta-interval handler multiple
// times
func WithPulse() DeltaOption {
	return func(c *deltaCfg) {
		c.pulse = true
	}
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
		deltaCfg: deltaCfg{
			pulse: true,
		},
	}
}

// OnDelta registers handler which is executed on (mul / div * BlockMeter()) block
// after basic interval reset.
//
// If WithPulse option is provided, handler is executed (mul / div * BlockMeter()) block
// during base interval.
func (t *BlockTimer) OnDelta(mul, div uint32, h BlockTickHandler, opts ...DeltaOption) {
	c := deltaCfg{
		pulse: false,
	}

	for i := range opts {
		opts[i](&c)
	}

	t.ps = append(t.ps, BlockTimer{
		mul: mul,
		div: div,
		h:   h,

		deltaCfg: c,
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
func (t *BlockTimer) Tick() {
	t.mtx.Lock()
	t.tick()
	t.mtx.Unlock()
}

func (t *BlockTimer) tick() {
	t.cur++

	if t.cur == t.tgt {
		// it would be advisable to optimize such execution, for example:
		//   1. push handler to worker pool t.wp.Submit(h);
		//   2. call t.tickH(h)
		t.h()

		t.cur = 0
		t.rolledBack = true
		t.reset()
	}

	for i := range t.ps {
		t.ps[i].tick()
	}
}
