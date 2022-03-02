package main

import (
	"sync"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/ticker"
)

type eigenTrustTickers struct {
	m sync.Mutex

	timers map[uint64]*ticker.IterationsTicker
}

func (e *eigenTrustTickers) addEpochTimer(epoch uint64, timer *ticker.IterationsTicker) {
	e.m.Lock()
	defer e.m.Unlock()

	e.timers[epoch] = timer
}

func (e *eigenTrustTickers) tick() {
	e.m.Lock()
	defer e.m.Unlock()

	for epoch, t := range e.timers {
		if !t.Tick() {
			delete(e.timers, epoch)
		}
	}
}

func tickBlockTimers(c *cfg) {
	c.cfgMorph.eigenTrustTicker.tick()
}

func newEigenTrustIterTimer(c *cfg) {
	c.cfgMorph.eigenTrustTicker = &eigenTrustTickers{
		// it is expected to have max 2 concurrent epoch
		// in normal mode work
		timers: make(map[uint64]*ticker.IterationsTicker, 2),
	}
}
