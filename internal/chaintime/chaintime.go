package chaintime

import (
	"sync/atomic"
	"time"
)

// AtomicChainTimeProvider is a simple provider backed by atomic value.
// It stores the latest chain timestamp (milliseconds precision) and returns it as [time.Time].
type AtomicChainTimeProvider struct {
	ms atomic.Uint64
}

// Set updates current chain time from header timestamp.
func (p *AtomicChainTimeProvider) Set(ms uint64) { p.ms.Store(ms) }

// Now returns last set time as [time.Time] in milliseconds precision.
// If no time was set yet, it returns local system time.
func (p *AtomicChainTimeProvider) Now() time.Time {
	v := p.ms.Load()
	if v == 0 {
		return time.Now()
	}
	return time.UnixMilli(int64(v))
}
