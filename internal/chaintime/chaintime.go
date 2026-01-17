package chaintime

import (
	"sync/atomic"
	"time"
)

// TimeProvider supplies current FS chain time without calling the chain.
// It should be updated from block header subscriptions and return time
// based on the latest observed header timestamp.
type TimeProvider interface {
	Now() time.Time
}

// New creates a new instance of [AtomicChainTimeProvider].
func New() *AtomicChainTimeProvider {
	return &AtomicChainTimeProvider{}
}

// AtomicChainTimeProvider is a simple provider backed by atomic value.
// It stores the latest chain timestamp (milliseconds precision) and returns it as [time.Time].
type AtomicChainTimeProvider struct {
	ms atomic.Uint64
}

// Set updates current chain time from header timestamp.
func (p *AtomicChainTimeProvider) Set(ms uint64) { p.ms.Store(ms) }

// Now returns last set time as [time.Time] in milliseconds. Zero if not set yet.
func (p *AtomicChainTimeProvider) Now() time.Time {
	v := p.ms.Load()
	if v == 0 {
		return time.Time{}
	}
	return time.UnixMilli(int64(v))
}
