package util

import (
	"github.com/panjf2000/ants/v2"
	"go.uber.org/atomic"
)

// WorkerPool represents the tool for control
// the execution of go-routine pool.
type WorkerPool interface {
	// Submit queues a function for execution
	// in a separate routine.
	//
	// Implementation must return any error encountered
	// that prevented the function from being queued.
	Submit(func()) error

	// Release releases worker pool resources. All `Submit` calls will
	// finish with ErrPoolClosed. It doesn't wait until all submitted
	// functions have returned so synchronization must be achieved
	// via other means (e.g. sync.WaitGroup).
	Release()
}

// pseudoWorkerPool represents pseudo worker pool which executes submitted job immediately in the caller's routine.
type pseudoWorkerPool struct {
	closed atomic.Bool
}

// ErrPoolClosed is returned when submitting task to a closed pool.
var ErrPoolClosed = ants.ErrPoolClosed

// NewPseudoWorkerPool returns new instance of a synchronous worker pool.
func NewPseudoWorkerPool() WorkerPool {
	return &pseudoWorkerPool{}
}

// Submit executes passed function immediately.
//
// Always returns nil.
func (p *pseudoWorkerPool) Submit(fn func()) error {
	if p.closed.Load() {
		return ErrPoolClosed
	}

	fn()

	return nil
}

// Release implements WorkerPool interface.
func (p *pseudoWorkerPool) Release() {
	p.closed.Store(true)
}
