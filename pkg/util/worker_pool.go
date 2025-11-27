package util

import (
	"sync/atomic"

	"github.com/panjf2000/ants/v2"
)

// WorkerPool represents a tool to control
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

	// Tune changes the capacity of this pool.
	Tune(int)
}

// pseudoWorkerPool represents a pseudo worker pool which executes the submitted job immediately in the caller's routine.
type pseudoWorkerPool struct {
	closed atomic.Bool
}

// ErrPoolClosed is returned when submitting task to a closed pool.
var ErrPoolClosed = ants.ErrPoolClosed

// NewPseudoWorkerPool returns a new instance of a synchronous worker pool.
func NewPseudoWorkerPool() WorkerPool {
	return &pseudoWorkerPool{}
}

// Submit executes the passed function immediately.
//
// Always returns nil.
func (p *pseudoWorkerPool) Submit(fn func()) error {
	if p.closed.Load() {
		return ErrPoolClosed
	}

	fn()

	return nil
}

// Release implements the WorkerPool interface.
func (p *pseudoWorkerPool) Release() {
	p.closed.Store(true)
}

// Tune implements the WorkerPool interface.
func (p *pseudoWorkerPool) Tune(_ int) {}

// SingleAsyncExecutingInstance returns func that works the same as f, but
// calling it is non-blocking and not more than a single routine is being
// executed at a time. The second return value is a blocking stop function
// that disallows f execution after calling it. Stop function blocks until
// f execution is finished (if any).
func SingleAsyncExecutingInstance(f func()) (func(), func()) {
	var (
		execQueue = make(chan struct{}, 1)

		stopCh     = make(chan struct{})
		finishedCh = make(chan struct{})
		stopFunc   = func() {
			close(stopCh)
			<-finishedCh
		}
	)
	go func() {
		for {
			select {
			case <-stopCh:
				close(finishedCh)
				return
			case <-execQueue:
				f()
			}
		}
	}()

	return func() {
		select {
		case <-stopCh:
			return
		default:
		}

		select {
		case execQueue <- struct{}{}:
		default:
		}
	}, stopFunc
}
