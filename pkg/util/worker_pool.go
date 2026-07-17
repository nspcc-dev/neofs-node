package util

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
