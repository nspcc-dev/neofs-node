package util

// WorkerPool represents the tool for control
// the execution of go-routine pool.
type WorkerPool interface {
	// Submit queues a function for execution
	// in a separate routine.
	//
	// Implementation must return any error encountered
	// that prevented the function from being queued.
	Submit(func()) error
}

// PseudoWorkerPool represents pseudo worker pool which executes submitted job immediately in the caller's routine..
type PseudoWorkerPool struct{}

// Submit executes passed function immediately.
//
// Always returns nil.
func (PseudoWorkerPool) Submit(fn func()) error {
	fn()

	return nil
}
