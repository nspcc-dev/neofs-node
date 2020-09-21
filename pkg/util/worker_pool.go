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

// SyncWorkerPool represents synchronous worker pool.
type SyncWorkerPool struct{}

// Submit executes passed function immediately.
//
// Always returns nil.
func (SyncWorkerPool) Submit(fn func()) error {
	fn()

	return nil
}
