package engine

import (
	"errors"
	"fmt"

	"go.uber.org/zap"
)

// Open opens all StorageEngine's components.
func (e *StorageEngine) Open() error {
	return e.open()
}

func (e *StorageEngine) open() error {
	e.mtx.RLock()
	defer e.mtx.RUnlock()

	for id, sh := range e.shards {
		if err := sh.Open(); err != nil {
			return fmt.Errorf("could not open shard %s: %w", id, err)
		}
	}

	return nil
}

// Init initializes all StorageEngine's components.
func (e *StorageEngine) Init() error {
	e.mtx.RLock()
	defer e.mtx.RUnlock()

	for id, sh := range e.shards {
		if err := sh.Init(); err != nil {
			return fmt.Errorf("could not initialize shard %s: %w", id, err)
		}
	}

	return nil
}

var errClosed = errors.New("storage engine is closed")

// Close releases all StorageEngine's components. Waits for all data-related operations to complete.
// After the call, all the next ones will fail.
//
// The method is supposed to be called when the application exits.
func (e *StorageEngine) Close() error {
	return e.setBlockExecErr(errClosed)
}

// closes all shards. Never returns an error, shard errors are logged.
func (e *StorageEngine) close(releasePools bool) error {
	e.mtx.RLock()
	defer e.mtx.RUnlock()

	if releasePools {
		for _, p := range e.shardPools {
			p.Release()
		}
	}

	for id, sh := range e.shards {
		if err := sh.Close(); err != nil {
			e.log.Debug("could not close shard",
				zap.String("id", id),
				zap.String("error", err.Error()),
			)
		}
	}

	return nil
}

// executes op if execution is not blocked, otherwise returns blocking error.
//
// Can be called concurrently with setBlockExecErr.
func (e *StorageEngine) execIfNotBlocked(op func() error) error {
	e.blockExec.mtx.RLock()
	defer e.blockExec.mtx.RUnlock()

	if e.blockExec.err != nil {
		return e.blockExec.err
	}

	return op()
}

// sets the flag of blocking execution of all data operations according to err:
//   * err != nil, then blocks the execution. If exec wasn't blocked, calls close method
//     (if err == errClosed => additionally releases pools and does not allow to resume executions).
//   * otherwise, resumes execution. If exec was blocked, calls open method.
//
// Can be called concurrently with exec. In this case it waits for all executions to complete.
func (e *StorageEngine) setBlockExecErr(err error) error {
	e.blockExec.mtx.Lock()
	defer e.blockExec.mtx.Unlock()

	prevErr := e.blockExec.err

	wasClosed := errors.Is(prevErr, errClosed)
	if wasClosed {
		return errClosed
	}

	e.blockExec.err = err

	if err == nil {
		if prevErr != nil { // block -> ok
			return e.open()
		}
	} else if prevErr == nil { // ok -> block
		return e.close(errors.Is(err, errClosed))
	}

	// otherwise do nothing

	return nil
}

// BlockExecution blocks the execution of any data-related operation. All blocked ops will return err.
// To resume the execution, use ResumeExecution method.
//
// Сan be called regardless of the fact of the previous blocking. If execution wasn't blocked, releases all resources
// similar to Close. Can be called concurrently with Close and any data related method (waits for all executions
// to complete). Returns error if any Close has been called before.
//
// Must not be called concurrently with either Open or Init.
//
// Note: technically passing nil error will resume the execution, otherwise, it is recommended to call ResumeExecution
// for this.
func (e *StorageEngine) BlockExecution(err error) error {
	return e.setBlockExecErr(err)
}

// ResumeExecution resumes the execution of any data-related operation.
// To block the execution, use BlockExecution method.
//
// Сan be called regardless of the fact of the previous blocking. If execution was blocked, prepares all resources
// similar to Open. Can be called concurrently with Close and any data related method (waits for all executions
// to complete). Returns error if any Close has been called before.
//
// Must not be called concurrently with either Open or Init.
func (e *StorageEngine) ResumeExecution() error {
	return e.setBlockExecErr(nil)
}
