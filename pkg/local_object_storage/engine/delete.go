package engine

import (
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Delete marks the objects to be removed.
//
// Returns an error if executions are blocked (see BlockExecution).
//
// NOTE: This is a forced removal, marks any object to be deleted (despite
// any prohibitions on operations with that object).
func (e *StorageEngine) Delete(addr oid.Address) error {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddDeleteDuration)()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return e.blockErr
	}
	return e.inhume([]oid.Address{addr}, true, nil, 0)
}
