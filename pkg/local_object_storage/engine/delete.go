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
	return e.execIfNotBlocked(func() error {
		return e.deleteObj(addr, true)
	})
}

func (e *StorageEngine) deleteObj(addr oid.Address, force bool) error {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddDeleteDuration)()
	}

	var inhumePrm InhumePrm
	inhumePrm.MarkAsGarbage(addr)
	if force {
		inhumePrm.WithForceRemoval()
	}

	_, err := e.inhumeInt(inhumePrm)

	return err
}
