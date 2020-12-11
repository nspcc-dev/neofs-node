package deletesvc

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
)

func (exec *execCtx) executeLocal() {
	exec.log.Debug("forming tombstone structure...")

	ok := exec.formTombstone()
	if !ok {
		return
	}

	exec.log.Debug("tombstone structure successfully formed, saving...")

	ok = exec.saveTombstone()
	if !ok {
		return
	}

	exec.log.Debug("tombstone successfilly saved, broadcasting...")

	exec.broadcastTombstone()
}

func (exec *execCtx) formTombstone() (ok bool) {
	exec.tombstone = objectSDK.NewTombstone()
	exec.addMembers([]*objectSDK.ID{exec.address().ObjectID()})

	exec.log.Debug("forming split info...")

	ok = exec.formSplitInfo()
	if !ok {
		return
	}

	exec.log.Debug("split info successfully formed, collecting members...")

	ok = exec.collectMembers()
	if !ok {
		return
	}

	exec.log.Debug("members successfully collected")

	ok = exec.initTombstoneObject()
	if !ok {
		return
	}

	return true
}
