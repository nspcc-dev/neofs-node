package deletesvc

import (
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

func (exec *execCtx) executeLocal() {
	exec.log.Debug("forming tombstone structure...")

	ok := exec.formTombstone()
	if !ok {
		return
	}

	exec.log.Debug("tombstone structure successfully formed, saving...")

	exec.saveTombstone()
}

func (exec *execCtx) formTombstone() (ok bool) {
	tsLifetime, err := exec.svc.netInfo.TombstoneLifetime()
	if err != nil {
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not read tombstone lifetime config",
			zap.String("error", err.Error()),
		)

		return false
	}

	exec.tombstone = object.NewTombstone()
	exec.tombstone.SetExpirationEpoch(
		exec.svc.netInfo.CurrentEpoch() + tsLifetime,
	)
	exec.addMembers([]oid.ID{exec.address().Object()})

	exec.log.Debug("forming split info...")

	ok = exec.formSplitInfo()
	if !ok {
		return
	}

	exec.log.Debug("split info successfully formed, collecting members...")

	exec.tombstone.SetSplitID(exec.splitInfo.SplitID())

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
