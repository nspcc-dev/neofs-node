package deletesvc

import (
	"strconv"

	"github.com/nspcc-dev/neofs-sdk-go/object"
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
			zap.Error(err),
		)

		return false
	}

	// By default make local node the tombstone object owner.
	var owner = exec.svc.netInfo.LocalNodeID()

	tokenSession := exec.commonParameters().SessionToken()
	if tokenSession != nil {
		owner = tokenSession.Issuer()
	}

	exec.tombstoneObj = object.New(exec.containerID(), owner)

	var a object.Attribute
	a.SetKey(object.AttributeExpirationEpoch)
	a.SetValue(strconv.FormatUint(exec.svc.netInfo.CurrentEpoch()+tsLifetime, 10))
	exec.tombstoneObj.SetAttributes(a)
	exec.tombstoneObj.AssociateDeleted(exec.address().Object())

	return true
}
