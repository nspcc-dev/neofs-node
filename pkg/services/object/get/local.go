package getsvc

import (
	"errors"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

func (exec *execCtx) executeLocal() {
	var err error

	exec.collectedObject, err = exec.svc.localStorage.get(exec)

	var errSplitInfo *objectSDK.SplitInfoError
	var errRemoved apistatus.ObjectAlreadyRemoved
	var errOutOfRange apistatus.ObjectOutOfRange

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("local get failed",
			zap.String("error", err.Error()),
		)
	case err == nil:
		exec.status = statusOK
		exec.err = nil
		exec.writeCollectedObject()
	case errors.As(err, &errRemoved):
		exec.status = statusINHUMED
		exec.err = errRemoved
	case errors.As(err, &errSplitInfo):
		exec.status = statusVIRTUAL
		mergeSplitInfo(exec.splitInfo(), errSplitInfo.SplitInfo())
		exec.err = objectSDK.NewSplitInfoError(exec.infoSplit)
	case errors.As(err, &errOutOfRange):
		exec.status = statusOutOfRange
		exec.err = errOutOfRange
	}
}
