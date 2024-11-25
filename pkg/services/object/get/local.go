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

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("local get failed",
			zap.Error(err),
		)
	case err == nil:
		exec.status = statusOK
		exec.err = nil
		exec.writeCollectedObject()
	case errors.Is(err, apistatus.Error):
		if errors.Is(err, apistatus.ErrObjectNotFound) {
			exec.status = statusNotFound
			exec.err = err

			return
		}

		exec.status = statusAPIResponse
		exec.err = err
	case errors.As(err, &errSplitInfo):
		exec.status = statusVIRTUAL
		mergeSplitInfo(exec.splitInfo(), errSplitInfo.SplitInfo())
		exec.err = objectSDK.NewSplitInfoError(exec.infoSplit)
	}
}
