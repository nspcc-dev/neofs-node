package getsvc

import (
	"context"
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

func (exec *execCtx) processNode(ctx context.Context, info client.NodeInfo) bool {
	exec.log.Debug("processing node...")

	client, ok := exec.remoteClient(info)
	if !ok {
		return true
	}

	obj, err := client.getObject(exec, info)

	var errSplitInfo *objectSDK.SplitInfoError
	var errRemoved *apistatus.ObjectAlreadyRemoved

	switch {
	default:
		var errNotFound apistatus.ObjectNotFound

		exec.status = statusUndefined
		exec.err = errNotFound

		exec.log.Debug("remote call failed",
			zap.String("error", err.Error()),
		)
	case err == nil:
		exec.status = statusOK
		exec.err = nil

		// both object and err are nil only if the original
		// request was forwarded to another node and the object
		// has already been streamed to the requesting party
		if obj != nil {
			exec.collectedObject = obj
			exec.writeCollectedObject()
		}
	case errors.As(err, &errRemoved):
		exec.status = statusINHUMED
		exec.err = errRemoved
	case errors.As(err, &errSplitInfo):
		exec.status = statusVIRTUAL
		mergeSplitInfo(exec.splitInfo(), errSplitInfo.SplitInfo())
		exec.err = objectSDK.NewSplitInfoError(exec.infoSplit)
	}

	return exec.status != statusUndefined
}
