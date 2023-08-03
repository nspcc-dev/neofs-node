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

	switch {
	default:
		exec.status = statusUndefined
		exec.err = apistatus.ErrObjectNotFound

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
	case errors.Is(err, apistatus.ErrObjectAlreadyRemoved):
		exec.status = statusINHUMED
		exec.err = err
	case errors.Is(err, apistatus.ErrObjectOutOfRange):
		exec.status = statusOutOfRange
		exec.err = err
	case errors.As(err, &errSplitInfo):
		exec.status = statusVIRTUAL
		mergeSplitInfo(exec.splitInfo(), errSplitInfo.SplitInfo())
		exec.err = objectSDK.NewSplitInfoError(exec.infoSplit)
	}

	return exec.status != statusUndefined
}
