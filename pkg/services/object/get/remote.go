package getsvc

import (
	"context"
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
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
		exec.err = object.ErrNotFound

		exec.log.Debug("remote call failed",
			zap.String("error", err.Error()),
		)
	case err == nil:
		exec.status = statusOK
		exec.err = nil
		exec.collectedObject = obj
		exec.writeCollectedObject()
	case errors.Is(err, object.ErrAlreadyRemoved):
		exec.status = statusINHUMED
		exec.err = object.ErrAlreadyRemoved
	case errors.As(err, &errSplitInfo):
		exec.status = statusVIRTUAL
		mergeSplitInfo(exec.splitInfo(), errSplitInfo.SplitInfo())
		exec.err = objectSDK.NewSplitInfoError(exec.infoSplit)
	}

	return exec.status != statusUndefined
}
