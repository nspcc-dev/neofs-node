package getsvc

import (
	"context"
	"errors"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"go.uber.org/zap"
)

func (exec *execCtx) processNode(ctx context.Context, addr network.AddressGroup) bool {
	exec.log.Debug("processing node...")

	client, ok := exec.remoteClient(addr)
	if !ok {
		return true
	}

	obj, err := client.getObject(exec, addr)

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
		exec.collectedObject = object.NewFromSDK(obj)
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
