package getsvc

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

func (exec *execCtx) processNode(info client.NodeInfo) bool {
	exec.log.Debug("processing node...", zap.Stringers("address group", info.AddressGroup()))

	remoteClient, ok := exec.remoteClient(info)
	if !ok {
		return true
	}

	obj, reader, err := remoteClient.getObject(exec, info)

	var errSplitInfo *object.SplitInfoError

	switch {
	default:
		exec.status = statusUndefined
		exec.err = apistatus.ErrObjectNotFound

		exec.log.Debug("remote call failed", zap.Stringers("address group", info.AddressGroup()),
			zap.Error(err),
		)
	case err == nil:
		exec.status = statusOK
		exec.err = nil

		// both object and err are nil only if the original
		// request was forwarded to another node and the object
		// has already been streamed to the requesting party,
		// or it is a GETRANGEHASH forwarded request whose
		// response is not an object
		if obj != nil || reader != nil {
			exec.collectedHeader = obj
			exec.collectedReader = reader
			exec.writeCollectedObject()
		}
	case errors.Is(err, apistatus.Error) && !errors.Is(err, apistatus.ErrObjectNotFound):
		exec.status = statusAPIResponse
		exec.err = err
	case errors.As(err, &errSplitInfo):
		exec.status = statusVIRTUAL
		mergeSplitInfo(exec.splitInfo(), errSplitInfo.SplitInfo())
		exec.err = object.NewSplitInfoError(exec.infoSplit)
	}

	return exec.status != statusUndefined
}
