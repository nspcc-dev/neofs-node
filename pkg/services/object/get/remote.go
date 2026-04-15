package getsvc

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

func (exec *execCtx) processNode(info client.NodeInfo) bool {
	exec.log.Info("processing node...", zap.Stringers("address group", info.AddressGroup()))

	remoteClient, err := exec.svc.clientCache.get(exec.context(), info)
	if err != nil {
		exec.status = statusUndefined
		exec.err = err
		exec.log.Info("could not construct remote node client", zap.Error(err))
		return false
	}

	obj, reader, err := remoteClient.getObject(exec)
	exec.log.Info("DEBUG: fetched object from remote node", zap.Bool("objIsNil", obj == nil), zap.Stringers("address group", info.AddressGroup()), zap.Error(err))

	var errSplitInfo *object.SplitInfoError

	switch {
	default:
		exec.status = statusUndefined
		exec.err = apistatus.ErrObjectNotFound

		exec.log.Info("remote call failed", zap.Stringers("address group", info.AddressGroup()),
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
