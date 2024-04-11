package getsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"go.uber.org/zap"
)

func (exec *execCtx) executeOnContainer() {
	if exec.isLocal() {
		exec.log.Debug("return result directly")
		return
	}

	exec.log.Debug("trying to execute in container...")

	// initialize epoch number
	epoch, err := exec.svc.currentEpochReceiver.currentEpoch()
	if err != nil {
		exec.status = statusUndefined
		exec.err = err
		exec.log.Debug("could not get current epoch number", zap.Error(err))
		return
	}

	exec.processEpoch(epoch)
}

func (exec *execCtx) processEpoch(epoch uint64) bool {
	exec.log.Debug("process epoch",
		zap.Uint64("number", epoch),
	)

	traverser, ok := exec.generateTraverser(exec.address(), epoch)
	if !ok {
		return true
	}

	ctx, cancel := context.WithCancel(exec.context())
	defer cancel()

	exec.status = statusUndefined
	mProcessedNodes := make(map[string]struct{})

	for {
		addrs := traverser.Next()
		if len(addrs) == 0 {
			exec.log.Debug("no more nodes, abort placement iteration")

			return false
		}

		for i := range addrs {
			select {
			case <-ctx.Done():
				exec.log.Debug("interrupt placement iteration by context",
					zap.String("error", ctx.Err().Error()),
				)

				return true
			default:
			}

			strKey := string(addrs[i].PublicKey())
			if _, ok = mProcessedNodes[strKey]; ok {
				continue
			}

			mProcessedNodes[strKey] = struct{}{}

			// TODO: #1142 consider parallel execution
			// TODO: #1142 consider optimization: if status == SPLIT we can continue until
			//  we reach the best result - split info with linking object ID.
			var info client.NodeInfo

			client.NodeInfoFromNetmapElement(&info, addrs[i])

			if exec.processNode(info) {
				exec.log.Debug("completing the operation")
				return true
			}
		}
	}
}
