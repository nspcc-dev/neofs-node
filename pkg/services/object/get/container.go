package getsvc

import (
	"context"

	"go.uber.org/zap"
)

func (exec *execCtx) executeOnContainer() {
	if exec.isLocal() {
		exec.log.Debug("return result directly")
		return
	}

	exec.log.Debug("trying to execute in container...")

	addr := exec.address()

	nodeLists := exec.nodeLists
	primaryCounts := exec.repRules
	if nodeLists == nil {
		var err error
		nodeLists, primaryCounts, _, err = exec.svc.neoFSNet.GetNodesForObject(addr)
		if err != nil {
			exec.status = statusUndefined
			exec.err = err
			exec.log.Debug("failed to list storage nodes for the object", zap.Error(err))
			return
		}
	}

	ctx, cancel := context.WithCancel(exec.context())
	defer cancel()

	exec.status = statusUndefined
	mProcessedNodes := make(map[string]struct{})
	var j, jLim uint
	primary := true

	for i := 0; i < len(primaryCounts); i++ { // do not use for-range!
		if primary {
			j, jLim = 0, primaryCounts[i]
		} else {
			j, jLim = primaryCounts[i], uint(len(nodeLists[i]))
		}

		for ; j < jLim; j++ {
			select {
			case <-ctx.Done():
				exec.log.Debug("interrupt placement iteration by context",
					zap.Error(ctx.Err()),
				)

				return
			default:
			}

			bKey := nodeLists[i][j].PublicKey()
			strKey := string(bKey)
			if _, ok := mProcessedNodes[strKey]; ok || exec.svc.neoFSNet.IsLocalNodePublicKey(bKey) {
				continue
			}

			mProcessedNodes[strKey] = struct{}{}

			// TODO: #1142 consider parallel execution
			// TODO: #1142 consider optimization: if status == SPLIT we can continue until
			//  we reach the best result - split info with linking object ID.
			if exec.processNode(nodeLists[i][j]) {
				exec.log.Debug("completing the operation")
				return
			}
		}

		if primary && i == len(primaryCounts)-1 {
			// switch to reserve nodes
			primary = false
			i = -1
		}
	}
}
