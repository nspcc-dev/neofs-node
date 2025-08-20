package getsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
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
	var endpoints network.AddressGroup
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

			if err := endpoints.FromIterator(network.NodeEndpointsIterator(nodeLists[i][j])); err != nil {
				// critical error that may ultimately block the storage service. Normally it
				// should not appear because entry into the network map under strict control
				exec.log.Error("failed to decode network endpoints of the storage node from the network map, skip the node",
					zap.String("public key", netmap.StringifyPublicKey(nodeLists[i][j])), zap.Error(err))
				continue
			}

			// TODO: #1142 consider parallel execution
			// TODO: #1142 consider optimization: if status == SPLIT we can continue until
			//  we reach the best result - split info with linking object ID.
			var info client.NodeInfo
			info.SetAddressGroup(endpoints)
			info.SetPublicKey(bKey)

			if exec.processNode(info) {
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
