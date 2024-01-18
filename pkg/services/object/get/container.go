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

	lookupDepth := exec.netmapLookupDepth()

	exec.log.Debug("trying to execute in container...",
		zap.Uint64("netmap lookup depth", lookupDepth),
	)

	// initialize epoch number
	ok := exec.initEpoch()
	if !ok {
		return
	}

	mProcessedNodes := make(map[string]struct{})

	for {
		if exec.processCurrentEpoch(mProcessedNodes) {
			break
		}

		// check the maximum depth has been reached
		if lookupDepth == 0 {
			break
		}

		lookupDepth--

		// go to the previous epoch
		exec.curProcEpoch--
	}
}

func (exec *execCtx) processCurrentEpoch(mProcessedNodes map[string]struct{}) bool {
	log := exec.log.With(zap.Uint64("epoch", exec.curProcEpoch))

	log.Debug("process epoch")

	nodeLists, primaryNums, err := exec.svc.node.GetObjectNodesAtEpoch(exec.address(), exec.curProcEpoch)
	if err != nil {
		exec.status = statusUndefined
		exec.err = err

		log.Debug("failed to get storage nodes from the network map for the object", zap.Error(err))

		return true
	}

	ctx, cancel := context.WithCancel(exec.context())
	defer cancel()

	exec.status = statusUndefined
	var j, jLim int
	primary := true

	for i := 0; i < len(nodeLists); i++ { // do not use for range!
		if primary {
			j = 0
			// legit cast since list length is >= then primary num
			jLim = int(primaryNums[i])
		} else {
			j = int(primaryNums[i])
			jLim = len(nodeLists[i])
		}

		for ; j < jLim; j++ {
			select {
			case <-ctx.Done():
				exec.log.Debug("interrupt placement iteration by context",
					zap.String("error", ctx.Err().Error()),
				)

				return true
			default:
			}

			bPubKey := nodeLists[i][j].PublicKey()
			if exec.svc.node.IsLocalPublicKey(bPubKey) {
				continue
			}

			strKey := string(bPubKey)
			if _, ok := mProcessedNodes[strKey]; ok {
				continue
			}

			mProcessedNodes[strKey] = struct{}{}

			var endpoints network.AddressGroup
			err := endpoints.FromIterator(network.NodeEndpointsIterator(nodeLists[i][j]))
			if err != nil {
				// critical error that may ultimately block the storage service. Normally it
				// should not appear because entry into the network map under strict control.
				log.Error("failed to decode network endpoints of the storage node from the network map, skip the node",
					zap.String("public key", netmap.StringifyPublicKey(nodeLists[i][j])), zap.Error(err))
				continue
			}

			// TODO: #1142 consider parallel execution
			// TODO: #1142 consider optimization: if status == SPLIT we can continue until
			//  we reach the best result - split info with linking object ID.
			var info client.NodeInfo

			if ext := nodeLists[i][j].ExternalAddresses(); len(ext) > 0 {
				var externalEndpoints network.AddressGroup
				err = externalEndpoints.FromStringSlice(ext)
				if err != nil {
					// less critical since the main ones must work, but also important
					log.Warn("failed to decode external network endpoints of the storage node from the network map, ignore them",
						zap.String("public key", netmap.StringifyPublicKey(nodeLists[i][j])),
						zap.Strings("endpoints", ext), zap.Error(err))
				} else {
					info.SetExternalAddressGroup(externalEndpoints)
				}
			}

			info.SetAddressGroup(endpoints)
			info.SetPublicKey(nodeLists[i][j].PublicKey())

			if exec.processNode(info) {
				exec.log.Debug("completing the operation")
				return true
			}
		}

		if primary && i == len(nodeLists)-1 {
			// switch to reserve nodes
			primary = false
			i = -1
		}
	}

	return false
}
