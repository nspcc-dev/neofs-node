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

	startEpoch := exec.netmapEpoch()
	lookupDepth := exec.netmapLookupDepth()

	if startEpoch > 0 {
		exec.log.Debug("trying to execute in container...",
			zap.Uint64("start from epoch", startEpoch),
			zap.Uint64("netmap lookup depth", lookupDepth),
		)
	} else {
		exec.log.Debug("trying to execute in container...",
			zap.Uint64("netmap lookup depth", lookupDepth),
		)
	}

	ctx, cancel := context.WithCancel(exec.context())
	defer cancel()

	err := exec.svc.storagePolicer.ForEachRemoteObjectNode(exec.prm.addr.Container(), exec.prm.addr.Object(), startEpoch, lookupDepth, func(storageNodeInfo netmap.NodeInfo) bool {
		select {
		case <-ctx.Done():
			exec.log.Debug("interrupt reading the object from remote container nodes by context",
				zap.String("error", ctx.Err().Error()))
			return false
		default:
		}

		// TODO: #1142 consider parallel execution
		// TODO: #1142 consider optimization: if status == SPLIT we can continue until
		//  we reach the best result - split info with linking object ID.
		var endpoints network.AddressGroup
		err := endpoints.FromIterator(network.NodeEndpointsIterator(storageNodeInfo))
		if err != nil {
			exec.log.Debug("failed to decode network endpoints of the storage node from the network map, skip search on it",
				zap.String("public key", netmap.StringifyPublicKey(storageNodeInfo)), zap.Error(err))
			return false
		}

		var clientInfo client.NodeInfo

		if ext := storageNodeInfo.ExternalAddresses(); len(ext) > 0 {
			var externalEndpoints network.AddressGroup
			err = externalEndpoints.FromStringSlice(ext)
			if err != nil {
				exec.log.Debug("failed to decode external network endpoints of the storage node from the network map, ignore them",
					zap.Strings("endpoints", ext), zap.Error(err))
			} else {
				clientInfo.SetExternalAddressGroup(externalEndpoints)
			}
		}

		clientInfo.SetAddressGroup(endpoints)
		clientInfo.SetPublicKey(storageNodeInfo.PublicKey())

		if exec.processNode(clientInfo) {
			exec.log.Debug("completing the operation")
			return false
		}

		return true
	})
	if err != nil {
		exec.status = statusUndefined
		exec.err = err
	}
}
