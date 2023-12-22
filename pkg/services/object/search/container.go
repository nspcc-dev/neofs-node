package searchsvc

import (
	"context"
	"sync"
	"sync/atomic"

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

	var wg sync.WaitGroup
	var mtx sync.Mutex
	var invalidEndpointDetected atomic.Value

	err := exec.svc.storagePolicer.ForEachRemoteContainerNode(exec.containerID(), startEpoch, lookupDepth, func(storageNodeInfo netmap.NodeInfo) bool {
		select {
		case <-ctx.Done():
			exec.log.Debug("interrupt searching for objects on remote container nodes by context",
				zap.String("error", ctx.Err().Error()))
			return false
		default:
			if invalidEndpointDetected.Load() != nil {
				return false
			}
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			pubKeyForLog := netmap.StringifyPublicKey(storageNodeInfo)

			var endpoints network.AddressGroup
			err := endpoints.FromIterator(network.NodeEndpointsIterator(storageNodeInfo))
			if err != nil {
				exec.log.Debug("failed to decode network endpoints of the storage node from the network map, skip search on it",
					zap.String("public key", pubKeyForLog), zap.Error(err))
				invalidEndpointDetected.Store(err)
				return
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

			exec.log.Debug("searching for objects on remote container node...", zap.String("public key", pubKeyForLog))

			c, err := exec.svc.clientConstructor.get(clientInfo)
			if err != nil {
				mtx.Lock()
				exec.status = statusUndefined
				exec.err = err
				mtx.Unlock()

				exec.log.Debug("could not construct remote node client", zap.Error(err))
				return
			}

			ids, err := c.searchObjects(exec, clientInfo)
			if err != nil {
				exec.log.Debug("remote operation failed", zap.String("error", err.Error()))
				return
			}

			mtx.Lock()
			exec.writeIDList(ids)
			mtx.Unlock()
		}()

		return invalidEndpointDetected.Load() == nil
	})
	wg.Wait()
	if err == nil {
		if v := invalidEndpointDetected.Load(); v != nil {
			err = v.(error)
		}
	}

	if err != nil {
		exec.status = statusUndefined
		exec.err = err
	}
}
