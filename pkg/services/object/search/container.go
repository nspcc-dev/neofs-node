package searchsvc

import (
	"context"
	"encoding/hex"
	"sync"

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

	exec.status = statusOK
	exec.err = nil
}

func (exec *execCtx) processCurrentEpoch(mProcessedNodes map[string]struct{}) bool {
	log := exec.log.With(zap.Uint64("epoch", exec.curProcEpoch))

	log.Debug("process epoch")

	nodeSets, err := exec.svc.node.GetContainerNodesAtEpoch(exec.containerID(), exec.curProcEpoch)
	if err != nil {
		log.Debug("failed to get storage nodes from the network map for the container, ignore error and abort",
			zap.Error(err))
		return true
	}

	ctx, cancel := context.WithCancel(exec.context())
	defer cancel()

	var wg sync.WaitGroup
	var mtx sync.Mutex

	for i := range nodeSets {
		for j := range nodeSets[i] {
			bPubKey := nodeSets[i][j].PublicKey()
			if exec.svc.node.IsLocalPublicKey(bPubKey) {
				continue
			}

			strPubKey := string(bPubKey)
			if _, ok := mProcessedNodes[strPubKey]; ok {
				continue
			}

			mProcessedNodes[strPubKey] = struct{}{}

			var endpoints network.AddressGroup
			err := endpoints.FromIterator(network.NodeEndpointsIterator(nodeSets[i][j]))
			if err != nil {
				// critical error that may ultimately block the storage service. Normally it
				// should not appear because entry into the network map under strict control.
				log.Error("failed to decode network endpoints of the storage node from the network map, skip the node",
					zap.String("public key", netmap.StringifyPublicKey(nodeSets[i][j])), zap.Error(err))
				continue
			}

			var info client.NodeInfo

			if ext := nodeSets[i][j].ExternalAddresses(); len(ext) > 0 {
				var externalEndpoints network.AddressGroup
				err = externalEndpoints.FromStringSlice(ext)
				if err != nil {
					// less critical since the main ones must work, but also important
					log.Warn("failed to decode external network endpoints of the storage node from the network map, ignore them",
						zap.String("public key", netmap.StringifyPublicKey(nodeSets[i][j])),
						zap.Strings("endpoints", ext), zap.Error(err))
				} else {
					info.SetExternalAddressGroup(externalEndpoints)
				}
			}

			info.SetAddressGroup(endpoints)
			info.SetPublicKey(nodeSets[i][j].PublicKey())

			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case <-ctx.Done():
					log.Debug("interrupt placement iteration by context",
						zap.String("error", ctx.Err().Error()))
					return
				default:
				}

				exec.log.Debug("processing node...", zap.String("public key", hex.EncodeToString(info.PublicKey())))

				c, err := exec.svc.clientConstructor.get(info)
				if err != nil {
					mtx.Lock()
					exec.status = statusUndefined
					exec.err = err
					mtx.Unlock()

					log.Debug("could not construct remote node client")
					return
				}

				ids, err := c.searchObjects(exec, info)
				if err != nil {
					log.Debug("remote operation failed",
						zap.String("error", err.Error()))

					return
				}

				mtx.Lock()
				exec.writeIDList(ids)
				mtx.Unlock()
			}()
		}

		wg.Wait()
	}

	return false
}
