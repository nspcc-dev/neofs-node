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

	exec.log.Debug("trying to execute in container...")

	ctx, cancel := context.WithCancel(exec.context())
	defer cancel()

	mProcessedNodes := make(map[string]struct{})
	var wg sync.WaitGroup
	var mtx sync.Mutex

	err := exec.svc.containers.ForEachRemoteContainerNode(exec.containerID(), func(node netmap.NodeInfo) {
		pubKey := node.PublicKey()
		strKey := string(pubKey)
		if _, ok := mProcessedNodes[strKey]; ok {
			return
		}

		mProcessedNodes[strKey] = struct{}{}

		var endpoints network.AddressGroup
		err := endpoints.FromIterator(network.NodeEndpointsIterator(node))
		if err != nil {
			// critical error that may ultimately block the storage service. Normally it
			// should not appear because entry into the network map under strict control.
			exec.log.Error("failed to decode network endpoints of the storage node from the network map, skip the node",
				zap.String("public key", netmap.StringifyPublicKey(node)), zap.Error(err))
			return
		}

		var info client.NodeInfo
		info.SetAddressGroup(endpoints)
		info.SetPublicKey(pubKey)
		if ext := node.ExternalAddresses(); len(ext) > 0 {
			var externalEndpoints network.AddressGroup
			err = externalEndpoints.FromStringSlice(ext)
			if err != nil {
				// less critical since the main ones must work, but also important
				exec.log.Warn("failed to decode external network endpoints of the storage node from the network map, ignore them",
					zap.String("public key", netmap.StringifyPublicKey(node)),
					zap.Strings("endpoints", ext), zap.Error(err))
			} else {
				info.SetExternalAddressGroup(externalEndpoints)
			}
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				exec.log.Debug("interrupt placement iteration by context",
					zap.String("error", ctx.Err().Error()))
				return
			default:
			}

			exec.log.Debug("processing node...", zap.String("public key", hex.EncodeToString(node.PublicKey())))

			c, err := exec.svc.clientConstructor.get(info)
			if err != nil {
				mtx.Lock()
				exec.status = statusUndefined
				exec.err = err
				mtx.Unlock()

				exec.log.Debug("could not construct remote node client")
				return
			}

			ids, err := c.searchObjects(exec, info)
			if err != nil {
				exec.log.Debug("remote operation failed",
					zap.String("error", err.Error()))

				return
			}

			mtx.Lock()
			exec.writeIDList(ids)
			mtx.Unlock()
		}()
	})
	wg.Wait()
	if err != nil {
		exec.status = statusUndefined
	} else {
		exec.status = statusOK
	}

	exec.err = err
}
