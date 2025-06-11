package searchsvc

import (
	"context"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
)

func (exec *execCtx) executeOnContainer(ectx context.Context) {
	if exec.isLocal() {
		exec.log.Debug("return result directly")
		return
	}

	exec.log.Debug("trying to execute in container...")

	ctx, cancel := context.WithCancel(ectx)
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

		lg := exec.log.With(zap.String("public key", netmap.StringifyPublicKey(node)))

		var endpoints network.AddressGroup
		err := endpoints.FromIterator(network.NodeEndpointsIterator(node))
		if err != nil {
			// critical error that may ultimately block the storage service. Normally it
			// should not appear because entry into the network map under strict control.
			lg.Error("failed to decode network endpoints of the storage node from the network map, skip the node",
				zap.Error(err))
			return
		}

		var info client.NodeInfo
		info.SetAddressGroup(endpoints)
		info.SetPublicKey(pubKey)

		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				lg.Debug("interrupt placement iteration by context",
					zap.Error(ctx.Err()))
				return
			default:
			}

			lg.Debug("processing node...")

			c, err := exec.svc.clientConstructor.get(info)
			if err != nil {
				mtx.Lock()
				exec.status = statusUndefined
				exec.err = err
				mtx.Unlock()

				lg.Debug("could not construct remote node client")
				return
			}

			ids, err := c.searchObjects(ctx, exec)
			if err != nil {
				lg.Debug("remote operation failed",
					zap.Error(err))

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
