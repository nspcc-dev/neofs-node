package searchsvc

import (
	"context"
	"encoding/hex"
	"sync"

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

	exec.status = statusOK
	exec.err = nil
}

func (exec *execCtx) processEpoch(epoch uint64) bool {
	exec.log.Debug("process epoch",
		zap.Uint64("number", epoch),
	)

	traverser, ok := exec.generateTraverser(exec.containerID(), epoch)
	if !ok {
		return true
	}

	ctx, cancel := context.WithCancel(exec.context())
	defer cancel()

	mProcessedNodes := make(map[string]struct{})

	for {
		addrs := traverser.Next()
		if len(addrs) == 0 {
			exec.log.Debug("no more nodes, abort placement iteration")
			break
		}

		var wg sync.WaitGroup
		var mtx sync.Mutex

		for i := range addrs {
			strKey := string(addrs[i].PublicKey())
			if _, ok = mProcessedNodes[strKey]; ok {
				continue
			}

			mProcessedNodes[strKey] = struct{}{}

			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				select {
				case <-ctx.Done():
					exec.log.Debug("interrupt placement iteration by context",
						zap.String("error", ctx.Err().Error()))
					return
				default:
				}

				var info client.NodeInfo

				client.NodeInfoFromNetmapElement(&info, addrs[i])

				exec.log.Debug("processing node...", zap.String("key", hex.EncodeToString(addrs[i].PublicKey())))

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
			}(i)
		}

		wg.Wait()
	}

	return false
}
