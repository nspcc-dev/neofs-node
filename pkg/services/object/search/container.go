package searchsvc

import (
	"context"
	"encoding/hex"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

func (exec *execCtx) executeOnContainer() {
	if exec.isLocal() {
		exec.log.Debug("return result directly")
		return
	}

	lookupDepth := exec.netmapLookupDepth()

	exec.log.Debug("trying to execute in container...",
		logger.FieldUint("netmap lookup depth", lookupDepth),
	)

	// initialize epoch number
	ok := exec.initEpoch()
	if !ok {
		return
	}

	for {
		if exec.processCurrentEpoch() {
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

func (exec *execCtx) processCurrentEpoch() bool {
	exec.log.Debug("process epoch",
		logger.FieldUint("number", exec.curProcEpoch),
	)

	traverser, ok := exec.generateTraverser(exec.containerID())
	if !ok {
		return true
	}

	ctx, cancel := context.WithCancel(exec.context())
	defer cancel()

	for {
		addrs := traverser.Next()
		if len(addrs) == 0 {
			exec.log.Debug("no more nodes, abort placement iteration")
			break
		}

		var wg sync.WaitGroup
		var mtx sync.Mutex

		for i := range addrs {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				select {
				case <-ctx.Done():
					exec.log.Debug("interrupt placement iteration by context",
						logger.FieldString("error", ctx.Err().Error()))
					return
				default:
				}

				var info client.NodeInfo

				client.NodeInfoFromNetmapElement(&info, addrs[i])

				exec.log.Debug("processing node...", logger.FieldString("key", hex.EncodeToString(addrs[i].PublicKey())))

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
						logger.FieldError(err))

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
