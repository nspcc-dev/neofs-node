package searchsvc

import (
	"context"

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
		zap.Uint64("number", exec.curProcEpoch),
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

		for i := range addrs {
			select {
			case <-ctx.Done():
				exec.log.Debug("interrupt placement iteration by context",
					zap.String("error", ctx.Err().Error()),
				)

				return true
			default:
			}

			// TODO: consider parallel execution
			exec.processNode(ctx, addrs[i].Addresses())
		}
	}

	return false
}
