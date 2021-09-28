package getsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
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
}

func (exec *execCtx) processCurrentEpoch() bool {
	exec.log.Debug("process epoch",
		zap.Uint64("number", exec.curProcEpoch),
	)

	traverser, ok := exec.generateTraverser(exec.address())
	if !ok {
		return true
	}

	ctx, cancel := context.WithCancel(exec.context())
	defer cancel()

	exec.status = statusUndefined

	for {
		addrs := traverser.Next()
		if len(addrs) == 0 {
			exec.log.Debug("no more nodes, abort placement iteration")

			return false
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
			// TODO: consider optimization: if status == SPLIT we can continue until
			//  we reach the best result - split info with linking object ID.
			var info client.NodeInfo

			info.SetAddressGroup(addrs[i].Addresses())

			if exec.processNode(ctx, info) {
				exec.log.Debug("completing the operation")
				return true
			}
		}
	}
}
