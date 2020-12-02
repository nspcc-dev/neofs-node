package getsvc

import (
	"context"

	"go.uber.org/zap"
)

func (exec *execCtx) executeOnContainer() {
	if exec.isLocal() {
		exec.log.Debug("return result directly")
		return
	}

	exec.log.Debug("trying to execute in container...")

	traverser, ok := exec.generateTraverser(exec.address())
	if !ok {
		return
	}

	ctx, cancel := context.WithCancel(exec.context())
	defer cancel()

	exec.status = statusUndefined

loop:
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
				break loop
			default:
			}

			// TODO: consider parallel execution
			// TODO: consider optimization: if status == SPLIT we can continue until
			//  we reach the best result - split info with linking object ID.
			if exec.processNode(ctx, addrs[i]) {
				exec.log.Debug("completing the operation")
				break loop
			}
		}
	}
}
