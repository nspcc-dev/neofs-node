package searchsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"go.uber.org/zap"
)

func (exec *execCtx) processNode(ctx context.Context, info client.NodeInfo) {
	exec.log.Debug("processing node...")

	client, ok := exec.remoteClient(info)
	if !ok {
		return
	}

	ids, err := client.searchObjects(exec, info)

	if err != nil {
		exec.log.Debug("local operation failed",
			zap.String("error", err.Error()),
		)

		return
	}

	exec.writeIDList(ids)
}
