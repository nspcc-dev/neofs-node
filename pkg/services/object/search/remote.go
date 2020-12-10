package searchsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/network"
	"go.uber.org/zap"
)

func (exec *execCtx) processNode(ctx context.Context, addr *network.Address) {
	log := exec.log.With(zap.Stringer("remote node", addr))

	log.Debug("processing node...")

	client, ok := exec.remoteClient(addr)
	if !ok {
		return
	}

	ids, err := client.searchObjects(exec)

	if err != nil {
		exec.log.Debug("local operation failed",
			zap.String("error", err.Error()),
		)

		return
	}

	exec.writeIDList(ids)
}
