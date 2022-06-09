package replicator

import (
	"context"
	"encoding/hex"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	"go.uber.org/zap"
)

// TaskResult is a replication result interface.
type TaskResult interface {
	// SubmitSuccessfulReplication must save successful
	// replication result. ID is a netmap identification
	// of a node that accepted the replica.
	SubmitSuccessfulReplication(id uint64)
}

// HandleTask executes replication task inside invoking goroutine.
// Passes all the nodes that accepted the replication to the TaskResult.
func (p *Replicator) HandleTask(ctx context.Context, task *Task, res TaskResult) {
	defer func() {
		p.log.Debug("finish work",
			zap.Uint32("amount of unfinished replicas", task.quantity),
		)
	}()

	obj, err := engine.Get(p.localStorage, task.addr)
	if err != nil {
		p.log.Error("could not get object from local storage",
			zap.Stringer("object", task.addr),
			zap.Error(err))

		return
	}

	prm := new(putsvc.RemotePutPrm).
		WithObject(obj)

	for i := 0; task.quantity > 0 && i < len(task.nodes); i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		log := p.log.With(
			zap.String("node", hex.EncodeToString(task.nodes[i].PublicKey())),
			zap.Stringer("object", task.addr),
		)

		callCtx, cancel := context.WithTimeout(ctx, p.putTimeout)

		err = p.remoteSender.PutObject(callCtx, prm.WithNodeInfo(task.nodes[i].NodeInfo))

		cancel()

		if err != nil {
			log.Error("could not replicate object",
				zap.String("error", err.Error()),
			)
		} else {
			log.Debug("object successfully replicated")

			task.quantity--

			res.SubmitSuccessfulReplication(task.nodes[i].ID)
		}
	}
}
