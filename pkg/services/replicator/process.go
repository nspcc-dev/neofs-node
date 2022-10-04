package replicator

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// TaskResult is a replication result interface.
type TaskResult interface {
	// SubmitSuccessfulReplication submits the successful object replication
	// to the given node.
	SubmitSuccessfulReplication(netmap.NodeInfo)
}

// HandleTask executes replication task inside invoking goroutine.
// Passes all the nodes that accepted the replication to the TaskResult.
func (p *Replicator) HandleTask(ctx context.Context, task Task, res TaskResult) {
	defer func() {
		p.log.Debug("finish work",
			logger.FieldUint("amount of unfinished replicas", uint64(task.quantity)),
		)
	}()

	if task.obj == nil {
		var err error
		task.obj, err = engine.Get(p.localStorage, task.addr)
		if err != nil {
			p.log.Error("could not get object from local storage",
				logger.FieldStringer("object", task.addr),
				logger.FieldError(err))

			return
		}
	}

	prm := new(putsvc.RemotePutPrm).
		WithObject(task.obj)

	for i := 0; task.quantity > 0 && i < len(task.nodes); i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		log := p.log.WithContext(
			logger.FieldString("node", netmap.StringifyPublicKey(task.nodes[i])),
			logger.FieldStringer("object", task.addr),
		)

		callCtx, cancel := context.WithTimeout(ctx, p.putTimeout)

		err := p.remoteSender.PutObject(callCtx, prm.WithNodeInfo(task.nodes[i]))

		cancel()

		if err != nil {
			log.Error("could not replicate object",
				logger.FieldError(err),
			)
		} else {
			log.Debug("object successfully replicated")

			task.quantity--

			res.SubmitSuccessfulReplication(task.nodes[i])
		}
	}
}
