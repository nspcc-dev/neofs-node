package replicator

import (
	"bytes"
	"context"
	"io"

	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
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
	if task.obj != nil {
		if l := task.obj.HeaderLen(); l > object.MaxHeaderLen {
			p.log.Warn("replication task with too big header",
				zap.Int("header len", l),
				zap.Int("max allowed header len", object.MaxHeaderLen))

			return
		}
	}

	defer func() {
		p.log.Debug("finish work",
			zap.Uint32("amount of unfinished replicas", task.quantity),
		)
	}()

	var err error
	var prm *putsvc.RemotePutPrm
	var stream io.ReadSeeker
	binReplication := task.obj == nil
	if binReplication {
		b, err := p.localStorage.GetBytes(task.addr)
		if err != nil {
			p.log.Error("could not get object from local storage",
				zap.Stringer("object", task.addr),
				zap.Error(err))

			return
		}
		stream = bytes.NewReader(b)
		if len(task.nodes) > 1 {
			stream = client.DemuxReplicatedObject(stream)
		}
	} else {
		prm = new(putsvc.RemotePutPrm).WithObject(task.obj)
	}

	for i := 0; task.quantity > 0 && i < len(task.nodes); i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		log := p.log.With(
			zap.String("node", netmap.StringifyPublicKey(task.nodes[i])),
			zap.Stringer("object", task.addr),
		)

		callCtx, cancel := context.WithTimeout(ctx, p.putTimeout)

		if binReplication {
			err = p.remoteSender.ReplicateObjectToNode(callCtx, task.addr.Object(), stream, task.nodes[i])
			// note that we don't need to reset stream because it is used exactly once
			// according to the client.DemuxReplicatedObject above
		} else {
			err = p.remoteSender.PutObject(callCtx, prm.WithNodeInfo(task.nodes[i]))
		}

		cancel()

		if err != nil {
			log.Error("could not replicate object",
				zap.Error(err),
			)
		} else {
			log.Debug("object successfully replicated")

			task.quantity--

			res.SubmitSuccessfulReplication(task.nodes[i])
		}
	}
}
