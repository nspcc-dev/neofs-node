package replicator

import (
	"bytes"
	"context"
	"io"

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
	var stream io.ReadSeeker
	var objBin []byte
	if task.obj != nil {
		objBin = task.obj.Marshal()
		stream = bytes.NewReader(objBin)
	} else {
		objBin, err = p.localStorage.GetBytes(task.addr)
		if err != nil {
			p.log.Error("could not get object from local storage",
				zap.Stringer("object", task.addr),
				zap.Error(err))

			return
		}
		stream = bytes.NewReader(objBin)
	}
	if len(task.nodes) > 1 {
		stream = client.DemuxReplicatedObject(stream)
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

		if p.localNodeKey.IsLocalNodePublicKey(task.nodes[i].PublicKey()) {
			if task.obj == nil {
				log.Debug("cannot put object to local storage: object not provided in task")
				continue
			}
			if err = p.localStorage.Put(task.obj, objBin); err != nil {
				log.Error("could not put object to local storage", zap.Error(err))
				continue
			}
			log.Debug("object successfully stored locally")
			task.quantity--
			res.SubmitSuccessfulReplication(task.nodes[i])
			continue
		}

		callCtx, cancel := context.WithTimeout(ctx, p.putTimeout)

		err = p.remoteSender.ReplicateObjectToNode(callCtx, task.addr.Object(), stream, task.nodes[i])
		// note that we don't need to reset stream because it is used exactly once
		// according to the client.DemuxReplicatedObject above

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
