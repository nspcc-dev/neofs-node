package replicator

import (
	"context"
	"io"

	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
			zap.Uint32("amount of unfinished replicas", task.quantity),
		)
	}()

	var binObjStream io.ReadSeekCloser // set it task.obj is unset only
	var err error
	if task.obj == nil {
		binObjStream, err = p.localStorage.OpenObjectStream(task.addr)
		if err != nil {
			p.log.Error("could not get object from local storage",
				zap.Stringer("object", task.addr),
				zap.Error(err))
			return
		}
		defer func() {
			if err := binObjStream.Close(); err != nil {
				p.log.Debug("failed to close replicated object's binary stream from the local storage",
					zap.Stringer("object", task.addr), zap.Error(err))
			}
		}()
	}

	prm := new(putsvc.RemotePutPrm).
		WithObject(task.obj)

	for i := 0; task.quantity > 0 && i < len(task.nodes); i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if i > 0 && binObjStream != nil {
			_, err = binObjStream.Seek(0, io.SeekStart)
			if err != nil {
				p.log.Error("failed to seek start of the replicated object's binary stream from the local storage",
					zap.Stringer("object", task.addr), zap.Error(err))
				return
			}
		}

		log := p.log.With(
			zap.String("node", netmap.StringifyPublicKey(task.nodes[i])),
			zap.Stringer("object", task.addr),
		)

		callCtx, cancel := context.WithTimeout(ctx, p.putTimeout)

		if binObjStream != nil {
			err = p.remoteSender.ReplicateObjectToNode(callCtx, task.nodes[i], task.addr.Object(), binObjStream)
			// FIXME: temporary workaround, see also
			// https://github.com/nspcc-dev/neofs-api/issues/201#issuecomment-1891383454
			if st, ok := status.FromError(err); ok && st.Code() == codes.Unimplemented {
				log.Debug("node does not support 'Replicate' RPC, fallback to 'Put'")
				err = p.remoteSender.PutObject(callCtx, prm.WithNodeInfo(task.nodes[i]))
			}
		} else {
			err = p.remoteSender.PutObject(callCtx, prm.WithNodeInfo(task.nodes[i]))
		}

		cancel()

		if err != nil {
			log.Error("could not replicate object",
				zap.String("error", err.Error()),
			)
		} else {
			log.Debug("object successfully replicated")

			task.quantity--

			res.SubmitSuccessfulReplication(task.nodes[i])
		}
	}
}
