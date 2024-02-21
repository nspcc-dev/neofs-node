package replicator

import (
	"context"

	"github.com/nspcc-dev/neofs-sdk-go/netmap"
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
	defer func() {
		p.log.Debug("finish work",
			zap.Uint32("amount of unfinished replicas", task.quantity),
		)
	}()

	blankReq, err := newBlankUnaryReplicateRequest(task.addr.Object(), p.signer)
	if err != nil {
		p.log.Error("failed to prepare replication request",
			zap.Stringer("object", task.addr), zap.Error(err))
		return
	}

	// prepare in-memory replication request
	var reqLayout unaryReplicateRequestLayout
	var req []byte
	if task.obj != nil {
		objv2 := task.obj.ToV2()
		objLen := objv2.StableSize()

		reqLayout = unaryReplicateRequestLayoutForObject(blankReq, objLen)
		req = make([]byte, objLen, reqLayout.fullLen)
		objv2.StableMarshal(req)
	} else {
		req, err = p.localStorage.GetBytes(task.addr, func(ln int) []byte {
			reqLayout = unaryReplicateRequestLayoutForObject(blankReq, ln)
			return make([]byte, ln, reqLayout.fullLen)
		})
		if err != nil {
			p.log.Error("could not get object from local storage",
				zap.Stringer("object", task.addr),
				zap.Error(err))

			return
		}
	}

	req = encodeUnaryReplicateRequestWithObject(reqLayout, req)

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

		err := p.transport.ReplicateToNode(callCtx, req, task.nodes[i])

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
