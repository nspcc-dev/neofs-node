package replicator

import (
	"context"
	"encoding/hex"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	"go.uber.org/zap"
)

func (p *Replicator) Run(ctx context.Context) {
	defer func() {
		close(p.ch)
		p.log.Info("routine stopped")
	}()

	p.ch = make(chan *Task, p.taskCap)

	p.log.Info("process routine",
		zap.Uint32("task queue capacity", p.taskCap),
		zap.Duration("put timeout", p.putTimeout),
	)

	for {
		select {
		case <-ctx.Done():
			p.log.Warn("context is done",
				zap.String("error", ctx.Err().Error()),
			)

			return
		case task, ok := <-p.ch:
			if !ok {
				p.log.Warn("trigger channel is closed")

				return
			}

			p.handleTask(ctx, task)
		}
	}
}

func (p *Replicator) handleTask(ctx context.Context, task *Task) {
	defer func() {
		p.log.Info("finish work",
			zap.Uint32("amount of unfinished replicas", task.quantity),
		)
	}()

	obj, err := engine.Get(p.localStorage, task.addr)
	if err != nil {
		p.log.Error("could not get object from local storage")

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

		log := p.log.With(zap.String("node", hex.EncodeToString(task.nodes[i].PublicKey())))

		var node network.AddressGroup

		err := node.FromIterator(task.nodes[i])
		if err != nil {
			log.Error("could not parse network address",
				zap.String("error", err.Error()),
			)

			continue
		}

		callCtx, cancel := context.WithTimeout(ctx, p.putTimeout)

		err = p.remoteSender.PutObject(callCtx, prm.WithNodeAddress(node))

		cancel()

		if err != nil {
			log.Error("could not replicate object",
				zap.String("error", err.Error()),
			)
		} else {
			log.Info("object successfully replicated")

			task.quantity--
		}
	}
}
