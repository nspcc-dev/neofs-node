package container

import (
	"crypto/sha256"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"go.uber.org/zap"
)

func (cp *Processor) handlePut(ev event.Event) {
	put := ev.(containerEvent.Put) // todo: check panic in production

	id := sha256.Sum256(put.Container())
	cp.log.Info("notification",
		zap.String("type", "container put"),
		zap.String("id", base58.Encode(id[:])))

	// send event to the worker pool

	err := cp.pool.Submit(func() { cp.processContainerPut(&put) })
	if err != nil {
		// todo: move into controlled degradation stage
		cp.log.Warn("container processor worker pool drained",
			zap.Int("capacity", cp.pool.Cap()))
	}
}

func (cp *Processor) handleDelete(ev event.Event) {
	del := ev.(containerEvent.Delete) // todo: check panic in production
	cp.log.Info("notification",
		zap.String("type", "container delete"),
		zap.String("id", base58.Encode(del.ContainerID())))

	// send event to the worker pool

	err := cp.pool.Submit(func() { cp.processContainerDelete(&del) })
	if err != nil {
		// todo: move into controlled degradation stage
		cp.log.Warn("container processor worker pool drained",
			zap.Int("capacity", cp.pool.Cap()))
	}
}
