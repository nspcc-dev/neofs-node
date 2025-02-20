package container

import (
	"crypto/sha256"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"go.uber.org/zap"
)

func (cp *Processor) handlePut(ev event.Event) {
	put := ev.(putEvent)

	id := sha256.Sum256(put.Container())
	cp.log.Info("notification",
		zap.String("type", "container put"),
		zap.String("id", base58.Encode(id[:])))

	// send an event to the worker pool

	err := cp.pool.Submit(func() { cp.processContainerPut(put) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		cp.log.Warn("container processor worker pool drained",
			zap.Int("capacity", cp.pool.Cap()))
	}
}

func (cp *Processor) handleDelete(ev event.Event) {
	del := ev.(containerEvent.Delete)
	cp.log.Info("notification",
		zap.String("type", "container delete"),
		zap.String("id", base58.Encode(del.ContainerID())))

	// send an event to the worker pool

	err := cp.pool.Submit(func() { cp.processContainerDelete(&del) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		cp.log.Warn("container processor worker pool drained",
			zap.Int("capacity", cp.pool.Cap()))
	}
}

func (cp *Processor) handleSetEACL(ev event.Event) {
	e := ev.(containerEvent.SetEACL)

	cp.log.Info("notification",
		zap.String("type", "set EACL"),
	)

	// send an event to the worker pool

	err := cp.pool.Submit(func() {
		cp.processSetEACL(e)
	})
	if err != nil {
		// there system can be moved into controlled degradation stage
		cp.log.Warn("container processor worker pool drained",
			zap.Int("capacity", cp.pool.Cap()))
	}
}

func (cp *Processor) handleAnnounceLoad(ev event.Event) {
	e := ev.(containerEvent.AnnounceLoad)

	cp.log.Info("notification",
		zap.String("type", "announce load"),
	)

	// send an event to the worker pool

	err := cp.pool.Submit(func() {
		cp.processAnnounceLoad(e)
	})
	if err != nil {
		// there system can be moved into controlled degradation stage
		cp.log.Warn("container processor worker pool drained",
			zap.Int("capacity", cp.pool.Cap()))
	}
}
