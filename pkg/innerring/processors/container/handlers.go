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

	cp.workers.Submit(func() { cp.processContainerPut(put) })
}

func (cp *Processor) handleDelete(ev event.Event) {
	del := ev.(containerEvent.Delete)
	cp.log.Info("notification",
		zap.String("type", "container delete"),
		zap.String("id", base58.Encode(del.ContainerID())))

	cp.workers.Submit(func() { cp.processContainerDelete(&del) })
}

func (cp *Processor) handleSetEACL(ev event.Event) {
	e := ev.(containerEvent.SetEACL)

	cp.log.Info("notification",
		zap.String("type", "set EACL"),
	)

	cp.workers.Submit(func() {
		cp.processSetEACL(e)
	})
}
