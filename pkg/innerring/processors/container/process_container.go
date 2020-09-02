package container

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/invoke"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"go.uber.org/zap"
)

// Process new container from the user by checking container sanity
// and sending approve tx back to morph.
func (cp *Processor) processContainerPut(put *containerEvent.Put) {
	if !cp.activeState.IsActive() {
		cp.log.Info("passive mode, ignore container put")
		return
	}

	err := invoke.RegisterContainer(cp.morphClient, cp.containerContract,
		&invoke.ContainerParams{
			Key:       put.PublicKey(),
			Container: put.Container(),
			Signature: put.Signature(),
		})
	if err != nil {
		cp.log.Error("can't invoke new container", zap.Error(err))
	}
}

// Process delete container operation from the user by checking container sanity
// and sending approve tx back to morph.
func (cp *Processor) processContainerDelete(delete *containerEvent.Delete) {
	if !cp.activeState.IsActive() {
		cp.log.Info("passive mode, ignore container put")
		return
	}

	err := invoke.RemoveContainer(cp.morphClient, cp.containerContract,
		&invoke.RemoveContainerParams{
			ContainerID: delete.ContainerID(),
			Signature:   delete.Signature(),
		})
	if err != nil {
		cp.log.Error("can't invoke delete container", zap.Error(err))
	}
}
