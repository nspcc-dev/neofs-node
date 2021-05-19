package container

import (
	containerSDK "github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"go.uber.org/zap"
)

const (
	deleteContainerMethod = "delete"
	putContainerMethod    = "put"
)

// Process new container from the user by checking container sanity
// and sending approve tx back to morph.
func (cp *Processor) processContainerPut(put *containerEvent.Put) {
	if !cp.alphabetState.IsAlphabet() {
		cp.log.Info("non alphabet mode, ignore container put")
		return
	}

	cnrData := put.Container()

	// unmarshal container structure
	cnr := containerSDK.New()
	if err := cnr.Unmarshal(cnrData); err != nil {
		cp.log.Info("could not unmarshal container structure",
			zap.String("error", err.Error()),
		)

		return
	}

	// perform format check
	if err := container.CheckFormat(cnr); err != nil {
		cp.log.Info("container with incorrect format detected",
			zap.String("error", err.Error()),
		)

		return
	}

	err := cp.morphClient.NotaryInvoke(cp.containerContract, cp.feeProvider.SideChainFee(), putContainerMethod,
		cnrData,
		put.Signature(),
		put.PublicKey())
	if err != nil {
		cp.log.Error("can't invoke new container", zap.Error(err))
	}
}

// Process delete container operation from the user by checking container sanity
// and sending approve tx back to morph.
func (cp *Processor) processContainerDelete(delete *containerEvent.Delete) {
	if !cp.alphabetState.IsAlphabet() {
		cp.log.Info("non alphabet mode, ignore container delete")
		return
	}

	err := cp.morphClient.NotaryInvoke(cp.containerContract, cp.feeProvider.SideChainFee(), deleteContainerMethod,
		delete.ContainerID(),
		delete.Signature())
	if err != nil {
		cp.log.Error("can't invoke delete container", zap.Error(err))
	}
}
