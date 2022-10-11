package alphabet

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/innerring/models"
	"go.uber.org/zap"
)

func (x *Processor) ProcessAlphabetGASEmissionTick(_ models.AlphabetGASEmissionTick) {
	x.log.Debug("sidechain GAS emission tick received, processing...")

	err := x.node.EmitSidechainGAS()
	if err != nil {
		if errors.Is(err, models.ErrNonAlphabet) {
			x.log.Debug("skip emission for non-alphabet node")
		} else {
			x.log.Error("failed to trigger sidechain GAS emission", zap.Error(err))
		}

		return
	}

	storageEmitAmount, err := x.neoFS.StorageEmissionAmount()
	if err != nil {
		if errors.Is(err, models.ErrStorageEmissionDisabled) {
			x.log.Debug("storage GAS emission is disabled in the network")
		} else {
			x.log.Error("failed to get storage emission GAS amount configuration", zap.Error(err))
		}

		return
	}

	storageAccs, err := x.neoFS.StorageNodes()
	if err != nil {
		x.log.Error("failed to list storage nodes in the network", zap.Error(err))
		return
	}

	n := uint64(len(storageAccs)) // positive according to StorageNodes docs
	gasPerNode := storageEmitAmount / n

	x.log.Debug("storage GAS amount calculated",
		zap.Uint64("per node", gasPerNode),
		zap.Uint64("full", storageEmitAmount),
		zap.Uint64("members", n),
	)

	if gasPerNode <= 0 {
		x.log.Debug("no GAS to distribute, skipping...")
		return
	}

	for i := range storageAccs {
		x.log.Debug("transferring GAS to the storage node",
			zap.Stringer("account", storageAccs[i]),
		)

		err = x.node.TransferGAS(gasPerNode, storageAccs[i])
		if err != nil {
			x.log.Warn("failed to transfer GAS to the storage node",
				zap.Error(err),
			)
		}
	}

	x.log.Debug("sidechain GAS emission/distribution is done")
}
