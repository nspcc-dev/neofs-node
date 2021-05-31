package alphabet

import (
	"crypto/elliptic"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"go.uber.org/zap"
)

const emitMethod = "emit"

func (np *Processor) processEmit() {
	index := np.irList.AlphabetIndex()
	if index < 0 {
		np.log.Info("non alphabet mode, ignore gas emission event")

		return
	}

	contract, ok := np.alphabetContracts.GetByIndex(index)
	if !ok {
		np.log.Debug("node is out of alphabet range, ignore gas emission event",
			zap.Int("index", index))

		return
	}

	// there is no signature collecting, so we don't need extra fee
	err := np.morphClient.Invoke(contract, 0, emitMethod)
	if err != nil {
		np.log.Warn("can't invoke alphabet emit method")

		return
	}

	if np.storageEmission == 0 {
		np.log.Info("storage node emission is off")

		return
	}

	networkMap, err := np.netmapClient.Snapshot()
	if err != nil {
		np.log.Warn("can't get netmap snapshot to emit gas to storage nodes",
			zap.String("error", err.Error()))

		return
	}

	ln := len(networkMap.Nodes)
	if ln == 0 {
		np.log.Debug("empty network map, do not emit gas")

		return
	}

	gasPerNode := fixedn.Fixed8(np.storageEmission / uint64(ln))

	for i := range networkMap.Nodes {
		keyBytes := networkMap.Nodes[i].PublicKey()

		key, err := keys.NewPublicKeyFromBytes(keyBytes, elliptic.P256())
		if err != nil {
			np.log.Warn("can't convert node public key to address",
				zap.String("error", err.Error()))

			continue
		}

		err = np.morphClient.TransferGas(key.GetScriptHash(), gasPerNode)
		if err != nil {
			np.log.Warn("can't transfer gas",
				zap.String("receiver", key.Address()),
				zap.Int64("amount", int64(gasPerNode)),
				zap.String("error", err.Error()),
			)
		}
	}
}
