package alphabet

import (
	"crypto/elliptic"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/invoke"
	"go.uber.org/zap"
)

func (np *Processor) processEmit() {
	index := np.irList.Index()
	if index < 0 {
		np.log.Info("passive mode, ignore gas emission event")

		return
	} else if int(index) >= len(np.alphabetContracts) {
		np.log.Debug("node is out of alphabet range, ignore gas emission event",
			zap.Int32("index", index))

		return
	}

	err := invoke.AlphabetEmit(np.morphClient, np.alphabetContracts[index])
	if err != nil {
		np.log.Warn("can't invoke alphabet emit method")

		return
	}

	if np.storageEmission == 0 {
		np.log.Info("storage node emission is off")

		return
	}

	networkMap, err := invoke.NetmapSnapshot(np.morphClient, np.netmapContract)
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
