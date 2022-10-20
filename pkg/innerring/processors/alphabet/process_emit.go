package alphabet

import (
	"crypto/elliptic"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

const emitMethod = "emit"

func (ap *Processor) processEmit() {
	index := ap.irList.AlphabetIndex()
	if index < 0 {
		ap.log.Info("non alphabet mode, ignore gas emission event")

		return
	}

	contract, ok := ap.alphabetContracts.GetByIndex(index)
	if !ok {
		ap.log.Debug("node is out of alphabet range, ignore gas emission event",
			logger.FieldInt("index", int64(index)),
		)

		return
	}

	// there is no signature collecting, so we don't need extra fee
	err := ap.morphClient.Invoke(contract, 0, emitMethod)
	if err != nil {
		ap.log.Warn("can't invoke alphabet emit method",
			logger.FieldError(err),
		)

		return
	}

	if ap.storageEmission == 0 {
		ap.log.Info("storage node emission is off")

		return
	}

	networkMap, err := ap.netmapClient.NetMap()
	if err != nil {
		ap.log.Warn("can't get netmap snapshot to emit gas to storage nodes",
			logger.FieldError(err),
		)

		return
	}

	nmNodes := networkMap.Nodes()

	ln := len(nmNodes)
	if ln == 0 {
		ap.log.Debug("empty network map, do not emit gas")

		return
	}

	gasPerNode := fixedn.Fixed8(ap.storageEmission / uint64(ln))

	for i := range nmNodes {
		keyBytes := nmNodes[i].PublicKey()

		key, err := keys.NewPublicKeyFromBytes(keyBytes, elliptic.P256())
		if err != nil {
			ap.log.Warn("can't parse node public key",
				logger.FieldError(err),
			)

			continue
		}

		err = ap.morphClient.TransferGas(key.GetScriptHash(), gasPerNode)
		if err != nil {
			ap.log.Warn("can't transfer gas",
				logger.FieldString("receiver", key.Address()),
				logger.FieldInt("amount", int64(gasPerNode)),
				logger.FieldError(err),
			)
		}
	}
}
