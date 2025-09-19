package alphabet

import (
	"context"
	"crypto/elliptic"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"go.uber.org/zap"
)

const emitMethod = "emit"

func (ap *Processor) processEmit() {
	index := ap.irList.AlphabetIndex()
	if index < 0 {
		ap.log.Info("non alphabet mode, ignore gas emission event")

		return
	}

	if index >= len(ap.alphabetContracts) {
		ap.log.Debug("node is out of alphabet range, ignore gas emission event",
			zap.Int("index", index))

		return
	}

	err := ap.fsChainClient.Invoke(context.TODO(), ap.alphabetContracts[index], false, false, 0, emitMethod)
	if err != nil {
		ap.log.Warn("can't invoke alphabet emit method", zap.Error(err))

		return
	}

	if ap.storageEmission == 0 {
		ap.log.Info("storage node emission is off")

		return
	}

	networkMap, err := ap.netmapClient.NetMap()
	if err != nil {
		ap.log.Warn("can't get netmap snapshot to emit gas to storage nodes",
			zap.Error(err))

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
				zap.Error(err))

			continue
		}

		err = ap.fsChainClient.TransferGas(key.GetScriptHash(), gasPerNode)
		if err != nil {
			ap.log.Warn("can't transfer gas",
				zap.String("receiver", key.Address()),
				zap.Int64("amount", int64(gasPerNode)),
				zap.Error(err),
			)

			continue
		}

		ap.log.Debug("sent gas to storage node",
			zap.String("receiver", key.Address()),
			zap.Int64("amount", int64(gasPerNode)),
		)
	}
}
