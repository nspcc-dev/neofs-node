package netmap

import (
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"go.uber.org/zap"
)

const updatePeerStateMethod = "updateState"

func (np *Processor) processNetmapCleanupTick(epoch uint64) {
	if !np.alphabetState.IsAlphabet() {
		np.log.Info("non alphabet mode, ignore new netmap cleanup tick")

		return
	}

	err := np.netmapSnapshot.forEachRemoveCandidate(epoch, func(s string) error {
		key, err := keys.NewPublicKeyFromString(s)
		if err != nil {
			np.log.Warn("can't decode public key of netmap node",
				zap.String("key", s))

			return nil
		}

		np.log.Info("vote to remove node from netmap", zap.String("key", s))

		err = np.morphClient.NotaryInvoke(np.netmapContract, np.feeProvider.SideChainFee(), updatePeerStateMethod,
			int64(netmap.NodeStateOffline.ToV2()),
			key.Bytes())
		if err != nil {
			np.log.Error("can't invoke netmap.UpdateState", zap.Error(err))
		}

		return nil
	})
	if err != nil {
		np.log.Warn("can't iterate on netmap cleaner cache",
			zap.String("error", err.Error()))
	}
}
