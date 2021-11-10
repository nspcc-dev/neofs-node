package netmap

import (
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	netmapclient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
)

func (np *Processor) processNetmapCleanupTick(ev netmapCleanupTick) {
	if !np.alphabetState.IsAlphabet() {
		np.log.Info("non alphabet mode, ignore new netmap cleanup tick")

		return
	}

	err := np.netmapSnapshot.forEachRemoveCandidate(ev.epoch, func(s string) error {
		key, err := keys.NewPublicKeyFromString(s)
		if err != nil {
			np.log.Warn("can't decode public key of netmap node",
				zap.String("key", s))

			return nil
		}

		np.log.Info("vote to remove node from netmap", zap.String("key", s))

		prm := netmapclient.UpdatePeerPrm{}

		prm.SetKey(key.Bytes())
		prm.SetState(netmap.NodeStateOffline)
		prm.SetHash(ev.TxHash())

		err = np.netmapClient.UpdatePeerState(prm)
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
