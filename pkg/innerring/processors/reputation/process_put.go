package reputation

import (
	"encoding/hex"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client/reputation/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	"go.uber.org/zap"
)

func (rp *Processor) processPut(epoch uint64, id reputation.PeerID, value []byte) {
	if !rp.alphabetState.IsAlphabet() {
		rp.log.Info("non alphabet mode, ignore reputation put notification")
		return
	}

	// todo: do sanity checks of value and epoch

	args := wrapper.PutArgs{}
	args.SetEpoch(epoch)
	args.SetPeerID(id)
	args.SetValue(value)

	err := rp.reputationWrp.PutViaNotary(args)
	if err != nil {
		rp.log.Warn("can't send approval tx for reputation value",
			zap.String("peer_id", hex.EncodeToString(id.Bytes())),
			zap.String("error", err.Error()))
	}
}
