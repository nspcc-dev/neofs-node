package reputation

import (
	"encoding/hex"

	"github.com/nspcc-dev/neofs-api-go/pkg/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/reputation/wrapper"
	"go.uber.org/zap"
)

func (rp *Processor) processPut(epoch uint64, id reputation.PeerID, value reputation.GlobalTrust) {
	if !rp.alphabetState.IsAlphabet() {
		rp.log.Info("non alphabet mode, ignore reputation put notification")
		return
	}

	// check if epoch is valid
	currentEpoch := rp.epochState.EpochCounter()
	if epoch >= currentEpoch {
		rp.log.Info("ignore reputation value",
			zap.String("reason", "invalid epoch number"),
			zap.Uint64("trust_epoch", epoch),
			zap.Uint64("local_epoch", currentEpoch))

		return
	}

	// check signature
	if err := value.VerifySignature(); err != nil {
		rp.log.Info("ignore reputation value",
			zap.String("reason", "invalid signature"),
			zap.String("error", err.Error()))

		return
	}

	// todo: do sanity checks of value

	args := wrapper.PutArgs{}
	args.SetEpoch(epoch)
	args.SetPeerID(id)
	args.SetValue(value)

	var err error

	if rp.notaryDisabled {
		err = rp.reputationWrp.Put(args)
	} else {
		err = rp.reputationWrp.PutViaNotary(args)
	}

	if err != nil {
		rp.log.Warn("can't send approval tx for reputation value",
			zap.String("peer_id", hex.EncodeToString(id.ToV2().GetValue())),
			zap.String("error", err.Error()))
	}
}
