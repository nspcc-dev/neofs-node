package reputation

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"

	apireputation "github.com/nspcc-dev/neofs-api-go/pkg/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/reputation/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	"go.uber.org/zap"
)

var errWrongManager = errors.New("got manager that is incorrect for peer")

func (rp *Processor) processPut(epoch uint64, id apireputation.PeerID, value apireputation.GlobalTrust) {
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

	// check if manager is correct
	if err := rp.checkManagers(epoch, *value.Manager(), id); err != nil {
		rp.log.Info("ignore reputation value",
			zap.String("reason", "wrong manager"),
			zap.String("error", err.Error()))

		return
	}

	args := wrapper.PutArgs{}
	args.SetEpoch(epoch)
	args.SetPeerID(id)
	args.SetValue(value)

	err := rp.reputationWrp.Put(args)
	if err != nil {
		rp.log.Warn("can't send approval tx for reputation value",
			zap.String("peer_id", hex.EncodeToString(id.ToV2().GetPublicKey())),
			zap.String("error", err.Error()))
	}
}

func (rp *Processor) checkManagers(e uint64, mng apireputation.PeerID, peer apireputation.PeerID) error {
	mm, err := rp.mngBuilder.BuildManagers(e, reputation.PeerIDFromBytes(peer.ToV2().GetPublicKey()))
	if err != nil {
		return fmt.Errorf("could not build managers: %w", err)
	}

	for _, m := range mm {
		if bytes.Equal(mng.ToV2().GetPublicKey(), m.PublicKey()) {
			return nil
		}
	}

	return errWrongManager
}
