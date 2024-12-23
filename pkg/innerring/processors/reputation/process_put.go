package reputation

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"

	reputationEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/reputation"
	apireputation "github.com/nspcc-dev/neofs-sdk-go/reputation"
	"go.uber.org/zap"
)

var errWrongManager = errors.New("got manager that is incorrect for peer")

func (rp *Processor) processPut(e *reputationEvent.Put) {
	if !rp.alphabetState.IsAlphabet() {
		rp.log.Info("non alphabet mode, ignore reputation put notification")
		return
	}

	epoch := e.Epoch()
	id := e.PeerID()
	value := e.Value()

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
	if !value.VerifySignature() {
		rp.log.Info("ignore reputation value",
			zap.String("reason", "invalid signature"),
		)

		return
	}

	// check if manager is correct
	if err := rp.checkManagers(epoch, value.Manager(), id); err != nil {
		rp.log.Info("ignore reputation value",
			zap.String("reason", "wrong manager"),
			zap.Error(err))

		return
	}

	rp.approvePutReputation(e)
}

func (rp *Processor) checkManagers(e uint64, mng apireputation.PeerID, peer apireputation.PeerID) error {
	mm, err := rp.mngBuilder.BuildManagers(e, peer)
	if err != nil {
		return fmt.Errorf("could not build managers: %w", err)
	}

	for _, m := range mm {
		if bytes.Equal(mng.PublicKey(), m.PublicKey()) {
			return nil
		}
	}

	return errWrongManager
}

func (rp *Processor) approvePutReputation(e *reputationEvent.Put) {
	var (
		id  = e.PeerID()
		err error
		nr  = e.NotaryRequest()
	)

	err = rp.reputationWrp.Morph().NotarySignAndInvokeTX(nr.MainTransaction, false)

	if err != nil {
		rp.log.Warn("can't send approval tx for reputation value",
			zap.String("peer_id", hex.EncodeToString(id.PublicKey())),
			zap.Error(err))
	}
}
