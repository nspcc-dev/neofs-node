package reputation

import (
	"encoding/hex"

	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	reputationEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

func (rp *Processor) handlePutReputation(ev event.Event) {
	put := ev.(reputationEvent.Put)
	peerID := put.PeerID()

	// FIXME: #1147 do not use `ToV2` method outside neofs-api-go library
	rp.log.Info("notification",
		logger.FieldString("type", "reputation put"),
		logger.FieldString("peer_id", hex.EncodeToString(peerID.PublicKey())))

	// send event to the worker pool

	err := rp.pool.Submit(func() { rp.processPut(&put) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		rp.log.Warn("reputation worker pool drained",
			logger.FieldInt("capacity", int64(rp.pool.Cap())),
		)
	}
}
