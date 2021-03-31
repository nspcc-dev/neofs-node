package reputation

import (
	"encoding/hex"

	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	reputationEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/reputation"
	"go.uber.org/zap"
)

func (rp *Processor) handlePutReputation(ev event.Event) {
	put := ev.(reputationEvent.Put)
	rp.log.Info("notification",
		zap.String("type", "reputation put"),
		zap.String("peer_id", hex.EncodeToString(put.PeerID().Bytes())))

	// send event to the worker pool

	err := rp.pool.Submit(func() {
		rp.processPut(
			put.Epoch(),
			put.PeerID(),
			put.Value(),
		)
	})
	if err != nil {
		// there system can be moved into controlled degradation stage
		rp.log.Warn("reputation worker pool drained",
			zap.Int("capacity", rp.pool.Cap()))
	}
}
