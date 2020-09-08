package balance

import (
	"encoding/hex"

	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	balanceEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/balance"
	"go.uber.org/zap"
)

func (bp *Processor) handleLock(ev event.Event) {
	lock := ev.(balanceEvent.Lock)
	bp.log.Info("notification",
		zap.String("type", "lock"),
		zap.String("value", hex.EncodeToString(lock.ID())))

	// send event to the worker pool

	err := bp.pool.Submit(func() { bp.processLock(&lock) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		bp.log.Warn("balance worker pool drained",
			zap.Int("capacity", bp.pool.Cap()))
	}
}
