package balance

import (
	"encoding/hex"

	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	balanceEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/balance"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

func (bp *Processor) handleLock(ev event.Event) {
	lock := ev.(balanceEvent.Lock)
	bp.log.Info("notification",
		logger.FieldString("type", "lock"),
		logger.FieldString("value", hex.EncodeToString(lock.ID())),
	)

	// send an event to the worker pool

	err := bp.pool.Submit(func() { bp.processLock(&lock) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		bp.log.Warn("balance worker pool drained",
			logger.FieldInt("capacity", int64(bp.pool.Cap())),
		)
	}
}
