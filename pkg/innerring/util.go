package innerring

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/timers"
)

type blockTimerWrapper timers.BlockTimer

func (t *blockTimerWrapper) ResetEpochTimer() error {
	return (*timers.BlockTimer)(t).Reset()
}
