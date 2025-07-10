package settlement

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

// BasicIncomeEvent is an event of the beginning of basic income cash settlements.
type BasicIncomeEvent struct {
	epoch uint64
}

// MorphEvent implements Neo: FS chain event.
func (e BasicIncomeEvent) MorphEvent() {}

// Epoch returns the number of the epoch
// in which the event was generated.
func (e BasicIncomeEvent) Epoch() uint64 {
	return e.epoch
}

// NewBasicIncomeEvent for epoch.
func NewBasicIncomeEvent(epoch uint64) event.Event {
	return BasicIncomeEvent{
		epoch: epoch,
	}
}
