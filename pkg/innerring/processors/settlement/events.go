package settlement

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

// AuditEvent is an event of the start of
// cash settlements for data audit.
type AuditEvent struct {
	epoch uint64
}

type (
	BasicIncomeCollectEvent    = AuditEvent
	BasicIncomeDistributeEvent = AuditEvent
)

// MorphEvent implements Neo: FS chain event.
func (e AuditEvent) MorphEvent() {}

// NewAuditEvent creates new AuditEvent for epoch.
func NewAuditEvent(epoch uint64) event.Event {
	return AuditEvent{
		epoch: epoch,
	}
}

// Epoch returns the number of the epoch
// in which the event was generated.
func (e AuditEvent) Epoch() uint64 {
	return e.epoch
}

// NewBasicIncomeCollectEvent for epoch.
func NewBasicIncomeCollectEvent(epoch uint64) event.Event {
	return BasicIncomeCollectEvent{
		epoch: epoch,
	}
}

// NewBasicIncomeDistributeEvent for epoch.
func NewBasicIncomeDistributeEvent(epoch uint64) event.Event {
	return BasicIncomeDistributeEvent{
		epoch: epoch,
	}
}
