package settlement

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

// AuditEvent is an event of the start of
// cash settlements for data audit.
type AuditEvent struct {
	epoch uint64
}

type BasicIncomeEvent = AuditEvent

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

// NewBasicIncomeEvent for epoch.
func NewBasicIncomeEvent(epoch uint64) event.Event {
	return BasicIncomeEvent{
		epoch: epoch,
	}
}
