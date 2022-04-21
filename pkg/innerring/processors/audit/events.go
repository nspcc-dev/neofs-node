package audit

// Start is an event to start a new round of data audit.
type Start struct {
	epoch uint64
}

// MorphEvent implements the Event interface.
func (a Start) MorphEvent() {}

func NewAuditStartEvent(epoch uint64) Start {
	return Start{
		epoch: epoch,
	}
}

func (a Start) Epoch() uint64 {
	return a.epoch
}
