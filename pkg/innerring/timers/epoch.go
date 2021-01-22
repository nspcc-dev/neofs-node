package timers

// NewEpochTick is a new epoch local ticker event.
type NewEpochTick struct{}

// MorphEvent implements Event interface.
func (NewEpochTick) MorphEvent() {}
