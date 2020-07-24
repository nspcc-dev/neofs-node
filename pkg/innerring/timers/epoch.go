package timers

// NewEpochTick is a new epoch local ticker event.
type NewEpochTick struct{}

// MorphEvent implements Event interface.
func (NewEpochTick) MorphEvent() {}

// ResetEpochTimer to start it again when event has been processed.
func (t *Timers) ResetEpochTimer() {
	t.epoch.timer.Reset(t.epoch.duration)
}
