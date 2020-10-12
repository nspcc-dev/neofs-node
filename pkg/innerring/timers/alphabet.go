package timers

// NewAlphabetEmitTick is a event for gas emission from alphabet contract.
type NewAlphabetEmitTick struct{}

// MorphEvent implements Event interface.
func (NewAlphabetEmitTick) MorphEvent() {}
