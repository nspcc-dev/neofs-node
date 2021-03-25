package governance

// Sync is a event to start governance synchronization.
type Sync struct{}

// MorphEvent implements Event interface.
func (s Sync) MorphEvent() {}

func NewSyncEvent() Sync {
	return Sync{}
}
