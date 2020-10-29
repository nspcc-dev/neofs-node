package netmap

// netmapCleanupTick is a event to remove offline nodes.
type netmapCleanupTick struct {
	epoch uint64
}

// MorphEvent implements Event interface.
func (netmapCleanupTick) MorphEvent() {}
