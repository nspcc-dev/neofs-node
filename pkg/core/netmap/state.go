package netmap

// State groups the current system state parameters.
type State interface {
	// CurrentEpoch returns the number of the current NeoFS epoch.
	CurrentEpoch() uint64
}
