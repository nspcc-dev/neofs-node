package netmap

// State groups current system state parameters.
type State interface {
	// CurrentEpoch returns number of current NeoFS epoch.
	CurrentEpoch() uint64
}
