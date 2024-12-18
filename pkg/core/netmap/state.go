package netmap

// State groups the current system state parameters.
type State interface {
	// CurrentEpoch returns the number of the current NeoFS epoch.
	CurrentEpoch() uint64
}

// StateDetailed groups block, epoch and its duration information about FS chain.
type StateDetailed interface {
	State
	CurrentBlock() uint32
	CurrentEpochDuration() uint64
}
