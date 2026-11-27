package netmapcore

// State groups the current system state parameters.
type State interface {
	// CurrentEpoch returns the number of the current NeoFS epoch.
	CurrentEpoch() uint64
}

// StateDetailed groups magic number, block, epoch and its duration information
// about FS chain.
type StateDetailed interface {
	State
	ChainMagicNumber() uint64
	CurrentBlock() uint32
	CurrentEpochDuration() uint64
}
