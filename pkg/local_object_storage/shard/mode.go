package shard

// Mode represents enumeration of Shard work modes.
type Mode uint32

// TODO: more detailed description of shard modes.

const (
	_ Mode = iota

	// ModeActive is a Mode value for active shard.
	ModeActive

	// ModeInactive is a Mode value for inactive shard.
	ModeInactive

	// ModeReadOnly is a Mode value for read-only shard.
	ModeReadOnly

	// ModeFault is a Mode value for faulty shard.
	ModeFault

	// ModeEvacuate is a Mode value for evacuating shard.
	ModeEvacuate
)

func (m Mode) String() string {
	switch m {
	default:
		return "UNDEFINED"
	case ModeActive:
		return "ACTIVE"
	case ModeInactive:
		return "INACTIVE"
	case ModeReadOnly:
		return "READ_ONLY"
	case ModeFault:
		return "FAULT"
	case ModeEvacuate:
		return "EVACUATE"
	}
}

// SetMode sets mode of the shard.
//
// Returns any error encountered that did not allow
// to set shard mode.
func (s *Shard) SetMode(m Mode) error {
	s.mode.Store(uint32(m))
}

func (s *Shard) getMode() Mode {
	return Mode(s.mode.Load())
}
