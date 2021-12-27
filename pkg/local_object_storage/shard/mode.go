package shard

import "errors"

// Mode represents enumeration of Shard work modes.
type Mode uint32

// ErrReadOnlyMode is returned when it is impossible to apply operation
// that changes shard's memory due to the "read-only" shard's mode.
var ErrReadOnlyMode = errors.New("shard is in read-only mode")

// TODO: more detailed description of shard modes.

const (
	// ModeReadWrite is a Mode value for shard that is available
	// for read and write operations. Default shard mode.
	ModeReadWrite Mode = iota

	// ModeReadOnly is a Mode value for shard that does not
	// accept write operation but is readable.
	ModeReadOnly
)

func (m Mode) String() string {
	switch m {
	default:
		return "UNDEFINED"
	case ModeReadWrite:
		return "READ_WRITE"
	case ModeReadOnly:
		return "READ_ONLY"
	}
}

// SetMode sets mode of the shard.
//
// Returns any error encountered that did not allow
// setting shard mode.
func (s *Shard) SetMode(m Mode) error {
	s.mode.Store(uint32(m))

	return nil
}

func (s *Shard) getMode() Mode {
	return Mode(s.mode.Load())
}
