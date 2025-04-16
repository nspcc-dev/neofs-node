package mode

import "math"

// Mode represents enumeration of Shard work modes.
type Mode uint32

const (
	// ReadWrite is a Mode value for shard that is available
	// for read and write operations. Default shard mode.
	ReadWrite Mode = 0

	// DegradedReadOnly is a Mode value for shard that is set automatically
	// after a certain number of errors is encountered. It is the same as
	// `mode.Degraded` but also is read-only.
	DegradedReadOnly = Degraded | ReadOnly

	// Disabled mode is a mode where a shard is disabled.
	// An existing shard can't have this mode, but it can be used in
	// the configuration or control service commands.
	Disabled = math.MaxUint32
)

const (
	// ReadOnly is a Mode value for shard that does not
	// accept write operation but is readable.
	ReadOnly Mode = 1 << iota

	// Degraded is a Mode value for shard when the metabase is unavailable.
	// It is hard to perform some modifying operations in this mode, thus it can only be set by an administrator.
	Degraded
)

func (m Mode) String() string {
	switch m {
	default:
		return "UNDEFINED"
	case ReadWrite:
		return "READ_WRITE"
	case ReadOnly:
		return "READ_ONLY"
	case Degraded:
		return "DEGRADED_READ_WRITE"
	case DegradedReadOnly:
		return "DEGRADED_READ_ONLY"
	case Disabled:
		return "DISABLED"
	}
}

// NoMetabase returns true iff m is operating without the metabase.
func (m Mode) NoMetabase() bool {
	return m&Degraded != 0
}

// ReadOnly returns true iff m prohibits modifying operations with shard.
func (m Mode) ReadOnly() bool {
	return m&ReadOnly != 0
}

// IsValid returns true iff m is a valid mode.
func (m Mode) IsValid() bool {
	switch m {
	case ReadWrite, ReadOnly, Degraded, DegradedReadOnly, Disabled:
		return true
	default:
		return false
	}
}
