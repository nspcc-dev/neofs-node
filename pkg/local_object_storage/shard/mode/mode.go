package mode

// Mode represents enumeration of Shard work modes.
type Mode uint32

const (
	// ReadWrite is a Mode value for shard that is available
	// for read and write operations. Default shard mode.
	ReadWrite Mode = iota

	// ReadOnly is a Mode value for shard that does not
	// accept write operation but is readable.
	ReadOnly

	// Degraded is a Mode value for shard that is set automatically
	// after a certain number of errors is encountered. It is the same as
	// `mode.ReadOnly` but also enables fallback algorithms for getting object
	// in case metabase is corrupted.
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
		return "DEGRADED"
	}
}
