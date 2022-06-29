package mode

// Mode represents enumeration of Shard work modes.
type Mode uint32

// ReadWrite is a Mode value for shard that is available
// for read and write operations. Default shard mode.
const ReadWrite Mode = 0

const (
	// ReadOnly is a Mode value for shard that does not
	// accept write operation but is readable.
	ReadOnly Mode = 1 << iota

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

// NoMetabase returns true iff m is operating without the metabase.
func (m Mode) NoMetabase() bool {
	return m&Degraded != 0
}

// ReadOnly returns true iff m prohibits modifying operations with shard.
func (m Mode) ReadOnly() bool {
	return m&ReadOnly != 0
}
