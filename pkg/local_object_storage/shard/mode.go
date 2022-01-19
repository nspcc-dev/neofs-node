package shard

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
)

// Mode represents enumeration of Shard work modes.
type Mode uint32

// ErrReadOnlyMode is returned when it is impossible to apply operation
// that changes shard's memory due to the "read-only" shard's mode.
var ErrReadOnlyMode = errors.New("shard is in read-only mode")

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
	s.m.Lock()
	defer s.m.Unlock()

	if s.hasWriteCache() {
		switch m {
		case ModeReadOnly:
			s.writeCache.SetMode(writecache.ModeReadOnly)
		case ModeReadWrite:
			s.writeCache.SetMode(writecache.ModeReadWrite)
		}
	}

	s.info.Mode = m

	return nil
}

// GetMode returns mode of the shard.
func (s *Shard) GetMode() Mode {
	s.m.RLock()
	defer s.m.RUnlock()

	return s.info.Mode
}
