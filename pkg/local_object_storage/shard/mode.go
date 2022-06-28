package shard

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
)

// ErrReadOnlyMode is returned when it is impossible to apply operation
// that changes shard's memory due to the "read-only" shard's mode.
var ErrReadOnlyMode = errors.New("shard is in read-only mode")

// SetMode sets mode of the shard.
//
// Returns any error encountered that did not allow
// setting shard mode.
func (s *Shard) SetMode(m mode.Mode) error {
	s.m.Lock()
	defer s.m.Unlock()

	if s.hasWriteCache() {
		s.writeCache.SetMode(m)
	}

	s.info.Mode = m

	return nil
}

// GetMode returns mode of the shard.
func (s *Shard) GetMode() mode.Mode {
	s.m.RLock()
	defer s.m.RUnlock()

	return s.info.Mode
}
