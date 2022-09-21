package shard

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
)

// ErrReadOnlyMode is returned when it is impossible to apply operation
// that changes shard's memory due to the "read-only" shard's mode.
var ErrReadOnlyMode = errors.New("shard is in read-only mode")

// ErrDegradedMode is returned when operation requiring metabase is executed in degraded mode.
var ErrDegradedMode = errors.New("shard is in degraded mode")

// SetMode sets mode of the shard.
//
// Returns any error encountered that did not allow
// setting shard mode.
func (s *Shard) SetMode(m mode.Mode) error {
	s.m.Lock()
	defer s.m.Unlock()

	components := []interface{ SetMode(mode.Mode) error }{
		s.metaBase, s.blobStor,
	}

	if s.hasWriteCache() {
		components = append(components, s.writeCache)
	}

	if s.pilorama != nil {
		components = append(components, s.pilorama)
	}

	// The usual flow of the requests (pilorama is independent):
	// writecache -> blobstor -> metabase
	// For mode.ReadOnly and mode.Degraded the order is:
	// writecache -> blobstor -> metabase
	// For mode.ReadWrite it is the opposite:
	// metabase -> blobstor -> writecache
	if m != mode.ReadWrite {
		if s.hasWriteCache() {
			components[0], components[2] = components[2], components[0]
		} else {
			components[0], components[1] = components[1], components[0]
		}
	}

	for i := range components {
		if err := components[i].SetMode(m); err != nil {
			return err
		}
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
