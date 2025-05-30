package shard

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"go.uber.org/zap"
)

// ErrReadOnlyMode is returned when it is impossible to apply operation
// that changes shard's memory due to the "read-only" shard's mode.
var ErrReadOnlyMode = logicerr.New("shard is in read-only mode")

// ErrDegradedMode is returned when operation requiring metabase is executed in degraded mode.
var ErrDegradedMode = logicerr.New("shard is in degraded mode")

// SetMode sets mode of the shard.
//
// Returns any error encountered that did not allow
// setting shard mode.
func (s *Shard) SetMode(m mode.Mode) error {
	s.m.Lock()
	defer s.m.Unlock()

	return s.setMode(m)
}

func (s *Shard) setMode(m mode.Mode) error {
	s.log.Info("setting shard mode",
		zap.Stringer("old_mode", s.info.Mode),
		zap.Stringer("new_mode", m))

	components := []func(mode.Mode) error{
		s.metaBase.SetMode, s.setModeStorage,
	}

	if s.hasWriteCache() {
		components = append(components, s.writeCache.SetMode)
	}

	// The usual flow of the requests:
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
		if err := components[i](m); err != nil {
			return err
		}
	}

	s.info.Mode = m
	if s.metricsWriter != nil {
		s.metricsWriter.SetReadonly(s.info.Mode != mode.ReadWrite)
	}

	s.log.Info("shard mode set successfully",
		zap.Stringer("mode", s.info.Mode))
	return nil
}

// GetMode returns mode of the shard.
func (s *Shard) GetMode() mode.Mode {
	s.m.RLock()
	defer s.m.RUnlock()

	return s.info.Mode
}

func (s *Shard) setModeStorage(m mode.Mode) error {
	if s.info.Mode == m {
		return nil
	}

	err := s.blobStor.Close()
	if err == nil {
		if err = s.blobStor.Open(m.ReadOnly()); err == nil && s.initedStorage {
			err = s.blobStor.Init()
		}
	}
	if err != nil {
		return fmt.Errorf("can't set storage mode (old=%s, new=%s): %w", s.info.Mode, m, err)
	}

	return nil
}
