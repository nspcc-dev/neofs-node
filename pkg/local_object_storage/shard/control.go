package shard

import (
	"errors"
	"fmt"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"go.uber.org/zap"
)

func (s *Shard) handleMetabaseFailure(stage string, err error) error {
	s.log.Error("metabase failure, switching mode",
		zap.String("stage", stage),
		zap.Stringer("mode", mode.ReadOnly),
		zap.Error(err))

	err = s.SetMode(mode.ReadOnly)
	if err == nil {
		return nil
	}

	s.log.Error("can't move shard to readonly, switch mode",
		zap.String("stage", stage),
		zap.Stringer("mode", mode.DegradedReadOnly),
		zap.Error(err))

	err = s.SetMode(mode.DegradedReadOnly)
	if err != nil {
		return fmt.Errorf("could not switch to mode %s", mode.DegradedReadOnly)
	}
	return nil
}

// Open opens all Shard's components.
func (s *Shard) Open() error {
	components := []interface{ Open(bool) error }{
		s.blobStor, s.metaBase,
	}

	if s.hasWriteCache() {
		components = append(components, s.writeCache)
	}

	for i, component := range components {
		if err := component.Open(false); err != nil {
			if component == s.metaBase {
				// We must first open all other components to avoid
				// opening non-existent DB in read-only mode.
				for j := i + 1; j < len(components); j++ {
					if err := components[j].Open(false); err != nil {
						// Other components must be opened, fail.
						return fmt.Errorf("could not open %T: %w", components[j], err)
					}
				}
				err = s.handleMetabaseFailure("open", err)
				if err != nil {
					return err
				}

				break
			}

			return fmt.Errorf("could not open %T: %w", component, err)
		}
	}
	return nil
}

// Init initializes all Shard's components.
func (s *Shard) Init() error {
	if s.info.ID == nil {
		return fmt.Errorf("shard ID is not resolved")
	}

	if err := s.compression.Init(); err != nil {
		return fmt.Errorf("could not initialize %T: %w", &s.compression, err)
	}

	if err := s.blobStor.Init(s.info.ID); err != nil {
		return fmt.Errorf("could not initialize %T: %w", s.blobStor, err)
	}
	s.initedStorage = true

	if !s.GetMode().NoMetabase() {
		if err := s.metaBase.Init(s.info.ID); err != nil {
			if errors.Is(err, meta.ErrOutdatedVersion) {
				return fmt.Errorf("metabase initialization: %w", err)
			}

			err = s.handleMetabaseFailure("init", err)
			if err != nil {
				return err
			}
		}
	}

	if s.hasWriteCache() {
		if err := s.writeCache.Init(); err != nil {
			return fmt.Errorf("could not initialize %T: %w", s.writeCache, err)
		}
	}

	s.initMetrics()

	s.gc = &gc{
		gcCfg:       &s.gcCfg,
		remover:     s.removeGarbage,
		stopChannel: make(chan struct{}),
		eventChan:   make(chan Event, 1),
		mEventHandler: map[eventType]*eventHandler{
			eventNewEpoch: {handler: s.setEpochEventHandler},
		},
	}

	s.gc.init()

	return nil
}

// Close releases all Shard's components.
func (s *Shard) Close() error {
	components := []interface{ Close() error }{}

	if s.hasWriteCache() {
		components = append(components, s.writeCache)
	}

	components = append(components, s.blobStor, &s.compression, s.metaBase)

	var lastErr error
	for _, component := range components {
		if err := component.Close(); err != nil {
			lastErr = err
			s.log.Error("could not close shard component", zap.Error(err))
		}
	}

	// If Init/Open was unsuccessful gc can be nil.
	if s.gc != nil {
		s.gc.stop()
	}

	return lastErr
}

// Reload reloads configuration portions that are necessary.
// If a config option is invalid, it logs an error and returns nil.
// If there was a problem with applying new configuration, an error is returned.
func (s *Shard) Reload(opts ...Option) error {
	// Do not use defaultCfg here missing options need not be reloaded.
	var c cfg
	for i := range opts {
		opts[i](&c)
	}

	s.m.Lock()
	defer s.m.Unlock()

	ok, err := s.metaBase.Reload(c.metaOpts...)
	if err != nil {
		if errors.Is(err, meta.ErrDegradedMode) {
			s.log.Error("can't open metabase, move to a degraded mode", zap.Error(err))
			_ = s.setMode(mode.DegradedReadOnly)
		}
		return err
	}
	if ok {
		err = s.metaBase.Init(s.info.ID)
		if err != nil {
			s.log.Error("can't initialize metabase, move to a degraded-read-only mode", zap.Error(err))
			_ = s.setMode(mode.DegradedReadOnly)
			return err
		}
	}

	s.log.Info("trying to set mode", zap.Stringer("mode", c.info.Mode))
	return s.setMode(c.info.Mode)
}
