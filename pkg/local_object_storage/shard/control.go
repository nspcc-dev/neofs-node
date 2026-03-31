package shard

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
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
	if err := s.blobStor.Open(false); err != nil {
		return fmt.Errorf("could not open %T: %w", s.blobStor, err)
	}

	metaErr := s.metaBase.Open(false)

	if s.hasWriteCache() {
		if err := s.writeCache.Open(false); err != nil {
			return fmt.Errorf("could not open %T: %w", s.writeCache, err)
		}
	}

	if metaErr == nil {
		return nil
	}

	if !s.initedStorage {
		s.metaBaseOpenErr = metaErr
		return nil
	}
	return s.handleMetabaseFailure("open", metaErr)
}

// Init initializes all Shard's components.
func (s *Shard) Init() error {
	if err := s.compression.Init(); err != nil {
		return fmt.Errorf("could not initialize %T: %w", &s.compression, err)
	}

	if err := s.blobStor.Init(common.ID{}); err != nil {
		return fmt.Errorf("could not initialize %T: %w", s.blobStor, err)
	}
	s.initedStorage = true

	shardID := s.blobStor.ShardID()
	if shardID.IsZero() {
		return fmt.Errorf("%T returned empty shard ID after init", s.blobStor)
	}

	s.info.ID = shardID
	if s.metricsWriter != nil {
		s.metricsWriter.SetShardID(shardID.String())
	}
	l := s.log.With(zap.String("shard_id", shardID.String()))
	s.log = l
	s.gcCfg.log = s.gcCfg.log.With(zap.String("shard_id", shardID.String()))

	if s.hasWriteCache() {
		if err := s.writeCache.Init(shardID); err != nil {
			return fmt.Errorf("could not initialize %T: %w", s.writeCache, err)
		}
	}

	if s.metaBaseOpenErr != nil {
		err := s.handleMetabaseFailure("open", s.metaBaseOpenErr)
		if err != nil {
			return err
		}
		s.metaBaseOpenErr = nil
	}

	if !s.GetMode().NoMetabase() {
		if err := s.metaBase.Init(shardID); err != nil {
			if errors.Is(err, meta.ErrOutdatedVersion) {
				return fmt.Errorf("metabase initialization: %w", err)
			}

			err = s.handleMetabaseFailure("init", err)
			if err != nil {
				return err
			}
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
		err = s.metaBase.Init(s.ID())
		if err != nil {
			s.log.Error("can't initialize metabase, move to a degraded-read-only mode", zap.Error(err))
			_ = s.setMode(mode.DegradedReadOnly)
			return err
		}
	}

	s.log.Info("trying to set mode", zap.Stringer("mode", c.info.Mode))
	return s.setMode(c.info.Mode)
}
