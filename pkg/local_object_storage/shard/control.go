package shard

import (
	"fmt"
)

// Open opens all Shard's components.
func (s *Shard) Open() error {
	components := []interface{ Open() error }{
		s.blobStor, s.metaBase,
	}

	if s.hasWriteCache() {
		components = append(components, s.writeCache)
	}

	for _, component := range components {
		if err := component.Open(); err != nil {
			return fmt.Errorf("could not open %T: %w", component, err)
		}
	}

	return nil
}

// Init initializes all Shard's components.
func (s *Shard) Init() error {
	components := []interface{ Init() error }{
		s.blobStor, s.metaBase,
	}

	if s.hasWriteCache() {
		components = append(components, s.writeCache)
	}

	for _, component := range components {
		if err := component.Init(); err != nil {
			return fmt.Errorf("could not initialize %T: %w", component, err)
		}
	}

	s.gc = &gc{
		gcCfg:       s.gcCfg,
		remover:     s.removeGarbage,
		stopChannel: make(chan struct{}),
		mEventHandler: map[eventType]*eventHandlers{
			eventNewEpoch: {
				cancelFunc: func() {},
				handlers: []eventHandler{
					s.collectExpiredObjects,
					s.collectExpiredTombstones,
				},
			},
		},
	}

	s.gc.init()

	return nil
}

// Close releases all Shard's components.
func (s *Shard) Close() error {
	components := []interface{ Close() error }{
		s.blobStor, s.metaBase,
	}

	if s.hasWriteCache() {
		components = append(components, s.writeCache)
	}

	for _, component := range components {
		if err := component.Close(); err != nil {
			return fmt.Errorf("could not close %s: %w", component, err)
		}
	}

	s.gc.stop()

	return nil
}
