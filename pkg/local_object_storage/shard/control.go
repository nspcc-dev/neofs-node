package shard

import (
	"github.com/pkg/errors"
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
			return errors.Wrapf(err, "could not open %T", component)
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
			return errors.Wrapf(err, "could not initialize %T", component)
		}
	}

	gc := &gc{
		gcCfg:   s.gcCfg,
		remover: s.removeGarbage,
		mEventHandler: map[eventType]*eventHandlers{
			eventNewEpoch: {
				cancelFunc: func() {},
				handlers: []eventHandler{
					s.collectExpiredObjects,
				},
			},
		},
	}

	gc.init()

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
			return errors.Wrapf(err, "could not close %s", component)
		}
	}

	return nil
}
