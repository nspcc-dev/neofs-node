package engine

import (
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Open opens all StorageEngine's components.
func (e *StorageEngine) Open() error {
	e.mtx.RLock()
	defer e.mtx.RUnlock()

	for id, sh := range e.shards {
		if err := sh.Open(); err != nil {
			return errors.Wrapf(err, "could not open shard %s", id)
		}
	}

	return nil
}

// Init initializes all StorageEngine's components.
func (e *StorageEngine) Init() error {
	e.mtx.RLock()
	defer e.mtx.RUnlock()

	for id, sh := range e.shards {
		if err := sh.Init(); err != nil {
			return errors.Wrapf(err, "could not initialize shard %s", id)
		}
	}

	if e.enableMetrics {
		registerMetrics()
	}

	return nil
}

// Close releases all StorageEngine's components.
func (e *StorageEngine) Close() error {
	e.mtx.RLock()
	defer e.mtx.RUnlock()

	for id, sh := range e.shards {
		if err := sh.Close(); err != nil {
			e.log.Debug("could not close shard",
				zap.String("id", id),
				zap.String("error", err.Error()),
			)
		}
	}

	return nil
}
