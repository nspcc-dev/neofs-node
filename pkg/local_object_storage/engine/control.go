package engine

import (
	"fmt"

	"go.uber.org/zap"
)

// Open opens all StorageEngine's components.
func (e *StorageEngine) Open() error {
	e.mtx.RLock()
	defer e.mtx.RUnlock()

	for id, sh := range e.shards {
		if err := sh.Open(); err != nil {
			return fmt.Errorf("could not open shard %s: %w", id, err)
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
			return fmt.Errorf("could not initialize shard %s: %w", id, err)
		}
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
