package writecache

import (
	"errors"
	"fmt"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
)

// ErrReadOnly is returned when Put/Write is performed in a read-only mode.
var ErrReadOnly = errors.New("write-cache is in read-only mode")

// SetMode sets write-cache mode of operation.
// When shard is put in read-only mode all objects in memory are flushed to disk
// and all background jobs are suspended.
func (c *cache) SetMode(m mode.Mode) error {
	c.modeMtx.Lock()
	defer c.modeMtx.Unlock()

	if m.ReadOnly() == c.readOnly() {
		c.mode = m
		return nil
	}

	if c.db != nil {
		if err := c.db.Close(); err != nil {
			return fmt.Errorf("can't close write-cache database: %w", err)
		}
		c.db = nil
	}

	// Suspend producers to ensure there are channel send operations in fly.
	// flushCh is populated by `flush` with `modeMtx` taken, thus waiting until it is empty
	// guarantees that there are no in-fly operations.
	for len(c.flushCh) != 0 {
		c.log.Info("waiting for channels to flush")
		time.Sleep(time.Second)
	}

	if err := c.openStore(m.ReadOnly()); err != nil {
		return err
	}

	c.mode = m
	return nil
}

// readOnly returns true if current mode is read-only.
// `c.modeMtx` must be taken.
func (c *cache) readOnly() bool {
	return c.mode.ReadOnly()
}
