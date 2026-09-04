package writecache

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
)

// ErrReadOnly is returned when Put/Write is performed in a read-only mode.
var ErrReadOnly = logicerr.New("write-cache is in read-only mode")

// SetMode sets write-cache mode of operation.
// When shard is put in read-only mode all objects in memory are flushed to disk
// and all background jobs are suspended.
func (c *cache) SetMode(m mode.Mode) error {
	c.modeMtx.Lock()
	defer c.modeMtx.Unlock()

	if m.NoMetabase() && !c.mode.NoMetabase() {
		err := c.flush(true)
		if err != nil {
			return err
		}
	}

	if m.NoMetabase() {
		c.mode = m
		return nil
	}

	fsTree, err := c.newStore(m.ReadOnly())
	if err != nil {
		return err
	}

	oldFSTree := c.fsTree
	if oldFSTree != nil {
		if err := oldFSTree.Close(); err != nil {
			return fmt.Errorf("close previous FSTree: %w", err)
		}
	}
	if c.initialized {
		if err := fsTree.Init(c.shardID); err != nil {
			if oldFSTree != nil {
				if restoreErr := oldFSTree.Init(c.shardID); restoreErr != nil {
					return fmt.Errorf("init FSTree: %w; restore previous FSTree: %w", err, restoreErr)
				}
			}
			return fmt.Errorf("init FSTree: %w", err)
		}
	}
	c.fsTree = fsTree

	c.mode = m
	return nil
}

// readOnly returns true if current mode is read-only.
// `c.modeMtx` must be taken.
func (c *cache) readOnly() bool {
	return c.mode.ReadOnly()
}
