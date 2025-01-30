package writecache

import (
	"errors"
	"fmt"
	"os"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// store represents persistent storage with in-memory LRU cache
// for flushed items on top of it.
type store struct {
	maxFlushedMarksCount int
	maxRemoveBatchSize   int

	// flushed contains addresses of objects that were already flushed to the main storage.
	// We use LRU cache instead of map here to facilitate removing of unused object in favour of
	// frequently read ones.
	// MUST NOT be used inside bolt db transaction because it's eviction handler
	// removes untracked items from the database.
	flushed simplelru.LRUCache[string, bool]

	fsKeysToRemove []string
}

const dbName = "small.bolt"

func (c *cache) openStore(readOnly bool) error {
	err := util.MkdirAllX(c.path, os.ModePerm)
	if err != nil {
		return err
	}

	c.fsTree = fstree.New(
		fstree.WithPath(c.path),
		fstree.WithPerm(os.ModePerm),
		fstree.WithDepth(1),
		fstree.WithDirNameLen(1),
		fstree.WithNoSync(c.noSync))
	if err := c.fsTree.Open(readOnly); err != nil {
		return fmt.Errorf("could not open FSTree: %w", err)
	}

	// Write-cache can be opened multiple times during `SetMode`.
	// flushed map must not be re-created in this case.
	if c.flushed == nil {
		c.flushed, _ = lru.NewWithEvict[string, bool](c.maxFlushedMarksCount, c.removeFlushed)
	}
	return nil
}

// removeFlushed removes an object from the writecache.
// To minimize interference with the client operations, the actual removal
// is done in batches.
// It is not thread-safe and is used only as an evict callback to LRU cache.
func (c *cache) removeFlushed(addr string, _ bool) {
	c.fsKeysToRemove = append(c.fsKeysToRemove, addr)

	if len(c.fsKeysToRemove) >= c.maxRemoveBatchSize {
		c.fsKeysToRemove = c.deleteFromDisk(c.fsKeysToRemove)
	}
}

func (c *cache) deleteFromDisk(keys []string) []string {
	if len(keys) == 0 {
		return keys
	}

	var copyIndex int
	var addr oid.Address

	for i := range keys {
		if err := addr.DecodeString(keys[i]); err != nil {
			c.log.Error("can't parse address", zap.String("address", keys[i]))
			continue
		}

		err := c.fsTree.Delete(addr)
		if err != nil && !errors.As(err, new(apistatus.ObjectNotFound)) {
			c.log.Error("can't remove object from write-cache", zap.Error(err))

			// Save the key for the next iteration.
			keys[copyIndex] = keys[i]
			copyIndex++
			continue
		} else if err == nil {
			storagelog.Write(c.log,
				storagelog.AddressField(keys[i]),
				storagelog.StorageTypeField(wcStorageType),
				storagelog.OpField("DELETE"),
			)
			c.objCounters.DecFS()
		}
	}

	return keys[:copyIndex]
}
