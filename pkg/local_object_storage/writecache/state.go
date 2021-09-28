package writecache

import (
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"go.uber.org/atomic"
)

// ObjectCounters is an interface of the storage of cached object amount.
type ObjectCounters interface {
	// Increments number of objects saved in FSTree.
	IncFS()
	// Decrements number of objects saved in FSTree.
	DecFS()
	// Returns number of objects saved in FSTree.
	FS() uint64

	// Reads number of objects saved in write-cache. It is called on write-cache initialization step.
	Read() error
	// Flushes the values and closes the storage. It is called on write-cache shutdown.
	FlushAndClose()
}

func (c *cache) estimateCacheSize() uint64 {
	return c.db.Metrics().DiskSpaceUsage() + c.objCounters.FS()*c.maxObjectSize
}

func (c *cache) incSizeFS(sz uint64) uint64 {
	return sz + c.maxObjectSize
}

type counters struct {
	cFS atomic.Uint64

	maxObjectSize uint64

	db *pebble.DB

	fs *fstree.FSTree
}

func (x *counters) IncFS() {
	x.cFS.Inc()
}

func (x *counters) DecFS() {
	x.cFS.Dec()
}

func (x *counters) FS() uint64 {
	return x.cFS.Load()
}

func (x *counters) Read() error {
	var size uint64
	err := filepath.Walk(x.fs.RootPath, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		size += uint64(info.Size())
		return err
	})

	if err != nil {
		return err
	}

	// To avoid traversing filesystem we maintain only a number of keys
	// stored in the FS. Here a pessimistic initial estimation is done.
	x.cFS.Store(size/x.maxObjectSize + 1)
	return nil
}

func (x *counters) FlushAndClose() {
	// values aren't stored
}
