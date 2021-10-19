package writecache

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"go.etcd.io/bbolt"
	"go.uber.org/atomic"
)

// ObjectCounters is an interface of the storage of cached object amount.
type ObjectCounters interface {
	// Increments number of objects saved in DB.
	IncDB()
	// Decrements number of objects saved in DB.
	DecDB()
	// Returns number of objects saved in DB.
	DB() uint64

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
	return c.objCounters.DB()*c.smallObjectSize + c.objCounters.FS()*c.maxObjectSize
}

func (c *cache) incSizeDB(sz uint64) uint64 {
	return sz + c.smallObjectSize
}

func (c *cache) incSizeFS(sz uint64) uint64 {
	return sz + c.maxObjectSize
}

type counters struct {
	cDB, cFS atomic.Uint64

	db *bbolt.DB

	fs *fstree.FSTree
}

func (x *counters) IncDB() {
	x.cDB.Inc()
}

func (x *counters) DecDB() {
	x.cDB.Dec()
}

func (x *counters) DB() uint64 {
	return x.cDB.Load()
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
	var inDB uint64

	err := x.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		if b != nil {
			inDB = uint64(b.Stats().KeyN)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("could not read write-cache DB counter: %w", err)
	}

	x.cDB.Store(inDB)

	inFS, err := x.fs.NumberOfObjects()
	if err != nil {
		return fmt.Errorf("could not read write-cache FS counter: %w", err)
	}

	x.cFS.Store(inFS)

	return nil
}

func (x *counters) FlushAndClose() {
	// values aren't stored
}
