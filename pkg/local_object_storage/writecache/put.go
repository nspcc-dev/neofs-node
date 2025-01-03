package writecache

import (
	"errors"

	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

var (
	// ErrBigObject is returned when object is too big to be placed in cache.
	ErrBigObject = errors.New("too big object")
	// ErrOutOfSpace is returned when there is no space left to put a new object.
	ErrOutOfSpace = errors.New("no space left in the write cache")
)

// Put puts object to write-cache.
func (c *cache) Put(addr oid.Address, obj *objectSDK.Object, data []byte) error {
	c.modeMtx.RLock()
	defer c.modeMtx.RUnlock()
	if c.readOnly() {
		return ErrReadOnly
	}

	sz := uint64(len(data))
	if sz > c.maxObjectSize {
		return ErrBigObject
	}

	oi := objectInfo{
		addr: addr.EncodeToString(),
		obj:  obj,
		data: data,
	}

	if sz <= c.smallObjectSize {
		return c.putSmall(oi)
	}
	return c.putBig(addr, oi)
}

// putSmall persists small objects to the write-cache database and
// pushes the to the flush workers queue.
func (c *cache) putSmall(obj objectInfo) error {
	cacheSize := c.estimateCacheSize()
	if c.maxCacheSize < c.incSizeDB(cacheSize) {
		return ErrOutOfSpace
	}

	err := c.db.Batch(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		return b.Put([]byte(obj.addr), obj.data)
	})
	if err == nil {
		storagelog.Write(c.log,
			storagelog.AddressField(obj.addr),
			storagelog.StorageTypeField(wcStorageType),
			storagelog.OpField("db PUT"),
		)
		c.objCounters.IncDB()
	}
	return err
}

// putBig writes object to FSTree and pushes it to the flush workers queue.
func (c *cache) putBig(addr oid.Address, obj objectInfo) error {
	cacheSz := c.estimateCacheSize()
	if c.maxCacheSize < c.incSizeFS(cacheSz) {
		return ErrOutOfSpace
	}

	err := c.fsTree.Put(addr, obj.data)
	if err != nil {
		return err
	}

	if c.blobstor.NeedsCompression(obj.obj) {
		c.mtx.Lock()
		c.compressFlags[obj.addr] = struct{}{}
		c.mtx.Unlock()
	}
	c.objCounters.IncFS()
	storagelog.Write(c.log,
		storagelog.AddressField(obj.addr),
		storagelog.StorageTypeField(wcStorageType),
		storagelog.OpField("fstree PUT"),
	)

	return nil
}
