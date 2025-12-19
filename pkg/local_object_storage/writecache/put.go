package writecache

import (
	"errors"

	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

var (
	// ErrOutOfSpace is returned when there is no space left to put a new object.
	ErrOutOfSpace = errors.New("no space left in the write cache")
)

// Put puts object to write-cache.
func (c *cache) Put(addr oid.Address, obj *object.Object, data []byte) error {
	c.modeMtx.RLock()
	defer c.modeMtx.RUnlock()
	if c.readOnly() {
		return ErrReadOnly
	}

	if c.metrics.mr != nil {
		defer elapsed(c.metrics.AddWCPutDuration)()
	}

	oi := objectInfo{
		addr: addr.EncodeToString(),
		obj:  obj,
		data: data,
	}

	return c.put(addr, oi)
}

// put writes object to FSTree and pushes it to the flush workers queue.
func (c *cache) put(addr oid.Address, obj objectInfo) error {
	cacheSz := c.objCounters.Size()
	objSz := uint64(len(obj.data))
	if c.maxCacheSize < cacheSz+objSz {
		return ErrOutOfSpace
	}

	err := c.fsTree.Put(addr, obj.data)
	if err != nil {
		return err
	}

	c.objCounters.Add(addr, objSz)
	c.metrics.IncWCObjectCount()
	c.metrics.AddWCSize(objSz)
	storagelog.Write(c.log,
		storagelog.AddressField(obj.addr),
		storagelog.StorageTypeField(wcStorageType),
		storagelog.OpField("PUT"),
	)

	return nil
}
