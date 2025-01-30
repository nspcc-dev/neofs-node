package writecache

import (
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Delete removes object from write-cache.
//
// Returns an error of type apistatus.ObjectNotFound if object is missing in write-cache.
func (c *cache) Delete(addr oid.Address) error {
	c.modeMtx.RLock()
	defer c.modeMtx.RUnlock()
	if c.readOnly() {
		return ErrReadOnly
	}

	saddr := addr.EncodeToString()

	err := c.fsTree.Delete(addr)
	if err == nil {
		storagelog.Write(c.log,
			storagelog.AddressField(saddr),
			storagelog.StorageTypeField(wcStorageType),
			storagelog.OpField("DELETE"),
		)
		c.objCounters.DecFS()
	}

	return err
}
