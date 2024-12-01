package writecache

import (
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
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

	// Check disk cache.
	var has int
	_ = c.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		has = len(b.Get([]byte(saddr)))
		return nil
	})

	if 0 < has {
		err := c.db.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket(defaultBucket)
			err := b.Delete([]byte(saddr))
			return err
		})
		if err != nil {
			return err
		}
		storagelog.Write(c.log,
			storagelog.AddressField(saddr),
			storagelog.StorageTypeField(wcStorageType),
			storagelog.OpField("db DELETE"),
		)
		c.objCounters.DecDB()
		return nil
	}

	err := c.fsTree.Delete(addr)
	if err == nil {
		storagelog.Write(c.log,
			storagelog.AddressField(saddr),
			storagelog.StorageTypeField(wcStorageType),
			storagelog.OpField("fstree DELETE"),
		)
		c.objCounters.DecFS()
	}

	return err
}
