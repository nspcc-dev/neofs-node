package writecache

import (
	"errors"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	"go.etcd.io/bbolt"
)

// Delete removes object from write-cache.
func (c *cache) Delete(addr *objectSDK.Address) error {
	saddr := addr.String()

	// Check memory cache.
	c.mtx.Lock()
	for i := range c.mem {
		if saddr == c.mem[i].addr {
			copy(c.mem[i:], c.mem[i+1:])
			c.mem = c.mem[:len(c.mem)-1]
			c.curMemSize -= uint64(len(c.mem[i].data))
			c.mtx.Unlock()
			storagelog.Write(c.log, storagelog.AddressField(saddr), storagelog.OpField("in-mem DELETE"))
			return nil
		}
	}
	c.mtx.Unlock()

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
		c.dbSize.Sub(uint64(has))
		storagelog.Write(c.log, storagelog.AddressField(saddr), storagelog.OpField("db DELETE"))
		c.objCounters.DecDB()
		return nil
	}

	err := c.fsTree.Delete(addr)
	if errors.Is(err, fstree.ErrFileNotFound) {
		err = object.ErrNotFound
	}

	if err == nil {
		storagelog.Write(c.log, storagelog.AddressField(saddr), storagelog.OpField("fstree DELETE"))
		c.objCounters.DecFS()
	}

	return err
}
