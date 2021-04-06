package writecache

import (
	"errors"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
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
			c.mtx.Unlock()
			return nil
		}
	}
	c.mtx.Unlock()

	// Check disk cache.
	has := false
	_ = c.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		has = b.Get([]byte(saddr)) != nil
		return nil
	})

	if has {
		return c.db.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket(defaultBucket)
			return b.Delete([]byte(saddr))
		})
	}

	err := c.fsTree.Delete(addr)
	if errors.Is(err, fstree.ErrFileNotFound) {
		err = object.ErrNotFound
	}

	return err
}
