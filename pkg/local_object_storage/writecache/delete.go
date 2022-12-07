package writecache

import (
	"runtime/debug"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// Delete removes object from write-cache.
//
// Returns an error of type apistatus.ObjectNotFound if object is missing in write-cache.
func (c *cache) Delete(addr oid.Address) (err error) {
	c.modeMtx.RLock()
	defer c.modeMtx.RUnlock()
	if c.readOnly() {
		return ErrReadOnly
	}

	saddr := addr.EncodeToString()

	// Check disk cache.
	var has int
	err = c.db.View(func(tx *bbolt.Tx) (err error) {
		defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
		defer common.BboltFatalHandler(&err)

		b := tx.Bucket(defaultBucket)
		has = len(b.Get([]byte(saddr)))
		return nil
	})
	if err != nil {
		return err
	}

	if 0 < has {
		err = c.db.Update(func(tx *bbolt.Tx) (err error) {
			defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
			defer common.BboltFatalHandler(&err)

			b := tx.Bucket(defaultBucket)
			return b.Delete([]byte(saddr))
		})
		if err != nil {
			return err
		}
		storagelog.Write(c.log, storagelog.AddressField(saddr), storagelog.OpField("db DELETE"))
		c.objCounters.DecDB()
		return nil
	}

	_, err = c.fsTree.Delete(common.DeletePrm{Address: addr})
	if err == nil {
		storagelog.Write(c.log, storagelog.AddressField(saddr), storagelog.OpField("fstree DELETE"))
		c.objCounters.DecFS()
	}

	return err
}
