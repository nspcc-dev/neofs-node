package writecache

import (
	"errors"

	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

func (c *cache) initFlushMarks() {
	c.log.Info("filling flush marks for objects in FSTree")

	var addrHandler = func(addr oid.Address) error {
		if c.flushStatus(addr) {
			err := c.fsTree.Delete(addr)
			if err == nil {
				storagelog.Write(c.log,
					storagelog.AddressField(addr),
					storagelog.StorageTypeField(wcStorageType),
					storagelog.OpField("DELETE"),
				)
			}
		}
		return nil
	}
	_ = c.fsTree.IterateAddresses(addrHandler, false)

	c.log.Info("finished updating flush marks")
}

// flushStatus returns info about the object state in the main storage.
// Return value is true iff object exists and can be safely removed.
func (c *cache) flushStatus(addr oid.Address) bool {
	_, err := c.metabase.Exists(addr, false)
	if err != nil {
		needRemove := errors.Is(err, meta.ErrObjectIsExpired) || errors.As(err, new(apistatus.ObjectAlreadyRemoved))
		return needRemove
	}

	exists, err := c.blobstor.Exists(addr)
	return err == nil && exists
}
