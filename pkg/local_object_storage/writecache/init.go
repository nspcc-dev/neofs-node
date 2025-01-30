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
		flushed, needRemove := c.flushStatus(addr)
		if flushed {
			c.store.flushed.Add(addr.EncodeToString(), true)
			if needRemove {
				err := c.fsTree.Delete(addr)
				if err == nil {
					storagelog.Write(c.log,
						storagelog.AddressField(addr),
						storagelog.StorageTypeField(wcStorageType),
						storagelog.OpField("DELETE"),
					)
				}
			}
		}
		return nil
	}
	_ = c.fsTree.IterateAddresses(addrHandler, false)

	c.log.Info("finished updating flush marks")
}

// flushStatus returns info about the object state in the main storage.
// First return value is true iff object exists.
// Second return value is true iff object can be safely removed.
func (c *cache) flushStatus(addr oid.Address) (bool, bool) {
	_, err := c.metabase.Exists(addr, false)
	if err != nil {
		needRemove := errors.Is(err, meta.ErrObjectIsExpired) || errors.As(err, new(apistatus.ObjectAlreadyRemoved))
		return needRemove, needRemove
	}

	sid, _ := c.metabase.StorageID(addr)
	exists, err := c.blobstor.Exists(addr, sid)
	return err == nil && exists, false
}
