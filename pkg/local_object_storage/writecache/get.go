package writecache

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Get returns object from write-cache.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in write-cache.
func (c *cache) Get(addr oid.Address) (*objectSDK.Object, error) {
	saddr := addr.EncodeToString()

	obj, err := c.fsTree.Get(addr)
	if err != nil {
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	c.flushed.Get(saddr)
	return obj, nil
}

// Head returns object header from write-cache.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in write-cache.
func (c *cache) Head(addr oid.Address) (*objectSDK.Object, error) {
	obj, err := c.Get(addr)
	if err != nil {
		return nil, err
	}

	return obj.CutPayload(), nil
}

func (c *cache) GetBytes(addr oid.Address) ([]byte, error) {
	saddr := addr.EncodeToString()

	b, err := c.fsTree.GetBytes(addr)
	if err != nil {
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	c.flushed.Get(saddr)
	return b, nil
}
