package writecache

import (
	"fmt"
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Get returns object from write-cache.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in write-cache.
func (c *cache) Get(addr oid.Address) (*objectSDK.Object, error) {
	if !c.objCounters.HasAddress(addr) {
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}
	obj, err := c.fsTree.Get(addr)
	if err != nil {
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	return obj, nil
}

// Head returns object header from write-cache.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in write-cache.
func (c *cache) Head(addr oid.Address) (*objectSDK.Object, error) {
	if !c.objCounters.HasAddress(addr) {
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}
	obj, err := c.fsTree.Head(addr)
	if err != nil {
		return nil, logicerr.Wrap(fmt.Errorf("%w: %w", apistatus.ErrObjectNotFound, err))
	}

	return obj, nil
}

func (c *cache) GetBytes(addr oid.Address) ([]byte, error) {
	if !c.objCounters.HasAddress(addr) {
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}
	b, err := c.fsTree.GetBytes(addr)
	if err != nil {
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	return b, nil
}

// GetStream returns an object stream from write-cache.
// On success, the reader is non-nil and must be closed;
// a nil reader is only returned with a non‑nil error.
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in write-cache.
func (c *cache) GetStream(addr oid.Address) (*objectSDK.Object, io.ReadCloser, error) {
	if !c.objCounters.HasAddress(addr) {
		return nil, nil, logicerr.Wrap(apistatus.ErrObjectNotFound)
	}
	stream, reader, err := c.fsTree.GetStream(addr)
	if err != nil {
		return nil, nil, logicerr.Wrap(apistatus.ErrObjectNotFound)
	}

	return stream, reader, nil
}
