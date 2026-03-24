package writecache

import (
	"fmt"
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Get returns object from write-cache.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in write-cache.
func (c *cache) Get(addr oid.Address) (*object.Object, error) {
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
func (c *cache) Head(addr oid.Address) (*object.Object, error) {
	if !c.objCounters.HasAddress(addr) {
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}
	obj, err := c.fsTree.Head(addr)
	if err != nil {
		return nil, logicerr.Wrap(fmt.Errorf("%w: %w", apistatus.ErrObjectNotFound, err))
	}

	return obj, nil
}

// ReadHeader reads first bytes of the referenced object's binary containing its
// full header from c into buf. Returns number of bytes read.
//
// Read part may include payload prefix.
//
// If object is missing, ReadHeader returns [apistatus.ErrObjectNotFound].
//
// Passed buf must have 2*[iobject.NonPayloadFieldsBufferLength] bytes len at least.
func (c *cache) ReadHeader(addr oid.Address, buf []byte) (int, error) {
	if !c.objCounters.HasAddress(addr) {
		return 0, apistatus.ErrObjectNotFound
	}

	n, err := c.fsTree.ReadHeader(addr, buf)
	if err != nil {
		return 0, err
	}

	return n, nil
}

// ReadObject reads first bytes of the referenced object's binary containing its
// full header from c into buf. Returns number of bytes read and stream of
// remainining bytes. The stream must be finally closed by the caller.
//
// Read part may include payload prefix.
//
// If object is missing, ReadHeader returns [apistatus.ErrObjectNotFound].
//
// Passed buf must have 2*[objectwire.NonPayloadFieldsBufferLength] bytes len at least.
func (c *cache) ReadObject(addr oid.Address, buf []byte) (int, io.ReadCloser, error) {
	if !c.objCounters.HasAddress(addr) {
		return 0, nil, apistatus.ErrObjectNotFound
	}

	return c.fsTree.ReadObject(addr, buf)
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
func (c *cache) GetStream(addr oid.Address) (*object.Object, io.ReadCloser, error) {
	if !c.objCounters.HasAddress(addr) {
		return nil, nil, logicerr.Wrap(apistatus.ErrObjectNotFound)
	}
	stream, reader, err := c.fsTree.GetStream(addr)
	if err != nil {
		return nil, nil, logicerr.Wrap(apistatus.ErrObjectNotFound)
	}

	return stream, reader, nil
}

// GetRangeStream reads payload range of the referenced object from c. Both zero
// off and ln mean full payload. The stream must be finally closed by the
// caller.
//
// If object is missing, GetRangeStream returns [apistatus.ErrObjectNotFound].
//
// If the range is out of payload bounds, GetRangeStream returns
// [apistatus.ErrObjectOutOfRange].
func (c *cache) GetRangeStream(addr oid.Address, off uint64, ln uint64) (io.ReadCloser, error) {
	if ln == 0 && off != 0 {
		return nil, fmt.Errorf("invalid range off=%d,ln=0", off)
	}

	if !c.objCounters.HasAddress(addr) {
		return nil, logicerr.Wrap(apistatus.ErrObjectNotFound)
	}

	stream, err := c.fsTree.GetRangeStream(addr, off, ln)
	if err != nil {
		return nil, fmt.Errorf("get range stream from underlying FS tree: %w", err)
	}

	return stream, nil
}
