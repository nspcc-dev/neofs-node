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

// TODO: docs.
// TODO: tests.
func (c *cache) HeadToBuffer(addr oid.Address, getBuffer func() []byte) (int, error) {
	if !c.objCounters.HasAddress(addr) {
		return 0, logicerr.Wrap(apistatus.ObjectNotFound{})
	}
	n, err := c.fsTree.HeadToBuffer(addr, getBuffer)
	if err != nil {
		return 0, fmt.Errorf("read header from underlying FS tree: %w", err)
	}

	return n, nil
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

// TODO: docs.
// TODO: tests.
func (c *cache) OpenStream(addr oid.Address, getBuffer func() []byte) (int, io.ReadCloser, error) {
	if !c.objCounters.HasAddress(addr) {
		return 0, nil, logicerr.Wrap(apistatus.ErrObjectNotFound)
	}
	n, stream, err := c.fsTree.OpenStream(addr, getBuffer)
	if err != nil {
		return 0, nil, fmt.Errorf("open stream in underlying FS tree: %w", err)
	}

	return n, stream, nil
}
