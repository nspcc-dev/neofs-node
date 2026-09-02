package writecache

import (
	"errors"
	"io"

	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

var (
	// ErrOutOfSpace is returned when there is no space left to put a new object.
	ErrOutOfSpace = errors.New("no space left in the write cache")
)

// Put puts object to write-cache. data MUST have serialized object, Object
// parameter is left for compatibility with blobstor only.
func (c *cache) Put(addr oid.Address, _ *object.Object, data []byte) error {
	c.modeMtx.RLock()
	defer c.modeMtx.RUnlock()
	if c.readOnly() {
		return ErrReadOnly
	}

	if c.metrics.mr != nil {
		defer elapsed(c.metrics.AddWCPutDuration)()
	}

	return c.put(addr, data)
}

// put writes object to FSTree and pushes it to the flush workers queue.
func (c *cache) put(addr oid.Address, data []byte) error {
	if err := c.checkAvailableSpace(len(data)); err != nil {
		return err
	}

	err := c.fsTree.Put(addr, data)
	if err != nil {
		return err
	}

	c.handleSavedObject(addr, len(data))

	return nil
}

func (c *cache) checkAvailableSpace(objSz int) error {
	cacheSz := c.objCounters.Size()
	if c.maxCacheSize < cacheSz+uint64(objSz) {
		return ErrOutOfSpace
	}

	return nil
}

func (c *cache) handleSavedObject(addr oid.Address, fullLen int) {
	c.objCounters.Add(addr, uint64(fullLen))
	c.metrics.IncWCObjectCount()
	c.metrics.AddWCSize(uint64(fullLen))
	storagelog.Write(c.log,
		storagelog.AddressField(addr),
		storagelog.StorageTypeField(wcStorageType),
		storagelog.OpField("PUT"),
	)
}

// TODO: docs.
func (c *cache) InitPut(addr oid.Address, fullDataLen int, dataPrefix []byte) (io.WriteCloser, func(), error) {
	c.modeMtx.RLock()
	defer c.modeMtx.RUnlock()
	if c.readOnly() {
		return nil, nil, ErrReadOnly
	}

	// TODO: metric?
	// if c.metrics.mr != nil {
	// 	defer elapsed(c.metrics.AddWCPutDuration)()
	// }

	if err := c.checkAvailableSpace(fullDataLen); err != nil {
		return nil, nil, err
	}

	blobStream, abortFn, err := c.fsTree.InitPut(addr, fullDataLen, dataPrefix)
	if err != nil {
		return nil, nil, err
	}

	return objectPayloadWriteStream{
		cache:      c,
		addr:       addr,
		fullLength: fullDataLen,
		blobStream: blobStream,
	}, abortFn, nil
}

type objectPayloadWriteStream struct {
	cache      *cache
	addr       oid.Address
	fullLength int
	blobStream io.WriteCloser
}

func (x objectPayloadWriteStream) Write(p []byte) (int, error) {
	// TODO: wrap error with component context (Close too)
	return x.blobStream.Write(p)
}

func (x objectPayloadWriteStream) Close() error {
	err := x.blobStream.Close()
	if err != nil {
		return err
	}

	x.cache.handleSavedObject(x.addr, x.fullLength)

	return nil
}
