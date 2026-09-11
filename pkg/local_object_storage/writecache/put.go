package writecache

import (
	"errors"
	"fmt"
	"io"
	"time"

	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
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

	err := c.putFunc(uint64(len(data)), func() error {
		return c.fsTree.Put(addr, data)
	})
	if err != nil {
		return err
	}

	c.handleSavedObject(addr, uint64(len(data)))

	return nil
}

func (c *cache) putFunc(objSz uint64, fn func() error) error {
	cacheSz := c.objCounters.Size()
	if c.maxCacheSize < cacheSz+objSz {
		return ErrOutOfSpace
	}

	return fn()
}

func (c *cache) handleSavedObject(addr oid.Address, objSz uint64) {
	c.objCounters.Add(addr, objSz)
	c.metrics.IncWCObjectCount()
	c.metrics.AddWCSize(objSz)
	storagelog.Write(c.log,
		storagelog.AddressField(addr),
		storagelog.StorageTypeField(wcStorageType),
		storagelog.OpField("PUT"),
	)
}

// InitPut calls [fstree.FSTree.InitPut] on the underlying FS tree.
//
// If c is in read-only mode, InitPut instantly returns [ErrReadOnly].
// Otherwise, if [WithMetrics] is used, InitPut passes duration to
// [MetricRegister.AddWCInitPutDuration].
func (c *cache) InitPut(addr oid.Address, headerLen uint64, payloadLen uint64, headerW io.WriterTo) (io.WriteCloser, func(), error) {
	c.modeMtx.RLock()
	defer c.modeMtx.RUnlock()
	if c.readOnly() {
		return nil, nil, ErrReadOnly
	}

	var st time.Time
	if c.metrics.mr != nil {
		st = time.Now()
	}

	dataLen := iobject.CalculateConcatProtobufLength(headerLen, payloadLen)

	var fsTreeStream io.WriteCloser
	var fstAbortFn func()

	err := c.putFunc(dataLen, func() error {
		var err error
		fsTreeStream, fstAbortFn, err = c.fsTree.InitPut(addr, headerLen, payloadLen, headerW)
		return err
	})
	if err != nil {
		c.metrics.submitFinishedInitPut(st)
		return nil, nil, err
	}

	res := newObjectPayloadWriteStream(c, addr, dataLen, fsTreeStream, fstAbortFn, st)

	return res, res.abort, nil
}

type objectPayloadWriteStream struct {
	writeCache    *cache
	addr          oid.Address
	dataLen       uint64
	fsTreeStream  io.WriteCloser
	fsTreeAbortFn func()
	startTime     time.Time
	aborted       bool
}

func newObjectPayloadWriteStream(writeCache *cache, addr oid.Address, dataLen uint64, fsTreeStream io.WriteCloser, fsTreeAbortFn func(), startTime time.Time) *objectPayloadWriteStream {
	return &objectPayloadWriteStream{
		writeCache:    writeCache,
		addr:          addr,
		dataLen:       dataLen,
		fsTreeStream:  fsTreeStream,
		fsTreeAbortFn: fsTreeAbortFn,
		startTime:     startTime,
	}
}

func (x *objectPayloadWriteStream) Write(p []byte) (int, error) {
	if x.aborted {
		return 0, logicerr.ErrStreamAborted
	}

	n, err := x.fsTreeStream.Write(p)
	if err != nil {
		x.finish()
		return n, fmt.Errorf("FSTree write: %w", err)
	}

	return n, nil
}

func (x *objectPayloadWriteStream) Close() error {
	if x.aborted {
		return logicerr.ErrStreamAborted
	}

	defer x.finish()

	err := x.fsTreeStream.Close()
	if err != nil {
		return fmt.Errorf("FSTree close: %w", err)
	}

	x.writeCache.handleSavedObject(x.addr, x.dataLen)

	return nil
}

func (x *objectPayloadWriteStream) abort() {
	if x.aborted {
		return
	}
	x.fsTreeAbortFn()
	x.finish()
}

func (x *objectPayloadWriteStream) finish() {
	x.writeCache.metrics.submitFinishedInitPut(x.startTime)
	x.aborted = true
}
