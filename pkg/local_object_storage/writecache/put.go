package writecache

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
)

// ErrBigObject is returned when object is too big to be placed in cache.
var ErrBigObject = errors.New("too big object")

// Put puts object to write-cache.
func (c *cache) Put(o *object.Object) error {
	c.modeMtx.RLock()
	defer c.modeMtx.RUnlock()
	if c.mode == ModeReadOnly {
		return ErrReadOnly
	}

	sz := uint64(o.ToV2().StableSize())
	if sz > c.maxObjectSize {
		return ErrBigObject
	}

	data, err := o.Marshal()
	if err != nil {
		return err
	}

	oi := objectInfo{
		addr: o.Address().String(),
		obj:  o,
		data: data,
	}

	c.mtx.Lock()

	if sz <= c.smallObjectSize && c.curMemSize+sz <= c.maxMemSize {
		c.curMemSize += sz
		c.mem = append(c.mem, oi)

		c.mtx.Unlock()

		storagelog.Write(c.log, storagelog.AddressField(oi.addr), storagelog.OpField("in-mem PUT"))

		return nil
	}

	c.mtx.Unlock()

	if sz <= c.smallObjectSize {
		c.persistSmallObjects([]objectInfo{oi})
	} else {
		c.persistBigObject(oi)
	}
	return nil
}
