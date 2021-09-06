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
	sz := uint64(o.ToV2().StableSize())
	if sz > c.maxObjectSize {
		return ErrBigObject
	}

	data, err := o.Marshal(nil)
	if err != nil {
		return err
	}

	oi := objectInfo{
		addr: o.Address().String(),
		obj:  o,
		data: data,
	}

	c.mtx.Lock()

	if sz < c.smallObjectSize && c.curMemSize+sz <= c.maxMemSize {
		c.curMemSize += sz
		c.mem = append(c.mem, oi)

		c.mtx.Unlock()

		storagelog.Write(c.log, storagelog.AddressField(oi.addr), storagelog.OpField("in-mem PUT"))

		return nil
	}

	c.mtx.Unlock()

	c.persistObjects([]objectInfo{oi})
	return nil
}
