package shard

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// GetRange reads part of an object from shard. If skipMeta is specified
// data will be fetched directly from the blobstor, bypassing metabase.
//
// Zero length is interpreted as requiring full object length independent of the
// offset.
//
// Returns any error encountered that
// did not allow to completely read the object part.
//
// Returns ErrRangeOutOfBounds if the requested object range is out of bounds.
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing.
// Returns an error of type apistatus.ObjectAlreadyRemoved if the requested object has been marked as removed in shard.
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
func (s *Shard) GetRange(addr oid.Address, offset uint64, length uint64, skipMeta bool) (*object.Object, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	var obj *object.Object

	cb := func(stor common.Storage) error {
		r, err := stor.GetRange(addr, offset, length)
		if err != nil {
			return err
		}

		obj = object.New()
		obj.SetPayload(r)

		return nil
	}

	wc := func(c writecache.Cache) error {
		o, err := c.Get(addr)
		if err != nil {
			return err
		}

		payload := o.Payload()
		pLen := uint64(len(payload))
		from := offset
		var to uint64
		if length != 0 {
			to = from + length
		} else {
			to = pLen
		}
		if to < from || pLen < from || pLen < to {
			return logicerr.Wrap(apistatus.ObjectOutOfRange{})
		}

		obj = object.New()
		obj.SetPayload(payload[from:to])
		return nil
	}

	skipMeta = skipMeta || s.info.Mode.NoMetabase()
	gotMeta, err := s.fetchObjectData(addr, skipMeta, cb, wc)
	if err != nil && gotMeta {
		err = fmt.Errorf("%w, %w", err, ErrMetaWithNoObject)
	}

	return obj, err
}
