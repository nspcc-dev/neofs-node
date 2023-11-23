package writecache

import (
	"errors"
	"fmt"
	"io"
	"io/fs"

	"github.com/nspcc-dev/neo-go/pkg/util/slice"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// Get returns object from write-cache.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in write-cache.
func (c *cache) Get(addr oid.Address) (*objectSDK.Object, error) {
	saddr := addr.EncodeToString()

	value, err := Get(c.db, []byte(saddr))
	if err == nil {
		obj := objectSDK.New()
		c.flushed.Get(saddr)
		return obj, obj.Unmarshal(value)
	}

	res, err := c.fsTree.Get(common.GetPrm{Address: addr})
	if err != nil {
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	c.flushed.Get(saddr)
	return res.Object, nil
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

// Get fetches object from the underlying database.
// Key should be a stringified address.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in db.
func Get(db *bbolt.DB, key []byte) ([]byte, error) {
	var value []byte
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		if b == nil {
			return ErrNoDefaultBucket
		}
		value = b.Get(key)
		if value == nil {
			return logicerr.Wrap(apistatus.ObjectNotFound{})
		}
		value = slice.Copy(value)
		return nil
	})
	return value, err
}

func (c *cache) OpenObjectStream(objAddr oid.Address) (io.ReadSeekCloser, error) {
	saddr := objAddr.EncodeToString()

	value, errDB := Get(c.db, []byte(saddr))
	if errDB == nil {
		c.flushed.Get(saddr) // copied from cache.Get
		return util.NewBytesReadSeekCloser(value), nil
	}

	res, errFS := c.fsTree.OpenObjectStream(objAddr)
	if errFS != nil {
		notFoundInDB := errors.Is(errDB, apistatus.ErrObjectNotFound)
		if errors.Is(errFS, fs.ErrNotExist) {
			if notFoundInDB {
				return nil, apistatus.ErrObjectNotFound
			}

			return nil, fmt.Errorf("get object from DB: %w", errDB)
		}

		if notFoundInDB {
			// no need to report such DB error
			return nil, fmt.Errorf("get object from FS tree: %w", errFS)
		}

		return nil, fmt.Errorf("get object from FS tree: %w (DB failure: %v)", errFS, errDB)
	}

	c.flushed.Get(saddr) // copied from cache.Get

	return res, nil
}
