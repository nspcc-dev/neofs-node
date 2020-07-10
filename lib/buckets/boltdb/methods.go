package boltdb

import (
	"os"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
)

// Get value by key or return error.
func (b *bucket) Get(key []byte) (data []byte, err error) {
	err = b.db.View(func(txn *bbolt.Tx) error {
		txn.Bucket(b.name).Cursor().Seek(key)
		val := txn.Bucket(b.name).Get(key)
		if val == nil {
			return errors.Wrapf(core.ErrNotFound, "key=%s", base58.Encode(key))
		}

		data = makeCopy(val)
		return nil
	})

	return
}

// Set value for key.
func (b *bucket) Set(key, value []byte) error {
	return b.db.Update(func(txn *bbolt.Tx) error {
		k, v := makeCopy(key), makeCopy(value)
		return txn.Bucket(b.name).Put(k, v)
	})
}

// Del removes item from bucket by key.
func (b *bucket) Del(key []byte) error {
	return b.db.Update(func(txn *bbolt.Tx) error {
		return txn.Bucket(b.name).Delete(key)
	})
}

// Has checks key exists.
func (b *bucket) Has(key []byte) bool {
	_, err := b.Get(key)
	return !errors.Is(errors.Cause(err), core.ErrNotFound)
}

// Size returns size of database.
func (b *bucket) Size() int64 {
	info, err := os.Stat(b.db.Path())
	if err != nil {
		return 0
	}

	return info.Size()
}

// List all items in bucket.
func (b *bucket) List() ([][]byte, error) {
	var items [][]byte

	if err := b.db.View(func(txn *bbolt.Tx) error {
		return txn.Bucket(b.name).ForEach(func(k, _ []byte) error {
			items = append(items, makeCopy(k))
			return nil
		})
	}); err != nil {
		return nil, err
	}

	return items, nil
}

// Filter elements by filter closure.
func (b *bucket) Iterate(handler core.FilterHandler) error {
	if handler == nil {
		return core.ErrNilFilterHandler
	}

	return b.db.View(func(txn *bbolt.Tx) error {
		return txn.Bucket(b.name).ForEach(func(k, v []byte) error {
			if !handler(makeCopy(k), makeCopy(v)) {
				return core.ErrIteratingAborted
			}
			return nil
		})
	})
}

// Close bucket database.
func (b *bucket) Close() error {
	return b.db.Close()
}
