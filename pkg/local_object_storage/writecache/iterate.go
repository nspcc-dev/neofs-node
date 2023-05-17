package writecache

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// ErrNoDefaultBucket is returned by IterateDB when default bucket for objects is missing.
var ErrNoDefaultBucket = errors.New("no default bucket")

// IterationPrm contains iteration parameters.
type IterationPrm struct {
	handler      func([]byte) error
	ignoreErrors bool
}

// WithHandler sets a callback to be executed on every object.
func (p *IterationPrm) WithHandler(f func([]byte) error) {
	p.handler = f
}

// WithIgnoreErrors sets a flag indicating that errors should be ignored.
func (p *IterationPrm) WithIgnoreErrors(ignore bool) {
	p.ignoreErrors = ignore
}

// Iterate iterates over all objects present in write cache.
// This is very difficult to do correctly unless write-cache is put in read-only mode.
// Thus we silently fail if shard is not in read-only mode to avoid reporting misleading results.
func (c *cache) Iterate(prm IterationPrm) error {
	c.modeMtx.RLock()
	defer c.modeMtx.RUnlock()
	if !c.readOnly() {
		return nil
	}

	err := c.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		return b.ForEach(func(k, data []byte) error {
			if _, ok := c.flushed.Peek(string(k)); ok {
				return nil
			}
			return prm.handler(data)
		})
	})
	if err != nil {
		return err
	}

	var fsPrm common.IteratePrm
	fsPrm.IgnoreErrors = prm.ignoreErrors
	fsPrm.LazyHandler = func(addr oid.Address, f func() ([]byte, error)) error {
		if _, ok := c.flushed.Peek(addr.EncodeToString()); ok {
			return nil
		}
		data, err := f()
		if err != nil {
			if prm.ignoreErrors || errors.As(err, new(apistatus.ObjectNotFound)) {
				// an object can be removed b/w iterating over it
				// and reading its payload; not an error
				return nil
			}
			return err
		}
		return prm.handler(data)
	}

	_, err = c.fsTree.Iterate(fsPrm)
	return err
}

// IterateDB iterates over all objects stored in bbolt.DB instance and passes them to f until error return.
// It is assumed that db is an underlying database of some WriteCache instance.
//
// Returns ErrNoDefaultBucket if there is no default bucket in db.
//
// DB must not be nil and should be opened.
func IterateDB(db *bbolt.DB, f func(oid.Address) error) error {
	return db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		if b == nil {
			return ErrNoDefaultBucket
		}

		var addr oid.Address

		return b.ForEach(func(k, v []byte) error {
			err := addr.DecodeString(string(k))
			if err != nil {
				return fmt.Errorf("could not parse object address: %w", err)
			}

			return f(addr)
		})
	})
}
