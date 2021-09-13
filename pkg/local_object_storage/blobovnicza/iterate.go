package blobovnicza

import (
	"fmt"

	"go.etcd.io/bbolt"
)

func (b *Blobovnicza) iterateBuckets(tx *bbolt.Tx, f func(uint64, uint64, *bbolt.Bucket) (bool, error)) error {
	return b.iterateBucketKeys(func(lower uint64, upper uint64, key []byte) (bool, error) {
		buck := tx.Bucket(key)
		if buck == nil {
			// expected to happen:
			//  - before initialization step (incorrect usage by design)
			//  - if DB is corrupted (in future this case should be handled)
			return false, fmt.Errorf("(%T) could not get bucket %s", b, stringifyBounds(lower, upper))
		}

		return f(lower, upper, buck)
	})
}

func (b *Blobovnicza) iterateBucketKeys(f func(uint64, uint64, []byte) (bool, error)) error {
	return b.iterateBounds(func(lower, upper uint64) (bool, error) {
		return f(lower, upper, bucketKeyFromBounds(upper))
	})
}

func (b *Blobovnicza) iterateBounds(f func(uint64, uint64) (bool, error)) error {
	objLimitBound := upperPowerOfTwo(b.objSizeLimit)

	for upper := firstBucketBound; upper <= max(objLimitBound, firstBucketBound); upper *= 2 {
		var lower uint64

		if upper == firstBucketBound {
			lower = 0
		} else {
			lower = upper/2 + 1
		}

		if stop, err := f(lower, upper); err != nil {
			return err
		} else if stop {
			break
		}
	}

	return nil
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}

	return b
}

// IterationElement represents a unit of elements through which Iterate operation passes.
type IterationElement struct {
	data []byte
}

// ObjectData returns stored object in a binary representation.
func (x IterationElement) ObjectData() []byte {
	return x.data
}

// IterationHandler is a generic processor of IterationElement.
type IterationHandler func(IterationElement) error

// IteratePrm groups the parameters of Iterate operation.
type IteratePrm struct {
	handler IterationHandler
}

// SetHandler sets handler to be called iteratively.
func (x *IteratePrm) SetHandler(h IterationHandler) {
	x.handler = h
}

// IterateRes groups resulting values of Iterate operation.
type IterateRes struct {
}

// Iterate goes through all stored objects, and passes their headers
// to parameterized handler until error return.
//
// Returns handler's errors directly. Returns nil after iterating finish.
//
// Handler should not retain object data. Handler must not be nil.
func (b *Blobovnicza) Iterate(prm IteratePrm) (*IterateRes, error) {
	var elem IterationElement

	if err := b.boltDB.View(func(tx *bbolt.Tx) error {
		return b.iterateBuckets(tx, func(lower, upper uint64, buck *bbolt.Bucket) (bool, error) {
			err := buck.ForEach(func(k, v []byte) error {
				elem.data = v
				return prm.handler(elem)
			})

			return err != nil, err
		})
	}); err != nil {
		return nil, err
	}

	return new(IterateRes), nil
}

// IterateObjects is a helper function which iterates over Blobovnicza and passes binary objects to f.
func IterateObjects(blz *Blobovnicza, f func([]byte) error) error {
	var prm IteratePrm

	prm.SetHandler(func(elem IterationElement) error {
		return f(elem.ObjectData())
	})

	_, err := blz.Iterate(prm)

	return err
}
