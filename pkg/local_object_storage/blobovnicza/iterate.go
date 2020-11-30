package blobovnicza

import (
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
)

func (b *Blobovnicza) iterateBuckets(tx *bbolt.Tx, f func(uint64, uint64, *bbolt.Bucket) (bool, error)) error {
	return b.iterateBucketKeys(func(lower uint64, upper uint64, key []byte) (bool, error) {
		buck := tx.Bucket(key)
		if buck == nil {
			// expected to happen:
			//  - before initialization step (incorrect usage by design)
			//  - if DB is corrupted (in future this case should be handled)
			return false, errors.Errorf("(%T) could not get bucket %s", b, stringifyBounds(lower, upper))
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
	for upper := firstBucketBound; upper <= max(b.objSizeLimit, firstBucketBound); upper *= 2 {
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
