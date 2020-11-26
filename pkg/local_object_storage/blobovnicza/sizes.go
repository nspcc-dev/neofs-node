package blobovnicza

import (
	"encoding/binary"
	"fmt"

	"code.cloudfoundry.org/bytefmt"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
)

const firstBucketBound = uint64(32 * 1 << 10) // 32KB

func stringifyBounds(lower, upper uint64) string {
	return fmt.Sprintf("[%s:%s]",
		stringifyByteSize(lower),
		stringifyByteSize(upper),
	)
}

func stringifyByteSize(sz uint64) string {
	return bytefmt.ByteSize(sz)
}

func bucketKeyFromBounds(upperBound uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)

	ln := binary.PutUvarint(buf, upperBound)

	return buf[:ln]
}

func bucketForSize(sz uint64) []byte {
	var upperBound uint64

	for upperBound = firstBucketBound; upperBound < sz; upperBound *= 2 {
	}

	return bucketKeyFromBounds(upperBound)
}

func (b *Blobovnicza) incSize(sz uint64) {
	b.filled.Add(sz)
}

func (b *Blobovnicza) decSize(sz uint64) {
	b.filled.Sub(sz)
}

func (b *Blobovnicza) full() bool {
	return b.filled.Load() >= b.fullSizeLimit
}

func (b *Blobovnicza) syncFullnessCounter() error {
	return errors.Wrap(b.boltDB.View(func(tx *bbolt.Tx) error {
		sz := uint64(0)

		if err := b.iterateBucketKeys(func(lower, upper uint64, key []byte) (bool, error) {
			buck := tx.Bucket(key)
			if buck == nil {
				return false, errors.Errorf("bucket not found %s", stringifyBounds(lower, upper))
			}

			sz += uint64(buck.Stats().KeyN) * (upper - lower)

			return false, nil
		}); err != nil {
			return err
		}

		b.filled.Store(sz)

		return nil
	}), "(%T) could not sync fullness counter")
}
