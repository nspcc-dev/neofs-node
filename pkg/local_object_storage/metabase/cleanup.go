package meta

import (
	"bytes"
	"strings"

	"go.etcd.io/bbolt"
)

// CleanUpPrm groups the parameters of CleanUp operation.
type CleanUpPrm struct{}

// CleanUpRes groups resulting values of CleanUp operation.
type CleanUpRes struct{}

// CleanUp removes empty buckets from metabase.
func (db *DB) CleanUp(prm *CleanUpPrm) (res *CleanUpRes, err error) {
	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			switch {
			case isFKBTBucket(name):
				cleanUpFKBT(tx, name, b)
			case isListBucket(name):
				cleanUpListBucket(tx, name, b)
			default:
				cleanUpUniqueBucket(tx, name, b)
			}

			return nil
		})
	})

	return
}

func isFKBTBucket(name []byte) bool {
	bucketName := string(name)

	switch {
	case
		strings.Contains(bucketName, userAttributePostfix),
		strings.Contains(bucketName, ownerPostfix):
		return true
	default:
		return false
	}
}

func isListBucket(name []byte) bool {
	bucketName := string(name)

	switch {
	case
		strings.Contains(bucketName, payloadHashPostfix),
		strings.Contains(bucketName, parentPostfix):
		return true
	default:
		return false
	}
}

func cleanUpUniqueBucket(tx *bbolt.Tx, name []byte, b *bbolt.Bucket) {
	switch { // clean well-known global metabase buckets
	case bytes.Equal(name, containerVolumeBucketName):
		_ = b.ForEach(func(k, v []byte) error {
			if parseContainerSize(v) == 0 {
				_ = b.Delete(k)
			}

			return nil
		})
	default:
		// do nothing
	}

	if b.Stats().KeyN == 0 {
		_ = tx.DeleteBucket(name) // ignore error, best effort there
	}
}

func cleanUpFKBT(tx *bbolt.Tx, name []byte, b *bbolt.Bucket) {
	removedBuckets := 0
	remainingBuckets := b.Stats().BucketN - 1

	_ = b.ForEach(func(k, _ []byte) error {
		fkbtRoot := b.Bucket(k)
		if fkbtRoot == nil {
			return nil
		}

		if fkbtRoot.Stats().KeyN == 0 {
			err := b.DeleteBucket(k)
			if err == nil {
				removedBuckets++
			}
		}

		return nil
	})

	if remainingBuckets == removedBuckets {
		_ = tx.DeleteBucket(name) // ignore error, best effort there
	}
}

func cleanUpListBucket(tx *bbolt.Tx, name []byte, b *bbolt.Bucket) {
	cleanUpUniqueBucket(tx, name, b)
}
