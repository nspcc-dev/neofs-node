package meta

import (
	"encoding/binary"
	"errors"
	"fmt"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"
)

func (db *DB) Containers() (list []cid.ID, err error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return nil, ErrDegradedMode
	}

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		list, err = db.containers(tx)

		return err
	})

	return list, err
}

func (db *DB) containers(tx *bbolt.Tx) ([]cid.ID, error) {
	result := make([]cid.ID, 0)
	unique := make(map[string]struct{})
	var cnr cid.ID

	err := tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
		if parseContainerID(&cnr, name, unique) {
			result = append(result, cnr)
			unique[string(name[1:bucketKeySize])] = struct{}{}
		}

		return nil
	})

	return result, err
}

func (db *DB) ContainerSize(id cid.ID) (size uint64, err error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return 0, ErrDegradedMode
	}

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		size, err = db.containerSize(tx, id)

		return err
	})

	return size, err
}

func (db *DB) containerSize(tx *bbolt.Tx, id cid.ID) (uint64, error) {
	containerVolume := tx.Bucket(containerVolumeBucketName)

	return parseContainerSize(containerVolume.Get(id[:])), nil
}

func resetContainerSize(tx *bbolt.Tx, cID cid.ID) error {
	containerVolume := tx.Bucket(containerVolumeBucketName)

	return containerVolume.Put(cID[:], make([]byte, 8))
}

func parseContainerID(dst *cid.ID, name []byte, ignore map[string]struct{}) bool {
	if len(name) != bucketKeySize {
		return false
	}
	if _, ok := ignore[string(name[1:bucketKeySize])]; ok {
		return false
	}
	return dst.Decode(name[1:bucketKeySize]) == nil
}

func parseContainerSize(v []byte) uint64 {
	if len(v) == 0 {
		return 0
	}

	return binary.LittleEndian.Uint64(v)
}

func changeContainerSize(tx *bbolt.Tx, id cid.ID, delta uint64, increase bool) error {
	containerVolume := tx.Bucket(containerVolumeBucketName)
	key := id[:]

	size := parseContainerSize(containerVolume.Get(key))

	if increase {
		size += delta
	} else if size > delta {
		size -= delta
	} else {
		size = 0
	}

	buf := make([]byte, 8) // consider using sync.Pool to decrease allocations
	binary.LittleEndian.PutUint64(buf, size)

	return containerVolume.Put(key, buf)
}

// DeleteContainer removes any information that the metabase has
// associated with the provided container (its objects) except
// the graveyard-related one.
func (db *DB) DeleteContainer(cID cid.ID) error {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return ErrReadOnlyMode
	}

	cIDRaw := cID[:]

	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		// Estimations
		bktEstimations := tx.Bucket(containerVolumeBucketName)
		err := bktEstimations.Delete(cIDRaw)
		if err != nil {
			return fmt.Errorf("estimations bucket cleanup: %w", err)
		}

		// Locked objects
		bktLocked := tx.Bucket(bucketNameLocked)
		err = bktLocked.DeleteBucket(cIDRaw)
		if err != nil && !errors.Is(err, bolterrors.ErrBucketNotFound) {
			return fmt.Errorf("locked bucket cleanup: %w", err)
		}

		// Metadata
		if err = tx.DeleteBucket(metaBucketKey(cID)); err != nil && !errors.Is(err, bolterrors.ErrBucketNotFound) {
			return fmt.Errorf("metadata bucket cleanup: %w", err)
		}

		cnrGCBkt := tx.Bucket(garbageContainersBucketName)
		if cnrGCBkt != nil {
			err = cnrGCBkt.Delete(cIDRaw)
			if err != nil {
				return fmt.Errorf("garbage containers cleanup: %w", err)
			}
		}

		return nil
	})
}
