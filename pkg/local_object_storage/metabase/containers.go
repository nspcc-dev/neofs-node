package meta

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/nspcc-dev/bbolt"
	bolterrors "github.com/nspcc-dev/bbolt/errors"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

const (
	containerStorageSizeKey   = 0
	containerObjectsNumberKey = 1
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

	err := tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
		cnr, prefix := parseContainerIDWithPrefix(name)
		if cnr.IsZero() || prefix != metadataPrefix {
			return nil
		}

		result = append(result, cnr)

		return nil
	})

	return result, err
}

// ContainerInfo groups metabase's objects status for a certain container.
type ContainerInfo struct {
	StorageSize   uint64
	ObjectsNumber uint64
}

// GetContainerInfo returns statistics about stored objects for specified
// container. If no info is found, empty [ContainerInfo] with no error are
// returned.
func (db *DB) GetContainerInfo(id cid.ID) (ContainerInfo, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ContainerInfo{}, ErrDegradedMode
	}

	var res ContainerInfo
	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		res = db.containerInfo(tx, id)
		return nil
	})
	if err != nil {
		return ContainerInfo{}, fmt.Errorf("fetching info from db: %w", err)
	}

	return res, nil
}

func (db *DB) containerInfo(tx *bbolt.Tx, id cid.ID) ContainerInfo {
	infoBkt := tx.Bucket(containerVolumeBucketName)
	cnrBkt := infoBkt.Bucket(id[:])
	if cnrBkt == nil {
		return ContainerInfo{}
	}

	var res ContainerInfo
	res.StorageSize = parseContainerCounter(cnrBkt.Get([]byte{containerStorageSizeKey}))
	res.ObjectsNumber = parseContainerCounter(cnrBkt.Get([]byte{containerObjectsNumberKey}))

	return res
}

func resetContainerSize(tx *bbolt.Tx, cID cid.ID) error {
	infoBkt := tx.Bucket(containerVolumeBucketName)
	cnrBkt := infoBkt.Bucket(cID[:])
	if cnrBkt != nil {
		err := cnrBkt.Put([]byte{containerStorageSizeKey}, make([]byte, 8))
		if err != nil {
			return fmt.Errorf("put zero storage size: %w", err)
		}
		err = cnrBkt.Put([]byte{containerObjectsNumberKey}, make([]byte, 8))
		if err != nil {
			return fmt.Errorf("put zero objects number: %w", err)
		}
	}

	return nil
}

func parseContainerCounter(v []byte) uint64 {
	if len(v) == 0 {
		return 0
	}

	return binary.LittleEndian.Uint64(v)
}

func changeContainerInfo(tx *bbolt.Tx, id cid.ID, storageSizeDelta, objectsNumberDelta int) error {
	var err error
	infoBkt := tx.Bucket(containerVolumeBucketName)
	cnrInfoBkt := infoBkt.Bucket(id[:])
	if cnrInfoBkt == nil {
		cnrInfoBkt, err = infoBkt.CreateBucket(id[:])
		if err != nil {
			return fmt.Errorf("create container info: %w", err)
		}
	}

	sizeOld := parseContainerCounter(cnrInfoBkt.Get([]byte{containerStorageSizeKey}))
	sizeNew := changeCounter(sizeOld, storageSizeDelta)
	buff := make([]byte, 8)
	binary.LittleEndian.PutUint64(buff, sizeNew)
	err = cnrInfoBkt.Put([]byte{containerStorageSizeKey}, buff)
	if err != nil {
		return fmt.Errorf("update container size value (from %d to %d): %w", sizeOld, sizeNew, err)
	}

	objsNumberOld := parseContainerCounter(cnrInfoBkt.Get([]byte{containerObjectsNumberKey}))
	objsNumberNew := changeCounter(objsNumberOld, objectsNumberDelta)
	buff = make([]byte, 8)
	binary.LittleEndian.PutUint64(buff, objsNumberNew)
	err = cnrInfoBkt.Put([]byte{containerObjectsNumberKey}, buff)
	if err != nil {
		return fmt.Errorf("update container objects number value (from %d to %d): %w", objsNumberOld, objsNumberNew, err)
	}

	return nil
}

func changeCounter(oldVal uint64, delta int) uint64 {
	if delta >= 0 {
		return oldVal + uint64(delta)
	}
	newVal := oldVal - uint64(-delta)
	if newVal > oldVal {
		return 0
	}
	return newVal
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
		err := bktEstimations.DeleteBucket(cIDRaw)
		if err != nil && !errors.Is(err, bolterrors.ErrBucketNotFound) {
			return fmt.Errorf("estimations bucket cleanup: %w", err)
		}

		// Metadata
		if err = tx.DeleteBucket(metaBucketKey(cID)); err != nil && !errors.Is(err, bolterrors.ErrBucketNotFound) {
			return fmt.Errorf("metadata bucket cleanup: %w", err)
		}

		return nil
	})
}
