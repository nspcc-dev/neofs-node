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
	metaBkt := tx.Bucket(metaBucketKey(id))
	if metaBkt == nil || containerMarkedGC(metaBkt.Cursor()) {
		return ContainerInfo{}
	}

	var res ContainerInfo
	res.StorageSize = parseContainerCounter(metaBkt.Get([]byte{metaPrefixPayloadCounter}))

	phy := parseContainerCounter(metaBkt.Get([]byte{metaPrefixPhyCounter}))
	gc := parseContainerCounter(metaBkt.Get([]byte{metaPrefixGCCounter}))
	if phy > gc {
		res.ObjectsNumber = phy - gc
	}

	return res
}

func parseContainerCounter(v []byte) uint64 {
	if len(v) == 0 {
		return 0
	}

	return binary.LittleEndian.Uint64(v)
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

	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		if err := tx.DeleteBucket(metaBucketKey(cID)); err != nil && !errors.Is(err, bolterrors.ErrBucketNotFound) {
			return fmt.Errorf("metadata bucket cleanup: %w", err)
		}

		return nil
	})
}
