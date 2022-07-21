package meta

import (
	"crypto/sha256"
	"encoding/binary"
	"strings"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"go.etcd.io/bbolt"
)

func (db *DB) Containers() (list []cid.ID, err error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

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
			unique[cnr.EncodeToString()] = struct{}{}
		}

		return nil
	})

	return result, err
}

func (db *DB) ContainerSize(id cid.ID) (size uint64, err error) {
	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		size, err = db.containerSize(tx, id)

		return err
	})

	return size, err
}

func (db *DB) containerSize(tx *bbolt.Tx, id cid.ID) (uint64, error) {
	containerVolume, err := tx.CreateBucketIfNotExists(containerVolumeBucketName)
	if err != nil {
		return 0, err
	}

	key := make([]byte, sha256.Size)
	id.Encode(key)

	return parseContainerSize(containerVolume.Get(key)), nil
}

func parseContainerID(dst *cid.ID, name []byte, ignore map[string]struct{}) bool {
	strContainerID := strings.Split(string(name), invalidBase58String)[0]

	if _, ok := ignore[strContainerID]; ok {
		return false
	}

	if err := dst.DecodeString(strContainerID); err != nil {
		return false
	}

	return true
}

func parseContainerSize(v []byte) uint64 {
	if len(v) == 0 {
		return 0
	}

	return binary.LittleEndian.Uint64(v)
}

func changeContainerSize(tx *bbolt.Tx, id cid.ID, delta uint64, increase bool) error {
	containerVolume, err := tx.CreateBucketIfNotExists(containerVolumeBucketName)
	if err != nil {
		return err
	}

	key := make([]byte, sha256.Size)
	id.Encode(key)

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
