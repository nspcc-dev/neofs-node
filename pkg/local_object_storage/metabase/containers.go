package meta

import (
	"encoding/binary"
	"strings"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"go.etcd.io/bbolt"
)

func (db *DB) Containers() (list []*container.ID, err error) {
	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		list, err = db.containers(tx)

		return err
	})

	return list, err
}

func (db *DB) containers(tx *bbolt.Tx) ([]*container.ID, error) {
	result := make([]*container.ID, 0)

	err := tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
		id, err := parseContainerID(name)
		if err != nil {
			return err
		}

		if id != nil {
			result = append(result, id)
		}

		return nil
	})

	return result, err
}

func (db *DB) ContainerSize(id *container.ID) (size uint64, err error) {
	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		size, err = db.containerSize(tx, id)

		return err
	})

	return size, err
}

func (db *DB) containerSize(tx *bbolt.Tx, id *container.ID) (uint64, error) {
	containerVolume, err := tx.CreateBucketIfNotExists(containerVolumeBucketName)
	if err != nil {
		return 0, err
	}

	key := id.ToV2().GetValue()

	return parseContainerSize(containerVolume.Get(key)), nil
}

func parseContainerID(name []byte) (*container.ID, error) {
	strName := string(name)

	if strings.Contains(strName, invalidBase58String) {
		return nil, nil
	}

	id := container.NewID()

	return id, id.Parse(strName)
}

func parseContainerSize(v []byte) uint64 {
	if len(v) == 0 {
		return 0
	}

	return binary.LittleEndian.Uint64(v)
}

func changeContainerSize(tx *bbolt.Tx, id *container.ID, delta uint64, increase bool) error {
	containerVolume, err := tx.CreateBucketIfNotExists(containerVolumeBucketName)
	if err != nil {
		return err
	}

	key := id.ToV2().GetValue()
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
