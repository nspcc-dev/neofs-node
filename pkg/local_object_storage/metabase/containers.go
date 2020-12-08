package meta

import (
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

func parseContainerID(name []byte) (*container.ID, error) {
	strName := string(name)

	if strings.Contains(strName, "_") {
		return nil, nil
	}

	id := container.NewID()

	return id, id.Parse(strName)
}
