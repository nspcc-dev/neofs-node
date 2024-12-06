package writecache

import (
	"errors"

	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// ObjectStatus represents the status of the object in the Writecache.
type ObjectStatus struct {
	PathDB     string
	PathFSTree string
}

// ObjectStatus returns the status of the object in the Writecache. It contains path to the DB and path to the FSTree.
func (c *cache) ObjectStatus(address oid.Address) (ObjectStatus, error) {
	saddr := address.EncodeToString()
	var value []byte
	var res ObjectStatus

	err := c.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		if b != nil {
			value = b.Get([]byte(saddr))
			if value != nil {
				res.PathDB = c.db.Path()
			}
		}
		return errors.New("value not found")
	})
	if err != nil {
		return res, err
	}
	_, err = c.fsTree.Get(address)
	if err == nil {
		res.PathFSTree = c.fsTree.Path()
	}
	return res, err
}
