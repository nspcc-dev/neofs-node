package meta

import (
	"fmt"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"go.etcd.io/bbolt"
)

// ToMoveIt marks objects to move it into another shard. This useful for
// faster HRW fetching.
func (db *DB) ToMoveIt(addr *objectSDK.Address) error {
	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		toMoveIt, err := tx.CreateBucketIfNotExists(toMoveItBucketName)
		if err != nil {
			return err
		}

		return toMoveIt.Put(addressKey(addr), zeroValue)
	})
}

// DoNotMove removes `MoveIt` mark from the object.
func (db *DB) DoNotMove(addr *objectSDK.Address) error {
	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		toMoveIt := tx.Bucket(toMoveItBucketName)
		if toMoveIt == nil {
			return nil
		}

		return toMoveIt.Delete(addressKey(addr))
	})
}

// Movable returns list of marked objects to move into other shard.
func (db *DB) Movable() ([]*objectSDK.Address, error) {
	var strAddrs []string

	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		toMoveIt := tx.Bucket(toMoveItBucketName)
		if toMoveIt == nil {
			return nil
		}

		return toMoveIt.ForEach(func(k, v []byte) error {
			strAddrs = append(strAddrs, string(k))

			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	// we can parse strings to structures in-place, but probably it seems
	// more efficient to keep bolt db TX code smaller because it might be
	// bottleneck.
	addrs := make([]*objectSDK.Address, 0, len(strAddrs))

	for i := range strAddrs {
		addr := objectSDK.NewAddress()

		err = addr.Parse(strAddrs[i])
		if err != nil {
			return nil, fmt.Errorf("can't parse object address %v: %w",
				strAddrs[i], err)
		}

		addrs = append(addrs, addr)
	}

	return addrs, nil
}
