package meta

import (
	"fmt"

	"github.com/nspcc-dev/bbolt"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ToMoveIt marks objects to move it into another shard. This useful for
// faster HRW fetching.
func (db *DB) ToMoveIt(addr oid.Address) error {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return ErrReadOnlyMode
	}

	key := make([]byte, addressKeySize)
	key = addressKey(addr, key)

	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		toMoveIt := tx.Bucket(toMoveItBucketName)
		return toMoveIt.Put(key, zeroValue)
	})
}

// DoNotMove removes `MoveIt` mark from the object.
func (db *DB) DoNotMove(addr oid.Address) error {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return ErrReadOnlyMode
	}

	key := make([]byte, addressKeySize)
	key = addressKey(addr, key)

	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		toMoveIt := tx.Bucket(toMoveItBucketName)
		return toMoveIt.Delete(key)
	})
}

// Movable returns list of marked objects to move into other shard.
func (db *DB) Movable() ([]oid.Address, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return nil, ErrDegradedMode
	}

	var strAddrs []string

	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		toMoveIt := tx.Bucket(toMoveItBucketName)
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
	addrs := make([]oid.Address, len(strAddrs))

	for i := range strAddrs {
		err = decodeAddressFromKey(&addrs[i], []byte(strAddrs[i]))
		if err != nil {
			return nil, fmt.Errorf("can't parse object address %v: %w",
				strAddrs[i], err)
		}
	}

	return addrs, nil
}
