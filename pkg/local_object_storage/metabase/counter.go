package meta

import (
	"encoding/binary"
	"fmt"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

var objectCounterKey = []byte("phy_counter")

// ObjectCounter returns object count that metabase has
// tracked since it was opened and initialized.
//
// Returns only the errors that do not allow reading counter
// in Bolt database.
func (db *DB) ObjectCounter() (counter uint64, err error) {
	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(shardInfoBucket)
		if b != nil {
			data := b.Get(objectCounterKey)
			if len(data) == 8 {
				counter = binary.LittleEndian.Uint64(data)
			}
		}

		return nil
	})

	return
}

// updateCounter updates the object counter. Tx MUST be writable.
// If inc == `true`, increases the counter, decreases otherwise.
func (db *DB) updateCounter(tx *bbolt.Tx, delta uint64, inc bool) error {
	b := tx.Bucket(shardInfoBucket)
	if b == nil {
		return nil
	}

	var counter uint64

	data := b.Get(objectCounterKey)
	if len(data) == 8 {
		counter = binary.LittleEndian.Uint64(data)
	}

	if inc {
		counter += delta
	} else if counter <= delta {
		counter = 0
	} else {
		counter -= delta
	}

	newCounter := make([]byte, 8)
	binary.LittleEndian.PutUint64(newCounter, counter)

	return b.Put(objectCounterKey, newCounter)
}

// syncCounter updates object counter according to metabase state:
// it counts all the physically stored objects using internal indexes.
// Tx MUST be writable.
//
// Does nothing if counter not empty.
func syncCounter(tx *bbolt.Tx, force bool) error {
	var counter uint64

	b, err := tx.CreateBucketIfNotExists(shardInfoBucket)
	if err != nil {
		return fmt.Errorf("could not get shard info bucket: %w", err)
	}

	data := b.Get(objectCounterKey)
	if len(data) == 8 && !force {
		return nil
	}

	err = iteratePhyObjects(tx, func(_ cid.ID, _ oid.ID) error {
		counter++
		return nil
	})
	if err != nil {
		return fmt.Errorf("count not iterate objects: %w", err)
	}

	data = make([]byte, 8)
	binary.LittleEndian.PutUint64(data, counter)

	err = b.Put(objectCounterKey, data)
	if err != nil {
		return fmt.Errorf("could not update object counter: %w", err)
	}

	return nil
}
