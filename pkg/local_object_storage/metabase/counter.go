package meta

import (
	"encoding/binary"

	"go.etcd.io/bbolt"
)

var shardCounterKey = []byte("counter")

// ObjectCounter returns object count that metabase has
// tracked since it was opened and initialized.
//
// Returns only the errors that do not allow reading counter
// in Bolt database.
func (db *DB) ObjectCounter() (counter uint64, err error) {
	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(shardInfoBucket)
		if b != nil {
			data := b.Get(shardCounterKey)
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

	data := b.Get(shardCounterKey)
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

	return b.Put(shardCounterKey, newCounter)
}
