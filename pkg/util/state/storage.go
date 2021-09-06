package state

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"go.etcd.io/bbolt"
)

// PersistentStorage is a wrapper around persistent K:V db that
// provides thread safe functions to set and fetch state variables
// of the Inner Ring and Storage applications.
type PersistentStorage struct {
	db *bbolt.DB
}

var stateBucket = []byte("state")

// NewPersistentStorage creates new instance of a storage with 0600 rights.
func NewPersistentStorage(path string) (*PersistentStorage, error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("can't open bbolt at %s: %w", path, err)
	}

	return &PersistentStorage{db: db}, nil
}

// SetUInt32 sets uint32 value in the storage.
func (p PersistentStorage) SetUInt32(key []byte, value uint32) error {
	return p.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(stateBucket)
		if err != nil {
			return fmt.Errorf("can't create state bucket in state persistent storage: %w", err)
		}

		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(value))

		return b.Put(key, buf)
	})
}

// UInt32 returns uint32 value from persistent storage. If value is not exists,
// returns 0.
func (p PersistentStorage) UInt32(key []byte) (n uint32, err error) {
	err = p.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(stateBucket)
		if b == nil {
			return nil // if bucket not exists yet, return default n = 0
		}

		buf := b.Get(key)
		if len(buf) != 8 {
			return fmt.Errorf("persistent storage does not store uint data in %s", hex.EncodeToString(key))
		}

		u64 := binary.LittleEndian.Uint64(buf)
		n = uint32(u64)

		return nil
	})

	return
}

// Close closes persistent database instance.
func (p PersistentStorage) Close() error {
	return p.db.Close()
}
