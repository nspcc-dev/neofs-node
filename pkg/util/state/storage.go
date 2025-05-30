package state

import (
	"bytes"
	"encoding/binary"
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

// NewPersistentStorage creates a new instance of a storage with 0o600 rights.
func NewPersistentStorage(path string) (*PersistentStorage, error) {
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{NoStatistics: true})
	if err != nil {
		return nil, fmt.Errorf("can't open bbolt at %s: %w", path, err)
	}

	return &PersistentStorage{db: db}, nil
}

// saves given KV in the storage.
func (p PersistentStorage) put(k, v []byte) error {
	return p.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(stateBucket)
		if err != nil {
			return fmt.Errorf("can't create state bucket in state persistent storage: %w", err)
		}

		return b.Put(k, v)
	})
}

// looks up for value in the storage by specified key and passes the value into
// provided handler. Nil corresponds to missing value. Handler's error is
// forwarded.
//
// Handler MUST NOT retain passed []byte, make a copy if needed.
func (p PersistentStorage) lookup(k []byte, f func(v []byte) error) error {
	return p.db.View(func(tx *bbolt.Tx) error {
		var v []byte

		b := tx.Bucket(stateBucket)
		if b != nil {
			v = b.Get(k)
		}

		return f(v)
	})
}

// SetUInt32 sets a uint32 value in the storage.
func (p PersistentStorage) SetUInt32(key []byte, value uint32) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(value))

	return p.put(key, buf)
}

// UInt32 returns a uint32 value from persistent storage. If the value does not exist,
// returns 0.
func (p PersistentStorage) UInt32(key []byte) (n uint32, err error) {
	err = p.lookup(key, func(v []byte) error {
		if v != nil {
			if len(v) != 8 {
				return fmt.Errorf("unexpected byte len: %d instead of %d", len(v), 8)
			}

			n = uint32(binary.LittleEndian.Uint64(v))
		}

		return nil
	})

	return
}

// Close closes persistent database instance.
func (p PersistentStorage) Close() error {
	return p.db.Close()
}

// SetBytes saves binary value in the storage by specified key.
func (p PersistentStorage) SetBytes(key []byte, value []byte) error {
	return p.put(key, value)
}

// Bytes reads binary value by specified key. Returns nil if value is missing.
func (p PersistentStorage) Bytes(key []byte) (res []byte, err error) {
	err = p.lookup(key, func(v []byte) error {
		if v != nil {
			res = bytes.Clone(v)
		}
		return nil
	})

	return
}

func (p PersistentStorage) Delete(key []byte) error {
	return p.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(stateBucket)
		if b != nil {
			err := b.Delete(key)
			if err != nil {
				return fmt.Errorf("can't delete state: %w", err)
			}
		}

		return nil
	})
}
