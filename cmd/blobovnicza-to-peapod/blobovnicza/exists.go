package blobovnicza

import (
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// Exists check if object with the specified address is stored in b.
func (b *Blobovnicza) Exists(addr oid.Address) (bool, error) {
	var (
		exists  bool
		addrKey = addressKey(addr)
	)

	err := b.boltDB.View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(_ []byte, buck *bbolt.Bucket) error {
			exists = buck.Get(addrKey) != nil
			if exists {
				return errInterruptForEach
			}
			return nil
		})
	})

	if err == errInterruptForEach {
		err = nil
	}
	return exists, err
}
