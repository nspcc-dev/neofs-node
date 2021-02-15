package meta

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"go.etcd.io/bbolt"
)

// InhumePrm encapsulates parameters for Inhume operation.
type InhumePrm struct {
	tomb *objectSDK.Address

	target []*objectSDK.Address
}

// InhumeRes encapsulates results of Inhume operation.
type InhumeRes struct{}

// WithAddresses sets list of object addresses that should be inhumed.
func (p *InhumePrm) WithAddresses(addrs ...*objectSDK.Address) *InhumePrm {
	if p != nil {
		p.target = addrs
	}

	return p
}

// WithTombstoneAddress sets tombstone address as the reason for inhume operation.
//
// addr should not be nil.
// Should not be called along with WithGCMark.
func (p *InhumePrm) WithTombstoneAddress(addr *objectSDK.Address) *InhumePrm {
	if p != nil {
		p.tomb = addr
	}

	return p
}

// WithGCMark marks the object to be physically removed.
//
// Should not be called along with WithTombstoneAddress.
func (p *InhumePrm) WithGCMark() *InhumePrm {
	if p != nil {
		p.tomb = nil
	}

	return p
}

// Inhume inhumes the object by specified address.
//
// tomb should not be nil.
func Inhume(db *DB, target, tomb *objectSDK.Address) error {
	_, err := db.Inhume(new(InhumePrm).
		WithAddresses(target).
		WithTombstoneAddress(tomb),
	)

	return err
}

const inhumeGCMarkValue = "GCMARK"

// Inhume marks objects as removed but not removes it from metabase.
func (db *DB) Inhume(prm *InhumePrm) (res *InhumeRes, err error) {
	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		graveyard, err := tx.CreateBucketIfNotExists(graveyardBucketName)
		if err != nil {
			return err
		}

		for i := range prm.target {
			obj, err := db.get(tx, prm.target[i], false, true)

			// if object is stored and it is regular object then update bucket
			// with container size estimations
			if err == nil && obj.Type() == objectSDK.TypeRegular {
				err := changeContainerSize(
					tx,
					obj.ContainerID(),
					obj.PayloadSize(),
					false,
				)
				if err != nil {
					return err
				}
			}

			var val []byte
			if prm.tomb != nil {
				val = addressKey(prm.tomb)
			} else {
				val = []byte(inhumeGCMarkValue)
			}

			// consider checking if target is already in graveyard?
			err = graveyard.Put(addressKey(prm.target[i]), val)
			if err != nil {
				return err
			}
		}

		return nil
	})

	return
}
