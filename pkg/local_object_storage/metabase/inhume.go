package meta

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"go.etcd.io/bbolt"
)

// InhumePrm encapsulates parameters for Inhume operation.
type InhumePrm struct {
	target, tomb *objectSDK.Address
}

// InhumeRes encapsulates results of Inhume operation.
type InhumeRes struct{}

// WithAddress sets object address that should be inhumed.
func (p *InhumePrm) WithAddress(addr *objectSDK.Address) *InhumePrm {
	if p != nil {
		p.target = addr
	}

	return p
}

// WithTombstoneAddress sets tombstone address as the reason for inhume operation.
func (p *InhumePrm) WithTombstoneAddress(addr *objectSDK.Address) *InhumePrm {
	if p != nil {
		p.tomb = addr
	}

	return p
}

// Inhume inhumes the object by specified address.
func Inhume(db *DB, target, tomb *objectSDK.Address) error {
	_, err := db.Inhume(new(InhumePrm).
		WithAddress(target).
		WithTombstoneAddress(tomb),
	)

	return err
}

// Inhume marks objects as removed but not removes it from metabase.
func (db *DB) Inhume(prm *InhumePrm) (res *InhumeRes, err error) {
	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		graveyard, err := tx.CreateBucketIfNotExists(graveyardBucketName)
		if err != nil {
			return err
		}

		obj, err := db.get(tx, prm.target, false, true)

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

		// consider checking if target is already in graveyard?
		return graveyard.Put(addressKey(prm.target), addressKey(prm.tomb))
	})

	return
}
