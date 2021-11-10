package meta

import (
	"bytes"
	"errors"
	"fmt"

	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
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

var errBreakBucketForEach = errors.New("bucket ForEach break")

// Inhume marks objects as removed but not removes it from metabase.
func (db *DB) Inhume(prm *InhumePrm) (res *InhumeRes, err error) {
	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		graveyard, err := tx.CreateBucketIfNotExists(graveyardBucketName)
		if err != nil {
			return err
		}

		var tombKey []byte
		if prm.tomb != nil {
			tombKey = addressKey(prm.tomb)

			// it is forbidden to have a tomb-on-tomb in NeoFS,
			// so graveyard keys must not be addresses of tombstones

			// tombstones can be marked for GC in graveyard, so exclude this case
			data := graveyard.Get(tombKey)
			if data != nil && !bytes.Equal(data, []byte(inhumeGCMarkValue)) {
				err := graveyard.Delete(tombKey)
				if err != nil {
					return fmt.Errorf("could not remove grave with tombstone key: %w", err)
				}
			}
		} else {
			tombKey = []byte(inhumeGCMarkValue)
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

			targetKey := addressKey(prm.target[i])

			if prm.tomb != nil {
				targetIsTomb := false

				// iterate over graveyard and check if target address
				// is the address of tombstone in graveyard.
				err = graveyard.ForEach(func(k, v []byte) error {
					// check if graveyard has record with key corresponding
					// to tombstone address (at least one)
					targetIsTomb = bytes.Equal(v, targetKey)

					if targetIsTomb {
						// break bucket iterator
						return errBreakBucketForEach
					}

					return nil
				})
				if err != nil && !errors.Is(err, errBreakBucketForEach) {
					return err
				}

				// do not add grave if target is a tombstone
				if targetIsTomb {
					continue
				}
			}

			// consider checking if target is already in graveyard?
			err = graveyard.Put(targetKey, tombKey)
			if err != nil {
				return err
			}
		}

		return nil
	})

	return
}
