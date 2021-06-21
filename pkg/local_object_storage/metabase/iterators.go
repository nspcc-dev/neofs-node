package meta

import (
	"errors"
	"fmt"
	"strconv"

	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
)

// ExpiredObject is a descriptor of expired object from DB.
type ExpiredObject struct {
	typ object.Type

	addr *object.Address
}

// Type returns type of the expired object.
func (e *ExpiredObject) Type() object.Type {
	return e.typ
}

// Address returns address of the expired object.
func (e *ExpiredObject) Address() *object.Address {
	return e.addr
}

// ExpiredObjectHandler is an ExpiredObject handling function.
type ExpiredObjectHandler func(*ExpiredObject) error

// ErrInterruptIterator is returned by iteration handlers
// as a "break" keyword.
var ErrInterruptIterator = errors.New("iterator is interrupted")

// IterateExpired iterates over all objects in DB which are out of date
// relative to epoch.
//
// If h returns ErrInterruptIterator, nil returns immediately.
// Returns other errors of h directly.
func (db *DB) IterateExpired(epoch uint64, h ExpiredObjectHandler) error {
	return db.iterateExpired(epoch, h)
}

func (db *DB) iterateExpired(epoch uint64, h ExpiredObjectHandler) error {
	iter := db.newPrefixIterator([]byte{attributePrefix})
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		parts := splitKey(iter.Key())

		if len(parts) < 4 || string(parts[1]) != objectV2.SysAttributeExpEpoch {
			continue
		}

		cidBytes := parts[0]
		var cidV2 refs.ContainerID
		cidV2.SetValue(cidBytes)
		cnrID := cid.NewFromV2(&cidV2)
		//err := cnrID.Parse(string(cidBytes))
		//if err != nil {
		//	return fmt.Errorf("could not parse container ID of expired bucket: %w", err)
		//}

		expKey := parts[2]
		expiresAt, err := strconv.ParseUint(string(expKey), 10, 64)
		if err != nil {
			return fmt.Errorf("could not parse expiration epoch: %w", err)
		} else if expiresAt >= epoch {
			continue
		}

		idKey := parts[3]
		id := object.NewID()

		err = id.Parse(string(idKey))
		if err != nil {
			return fmt.Errorf("could not parse ID of expired object: %w", err)
		}

		addr := object.NewAddress()
		addr.SetContainerID(cnrID)
		addr.SetObjectID(id)

		err = h(&ExpiredObject{
			typ:  db.objectType(cnrID, idKey),
			addr: addr,
		})

		if err != nil {
			if errors.Is(err, ErrInterruptIterator) {
				return nil
			}
			return err
		}
	}

	return nil
}

func (db *DB) objectType(cid *cid.ID, oidBytes []byte) object.Type {
	switch {
	default:
		return object.TypeRegular
	case db.hasKey(cidBucketKey(cid, tombstonePrefix, oidBytes)):
		return object.TypeTombstone
	case db.hasKey(cidBucketKey(cid, storageGroupPrefix, oidBytes)):
		return object.TypeStorageGroup
	}
}

// IterateCoveredByTombstones iterates over all objects in DB which are covered
// by tombstone with string address from tss.
//
// If h returns ErrInterruptIterator, nil returns immediately.
// Returns other errors of h directly.
//
// Does not modify tss.
func (db *DB) IterateCoveredByTombstones(tss map[string]struct{}, h func(*object.Address) error) error {
	return db.iterateCoveredByTombstones(tss, h)
}

func (db *DB) iterateCoveredByTombstones(tss map[string]struct{}, h func(*object.Address) error) error {
	iter := db.newPrefixIterator([]byte{graveyardPrefix})
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if _, ok := tss[string(iter.Value()[1:])]; !ok {
			continue
		}

		addr, err := addressFromKey(iter.Key()[1:])
		if err != nil {
			return fmt.Errorf("could not parse address of the object under tombstone: %w", err)
		}

		err = h(addr)
		if err != nil {
			if errors.Is(err, ErrInterruptIterator) {
				return nil
			}
			return err
		}
	}

	return nil
}
