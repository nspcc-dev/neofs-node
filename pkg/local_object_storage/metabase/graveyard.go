package meta

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/nspcc-dev/bbolt"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// GarbageObject represents descriptor of the
// object that has been marked with GC.
type GarbageObject struct {
	addr oid.Address
}

// Address returns garbage object address.
func (g GarbageObject) Address() oid.Address {
	return g.addr
}

// GarbageHandler is a GarbageObject handling function.
type GarbageHandler func(GarbageObject) error

// IterateOverGarbage iterates over all objects marked with GC mark.
//
// The handler will be applied to the next after the
// specified offset if any are left.
//
// Note: if offset is not found in db, iteration starts
// from the element that WOULD BE the following after the
// offset if offset was presented. That means that it is
// safe to delete offset element and pass if to the
// iteration once again: iteration would start from the
// next element.
//
// Nil offset means start an integration from the beginning.
//
// If h returns ErrInterruptIterator, nil returns immediately.
// Returns other errors of h directly.
func (db *DB) IterateOverGarbage(h GarbageHandler, offset *oid.Address) error {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	}

	return db.boltDB.View(func(tx *bbolt.Tx) error {
		return db.iterateDeletedObj(tx, gcHandler{h}, offset)
	})
}

type gcHandler struct {
	h GarbageHandler
}

func (g gcHandler) handleKV(k, _ []byte) error {
	o, err := garbageFromKV(k)
	if err != nil {
		return fmt.Errorf("could not parse garbage object: %w", err)
	}

	return g.h(o)
}

func (db *DB) iterateDeletedObj(tx *bbolt.Tx, h gcHandler, offset *oid.Address) error {
	var bkt = tx.Bucket(garbageObjectsBucketName)

	if bkt == nil {
		return nil
	}

	c := bkt.Cursor()
	var k, v []byte

	if offset == nil {
		k, v = c.First()
	} else {
		rawAddr := addressKey(*offset, make([]byte, addressKeySize))

		k, v = c.Seek(rawAddr)
		if bytes.Equal(k, rawAddr) {
			// offset was found, move
			// cursor to the next element
			k, v = c.Next()
		}
	}

	for ; k != nil; k, v = c.Next() {
		err := h.handleKV(k, v)
		if err != nil {
			if errors.Is(err, ErrInterruptIterator) {
				return nil
			}

			return err
		}
	}

	return nil
}

func garbageFromKV(k []byte) (res GarbageObject, err error) {
	err = decodeAddressFromKey(&res.addr, k)
	if err != nil {
		err = fmt.Errorf("could not parse address: %w", err)
	}

	return
}

// GetGarbage returns garbage according to the metabase state. Garbage includes
// objects marked with GC mark (expired, tombstoned but not deleted from disk,
// extra replicated, etc.) and removed containers.
// The first return value describes garbage objects. These objects should be
// removed. The second return value describes garbage containers whose _all_
// garbage objects were included in the first return value and, therefore,
// these containers can be deleted (if their objects are handled and deleted too).
func (db *DB) GetGarbage(limit int) ([]oid.Address, []cid.ID, error) {
	if limit <= 0 {
		return nil, nil, nil
	}

	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return nil, nil, ErrDegradedMode
	}

	const reasonableLimit = 1000
	initCap := min(limit, reasonableLimit)

	var addrBuff oid.Address
	alreadyHandledContainers := make(map[cid.ID]struct{})
	resObjects := make([]oid.Address, 0, initCap)
	resContainers := make([]cid.ID, 0)

	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		// start from the deleted containers since
		// an object can be deleted manually but
		// also be deleted as a part of non-existing
		// container so no need to handle it twice

		var inhumedCnrs []cid.ID
		err := tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			if name[0] == metadataPrefix && containerMarkedGC(b.Cursor()) {
				var cnr cid.ID
				cidRaw, prefix := parseContainerIDWithPrefix(&cnr, name)
				if cidRaw == nil || prefix != metadataPrefix {
					return nil
				}
				inhumedCnrs = append(inhumedCnrs, cnr)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("scanning inhumed containers: %w", err)
		}

		for _, cnr := range inhumedCnrs {
			resObjects, err = listContainerObjects(tx, cnr, resObjects, limit)
			if err != nil {
				return fmt.Errorf("listing objects for %s container: %w", cnr, err)
			}

			alreadyHandledContainers[cnr] = struct{}{}

			if len(resObjects) < limit {
				// all the objects from the container were listed,
				// container can be removed
				resContainers = append(resContainers, cnr)
			} else {
				return nil
			}
		}

		// deleted containers are not enough to reach the limit,
		// check manually deleted objects then

		bkt := tx.Bucket(garbageObjectsBucketName)
		c := bkt.Cursor()

		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			err := decodeAddressFromKey(&addrBuff, k)
			if err != nil {
				return fmt.Errorf("parsing deleted address: %w", err)
			}

			if _, handled := alreadyHandledContainers[addrBuff.Container()]; handled {
				continue
			}

			resObjects = append(resObjects, addrBuff)

			if len(resObjects) >= limit {
				return nil
			}
		}

		return nil
	})

	return resObjects, resContainers, err
}
