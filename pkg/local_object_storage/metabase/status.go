package meta

import (
	"bytes"
	"slices"

	"github.com/nspcc-dev/bbolt"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// HeaderField is object header's field index.
type HeaderField struct {
	K []byte
	V []byte
}

// ObjectStatus represents the status of the object in the Metabase.
type ObjectStatus struct {
	Version     uint64
	HeaderIndex []HeaderField
	State       []string
	Path        string
	Error       error
}

// ObjectStatus returns the status of the object in the Metabase. It contains state, path
// and indexed information about an object.
func (db *DB) ObjectStatus(address oid.Address) (ObjectStatus, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()
	var res ObjectStatus
	if db.mode.NoMetabase() {
		return res, nil
	}
	currEpoch := db.epochState.CurrentEpoch()

	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		res.Version, _ = getVersion(tx)

		oID := address.Object()
		cID := address.Container()

		metaBucket := tx.Bucket(metaBucketKey(cID))
		if metaBucket == nil {
			return nil // No data.
		}
		var metaCursor = metaBucket.Cursor()

		res.HeaderIndex = readAttributes(metaCursor, oID)

		var objLocked = objectLocked(currEpoch, metaCursor, oID)

		if objLocked {
			res.State = append(res.State, "LOCKED")
		}

		removedStatus := inGarbage(metaCursor, oID)

		var existsRegular bool

		typ, err := fetchTypeForID(metaCursor, oID)
		existsRegular = (err == nil && typ == object.TypeRegular)

		if (removedStatus != statusAvailable && objLocked) || existsRegular {
			res.State = append(res.State, "AVAILABLE")
		}
		if removedStatus == statusGCMarked {
			res.State = append(res.State, "GC MARKED")
		}
		if removedStatus == statusTombstoned {
			res.State = append(res.State, "IN GRAVEYARD")
		}
		return nil
	})
	res.Path = db.boltDB.Path()
	res.Error = err
	return res, err
}

func readAttributes(c *bbolt.Cursor, oID oid.ID) []HeaderField {
	var newIndexes []HeaderField

	pref := slices.Concat([]byte{metaPrefixIDAttr}, oID[:])
	k, _ := c.Seek(pref)
	for ; bytes.HasPrefix(k, pref); k, _ = c.Next() {
		kCut := k[len(pref):]
		k, v, found := bytes.Cut(kCut, objectcore.MetaAttributeDelimiter)
		if !found {
			continue
		}

		newIndexes = append(newIndexes, HeaderField{K: slices.Clone(k), V: slices.Clone(v)})
	}

	return newIndexes
}
