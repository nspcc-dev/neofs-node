package meta

import (
	"bytes"
	"slices"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// BucketValue pairs a bucket index and a value that relates
// an object.
type BucketValue struct {
	BucketIndex int
	Value       []byte
}

// HeaderField is object header's field index.
type HeaderField struct {
	K []byte
	V []byte
}

// ObjectStatus represents the status of the object in the Metabase.
type ObjectStatus struct {
	Version     uint64
	Buckets     []BucketValue
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

		res.Buckets, res.HeaderIndex = readBuckets(tx, cID, oID)
		metaBucket := tx.Bucket(metaBucketKey(cID))
		var metaCursor *bbolt.Cursor
		if metaBucket != nil {
			metaCursor = metaBucket.Cursor()
		}

		var objLocked = objectLocked(tx, currEpoch, metaCursor, cID, oID)

		if objLocked {
			res.State = append(res.State, "LOCKED")
		}

		removedStatus := inGraveyard(tx, metaCursor, address)

		var existsRegular bool
		if metaBucket != nil {
			var typPrefix = make([]byte, metaIDTypePrefixSize)

			fillIDTypePrefix(typPrefix)
			typ, err := fetchTypeForID(metaBucket.Cursor(), typPrefix, oID)
			existsRegular = (err == nil && typ == objectSDK.TypeRegular)
		}

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

func readBuckets(tx *bbolt.Tx, cID cid.ID, oID oid.ID) ([]BucketValue, []HeaderField) {
	var oldIndexes []BucketValue
	var newIndexes []HeaderField
	addr := slices.Concat(cID[:], oID[:])
	objKey := addr[cid.Size:]

	objectBuckets := [][]byte{
		graveyardBucketName,
		garbageObjectsBucketName,
		toMoveItBucketName,
	}

	for _, bucketKey := range objectBuckets {
		b := tx.Bucket(bucketKey)
		if b == nil {
			continue
		}

		v := b.Get(addr)
		if v == nil {
			continue
		}

		oldIndexes = append(oldIndexes, BucketValue{
			BucketIndex: int(bucketKey[0]), // the first byte is always a prefix
			Value:       bytes.Clone(v),
		})
	}

	mBucket := tx.Bucket(metaBucketKey(cID))
	if mBucket == nil {
		return oldIndexes, nil
	}

	c := mBucket.Cursor()
	pref := slices.Concat([]byte{metaPrefixIDAttr}, objKey)
	k, _ := c.Seek(pref)
	for ; bytes.HasPrefix(k, pref); k, _ = c.Next() {
		kCut := k[len(pref):]
		k, v, found := bytes.Cut(kCut, object.MetaAttributeDelimiter)
		if !found {
			continue
		}

		newIndexes = append(newIndexes, HeaderField{K: slices.Clone(k), V: slices.Clone(v)})
	}

	return oldIndexes, newIndexes
}
