package meta

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/nspcc-dev/bbolt"
	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// RemovedObjects describes single item handled by [DB.Delete].
type RemovedObject struct {
	Address    oid.Address
	PayloadLen uint64
}

// DeleteRes groups the resulting values of Delete operation.
type DeleteRes struct {
	// Actually removed objects. First len(addrs) elements always contain addrs
	// passed to [DB.Delete], but order is different in general.
	RemovedObjects []RemovedObject
	// CountersDiff describes counters changes after [DB.Delete] operation.
	Counters CountersDiff
}

// Delete removes object records from metabase indexes.
// Does not stop on an error if there are more objects to handle requested;
// returns the first error appeared with a number of deleted objects wrapped.
//
// Delete also looks up for objects that are hardly linked with elements of
// addrs list but not in the list themselves. If there are any, they are also
// deleted.
func (db *DB) Delete(addrs []oid.Address) (DeleteRes, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return DeleteRes{}, ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return DeleteRes{}, ErrReadOnlyMode
	}

	var diff CountersDiff
	var err error
	var removed []RemovedObject

	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		// We need to clear slice because tx can try to execute multiple times.
		diff, removed, err = db.deleteGroup(tx, addrs)
		return err
	})
	if err == nil {
		for i := range addrs {
			storagelog.Write(db.log,
				storagelog.AddressField(addrs[i]),
				storagelog.OpField("metabase DELETE"))
		}
	}
	return DeleteRes{
		Counters:       diff,
		RemovedObjects: removed,
	}, err
}

// deleteGroup deletes object from the metabase. Handles removal of the
// references of the split objects.
// The first return value is a physical objects removed number: physical
// objects that were stored. The second return value is a logical objects
// removed number: objects that were available (without Tombstones, GCMarks
// non-expired, etc.)
func (db *DB) deleteGroup(tx *bbolt.Tx, addrs []oid.Address) (CountersDiff, []RemovedObject, error) {
	var errorCount int
	var firstErr error
	var diff CountersDiff

	removedObjs, err := supplementRemovedObjects(tx, addrs)
	if err != nil {
		return diff, nil, fmt.Errorf("extend removed objects: %w", err)
	}

	for i := range removedObjs {
		objectDiff, err := db.delete(tx, removedObjs[i].Address)
		if err != nil {
			errorCount++
			db.log.Warn("failed to delete object", zap.Stringer("addr", removedObjs[i].Address), zap.Error(err))
			if firstErr == nil {
				firstErr = fmt.Errorf("%s object delete fail: %w", removedObjs[i].Address, err)
			}

			continue
		}

		diff.add(objectDiff)
		removedObjs[i].PayloadLen = uint64(-objectDiff.Payload)
	}

	if firstErr != nil {
		all := len(removedObjs)
		success := all - errorCount
		return diff, nil, fmt.Errorf("deleted %d out of %d objects, first error: %w", success, all, firstErr)
	}

	return diff, removedObjs, nil
}

// delete removes object indexes from the metabase.
func (db *DB) delete(tx *bbolt.Tx, addr oid.Address) (CountersDiff, error) {
	cID := addr.Container()
	metaBucket := tx.Bucket(metaBucketKey(cID))
	if metaBucket == nil {
		return CountersDiff{}, nil
	}
	var metaCursor = metaBucket.Cursor()

	diff, err := deleteMetadata(metaCursor, db.log, addr.Container(), addr.Object(), false)
	if err != nil {
		if !errors.Is(err, errNonPhy) {
			return CountersDiff{}, fmt.Errorf("can't remove metadata indexes: %w", err)
		}
	}

	return diff, nil
}

// forms list of objects from addrs and their missing parts.
// [RemovedObject.PayloadLen] is not initialized.
func supplementRemovedObjects(tx *bbolt.Tx, addrs []oid.Address) ([]RemovedObject, error) {
	cnrMetaBktKey := make([]byte, 1+cid.Size)
	cnrMetaBktKey[0] = metadataPrefix

	res := make([]RemovedObject, len(addrs))
	for i := range addrs {
		res[i].Address = addrs[i]
	}

	slices.SortFunc(res, func(a, b RemovedObject) int {
		return a.Address.Compare(b.Address) // Container-only sorting is sufficient here, but Compare() is more convenient anyway.
	})

	var err error
	var cnrMetaBkt *bbolt.Bucket
	var cnrMetaCrs *bbolt.Cursor
	for i := range res {
		cnr := res[i].Address.Container()

		if i == 0 || cnr != res[i-1].Address.Container() {
			copy(cnrMetaBktKey[1:], cnr[:])

			cnrMetaBkt = tx.Bucket(cnrMetaBktKey)
			if cnrMetaBkt == nil {
				continue
			}
			cnrMetaCrs = cnrMetaBkt.Cursor()
		} else if cnrMetaBkt == nil {
			continue
		}

		res, err = supplementRemovedECParts(res, cnrMetaCrs, addrs, res[i].Address)
		if err != nil {
			return nil, fmt.Errorf("collect EC parts for %s: %w", res[i].Address, err)
		}
	}

	return res, nil
}

// extends res with EC parts of addr which are not in addrs and returns updated res.
func supplementRemovedECParts(res []RemovedObject, cnrMetaCrs *bbolt.Cursor, addrs []oid.Address, addr oid.Address) ([]RemovedObject, error) {
	cnr := addr.Container()
	parent := addr.Object()

	var partCrs *bbolt.Cursor
	var ecPref []byte
	for id := range iterAttrVal(cnrMetaCrs, object.FilterParentID, parent[:]) {
		if id.IsZero() {
			return nil, fmt.Errorf("invalid child of %s parent: %w", parent, oid.ErrZero)
		}

		if partCrs == nil {
			partCrs = cnrMetaCrs.Bucket().Cursor()
		}

		if ecPref == nil {
			ecPref = slices.Concat([]byte{metaPrefixIDAttr}, id[:], []byte(iec.AttributePrefix)) // any of EC attributes
		} else {
			copy(ecPref[1:], id[:])
		}

		k, _ := partCrs.Seek(ecPref)
		if !bytes.HasPrefix(k, ecPref) {
			continue
		}

		if !slices.ContainsFunc(addrs, func(addr oid.Address) bool { return addr.Container() == cnr && addr.Object() == id }) {
			res = append(res, RemovedObject{
				Address: oid.NewAddress(cnr, id),
			})
		}
	}

	return res, nil
}
